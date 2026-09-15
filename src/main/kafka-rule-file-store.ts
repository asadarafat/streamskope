import { readFile, stat } from "node:fs/promises";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostErrorCode,
  type HostErrorStage,
  type KafkaRuleStoreCapability,
} from "../kafka/contracts";
import {
  cloneKafkaRuleDocument,
  type KafkaRuleDocument,
  type KafkaRuleStore,
  type KafkaRuleStructuredError,
} from "../kafka/application";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

const RULE_FILE_VERSION = 1 as const;
const DEFAULT_MAXIMUM_FILE_BYTES = 4 * 1_048_576;
const CORRUPT_RULE_FILE_RECOVERY =
  "Preserve the rule file, restore a known-good copy, or move it aside after confirming a backup.";

type UnknownRecord = Record<string, unknown>;

export interface KafkaRuleFileStoreOptions {
  readonly createTempId?: () => string;
  readonly maximumFileBytes?: number;
}

abstract class KafkaRuleFileError extends Error implements KafkaRuleStructuredError {
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  readonly stage: HostErrorStage = "storage";
  readonly target = undefined;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class KafkaRuleFileCorruptError extends KafkaRuleFileError {
  readonly code = "RULE_CORRUPT" as const;
  readonly recovery = CORRUPT_RULE_FILE_RECOVERY;

  constructor() {
    super("The Kafka rule file is corrupt or uses an unsupported schema.");
  }
}

export class KafkaRuleFileWriteError extends KafkaRuleFileError {
  readonly code = "RULE_STORE_UNAVAILABLE" as const;
  readonly recovery = "Check application-data permissions and retry the rule change.";

  constructor() {
    super("Kafka rule storage could not commit the requested change.");
  }
}

function valueRecord(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new KafkaRuleFileCorruptError();
  }
  return value as UnknownRecord;
}

function exactKeys(value: UnknownRecord, allowed: readonly string[]): void {
  const keys = new Set(allowed);
  if (Object.keys(value).some((key) => !keys.has(key))) {
    throw new KafkaRuleFileCorruptError();
  }
}

function parseDocument(value: unknown): KafkaRuleDocument {
  const document = valueRecord(value);
  exactKeys(document, ["rules", "version"]);
  if (document.version !== RULE_FILE_VERSION) {
    throw new KafkaRuleFileCorruptError();
  }
  try {
    const event = parseHostEvent({
      event: "rules.changed",
      payload: {
        rules: document.rules,
        store: { durability: "durable", state: "ready" },
      },
      sequence: 0,
      version: HOST_PROTOCOL_VERSION,
    });
    if (event.event !== "rules.changed") {
      throw new KafkaRuleFileCorruptError();
    }
    return cloneKafkaRuleDocument({ rules: event.payload.rules });
  } catch (error) {
    throw error instanceof KafkaRuleFileCorruptError ? error : new KafkaRuleFileCorruptError();
  }
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export class AtomicKafkaRuleFileStore implements KafkaRuleStore {
  private readonly createTempId;
  private currentCapability: KafkaRuleStoreCapability = {
    durability: "durable",
    state: "ready",
  };
  private readonly maximumFileBytes;

  constructor(
    private readonly path: string,
    options: KafkaRuleFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
    this.maximumFileBytes = options.maximumFileBytes ?? DEFAULT_MAXIMUM_FILE_BYTES;
  }

  capability(): KafkaRuleStoreCapability {
    return { ...this.currentCapability };
  }

  async commit(document: KafkaRuleDocument, signal?: AbortSignal): Promise<void> {
    try {
      signal?.throwIfAborted();
      const validated = parseDocument({
        rules: document.rules,
        version: RULE_FILE_VERSION,
      });
      const serialized = `${JSON.stringify({
        rules: validated.rules,
        version: RULE_FILE_VERSION,
      })}\n`;
      if (Buffer.byteLength(serialized, "utf8") > this.maximumFileBytes) {
        throw new KafkaRuleFileWriteError();
      }
      await writeAtomicPrivateTextFile({
        contents: serialized,
        createTempId: this.createTempId,
        path: this.path,
        ...(signal === undefined ? {} : { signal }),
      });
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw error instanceof KafkaRuleFileWriteError ? error : new KafkaRuleFileWriteError();
    }
  }

  async load(signal?: AbortSignal): Promise<KafkaRuleDocument | undefined> {
    let contents: Buffer;
    try {
      signal?.throwIfAborted();
      const metadata = await stat(this.path);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes) {
        throw new KafkaRuleFileCorruptError();
      }
      contents = await readFile(this.path);
      signal?.throwIfAborted();
      if (contents.length > this.maximumFileBytes) {
        throw new KafkaRuleFileCorruptError();
      }
    } catch (error) {
      if (isMissingFile(error)) {
        return undefined;
      }
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaRuleFileCorruptError ? error : new KafkaRuleFileCorruptError();
    }

    try {
      return parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaRuleFileCorruptError ? error : new KafkaRuleFileCorruptError();
    }
  }

  private markUnavailable(): void {
    this.currentCapability = {
      durability: "durable",
      recovery: CORRUPT_RULE_FILE_RECOVERY,
      state: "unavailable",
    };
  }
}
