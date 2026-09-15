import { readFile, stat } from "node:fs/promises";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type ConnectionTemplateStoreCapability,
  type HostErrorCode,
  type HostErrorStage,
} from "../kafka/contracts";
import {
  cloneConnectionTemplateDocument,
  type KafkaConnectionTemplateDocument,
  type KafkaConnectionTemplateStore,
  type KafkaConnectionTemplateStructuredError,
} from "../kafka/application";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

const TEMPLATE_FILE_VERSION = 1 as const;
const DEFAULT_MAXIMUM_FILE_BYTES = 4 * 1_048_576;

type UnknownRecord = Record<string, unknown>;

export interface KafkaConnectionTemplateFileStoreOptions {
  readonly createTempId?: () => string;
  readonly maximumFileBytes?: number;
}

abstract class KafkaConnectionTemplateFileError
  extends Error
  implements KafkaConnectionTemplateStructuredError
{
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  readonly stage: HostErrorStage = "template";
  readonly target = undefined;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class KafkaConnectionTemplateFileCorruptError extends KafkaConnectionTemplateFileError {
  readonly code = "TEMPLATE_CORRUPT" as const;
  readonly recovery =
    "Preserve the template file, restore a known-good copy, or move it aside after confirming a backup.";

  constructor() {
    super("The connection template file is corrupt or uses an unsupported schema.");
  }
}

export class KafkaConnectionTemplateFileWriteError extends KafkaConnectionTemplateFileError {
  readonly code = "TEMPLATE_STORE_UNAVAILABLE" as const;
  readonly recovery = "Check application-data permissions and retry the template change.";

  constructor() {
    super("Connection template storage could not commit the requested change.");
  }
}

function valueRecord(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new KafkaConnectionTemplateFileCorruptError();
  }
  return value as UnknownRecord;
}

function exactKeys(value: UnknownRecord, allowed: readonly string[]): void {
  const keys = new Set(allowed);
  if (Object.keys(value).some((key) => !keys.has(key))) {
    throw new KafkaConnectionTemplateFileCorruptError();
  }
}

function parseDocument(value: unknown): KafkaConnectionTemplateDocument {
  const document = valueRecord(value);
  exactKeys(document, ["catalogs", "version"]);
  if (document.version !== TEMPLATE_FILE_VERSION) {
    throw new KafkaConnectionTemplateFileCorruptError();
  }
  try {
    const event = parseHostEvent({
      event: "templates.changed",
      payload: {
        catalogs: document.catalogs,
        store: { durability: "durable", state: "ready" },
      },
      sequence: 0,
      version: HOST_PROTOCOL_VERSION,
    });
    if (event.event !== "templates.changed") {
      throw new KafkaConnectionTemplateFileCorruptError();
    }
    return cloneConnectionTemplateDocument({ catalogs: event.payload.catalogs });
  } catch (error) {
    throw error instanceof KafkaConnectionTemplateFileCorruptError
      ? error
      : new KafkaConnectionTemplateFileCorruptError();
  }
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export class AtomicKafkaConnectionTemplateFileStore implements KafkaConnectionTemplateStore {
  private readonly createTempId;
  private currentCapability: ConnectionTemplateStoreCapability = {
    durability: "durable",
    state: "ready",
  };
  private readonly maximumFileBytes;

  constructor(
    private readonly path: string,
    options: KafkaConnectionTemplateFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
    this.maximumFileBytes = options.maximumFileBytes ?? DEFAULT_MAXIMUM_FILE_BYTES;
  }

  capability(): ConnectionTemplateStoreCapability {
    return this.currentCapability;
  }

  async commit(document: KafkaConnectionTemplateDocument, signal?: AbortSignal): Promise<void> {
    try {
      signal?.throwIfAborted();
      const validated = parseDocument({
        catalogs: document.catalogs,
        version: TEMPLATE_FILE_VERSION,
      });
      const serialized = `${JSON.stringify({
        catalogs: validated.catalogs,
        version: TEMPLATE_FILE_VERSION,
      })}\n`;
      if (Buffer.byteLength(serialized, "utf8") > this.maximumFileBytes) {
        throw new KafkaConnectionTemplateFileWriteError();
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
      throw error instanceof KafkaConnectionTemplateFileWriteError
        ? error
        : new KafkaConnectionTemplateFileWriteError();
    }
  }

  async load(signal?: AbortSignal): Promise<KafkaConnectionTemplateDocument | undefined> {
    let contents: Buffer;
    try {
      signal?.throwIfAborted();
      const metadata = await stat(this.path);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes) {
        throw new KafkaConnectionTemplateFileCorruptError();
      }
      contents = await readFile(this.path);
      signal?.throwIfAborted();
      if (contents.length > this.maximumFileBytes) {
        throw new KafkaConnectionTemplateFileCorruptError();
      }
    } catch (error) {
      if (isMissingFile(error)) {
        return undefined;
      }
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaConnectionTemplateFileCorruptError
        ? error
        : new KafkaConnectionTemplateFileCorruptError();
    }

    try {
      return parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaConnectionTemplateFileCorruptError
        ? error
        : new KafkaConnectionTemplateFileCorruptError();
    }
  }

  private markUnavailable(): void {
    this.currentCapability = {
      durability: "durable",
      recovery:
        "Preserve the template file, restore a known-good copy, or move it aside after confirming a backup.",
      state: "unavailable",
    };
  }
}
