import { readFile, stat } from "node:fs/promises";

import {
  KAFKA_TOPIC_CONFIGURATION_LIMITS,
  parseKafkaTopicConfigurationHistoryEntry,
  type HostErrorCode,
  type HostErrorStage,
  type KafkaTopicConfigurationHistoryStoreCapability,
} from "../kafka/contracts";
import {
  cloneKafkaTopicConfigurationHistoryDocument,
  type KafkaTopicConfigurationHistoryDocument,
  type KafkaTopicConfigurationHistoryStore,
} from "../kafka/application";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

const HISTORY_FILE_VERSION = 1 as const;
const DEFAULT_MAXIMUM_FILE_BYTES = 4 * 1_048_576;
const CORRUPT_HISTORY_FILE_RECOVERY =
  "Preserve the history file, restore a known-good copy, or move it aside after confirming a backup.";

type UnknownRecord = Record<string, unknown>;

export interface KafkaTopicConfigurationHistoryFileStoreOptions {
  readonly createTempId?: () => string;
  readonly maximumFileBytes?: number;
}

abstract class KafkaTopicConfigurationHistoryFileError extends Error {
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

export class KafkaTopicConfigurationHistoryFileCorruptError extends KafkaTopicConfigurationHistoryFileError {
  readonly code = "TOPIC_CONFIG_HISTORY_CORRUPT" as const;
  readonly recovery = CORRUPT_HISTORY_FILE_RECOVERY;

  constructor() {
    super("The topic configuration history file is corrupt or uses an unsupported schema.");
  }
}

export class KafkaTopicConfigurationHistoryFileWriteError extends KafkaTopicConfigurationHistoryFileError {
  readonly code = "TOPIC_CONFIG_HISTORY_UNAVAILABLE" as const;
  readonly recovery =
    "Check application-data permissions and retry the topic configuration operation.";

  constructor() {
    super("Topic configuration history storage could not commit the operation.");
  }
}

function valueRecord(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new KafkaTopicConfigurationHistoryFileCorruptError();
  }
  return value as UnknownRecord;
}

function exactKeys(value: UnknownRecord, allowed: readonly string[]): void {
  const keys = new Set(allowed);
  if (Object.keys(value).some((key) => !keys.has(key))) {
    throw new KafkaTopicConfigurationHistoryFileCorruptError();
  }
}

function parseDocument(value: unknown): KafkaTopicConfigurationHistoryDocument {
  const document = valueRecord(value);
  exactKeys(document, ["entries", "version"]);
  if (
    document.version !== HISTORY_FILE_VERSION ||
    !Array.isArray(document.entries) ||
    document.entries.length > KAFKA_TOPIC_CONFIGURATION_LIMITS.historyEntries
  ) {
    throw new KafkaTopicConfigurationHistoryFileCorruptError();
  }
  try {
    const entries = document.entries.map((entry, index) =>
      parseKafkaTopicConfigurationHistoryEntry(entry, `history.entries[${String(index)}]`),
    );
    if (new Set(entries.map((entry) => entry.id)).size !== entries.length) {
      throw new KafkaTopicConfigurationHistoryFileCorruptError();
    }
    return cloneKafkaTopicConfigurationHistoryDocument({ entries });
  } catch (error) {
    throw error instanceof KafkaTopicConfigurationHistoryFileCorruptError
      ? error
      : new KafkaTopicConfigurationHistoryFileCorruptError();
  }
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export class AtomicKafkaTopicConfigurationHistoryFileStore implements KafkaTopicConfigurationHistoryStore {
  private readonly createTempId;
  private currentCapability: KafkaTopicConfigurationHistoryStoreCapability = {
    durability: "durable",
    state: "ready",
  };
  private readonly maximumFileBytes;

  constructor(
    private readonly path: string,
    options: KafkaTopicConfigurationHistoryFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
    this.maximumFileBytes = options.maximumFileBytes ?? DEFAULT_MAXIMUM_FILE_BYTES;
  }

  capability(): KafkaTopicConfigurationHistoryStoreCapability {
    return { ...this.currentCapability };
  }

  async commit(
    document: KafkaTopicConfigurationHistoryDocument,
    signal?: AbortSignal,
  ): Promise<void> {
    try {
      signal?.throwIfAborted();
      if (this.currentCapability.state === "unavailable") {
        throw new KafkaTopicConfigurationHistoryFileWriteError();
      }
      const validated = parseDocument({
        entries: document.entries,
        version: HISTORY_FILE_VERSION,
      });
      const serialized = `${JSON.stringify({
        entries: validated.entries,
        version: HISTORY_FILE_VERSION,
      })}\n`;
      if (Buffer.byteLength(serialized, "utf8") > this.maximumFileBytes) {
        throw new KafkaTopicConfigurationHistoryFileWriteError();
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
      throw error instanceof KafkaTopicConfigurationHistoryFileWriteError
        ? error
        : new KafkaTopicConfigurationHistoryFileWriteError();
    }
  }

  async load(signal?: AbortSignal): Promise<KafkaTopicConfigurationHistoryDocument | undefined> {
    let contents: Buffer;
    try {
      signal?.throwIfAborted();
      const metadata = await stat(this.path);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes) {
        throw new KafkaTopicConfigurationHistoryFileCorruptError();
      }
      contents = await readFile(this.path);
      signal?.throwIfAborted();
      if (contents.length > this.maximumFileBytes) {
        throw new KafkaTopicConfigurationHistoryFileCorruptError();
      }
    } catch (error) {
      if (isMissingFile(error)) {
        return undefined;
      }
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaTopicConfigurationHistoryFileCorruptError
        ? error
        : new KafkaTopicConfigurationHistoryFileCorruptError();
    }

    try {
      return parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaTopicConfigurationHistoryFileCorruptError
        ? error
        : new KafkaTopicConfigurationHistoryFileCorruptError();
    }
  }

  private markUnavailable(): void {
    this.currentCapability = {
      durability: "durable",
      recovery: CORRUPT_HISTORY_FILE_RECOVERY,
      state: "unavailable",
    };
  }
}
