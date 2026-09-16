import { readFile, stat } from "node:fs/promises";

import {
  parseKafkaOperationalPreferences,
  type HostErrorCode,
  type HostErrorStage,
  type KafkaOperationalPreferences,
  type KafkaOperationalPreferenceStoreCapability,
} from "../../../features/kafka/contracts";
import {
  cloneKafkaOperationalPreferences,
  type KafkaOperationalPreferenceStore,
  type KafkaOperationalPreferenceStructuredError,
} from "../../../features/kafka/application";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

const PREFERENCE_FILE_VERSION = 1 as const;
const DEFAULT_MAXIMUM_FILE_BYTES = 32 * 1_024;
const CORRUPT_PREFERENCE_FILE_RECOVERY =
  "Reset operational preferences to replace the unreadable file.";

type UnknownRecord = Record<string, unknown>;

export interface KafkaOperationalPreferenceFileStoreOptions {
  readonly createTempId?: () => string;
  readonly maximumFileBytes?: number;
}

abstract class KafkaOperationalPreferenceFileError
  extends Error
  implements KafkaOperationalPreferenceStructuredError
{
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  readonly stage: HostErrorStage = "preference";
  readonly target = undefined;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class KafkaOperationalPreferenceFileCorruptError extends KafkaOperationalPreferenceFileError {
  readonly code = "PREFERENCE_CORRUPT" as const;
  readonly recovery = CORRUPT_PREFERENCE_FILE_RECOVERY;

  constructor() {
    super("The operational preference file is corrupt or uses an unsupported schema.");
  }
}

export class KafkaOperationalPreferenceFileWriteError extends KafkaOperationalPreferenceFileError {
  readonly code = "PREFERENCE_STORE_UNAVAILABLE" as const;
  readonly recovery =
    "Check application-data permissions and retry or reset the operational preferences.";

  constructor() {
    super("Operational preference storage could not commit the requested change.");
  }
}

function valueRecord(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new KafkaOperationalPreferenceFileCorruptError();
  }
  return value as UnknownRecord;
}

function exactKeys(value: UnknownRecord, allowed: readonly string[]): void {
  const keys = new Set(allowed);
  if (
    Object.keys(value).length !== allowed.length ||
    Object.keys(value).some((key) => !keys.has(key))
  ) {
    throw new KafkaOperationalPreferenceFileCorruptError();
  }
}

function parseDocument(value: unknown): KafkaOperationalPreferences {
  const document = valueRecord(value);
  exactKeys(document, ["preferences", "version"]);
  if (document.version !== PREFERENCE_FILE_VERSION) {
    throw new KafkaOperationalPreferenceFileCorruptError();
  }
  try {
    return cloneKafkaOperationalPreferences(
      parseKafkaOperationalPreferences(document.preferences, "storedPreferences"),
    );
  } catch (error) {
    throw error instanceof KafkaOperationalPreferenceFileCorruptError
      ? error
      : new KafkaOperationalPreferenceFileCorruptError();
  }
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export class AtomicKafkaOperationalPreferenceFileStore implements KafkaOperationalPreferenceStore {
  private readonly createTempId;
  private currentCapability: KafkaOperationalPreferenceStoreCapability = {
    durability: "durable",
    state: "ready",
  };
  private readonly maximumFileBytes;

  constructor(
    private readonly path: string,
    options: KafkaOperationalPreferenceFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
    this.maximumFileBytes = options.maximumFileBytes ?? DEFAULT_MAXIMUM_FILE_BYTES;
  }

  capability(): KafkaOperationalPreferenceStoreCapability {
    return { ...this.currentCapability };
  }

  async commit(preferences: KafkaOperationalPreferences, signal?: AbortSignal): Promise<void> {
    try {
      signal?.throwIfAborted();
      const validated = parseDocument({
        preferences,
        version: PREFERENCE_FILE_VERSION,
      });
      const serialized = `${JSON.stringify({
        preferences: validated,
        version: PREFERENCE_FILE_VERSION,
      })}\n`;
      if (Buffer.byteLength(serialized, "utf8") > this.maximumFileBytes) {
        throw new KafkaOperationalPreferenceFileWriteError();
      }
      await writeAtomicPrivateTextFile({
        contents: serialized,
        createTempId: this.createTempId,
        path: this.path,
        ...(signal === undefined ? {} : { signal }),
      });
      this.currentCapability = {
        durability: "durable",
        state: "ready",
      };
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw error instanceof KafkaOperationalPreferenceFileWriteError
        ? error
        : new KafkaOperationalPreferenceFileWriteError();
    }
  }

  async load(signal?: AbortSignal): Promise<KafkaOperationalPreferences | undefined> {
    let contents: Buffer;
    try {
      signal?.throwIfAborted();
      const metadata = await stat(this.path);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes) {
        throw new KafkaOperationalPreferenceFileCorruptError();
      }
      contents = await readFile(this.path);
      signal?.throwIfAborted();
      if (contents.length > this.maximumFileBytes) {
        throw new KafkaOperationalPreferenceFileCorruptError();
      }
    } catch (error) {
      if (isMissingFile(error)) {
        return undefined;
      }
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaOperationalPreferenceFileCorruptError
        ? error
        : new KafkaOperationalPreferenceFileCorruptError();
    }

    try {
      return parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaOperationalPreferenceFileCorruptError
        ? error
        : new KafkaOperationalPreferenceFileCorruptError();
    }
  }

  private markUnavailable(): void {
    this.currentCapability = {
      durability: "durable",
      recovery: CORRUPT_PREFERENCE_FILE_RECOVERY,
      state: "unavailable",
    };
  }
}
