import { chmod, lstat, open, unlink } from "node:fs/promises";
import { dirname } from "node:path";

import {
  parseKafkaOperationalPreferences,
  type HostErrorCode,
  type HostErrorStage,
  type KafkaOperationalPreferences,
  type KafkaOperationalPreferenceStoreCapability,
} from "../../features/kafka/contracts";
import {
  cloneKafkaOperationalPreferences,
  type KafkaOperationalPreferenceStore,
  type KafkaOperationalPreferenceStructuredError,
} from "../../features/kafka/application";

import { readOptionalBoundedJsonFile } from "./bounded-json-file";
import { readBoundedFile } from "./bounded-file";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

const PREFERENCE_FILE_VERSION = 2 as const;
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
  if (document.version !== 1 && document.version !== PREFERENCE_FILE_VERSION) {
    throw new KafkaOperationalPreferenceFileCorruptError();
  }
  try {
    if (
      document.version === PREFERENCE_FILE_VERSION &&
      (!Object.hasOwn(valueRecord(document.preferences), "codecs") ||
        !Object.hasOwn(valueRecord(document.preferences), "protection"))
    )
      throw new KafkaOperationalPreferenceFileCorruptError();
    if (document.version === 1 && Object.hasOwn(valueRecord(document.preferences), "codecs"))
      throw new KafkaOperationalPreferenceFileCorruptError();
    return cloneKafkaOperationalPreferences(
      parseKafkaOperationalPreferences(document.preferences, "storedPreferences"),
    );
  } catch (error) {
    throw error instanceof KafkaOperationalPreferenceFileCorruptError
      ? error
      : new KafkaOperationalPreferenceFileCorruptError();
  }
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
      const beforeCommit = await this.preservePredecessor(signal);
      await writeAtomicPrivateTextFile({
        ...(beforeCommit === undefined ? {} : { beforeCommit }),
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
    try {
      return await readOptionalBoundedJsonFile(
        this.path,
        this.maximumFileBytes,
        parseDocument,
        signal,
      );
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

  /** Keep exact legacy settings before introducing codecs that older hosts cannot read. */
  private async preservePredecessor(
    signal?: AbortSignal,
  ): Promise<(() => Promise<void>) | undefined> {
    let original: Buffer;
    try {
      const predecessor = await lstat(this.path);
      if (!predecessor.isFile()) throw new KafkaOperationalPreferenceFileWriteError();
      if (predecessor.size > this.maximumFileBytes) {
        // An unreadable oversized regular file can be reset without allocating its bytes.
        // Recheck identity after preparing the replacement so a changed file is never reset.
        return async () => {
          signal?.throwIfAborted();
          const current = await lstat(this.path);
          if (
            !current.isFile() ||
            current.dev !== predecessor.dev ||
            current.ino !== predecessor.ino ||
            current.size !== predecessor.size ||
            current.mtimeMs !== predecessor.mtimeMs ||
            current.ctimeMs !== predecessor.ctimeMs ||
            current.nlink !== predecessor.nlink
          )
            throw new KafkaOperationalPreferenceFileWriteError();
        };
      }
      original = await readBoundedFile(this.path, this.maximumFileBytes, {
        signal,
        rejectSymlinks: true,
      });
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return;
      // Explicit preference reset also repairs corrupt files; do not overwrite a readable legacy document without a backup.
      throw error;
    }
    let document: UnknownRecord;
    try {
      document = valueRecord(JSON.parse(original.toString("utf8")) as unknown);
      parseDocument(document);
    } catch {
      return;
    }
    if (document.version !== 1) return;
    await chmod(dirname(this.path), 0o700);
    for (let generation = 0; generation < 100; generation += 1) {
      const path = `${this.path}.pre-codecs-v1${generation === 0 ? "" : `.${generation}`}`;
      let handle: Awaited<ReturnType<typeof open>>;
      try {
        handle = await open(path, "wx", 0o600);
      } catch (error) {
        if (
          error !== null &&
          typeof error === "object" &&
          "code" in error &&
          error.code === "EEXIST"
        )
          continue;
        throw error;
      }
      try {
        signal?.throwIfAborted();
        await handle.writeFile(original);
        await handle.sync();
        await handle.close();
        const checked = await readBoundedFile(path, this.maximumFileBytes, {
          signal,
          rejectSymlinks: true,
        });
        if (!checked.equals(original)) throw new KafkaOperationalPreferenceFileWriteError();
        if (process.platform !== "win32") {
          const directory = await open(dirname(path), "r");
          try {
            await directory.sync();
          } finally {
            await directory.close();
          }
        }
        return;
      } catch (error) {
        await handle.close().catch(() => undefined);
        await unlink(path).catch(() => undefined);
        throw error;
      }
    }
    throw new KafkaOperationalPreferenceFileWriteError();
  }
}
