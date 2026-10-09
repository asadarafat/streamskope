import { constants, type Stats } from "node:fs";
import { chmod, lstat, mkdir, open, unlink } from "node:fs/promises";
import { dirname } from "node:path";

import {
  KAFKA_QUERY_LIBRARY_LIMITS,
  inspectKafkaQueryLibraryDocument,
  serializeKafkaQueryLibraryDocument,
  type KafkaInvestigationLibraryState,
} from "../../features/kafka/contracts";
import { KafkaQueryLibraryError, type KafkaQueryStore } from "../../features/kafka/application";

import { readBoundedFile } from "./bounded-file";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

interface SourceSnapshot {
  readonly bytes: Buffer;
  readonly metadata: Stats;
  readonly document: ReturnType<typeof inspectKafkaQueryLibraryDocument>;
}
export interface KafkaQueryFileStoreOptions {
  readonly createTempId?: () => string;
  readonly syncDirectory?: (path: string) => Promise<void>;
}
function codeIs(error: unknown, code: string): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === code;
}
function sameFile(left: Stats, right: Stats): boolean {
  return (
    left.isFile() &&
    right.isFile() &&
    left.dev === right.dev &&
    left.ino === right.ino &&
    left.size === right.size &&
    left.mtimeMs === right.mtimeMs &&
    left.ctimeMs === right.ctimeMs &&
    left.mode === right.mode &&
    left.nlink === right.nlink
  );
}
async function syncDirectory(path: string): Promise<void> {
  if (process.platform === "win32") return;
  const directory = await open(path, "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

/** One serialized library owns writes; loading and compatibility inspection never migrate. */
export class AtomicKafkaQueryFileStore implements KafkaQueryStore {
  readonly durability = "durable" as const;
  private loaded: SourceSnapshot | null | undefined;
  private readonly createTempId: () => string;
  private readonly syncDirectory: (path: string) => Promise<void>;

  constructor(
    private readonly path: string,
    options: KafkaQueryFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
    this.syncDirectory = options.syncDirectory ?? syncDirectory;
  }

  async load(): Promise<KafkaInvestigationLibraryState> {
    try {
      const source = await this.readSource(this.path);
      this.loaded = source;
      return { queries: source?.document.queries ?? [], topics: source?.document.topics ?? [] };
    } catch {
      throw new KafkaQueryLibraryError(
        "Investigation-library storage is unreadable or uses an unsupported schema. The file was not replaced.",
      );
    }
  }

  async commit(state: KafkaInvestigationLibraryState): Promise<void> {
    const contents = serializeKafkaQueryLibraryDocument(state);
    if (Buffer.byteLength(contents, "utf8") > KAFKA_QUERY_LIBRARY_LIMITS.fileBytes)
      throw new KafkaQueryLibraryError(
        "The investigation library exceeds its shared 1 MiB storage limit. Remove some views or notes before retrying.",
      );
    let replaced = false;
    try {
      const original = this.loaded === undefined ? await this.readSource(this.path) : this.loaded;
      await this.assertUnchanged(original);
      const directory = dirname(this.path);
      await mkdir(directory, { recursive: true, mode: 0o700 });
      if (!(await lstat(directory)).isDirectory()) throw new Error("Unsafe library directory.");
      await chmod(directory, 0o700);
      const backup =
        original !== null && original.document.schemaVersion < 4
          ? await this.preservePredecessor(original)
          : undefined;
      await writeAtomicPrivateTextFile({
        path: this.path,
        contents,
        createTempId: this.createTempId,
        beforeCommit: async (): Promise<void> => {
          await this.assertUnchanged(original);
          if (backup !== undefined && original !== null) await this.verifyBackup(backup, original);
        },
      });
      replaced = true;
      await this.syncDirectory(directory);
      const current = await this.readSource(this.path);
      if (current === null || !current.bytes.equals(Buffer.from(contents)))
        throw new Error("Committed library changed.");
      this.loaded = current;
    } catch {
      // A completed rename cannot be called a failed no-op if directory durability or
      // readback is uncertain. Re-listing establishes the current authoritative bytes.
      this.loaded = undefined;
      throw new KafkaQueryLibraryError(
        replaced
          ? "The investigation-library replacement occurred, but durable completion could not be confirmed. Reopen Saved views or Local notes to inspect current state before retrying. Any existing predecessor backup was retained."
          : "Investigation-library storage could not commit the change. The current file was not replaced. Check permissions and preserved backups, then reopen Saved views or Local notes before retrying.",
      );
    }
  }

  private async readSource(path: string): Promise<SourceSnapshot | null> {
    let metadata: Stats;
    try {
      metadata = await lstat(path);
    } catch (error) {
      if (codeIs(error, "ENOENT")) return null;
      throw error;
    }
    if (!metadata.isFile() || metadata.nlink !== 1)
      throw new Error("Expected a single-link regular library file.");
    if (!(await lstat(dirname(path))).isDirectory()) throw new Error("Unsafe library directory.");
    const bytes = await readBoundedFile(path, KAFKA_QUERY_LIBRARY_LIMITS.fileBytes, {
      rejectSymlinks: true,
    });
    if (!sameFile(metadata, await lstat(path))) throw new Error("Library changed while reading.");
    return {
      bytes,
      metadata,
      document: inspectKafkaQueryLibraryDocument(JSON.parse(bytes.toString("utf8")) as unknown),
    };
  }

  private async assertUnchanged(original: SourceSnapshot | null): Promise<void> {
    const current = await this.readSource(this.path);
    if (
      original === null
        ? current !== null
        : current === null ||
          !sameFile(original.metadata, current.metadata) ||
          !original.bytes.equals(current.bytes)
    )
      throw new Error("Library changed after loading.");
  }

  private async verifyBackup(path: string, original: SourceSnapshot): Promise<void> {
    const backup = await this.readSource(path);
    if (
      backup === null ||
      backup.document.schemaVersion !== original.document.schemaVersion ||
      !backup.bytes.equals(original.bytes) ||
      (process.platform !== "win32" && (backup.metadata.mode & 0o777) !== 0o600)
    )
      throw new Error("Legacy backup could not be verified.");
  }

  private async preservePredecessor(original: SourceSnapshot): Promise<string> {
    const suffix =
      original.document.schemaVersion === 1
        ? ".pre-views-v1"
        : original.document.schemaVersion === 2
          ? ".pre-records-v2"
          : original.document.schemaVersion === 3
            ? ".pre-catalog-v3"
            : undefined;
    if (suffix === undefined) throw new Error("No predecessor migration was selected.");
    for (let generation = 0; generation < 100; generation += 1) {
      const path = `${this.path}${suffix}${generation === 0 ? "" : `.${generation}`}`;
      let handle: Awaited<ReturnType<typeof open>>;
      try {
        handle = await open(
          path,
          constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW ?? 0),
          0o600,
        );
      } catch (error) {
        if (!codeIs(error, "EEXIST")) throw error;
        const previous = await this.readSource(path);
        if (
          previous === null ||
          previous.document.schemaVersion !== original.document.schemaVersion ||
          (process.platform !== "win32" && (previous.metadata.mode & 0o777) !== 0o600)
        )
          throw new Error("Existing predecessor backup is unsafe.", { cause: error });
        if (!previous.bytes.equals(original.bytes)) continue;
        await this.verifyBackup(path, original);
        await this.syncDirectory(dirname(path));
        return path;
      }
      let created: Stats | undefined;
      let verified = false;
      try {
        created = await handle.stat();
        await handle.writeFile(original.bytes);
        await handle.sync();
        await handle.close();
        await this.verifyBackup(path, original);
        verified = true;
        await this.syncDirectory(dirname(path));
        return path;
      } catch (error) {
        await handle.close().catch(() => undefined);
        if (!verified) {
          const current = await lstat(path).catch(() => undefined);
          if (created !== undefined && current?.dev === created.dev && current.ino === created.ino)
            await unlink(path).catch(() => undefined);
        }
        throw error;
      }
    }
    throw new Error("Legacy backup capacity is exhausted.");
  }
}
