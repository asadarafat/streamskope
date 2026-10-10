import { lstat } from "node:fs/promises";
import { dirname } from "node:path";

import type { ObservationStore } from "../../features/kafka/application/observation-store";
import {
  OBSERVATION_LIMITS,
  type ObservationHistory,
  emptyObservationHistory,
} from "../../features/kafka/contracts/observations";
import { parseObservationHistory } from "../../features/kafka/contracts/observation-validation";

import { readBoundedFile } from "./bounded-file";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
  syncPrivateFileDirectory,
} from "./atomic-private-text-file";
import { preservePrivatePredecessor } from "./private-predecessor-file";

function missing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}
export class AtomicObservationFileStore implements ObservationStore {
  readonly durability = "durable" as const;
  private loaded: Buffer | null | undefined;
  private loadedFormat: 1 | 2 | undefined;
  constructor(
    private readonly path: string,
    private readonly syncDirectory = syncPrivateFileDirectory,
  ) {}
  private async bytes(): Promise<Buffer | null> {
    const directory = await lstat(dirname(this.path)).catch((error: unknown) => {
      if (missing(error)) return null;
      throw error;
    });
    if (directory !== null && !directory.isDirectory())
      throw new Error("Unsafe observation directory.");
    const metadata = await lstat(this.path).catch((error: unknown) => {
      if (missing(error)) return null;
      throw error;
    });
    if (metadata === null) return null;
    if (!metadata.isFile() || metadata.nlink !== 1) throw new Error("Unsafe observation history.");
    return readBoundedFile(this.path, OBSERVATION_LIMITS.fileBytes, { rejectSymlinks: true });
  }
  async load(): Promise<ObservationHistory> {
    try {
      const bytes = await this.bytes();
      if (bytes === null) {
        this.loaded = null;
        this.loadedFormat = undefined;
        return emptyObservationHistory();
      }
      const history = parseObservationHistory(JSON.parse(bytes.toString("utf8")) as unknown);
      this.loaded = bytes;
      this.loadedFormat = history.schemaVersion;
      return history;
    } catch (error) {
      throw new Error(
        "Observation history is unreadable or unsupported. It has not been replaced.",
        { cause: error },
      );
    }
  }
  async commit(history: ObservationHistory): Promise<void> {
    const parsed = parseObservationHistory(history);
    if (this.loaded === undefined) await this.load();
    if (this.loadedFormat === 2 && parsed.schemaVersion === 1)
      throw new Error("Observation downgrade requires restoring the complete predecessor backup.");
    await this.write(parsed, this.loaded!, this.loadedFormat);
  }
  /** A deliberate clear can discard corrupt bounded bytes; an ordinary write cannot. */
  async clear(): Promise<void> {
    await this.write(emptyObservationHistory(), await this.bytes(), undefined);
  }
  private async write(
    history: ObservationHistory,
    previous: Buffer | null,
    format: 1 | 2 | undefined,
  ): Promise<void> {
    const contents = JSON.stringify(history) + "\n";
    if (Buffer.byteLength(contents) > OBSERVATION_LIMITS.fileBytes)
      throw new Error("Observation history exceeds its file limit.");
    const assertUnchanged = async (): Promise<void> => {
      const current = await this.bytes();
      if (previous === null ? current !== null : current === null || !previous.equals(current))
        throw new Error("Observation history changed after loading; no replacement was made.");
    };
    await assertUnchanged();
    await writeAtomicPrivateTextFile({
      path: this.path,
      contents,
      createTempId: createAtomicPrivateFileTempId,
      beforeCommit: async (): Promise<void> => {
        await assertUnchanged();
        if (format === 1 && history.schemaVersion === 2 && previous !== null) {
          await preservePrivatePredecessor({
            path: `${this.path}.pre-observation-v1`,
            bytes: previous,
            maximumBytes: OBSERVATION_LIMITS.fileBytes,
            syncDirectory: this.syncDirectory,
          });
          await assertUnchanged();
        }
      },
    });
    this.loaded = Buffer.from(contents);
    this.loadedFormat = history.schemaVersion;
    await this.syncDirectory(dirname(this.path));
  }
}
