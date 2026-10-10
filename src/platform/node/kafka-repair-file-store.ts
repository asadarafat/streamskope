import { lstat, open } from "node:fs/promises";
import { dirname } from "node:path";

import type { RepairJobStore } from "../../features/kafka/application/repair-journal";
import {
  parseRepairJournalDocument,
  REPAIR_JOB_LIMITS,
  type RepairJournalDocument,
} from "../../features/kafka/contracts/repair-jobs";

import { readBoundedFile } from "./bounded-file";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";
import type { ProfileProtector } from "./profile-protector";
import { preservePrivatePredecessor } from "./private-predecessor-file";

export const REPAIR_ENVELOPE_MAX_BYTES = REPAIR_JOB_LIMITS.fileBytes * 2;
export function repairEnvelopeFormat(value: unknown): 1 | 2 {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid protected repair envelope.");
  const v = value as Record<string, unknown>;
  if (
    Object.keys(v).length !== 2 ||
    (v.schemaVersion !== 1 && v.schemaVersion !== 2) ||
    typeof v.protected !== "string" ||
    v.protected.length === 0 ||
    v.protected.length > REPAIR_ENVELOPE_MAX_BYTES ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u.test(v.protected)
  )
    throw new Error("Unsupported protected repair envelope.");
  return v.schemaVersion;
}
export function inspectRepairEnvelope(value: unknown): Buffer {
  repairEnvelopeFormat(value);
  return Buffer.from((value as { protected: string }).protected, "base64");
}
/** Payloads and receipts use the same host protection authority as saved credentials. */
export class AtomicRepairFileStore implements RepairJobStore {
  readonly durability = "durable" as const;
  private loaded: Buffer | null | undefined;
  private loadedFormat: 1 | 2 | undefined;
  constructor(
    private readonly path: string,
    private readonly protector: ProfileProtector,
    private readonly syncDirectory: (directory: string) => Promise<void> = async (directory) => {
      if (process.platform === "win32") return;
      const handle = await open(directory, "r");
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
    },
  ) {}
  async load(): Promise<RepairJournalDocument> {
    try {
      const stat = await lstat(this.path).catch((error: unknown) => {
        if (
          error !== null &&
          typeof error === "object" &&
          "code" in error &&
          error.code === "ENOENT"
        )
          return null;
        throw error;
      });
      if (stat === null) {
        this.loaded = null;
        this.loadedFormat = undefined;
        return { schemaVersion: 2, jobs: [] };
      }
      if (!stat.isFile() || stat.nlink !== 1) throw new Error("Unsafe repair journal.");
      if (!(await lstat(dirname(this.path))).isDirectory())
        throw new Error("Unsafe repair directory.");
      const bytes = await readBoundedFile(this.path, REPAIR_ENVELOPE_MAX_BYTES, {
        rejectSymlinks: true,
      });
      const envelope: unknown = JSON.parse(bytes.toString("utf8"));
      const format = repairEnvelopeFormat(envelope);
      const protectedValue = inspectRepairEnvelope(envelope);
      const { plaintext } = await this.protector.unprotect(protectedValue);
      const payload: unknown = JSON.parse(plaintext);
      if (
        payload === null ||
        typeof payload !== "object" ||
        !("schemaVersion" in payload) ||
        payload.schemaVersion !== format
      )
        throw new Error("Repair envelope differs from its protected format.");
      const document = parseRepairJournalDocument(payload);
      this.loaded = bytes;
      this.loadedFormat = format;
      return document;
    } catch (error) {
      throw new Error(
        "Protected repair history is unreadable or unsupported. Preserve it and restore the complete vault backup; no replacement was made.",
        { cause: error },
      );
    }
  }
  async commit(document: RepairJournalDocument): Promise<void> {
    if (this.loaded === undefined) await this.load();
    const previous = this.loaded;
    const parsed = parseRepairJournalDocument(document);
    const protectedValue = await this.protector.protect(JSON.stringify(parsed));
    const contents =
      JSON.stringify({ schemaVersion: 2, protected: protectedValue.toString("base64") }) + "\n";
    if (Buffer.byteLength(contents) > REPAIR_ENVELOPE_MAX_BYTES)
      throw new Error("Protected repair history exceeds its file bound.");
    await writeAtomicPrivateTextFile({
      path: this.path,
      contents,
      createTempId: createAtomicPrivateFileTempId,
      beforeCommit: async () => {
        if (!(await lstat(dirname(this.path))).isDirectory())
          throw new Error("Unsafe repair directory.");
        let current: Buffer | null = null;
        try {
          const stat = await lstat(this.path);
          if (!stat.isFile() || stat.nlink !== 1) throw new Error("Unsafe repair journal.");
          current = await readBoundedFile(this.path, REPAIR_ENVELOPE_MAX_BYTES, {
            rejectSymlinks: true,
          });
        } catch (error) {
          if (!(
            error !== null &&
            typeof error === "object" &&
            "code" in error &&
            error.code === "ENOENT"
          ))
            throw error;
        }
        if (
          previous === null
            ? current !== null
            : previous === undefined || current === null || !previous.equals(current)
        )
          throw new Error("Repair journal changed after loading; no replacement was made.");
        if (this.loadedFormat === 1 && previous !== null && previous !== undefined) {
          await preservePrivatePredecessor({
            path: `${this.path}.pre-repair-v1`,
            bytes: previous,
            maximumBytes: REPAIR_ENVELOPE_MAX_BYTES,
            syncDirectory: this.syncDirectory,
          });
          const afterBackup = await lstat(this.path);
          if (
            !afterBackup.isFile() ||
            afterBackup.nlink !== 1 ||
            !previous.equals(
              await readBoundedFile(this.path, REPAIR_ENVELOPE_MAX_BYTES, { rejectSymlinks: true }),
            )
          )
            throw new Error(
              "Repair journal changed during predecessor preservation; no replacement was made.",
            );
        }
      },
    });
    await this.syncDirectory(dirname(this.path));
    const current = await readBoundedFile(this.path, REPAIR_ENVELOPE_MAX_BYTES, {
      rejectSymlinks: true,
    });
    if (!current.equals(Buffer.from(contents)))
      throw new Error("Repair commit readback differs; durable completion is uncertain.");
    this.loaded = current;
    this.loadedFormat = 2;
  }
}
