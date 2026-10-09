import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  randomUUID,
  type Hash,
} from "node:crypto";
import { constants } from "node:fs";
import { chmod, mkdtemp, open, rm, type FileHandle } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type {
  RecordExportArtifacts,
  RecordExportSink,
} from "../../features/kafka/application/record-export-artifacts";
import {
  RECORD_EXPORT_LIMITS,
  type RecordExportArtifact,
  type RecordExportReceiptDetails,
} from "../../features/kafka/contracts/record-export";
import { parseArtifactReference, type ArtifactReference } from "../desktop";

const CHUNK_BYTES = 64 * 1024;
const FRAME_HEADER_BYTES = 16;
const TAG_BYTES = 16;
type CreateInput = Parameters<RecordExportArtifacts["create"]>[0];

export interface RecordExportFile {
  readonly fileName: string;
  readonly mediaType: "text/csv; charset=utf-8" | "application/x-ndjson" | "application/json";
  readonly bytes: number;
  readonly sha256: string;
}
export interface RecordExportDownloadAuthority {
  readonly signal: AbortSignal;
  readonly assertCurrent: () => void;
}
/** Host-only delivery authority. Never put this port, paths or bytes on renderer RPC. */
export interface RecordExportDelivery {
  describe(reference: ArtifactReference): RecordExportFile;
  withDownload<T>(
    reference: ArtifactReference,
    authority: RecordExportDownloadAuthority,
    consume: (chunks: AsyncIterable<Uint8Array>, signal: AbortSignal) => Promise<T>,
  ): Promise<T>;
}

export class RecordExportFileError extends Error {
  constructor(
    readonly code: "unavailable" | "busy" | "storage" | "cleanup",
    options?: ErrorOptions,
  ) {
    super(
      code === "busy"
        ? "Two export downloads are already active. Wait for one to finish."
        : code === "cleanup"
          ? "Export file cleanup could not be confirmed. Retry cleanup before another export."
          : code === "storage"
            ? "The export file could not be written or verified. Create a new export."
            : "This export is unavailable or expired. Create a new export.",
      options,
    );
    this.name = "RecordExportFileError";
  }
}

interface EncryptedFile {
  readonly path: string;
  readonly part: ArtifactReference["part"];
  readonly firstNonce: number;
  frames: number;
  bytes: number;
  wireBytes: number;
  sha256: string;
}
interface ArtifactEntry {
  readonly id: string;
  readonly input: CreateInput;
  readonly abort: AbortController;
  readonly key: Buffer;
  readonly noncePrefix: Buffer;
  readonly hash: Hash;
  readonly downloads: Set<Promise<unknown>>;
  readonly handles: Set<FileHandle>;
  readonly stop: () => void;
  directory?: string;
  data?: EncryptedFile;
  receipt?: EncryptedFile;
  writer?: FileHandle;
  descriptor?: RecordExportArtifact;
  pending: Promise<void>;
  cleanup?: Promise<void>;
  cleanupFailure?: unknown;
  timer?: ReturnType<typeof setTimeout>;
  nonce: number;
  state: "writing" | "sealed" | "poisoned" | "revoked";
}
interface ArtifactOptions {
  readonly temporaryRoot?: string;
  readonly downloadTimeoutMs?: number;
  /** Narrow filesystem fault seam: production uses FileHandle.write. */
  readonly write?: (file: FileHandle, bytes: Uint8Array, offset: number) => Promise<number>;
  readonly remove?: (directory: string) => Promise<void>;
  readonly close?: (file: FileHandle) => Promise<void>;
}

/** One lazily-created transient encrypted output owner, separate from persistent vault data. */
export class NodeRecordExportArtifacts implements RecordExportArtifacts {
  private readonly entries = new Set<ArtifactEntry>();
  private generation = 0;
  private creating = false;
  readonly delivery: RecordExportDelivery = {
    describe: (reference) => this.describe(reference),
    withDownload: (reference, authority, consume) => this.download(reference, authority, consume),
  };
  constructor(private readonly options: ArtifactOptions = {}) {}

  async create(input: CreateInput): Promise<RecordExportSink> {
    input.signal.throwIfAborted();
    input.assertCurrent();
    if (this.creating || [...this.entries].some((entry) => entry.state === "writing"))
      throw new RecordExportFileError("busy");
    if (
      !Number.isSafeInteger(input.maximumBytes) ||
      input.maximumBytes < 1 ||
      input.maximumBytes > RECORD_EXPORT_LIMITS.bytes ||
      !Number.isSafeInteger(input.lifetimeMs) ||
      input.lifetimeMs < 1 ||
      input.lifetimeMs > RECORD_EXPORT_LIMITS.artifactLifetimeMs ||
      !["csv", "jsonl"].includes(input.format)
    )
      throw new RecordExportFileError("storage");
    this.creating = true;
    this.revoke();
    const generation = this.generation;
    try {
      await this.drain();
      input.signal.throwIfAborted();
      input.assertCurrent();
      if (generation !== this.generation) throw new RecordExportFileError("unavailable");
      const abort = new AbortController();
      const entry: ArtifactEntry = {
        id: randomUUID(),
        input,
        abort,
        key: randomBytes(32),
        noncePrefix: randomBytes(4),
        hash: createHash("sha256"),
        downloads: new Set(),
        handles: new Set(),
        pending: Promise.resolve(),
        nonce: 0,
        state: "writing",
        stop: () => {
          this.revokeEntry(entry);
        },
      };
      this.entries.add(entry);
      input.signal.addEventListener("abort", entry.stop, { once: true });
      try {
        await this.operation(entry, async () => {
          entry.directory = await mkdtemp(
            join(this.options.temporaryRoot ?? tmpdir(), "streamskope-export-"),
          );
          await chmod(entry.directory, 0o700);
          this.assertEntry(entry);
          entry.data = this.file(entry, "data");
          entry.writer = await open(entry.data.path, "wx", 0o600);
          entry.handles.add(entry.writer);
          this.assertEntry(entry);
        });
      } catch (cause) {
        this.revokeEntry(entry);
        try {
          await this.clean(entry);
        } catch {
          /* drain retains cleanup debt */
        }
        throw new RecordExportFileError("storage", { cause });
      }
      return {
        write: (row) => this.operation(entry, () => this.writeRow(entry, row)),
        seal: (receipt) => this.seal(entry, receipt),
        discard: async (): Promise<void> => {
          this.revokeEntry(entry);
          await this.clean(entry);
        },
      };
    } finally {
      this.creating = false;
    }
  }

  revoke(): void {
    this.generation += 1;
    for (const entry of this.entries) this.revokeEntry(entry);
  }

  async drain(): Promise<void> {
    const results = await Promise.allSettled(
      [...this.entries].map((entry) =>
        entry.state === "revoked" ? this.clean(entry) : entry.pending,
      ),
    );
    const failures = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason as unknown] : [],
    );
    if (failures.length > 0)
      throw new RecordExportFileError("cleanup", { cause: new AggregateError(failures) });
  }

  private file(entry: ArtifactEntry, part: ArtifactReference["part"]): EncryptedFile {
    return {
      path: join(entry.directory!, `${part}.encrypted`),
      part,
      firstNonce: entry.nonce,
      frames: 0,
      bytes: 0,
      wireBytes: 0,
      sha256: "",
    };
  }
  private assertEntry(entry: ArtifactEntry): void {
    if (entry.abort.signal.aborted || entry.state === "revoked" || entry.state === "poisoned")
      throw new RecordExportFileError("unavailable");
    entry.input.signal.throwIfAborted();
    entry.input.assertCurrent();
    if (entry.descriptor !== undefined && Date.parse(entry.descriptor.expiresAt) <= Date.now()) {
      this.revokeEntry(entry);
      throw new RecordExportFileError("unavailable");
    }
  }
  private operation(entry: ArtifactEntry, run: () => Promise<void>): Promise<void> {
    const operation = entry.pending.then(async () => {
      this.assertEntry(entry);
      if (entry.state !== "writing") throw new RecordExportFileError("unavailable");
      try {
        await run();
      } catch (cause) {
        if (!entry.abort.signal.aborted) entry.state = "poisoned";
        throw new RecordExportFileError("storage", { cause });
      }
    });
    entry.pending = operation.catch(() => undefined);
    return operation;
  }
  private async writeAll(file: FileHandle, bytes: Uint8Array): Promise<void> {
    let offset = 0;
    while (offset < bytes.byteLength) {
      const written =
        this.options.write === undefined
          ? (await file.write(bytes, offset, bytes.byteLength - offset, null)).bytesWritten
          : await this.options.write(file, bytes, offset);
      if (!Number.isSafeInteger(written) || written <= 0 || written > bytes.byteLength - offset)
        throw new RecordExportFileError("storage");
      offset += written;
    }
  }
  private aad(entry: ArtifactEntry, file: EncryptedFile, index: number, bytes: number): Buffer {
    return Buffer.from(
      `streamskope.export/v1:${entry.id}:${file.part}:${String(index)}:${String(bytes)}`,
      "utf8",
    );
  }
  private async encryptedWrite(
    entry: ArtifactEntry,
    file: EncryptedFile,
    handle: FileHandle,
    bytes: Uint8Array,
  ): Promise<void> {
    for (let offset = 0; offset < bytes.byteLength; offset += CHUNK_BYTES) {
      this.assertEntry(entry);
      const chunk = bytes.subarray(offset, Math.min(offset + CHUNK_BYTES, bytes.byteLength));
      const header = Buffer.alloc(FRAME_HEADER_BYTES);
      entry.noncePrefix.copy(header);
      header.writeBigUInt64BE(BigInt(entry.nonce++), 4);
      header.writeUInt32BE(chunk.byteLength, 12);
      const cipher = createCipheriv("aes-256-gcm", entry.key, header.subarray(0, 12));
      cipher.setAAD(this.aad(entry, file, file.frames, chunk.byteLength));
      const ciphertext = Buffer.concat([
        header,
        cipher.update(chunk),
        cipher.final(),
        cipher.getAuthTag(),
      ]);
      await this.writeAll(handle, ciphertext);
      file.frames += 1;
      file.bytes += chunk.byteLength;
      file.wireBytes += ciphertext.byteLength;
      this.assertEntry(entry);
    }
  }
  private async writeRow(entry: ArtifactEntry, row: Uint8Array): Promise<void> {
    if (row.byteLength > entry.input.maximumBytes - entry.data!.bytes)
      throw new RecordExportFileError("storage");
    await this.encryptedWrite(entry, entry.data!, entry.writer!, row);
    entry.hash.update(row);
  }
  private async closeHandle(entry: ArtifactEntry, file: FileHandle): Promise<void> {
    if (this.options.close) await this.options.close(file);
    else await file.close();
    entry.handles.delete(file);
  }
  private async seal(
    entry: ArtifactEntry,
    receipt: RecordExportReceiptDetails,
  ): Promise<RecordExportArtifact> {
    await this.operation(entry, async () => {
      const data = entry.data!;
      if (receipt.counts.writtenBytes !== data.bytes) throw new RecordExportFileError("storage");
      await entry.writer!.sync();
      await this.closeHandle(entry, entry.writer!);
      data.sha256 = entry.hash.digest("hex");
      const output = {
        format: entry.input.format,
        fileName: `streamskope-export-${entry.id}.${entry.input.format}`,
        bytes: data.bytes,
        sha256: data.sha256,
      };
      const receiptBytes = Buffer.from(
        `${JSON.stringify({ ...receipt, schema: "streamskope.record-export/v1", output }, null, 2)}\n`,
        "utf8",
      );
      if (receiptBytes.byteLength > RECORD_EXPORT_LIMITS.receiptBytes)
        throw new RecordExportFileError("storage");
      entry.receipt = this.file(entry, "receipt");
      const file = await open(entry.receipt.path, "wx", 0o600);
      entry.handles.add(file);
      await this.encryptedWrite(entry, entry.receipt, file, receiptBytes);
      entry.receipt.sha256 = createHash("sha256").update(receiptBytes).digest("hex");
      await file.sync();
      await this.closeHandle(entry, file);
      this.assertEntry(entry);
      entry.descriptor = Object.freeze({
        artifactId: entry.id,
        output: Object.freeze(output),
        receiptBytes: receiptBytes.byteLength,
        receiptSha256: entry.receipt.sha256,
        expiresAt: new Date(Date.now() + entry.input.lifetimeMs).toISOString(),
      });
      entry.state = "sealed";
      entry.timer = setTimeout(() => this.revokeEntry(entry), entry.input.lifetimeMs);
      entry.timer.unref();
    });
    return entry.descriptor!;
  }
  private select(reference: ArtifactReference): { entry: ArtifactEntry; file: EncryptedFile } {
    const valid = parseArtifactReference(reference);
    const entry = [...this.entries].find((item) => item.id === valid.artifactId);
    if (entry === undefined || entry.state !== "sealed")
      throw new RecordExportFileError("unavailable");
    this.assertEntry(entry);
    return { entry, file: valid.part === "data" ? entry.data! : entry.receipt! };
  }
  private describe(reference: ArtifactReference): RecordExportFile {
    const { entry, file } = this.select(reference);
    return {
      fileName:
        reference.part === "receipt"
          ? `streamskope-export-${entry.id}.receipt.json`
          : entry.descriptor!.output.fileName,
      mediaType:
        reference.part === "receipt"
          ? "application/json"
          : entry.input.format === "csv"
            ? "text/csv; charset=utf-8"
            : "application/x-ndjson",
      bytes: file.bytes,
      sha256: file.sha256,
    };
  }
  private async download<T>(
    reference: ArtifactReference,
    authority: RecordExportDownloadAuthority,
    consume: (chunks: AsyncIterable<Uint8Array>, signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    authority.signal.throwIfAborted();
    authority.assertCurrent();
    const { entry, file } = this.select(reference);
    if (entry.downloads.size >= RECORD_EXPORT_LIMITS.downloads)
      throw new RecordExportFileError("busy");
    const timeout = this.options.downloadTimeoutMs ?? RECORD_EXPORT_LIMITS.downloadDurationMs;
    if (
      !Number.isSafeInteger(timeout) ||
      timeout < 1 ||
      timeout > RECORD_EXPORT_LIMITS.downloadDurationMs
    )
      throw new RecordExportFileError("storage");
    const signal = AbortSignal.any([
      entry.abort.signal,
      authority.signal,
      AbortSignal.timeout(timeout),
    ]);
    const assertCurrent = (): void => {
      signal.throwIfAborted();
      authority.assertCurrent();
      this.assertEntry(entry);
    };
    const chunks = this.read(entry, file, assertCurrent);
    const operation = Promise.resolve().then(async () => {
      try {
        assertCurrent();
        const result = await consume(chunks, signal);
        assertCurrent();
        return result;
      } finally {
        await chunks.return(undefined);
      }
    });
    entry.downloads.add(operation);
    try {
      return await operation;
    } finally {
      entry.downloads.delete(operation);
    }
  }
  private async *read(
    entry: ArtifactEntry,
    file: EncryptedFile,
    assertCurrent: () => void,
  ): AsyncGenerator<Uint8Array> {
    assertCurrent();
    const handle = await open(
      file.path,
      constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0),
    );
    entry.handles.add(handle);
    try {
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size !== file.wireBytes || stat.nlink !== 1)
        throw new RecordExportFileError("storage");
      let position = 0;
      let bytes = 0;
      const hash = createHash("sha256");
      const readExactly = async (size: number): Promise<Buffer> => {
        const buffer = Buffer.alloc(size);
        let offset = 0;
        while (offset < size) {
          assertCurrent();
          const result = await handle.read(buffer, offset, size - offset, position + offset);
          if (result.bytesRead === 0) throw new RecordExportFileError("storage");
          offset += result.bytesRead;
        }
        position += size;
        return buffer;
      };
      let last: Buffer | undefined;
      for (let index = 0; index < file.frames; index += 1) {
        const header = await readExactly(FRAME_HEADER_BYTES);
        const size = header.readUInt32BE(12);
        if (
          size < 1 ||
          size > CHUNK_BYTES ||
          !header.subarray(0, 4).equals(entry.noncePrefix) ||
          header.readBigUInt64BE(4) !== BigInt(file.firstNonce + index)
        )
          throw new RecordExportFileError("storage");
        const ciphertext = await readExactly(size + TAG_BYTES);
        const decipher = createDecipheriv("aes-256-gcm", entry.key, header.subarray(0, 12));
        decipher.setAAD(this.aad(entry, file, index, size));
        decipher.setAuthTag(ciphertext.subarray(size));
        const plaintext = decipher.update(ciphertext.subarray(0, size));
        try {
          decipher.final();
        } catch (cause) {
          plaintext.fill(0);
          throw new RecordExportFileError("storage", { cause });
        }
        hash.update(plaintext);
        bytes += plaintext.byteLength;
        assertCurrent();
        if (index === file.frames - 1) last = plaintext;
        else yield plaintext;
      }
      const trailing = await handle.read(Buffer.alloc(1), 0, 1, position);
      if (
        trailing.bytesRead !== 0 ||
        (await handle.stat()).size !== file.wireBytes ||
        bytes !== file.bytes ||
        hash.digest("hex") !== file.sha256
      )
        throw new RecordExportFileError("storage");
      assertCurrent();
      if (last !== undefined) yield last;
    } finally {
      await this.closeDownloadHandle(entry, handle);
    }
  }
  private async closeDownloadHandle(entry: ArtifactEntry, handle: FileHandle): Promise<void> {
    try {
      await this.closeHandle(entry, handle);
    } catch (cause) {
      this.revokeEntry(entry);
      throw new RecordExportFileError("cleanup", { cause });
    }
  }
  private revokeEntry(entry: ArtifactEntry): void {
    entry.state = "revoked";
    clearTimeout(entry.timer);
    entry.abort.abort(new RecordExportFileError("unavailable"));
    void this.clean(entry).catch(() => undefined); // drain/discard observe retained cleanup debt.
  }
  private clean(entry: ArtifactEntry): Promise<void> {
    if (entry.cleanup !== undefined) return entry.cleanup;
    const operation = (async (): Promise<void> => {
      await entry.pending;
      await Promise.allSettled([...entry.downloads]);
      const closed = await Promise.allSettled(
        [...entry.handles].map((handle) => this.closeHandle(entry, handle)),
      );
      entry.key.fill(0);
      if (closed.some((result) => result.status === "rejected"))
        throw new RecordExportFileError("cleanup");
      if (entry.directory !== undefined) {
        if (this.options.remove) await this.options.remove(entry.directory);
        else await rm(entry.directory, { recursive: true, force: true });
      }
      entry.input.signal.removeEventListener("abort", entry.stop);
      entry.cleanupFailure = undefined;
      this.entries.delete(entry);
    })();
    entry.cleanup = operation;
    void operation
      .catch((error: unknown) => {
        entry.cleanupFailure = error;
      })
      .finally(() => {
        delete entry.cleanup;
      });
    return operation;
  }
}
