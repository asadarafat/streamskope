import { randomUUID } from "node:crypto";
import { open, rename, unlink, type FileHandle } from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";

import type { ArtifactReference } from "../desktop";

import {
  RecordExportFileError,
  type RecordExportDelivery,
  type RecordExportDownloadAuthority,
} from "./record-export-artifacts";

interface SaveLease {
  readonly temporary: string;
  file?: FileHandle;
  created: boolean;
  committed: boolean;
}

/** Keeps native file ownership until close/removal are confirmed, including failed saves. */
export class NodeRecordExportSaver {
  private readonly leases = new Set<SaveLease>();
  private readonly pending = new Set<Promise<void>>();
  constructor(
    private readonly delivery: RecordExportDelivery,
    private readonly options: {
      readonly close?: (file: FileHandle) => Promise<void>;
      readonly remove?: (path: string) => Promise<void>;
      readonly rename?: (temporary: string, destination: string) => Promise<void>;
    } = {},
  ) {}

  save(
    reference: ArtifactReference,
    destination: string,
    authority: RecordExportDownloadAuthority,
  ): Promise<void> {
    if (this.pending.size === 0 && this.leases.size !== 0)
      return Promise.reject(new RecordExportFileError("cleanup"));
    const operation = this.run(reference, destination, authority);
    this.pending.add(operation);
    void operation.then(
      () => this.pending.delete(operation),
      () => this.pending.delete(operation),
    );
    return operation;
  }

  async drain(): Promise<void> {
    await Promise.allSettled([...this.pending]);
    const results = await Promise.allSettled([...this.leases].map((lease) => this.cleanup(lease)));
    if (results.some((result) => result.status === "rejected"))
      throw new RecordExportFileError("cleanup");
  }

  private async close(lease: SaveLease): Promise<void> {
    if (lease.file === undefined) return;
    if (this.options.close) await this.options.close(lease.file);
    else await lease.file.close();
    delete lease.file;
  }
  private async cleanup(lease: SaveLease): Promise<void> {
    await this.close(lease);
    if (lease.created && !lease.committed) {
      if (this.options.remove) await this.options.remove(lease.temporary);
      else await unlink(lease.temporary);
    }
    this.leases.delete(lease);
  }
  private async run(
    reference: ArtifactReference,
    destination: string,
    authority: RecordExportDownloadAuthority,
  ): Promise<void> {
    if (!isAbsolute(destination)) throw new RecordExportFileError("storage");
    const lease: SaveLease = {
      temporary: join(dirname(destination), `.${basename(destination)}.${randomUUID()}.partial`),
      created: false,
      committed: false,
    };
    this.leases.add(lease);
    let failed = false;
    let cause: unknown;
    try {
      await this.delivery.withDownload(reference, authority, async (chunks, signal) => {
        lease.file = await open(lease.temporary, "wx", 0o600);
        lease.created = true;
        for await (const chunk of chunks) {
          let offset = 0;
          while (offset < chunk.byteLength) {
            signal.throwIfAborted();
            authority.assertCurrent();
            const result = await lease.file.write(chunk, offset, chunk.byteLength - offset, null);
            if (result.bytesWritten <= 0) throw new RecordExportFileError("storage");
            offset += result.bytesWritten;
          }
        }
        await lease.file.sync();
        await this.close(lease);
        signal.throwIfAborted();
        authority.assertCurrent();
        if (this.options.rename) await this.options.rename(lease.temporary, destination);
        else await rename(lease.temporary, destination);
        lease.committed = true;
      });
    } catch (error) {
      failed = true;
      cause = error;
    }
    try {
      await this.cleanup(lease);
    } catch (cleanup) {
      throw new RecordExportFileError("cleanup", { cause: new AggregateError([cause, cleanup]) });
    }
    // Revocation cannot recall an admitted rename. Preserve its confirmed Save result.
    if (failed && !lease.committed) throw new RecordExportFileError("storage", { cause });
  }
}
