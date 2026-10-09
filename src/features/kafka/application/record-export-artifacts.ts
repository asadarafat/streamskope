import type {
  RecordExportArtifact,
  RecordExportFormat,
  RecordExportReceiptDetails,
} from "../contracts/record-export";

/** Resolves writes only after one complete encoded row is accepted. A failed write poisons sealing. */
export interface RecordExportSink {
  write(row: Uint8Array): Promise<void>;
  seal(receipt: RecordExportReceiptDetails): Promise<RecordExportArtifact>;
  discard(): Promise<void>;
}

/** Application authority over one transient output; no filesystem or delivery details escape. */
export interface RecordExportArtifacts {
  create(input: {
    readonly jobId: string;
    readonly format: RecordExportFormat;
    readonly maximumBytes: number;
    readonly lifetimeMs: number;
    readonly signal: AbortSignal;
    readonly assertCurrent: () => void;
  }): Promise<RecordExportSink>;
  /** Immediately fences old delivery/build admission and starts owned cleanup. */
  revoke(): void;
  /** Waits for admitted work/cleanup and preserves unconfirmed failures. */
  drain(): Promise<void>;
}
