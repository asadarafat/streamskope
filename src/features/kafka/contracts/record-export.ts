import type { HostError } from "./host-errors";
import type { KafkaRecordProtection } from "./operational-preference-types";
import type { KafkaReadCoverage, KafkaSearchFilter } from "./query-search";
import type { RecordCodecPreferences } from "./structured-record";

export const RECORD_EXPORT_LIMITS = Object.freeze({
  records: 100_000,
  scanRecords: 1_000_000,
  scanBytes: 1024 * 1_048_576,
  bytes: 256 * 1_048_576,
  durationMs: 5 * 60_000,
  passes: 1_000,
  artifactLifetimeMs: 15 * 60_000,
  receiptBytes: 1_048_576,
  downloads: 2,
  downloadDurationMs: 5 * 60_000,
});
export type RecordExportLimits = { readonly [Key in keyof typeof RECORD_EXPORT_LIMITS]: number };
export type RecordExportFormat = "csv" | "jsonl";
export type RecordExportRange =
  | { readonly mode: "earliest" }
  | { readonly mode: "time-window"; readonly startTimeMs: number; readonly endTimeMs: number };
export interface RecordExportInput {
  readonly requestId: string;
  readonly topic: string;
  readonly range: RecordExportRange;
  readonly search: KafkaSearchFilter;
  readonly format: RecordExportFormat;
  readonly maxRecords: number;
}
export interface RecordExportSettings {
  readonly codecs: RecordCodecPreferences;
  readonly protection: KafkaRecordProtection;
}
export interface RecordExportSource {
  readonly connectionName: string;
  readonly clusterId: string | null;
  readonly topicId: string | null;
}
export interface RecordExportCounts {
  readonly passes: number;
  readonly scannedRecords: number;
  readonly scannedBytes: number;
  readonly writtenRecords: number;
  readonly writtenBytes: number;
  readonly unavailableRecords: number;
  readonly decodeErrorRecords: number;
  readonly originalUnavailableRecords: number;
}
export const RECORD_EXPORT_REASONS = [
  "range-complete",
  "records-unavailable",
  "record-limit",
  "byte-limit",
  "scan-limit",
  "deadline",
  "pass-limit",
  "cancelled",
  "checkpoint-unavailable",
  "read-failed",
  "storage-failed",
  "cleanup-failed",
  "revoked",
] as const;
export type RecordExportReason = (typeof RECORD_EXPORT_REASONS)[number];
export const RECORD_EXPORT_STATES = [
  "preparing",
  "reading",
  "stopping",
  "completed",
  "partial",
  "failed",
  "expired",
] as const;
export type RecordExportState = (typeof RECORD_EXPORT_STATES)[number];
export interface RecordExportOutput {
  readonly format: RecordExportFormat;
  readonly fileName: string;
  readonly bytes: number;
  readonly sha256: string;
}
export interface RecordExportArtifact {
  readonly artifactId: string;
  readonly output: RecordExportOutput;
  readonly receiptBytes: number;
  readonly receiptSha256: string;
  readonly expiresAt: string;
}
export interface RecordExportOperation {
  readonly jobId: string;
  readonly state: RecordExportState;
  readonly input: RecordExportInput;
  readonly source: RecordExportSource;
  readonly settings: RecordExportSettings;
  readonly limits: RecordExportLimits;
  readonly startedAt: string;
  readonly completedAt: string | null;
  readonly counts: RecordExportCounts;
  readonly coverage: KafkaReadCoverage | null;
  readonly reason: RecordExportReason | null;
  readonly artifact: RecordExportArtifact | null;
  readonly error: HostError | null;
}
export interface RecordExportSnapshot {
  readonly scopeId: string;
  readonly revision: number;
  readonly available: boolean;
  readonly operation: RecordExportOperation | null;
}
export interface RecordExportReceiptDetails {
  readonly limits: RecordExportLimits;
  readonly jobId: string;
  readonly input: RecordExportInput;
  readonly source: RecordExportSource;
  readonly settings: RecordExportSettings;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly outcome: "complete" | "partial";
  readonly reason: RecordExportReason;
  readonly counts: RecordExportCounts;
  readonly coverage: KafkaReadCoverage | null;
}
export interface RecordExportReceipt extends RecordExportReceiptDetails {
  readonly schema: "streamskope.record-export/v1";
  readonly output: RecordExportOutput;
}
