import {
  FINITE_RECORD_READ_LIMITS,
  type FiniteRecordInput,
  type RecordReadSettings,
  type RecordReadSource,
} from "./finite-record-read";
import type { HostError } from "./host-errors";
import type { KafkaReadCoverage } from "./query-search";
import { KAFKA_RULE_LIMITS } from "./rule-types";

export const RECORD_ANALYSIS_LIMITS = Object.freeze({
  ...FINITE_RECORD_READ_LIMITS,
  columns: 12,
  pathCharacters: 256,
  pathSegments: 16,
  groups: 256,
  groupKeyBytes: 1_024,
  cellBytes: 1_024,
  previewRows: 200,
  previewBytes: 256 * 1_024,
  resultBytes: 384 * 1_024,
  work: 50_000_000,
  recordWork: KAFKA_RULE_LIMITS.evaluationWork,
});
export type RecordAnalysisLimits = {
  readonly [Key in keyof typeof RECORD_ANALYSIS_LIMITS]: number;
};

export interface RecordAnalysisColumn {
  readonly id: string;
  readonly label: string;
  readonly source: "key" | "value";
  readonly path: string;
}

export interface RecordAnalysisInput extends FiniteRecordInput {
  readonly requestId: string;
  readonly columns: readonly RecordAnalysisColumn[];
  readonly groupBy: string | null;
}

export const RECORD_ANALYSIS_UNAVAILABLE = [
  "not-captured",
  "decoding-error",
  "bytes",
  "not-json",
  "object",
  "array",
  "sample-limit",
  "value-limit",
] as const;
export type RecordAnalysisUnavailable = (typeof RECORD_ANALYSIS_UNAVAILABLE)[number];
export type RecordAnalysisGroupKey =
  | { readonly state: "scalar"; readonly value: string | number | boolean | null }
  | { readonly state: "missing" | "null-key" | "tombstone" };
export type RecordAnalysisCell =
  | RecordAnalysisGroupKey
  | { readonly state: "masked" }
  | { readonly state: "unavailable"; readonly reason: RecordAnalysisUnavailable };

export interface RecordAnalysisRow {
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string;
  readonly cells: readonly RecordAnalysisCell[];
}
export interface RecordAnalysisColumnCounts {
  readonly columnId: string;
  readonly scalar: number;
  readonly missing: number;
  readonly nullKey: number;
  readonly tombstone: number;
  readonly masked: number;
  readonly unavailable: number;
}
export interface RecordAnalysisGrouping {
  readonly groups: readonly { readonly key: RecordAnalysisGroupKey; readonly count: number }[];
  readonly groupedRecords: number;
  readonly excluded: { readonly masked: number; readonly unavailable: number };
}
export interface RecordAnalysisResult {
  readonly columns: readonly RecordAnalysisColumnCounts[];
  readonly grouping: RecordAnalysisGrouping | null;
  readonly preview: readonly RecordAnalysisRow[];
  readonly previewOmittedRecords: number;
  readonly previewBytes: number;
  readonly workUnits: number;
}
export interface RecordAnalysisCounts {
  readonly passes: number;
  readonly scannedRecords: number;
  readonly scannedBytes: number;
  readonly countedRecords: number;
  readonly unavailableRecords: number;
}
export const RECORD_ANALYSIS_REASONS = [
  "range-complete",
  "records-unavailable",
  "record-limit",
  "scan-limit",
  "deadline",
  "pass-limit",
  "cancelled",
  "checkpoint-unavailable",
  "group-limit",
  "group-key-limit",
  "result-byte-limit",
  "work-limit",
  "read-failed",
  "analysis-failed",
  "cleanup-failed",
  "revoked",
] as const;
export type RecordAnalysisReason = (typeof RECORD_ANALYSIS_REASONS)[number];
export type RecordAnalysisLimitReason = Extract<
  RecordAnalysisReason,
  "group-limit" | "group-key-limit" | "result-byte-limit" | "work-limit"
>;
export const RECORD_ANALYSIS_STATES = [
  "preparing",
  "reading",
  "stopping",
  "completed",
  "partial",
  "failed",
  "revoked",
] as const;
export type RecordAnalysisState = (typeof RECORD_ANALYSIS_STATES)[number];
export interface RecordAnalysisOperation {
  readonly jobId: string;
  readonly input: RecordAnalysisInput;
  readonly state: RecordAnalysisState;
  readonly source: RecordReadSource;
  readonly settings: RecordReadSettings;
  readonly limits: RecordAnalysisLimits;
  readonly startedAt: string;
  readonly completedAt: string | null;
  readonly counts: RecordAnalysisCounts;
  readonly coverage: KafkaReadCoverage | null;
  readonly reason: RecordAnalysisReason | null;
  readonly result: RecordAnalysisResult | null;
  readonly error: HostError | null;
}
export interface RecordAnalysisSnapshot {
  readonly scopeId: string;
  readonly revision: number;
  readonly operation: RecordAnalysisOperation | null;
}

/** Scalar JSON encoding keeps numeric-looking strings distinct from numbers. */
export function recordAnalysisGroupIdentity(key: RecordAnalysisGroupKey): string {
  return key.state === "scalar"
    ? JSON.stringify([key.state, key.value])
    : JSON.stringify([key.state]);
}
