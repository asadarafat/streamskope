import type { KafkaRecordProtection } from "./operational-preference-types";
import type { KafkaSearchFilter } from "./query-search";
import type { RecordCodecPreferences } from "./structured-record";

export const FINITE_RECORD_READ_LIMITS = Object.freeze({
  records: 100_000,
  scanRecords: 1_000_000,
  scanBytes: 1024 * 1_048_576,
  durationMs: 5 * 60_000,
  passes: 1_000,
});

export type FiniteRecordRange =
  | { readonly mode: "earliest" }
  | { readonly mode: "time-window"; readonly startTimeMs: number; readonly endTimeMs: number };

export interface FiniteRecordInput {
  readonly topic: string;
  readonly range: FiniteRecordRange;
  readonly search: KafkaSearchFilter;
  readonly maxRecords: number;
}

export interface RecordReadSettings {
  readonly codecs: RecordCodecPreferences;
  readonly protection: KafkaRecordProtection;
}

export interface RecordReadSource {
  readonly connectionName: string;
  readonly clusterId: string | null;
  readonly topicId: string | null;
}
