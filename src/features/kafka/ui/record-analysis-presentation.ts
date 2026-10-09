import type {
  RecordAnalysisCell,
  RecordAnalysisOperation,
  RecordAnalysisReason,
} from "../contracts/record-analysis";

const unavailable = {
  "not-captured": "Original field was not captured",
  "decoding-error": "Decoding failed",
  bytes: "Bytes encoding has no scalar projection",
  "not-json": "Nested paths require decoded JSON",
  object: "Unsupported: object",
  array: "Unsupported: array",
  "sample-limit": "Field exceeds evaluation limits",
  "value-limit": "Value exceeds the scalar byte limit",
} as const;
export function analysisCell(cell: RecordAnalysisCell): {
  readonly type: string;
  readonly text: string;
} {
  if (cell.state === "scalar")
    return {
      type: cell.value === null ? "JSON null" : typeof cell.value,
      text: JSON.stringify(cell.value),
    };
  if (cell.state === "missing") return { type: "Missing", text: "Missing path" };
  if (cell.state === "null-key") return { type: "Null key", text: "Null key" };
  if (cell.state === "tombstone") return { type: "Tombstone", text: "Tombstone" };
  if (cell.state === "masked") return { type: "Masked", text: "Masked" };
  if (cell.state === "unavailable") return { type: "Unavailable", text: unavailable[cell.reason] };
  throw new Error("Unsupported analysis cell state.");
}
export const ANALYSIS_REASONS: Record<RecordAnalysisReason, string> = {
  "range-complete": "The captured offset ranges were scanned completely.",
  "records-unavailable":
    "Some records could not be evaluated by the filter; the match count is partial.",
  "record-limit": "The matching-record limit was reached.",
  "scan-limit": "The record or byte scan limit was reached.",
  deadline: "The analysis time limit was reached.",
  "pass-limit": "The read-pass limit was reached.",
  cancelled: "Analysis stopped after the confirmed records were counted.",
  "checkpoint-unavailable": "The host could not continue the captured range.",
  "group-limit": "The distinct-group limit was reached; the next record was not counted.",
  "group-key-limit": "A grouping value exceeded its byte limit; the next record was not counted.",
  "result-byte-limit": "The result byte limit was reached; the next record was not counted.",
  "work-limit": "The evaluation work limit was reached; the next record was not counted.",
  "read-failed": "The Kafka read failed.",
  "analysis-failed": "The host could not finish the analysis.",
  "cleanup-failed":
    "Reader cleanup is unresolved. Follow the recovery message before starting again.",
  revoked: "Connection or host authority changed. Retained analysis data was cleared.",
};
export function analysisCountLabel(operation: RecordAnalysisOperation): string {
  const count = operation.counts.countedRecords.toLocaleString();
  if (operation.state === "completed")
    return `Count for captured range complete: ${count} matching records.`;
  if (operation.state === "partial") return `Partial count: ${count} matching records counted.`;
  if (operation.state === "failed" || operation.state === "revoked")
    return `Analysis ${operation.state}; no complete result is available.`;
  return `${count} matching records counted so far.`;
}
