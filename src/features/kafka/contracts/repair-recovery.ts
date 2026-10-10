import { offsetPosition } from "./offset-reset";
import { parseRecordReplayReview, type RecordReplayReview } from "./record-replay";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export interface RepairFinding {
  readonly id: string;
  readonly recordIndex: number;
  readonly offset: string;
  readonly observedAt: string;
  readonly state: "equivalent" | "different" | "not-observed" | "unavailable";
  readonly cleanup: "complete" | "unavailable";
}
export interface RepairContinuationInput {
  readonly jobId: string;
  readonly targetProfile: { readonly id: string; readonly revision: number } | null;
}
export interface RepairReconciliationInput extends RepairContinuationInput {
  readonly recordIndex: number;
  readonly offset: string;
}
export interface RepairContinuationReview {
  readonly parentJobId: string;
  readonly skipped: {
    readonly acknowledged: number;
    readonly rejected: number;
    readonly uncertain: number;
  };
  readonly review: RecordReplayReview;
}
export interface RepairArchiveInput {
  readonly jobId: string;
  readonly confirmation: string;
  readonly chain: readonly { readonly id: string; readonly revision: number }[];
}
function target(value: unknown): RepairContinuationInput["targetProfile"] {
  if (value === null) return null;
  const p = record(value, "targetProfile");
  exactKeys(p, ["id", "revision"], "targetProfile");
  return {
    id: text(p.id, "id", 128),
    revision: positiveBoundedInteger(p.revision, "revision", Number.MAX_SAFE_INTEGER),
  };
}
export function parseRepairContinuationInput(value: unknown): RepairContinuationInput {
  const p = record(value, "repairContinuation");
  exactKeys(p, ["jobId", "targetProfile"], "repairContinuation");
  return { jobId: text(p.jobId, "jobId", 128), targetProfile: target(p.targetProfile) };
}
export function parseRepairReconciliationInput(value: unknown): RepairReconciliationInput {
  const p = record(value, "repairReconciliation");
  exactKeys(p, ["jobId", "targetProfile", "recordIndex", "offset"], "repairReconciliation");
  const recordIndex = nonNegativeInteger(p.recordIndex, "recordIndex");
  if (recordIndex >= 50) throw new Error("Repair record index exceeds the supported bound.");
  return {
    jobId: text(p.jobId, "jobId", 128),
    targetProfile: target(p.targetProfile),
    recordIndex,
    offset: offsetPosition(p.offset, "offset"),
  };
}
export function parseRepairFinding(value: unknown): RepairFinding {
  const p = record(value, "repairFinding");
  exactKeys(p, ["id", "recordIndex", "offset", "observedAt", "state", "cleanup"], "repairFinding");
  const recordIndex = nonNegativeInteger(p.recordIndex, "recordIndex"),
    observedAt = text(p.observedAt, "observedAt", 64);
  if (recordIndex >= 50 || !Number.isFinite(Date.parse(observedAt)))
    throw new Error("Invalid repair observation.");
  return {
    id: text(p.id, "id", 128),
    recordIndex,
    offset: offsetPosition(p.offset, "offset"),
    observedAt,
    state: declaredValue(
      p.state,
      ["equivalent", "different", "not-observed", "unavailable"] as const,
      "state",
    ),
    cleanup: declaredValue(p.cleanup, ["complete", "unavailable"] as const, "cleanup"),
  };
}
export function parseRepairContinuationReview(value: unknown): RepairContinuationReview {
  const p = record(value, "repairContinuationReview");
  exactKeys(p, ["parentJobId", "skipped", "review"], "repairContinuationReview");
  const s = record(p.skipped, "skipped");
  exactKeys(s, ["acknowledged", "rejected", "uncertain"], "skipped");
  const skipped = {
      acknowledged: nonNegativeInteger(s.acknowledged, "acknowledged"),
      rejected: nonNegativeInteger(s.rejected, "rejected"),
      uncertain: nonNegativeInteger(s.uncertain, "uncertain"),
    },
    review = parseRecordReplayReview(p.review);
  if (
    skipped.acknowledged + skipped.rejected + skipped.uncertain + review.batch.records.length >
    50
  )
    throw new Error("Inconsistent repair continuation counts.");
  return { parentJobId: text(p.parentJobId, "parentJobId", 128), skipped, review };
}
export function parseRepairArchiveInput(value: unknown): RepairArchiveInput {
  const p = record(value, "repairArchive");
  exactKeys(p, ["jobId", "confirmation", "chain"], "repairArchive");
  if (!Array.isArray(p.chain) || p.chain.length < 1 || p.chain.length > 32)
    throw new Error("Invalid repair archive chain.");
  const chain = p.chain.map((value: unknown) => {
    const row = record(value, "chainItem");
    exactKeys(row, ["id", "revision"], "chainItem");
    return {
      id: text(row.id, "id", 128),
      revision: positiveBoundedInteger(row.revision, "revision", Number.MAX_SAFE_INTEGER),
    };
  });
  if (new Set(chain.map((row) => row.id)).size !== chain.length)
    throw new Error("Duplicate repair archive job.");
  return {
    jobId: text(p.jobId, "jobId", 128),
    confirmation: text(p.confirmation, "confirmation", 128),
    chain,
  };
}
