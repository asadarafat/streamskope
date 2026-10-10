import { parseRecordReplayReview, type RecordReplayReview } from "./record-replay";
import { parseKafkaWriteOutcome, type KafkaWriteOutcome } from "./reviewed-writes";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

export const REPAIR_JOB_LIMITS = Object.freeze({ jobs: 32, fileBytes: 4 * 1_048_576 });
export interface RepairJob {
  readonly id: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly review: RecordReplayReview;
  readonly outcomes: readonly KafkaWriteOutcome[];
  readonly pendingIndex: number | null;
  readonly status: "running" | "stopped" | "complete";
  readonly cleanup: "pending" | "complete" | "unavailable";
}
export interface RepairJournalDocument {
  readonly schemaVersion: 1;
  readonly jobs: readonly RepairJob[];
}
/** Public metadata deliberately excludes exact reviewed payloads and credentials. */
export interface RepairJobSummary {
  readonly id: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly targetName: string;
  readonly topic: string;
  readonly partition: number;
  readonly status: "running" | "interrupted" | "stopped" | "complete";
  readonly cleanup: "pending" | "complete" | "unavailable";
  readonly total: number;
  readonly unsent: number;
  readonly outcomes: readonly KafkaWriteOutcome[];
  readonly uncertainIndex: number | null;
}
export function parseRepairJournalDocument(value: unknown): RepairJournalDocument {
  const d = record(value, "repairJournal");
  exactKeys(d, ["schemaVersion", "jobs"], "repairJournal");
  if (d.schemaVersion !== 1 || !Array.isArray(d.jobs) || d.jobs.length > REPAIR_JOB_LIMITS.jobs)
    throw new Error("Unsupported or oversized repair journal.");
  const jobs = d.jobs.map((value: unknown): RepairJob => {
    const j = record(value, "repairJob");
    exactKeys(
      j,
      ["id", "createdAt", "updatedAt", "review", "outcomes", "pendingIndex", "status", "cleanup"],
      "repairJob",
    );
    const review = parseRecordReplayReview(j.review);
    if (!Array.isArray(j.outcomes) || j.outcomes.length > review.batch.records.length)
      throw new Error("Invalid repair receipts.");
    const outcomes = j.outcomes.map(parseKafkaWriteOutcome);
    const pendingIndex =
      j.pendingIndex === null ? null : nonNegativeInteger(j.pendingIndex, "pendingIndex");
    const status = declaredValue(j.status, ["running", "stopped", "complete"] as const, "status");
    if (
      (pendingIndex !== null &&
        (pendingIndex !== outcomes.length || pendingIndex >= review.batch.records.length)) ||
      (status === "complete" &&
        (pendingIndex !== null ||
          outcomes.length !== review.batch.records.length ||
          outcomes.some((o) => o.state !== "acknowledged")))
    )
      throw new Error("Inconsistent repair journal.");
    const id = text(j.id, "id", 128);
    if (id !== review.planId)
      throw new Error("Repair job identity differs from its reviewed plan.");
    const createdAt = text(j.createdAt, "createdAt", 64),
      updatedAt = text(j.updatedAt, "updatedAt", 64);
    if (!Number.isFinite(Date.parse(createdAt)) || !Number.isFinite(Date.parse(updatedAt)))
      throw new Error("Invalid repair timestamps.");
    return {
      id,
      createdAt,
      updatedAt,
      review,
      outcomes,
      pendingIndex,
      status,
      cleanup: declaredValue(j.cleanup, ["pending", "complete", "unavailable"] as const, "cleanup"),
    };
  });
  if (new Set(jobs.map((j) => j.id)).size !== jobs.length) throw new Error("Duplicate repair job.");
  const document = { schemaVersion: 1 as const, jobs };
  if (new TextEncoder().encode(JSON.stringify(document)).length > REPAIR_JOB_LIMITS.fileBytes)
    throw new Error("Repair journal exceeds its protected storage bound.");
  return document;
}
export function summarizeRepairJob(job: RepairJob): RepairJobSummary {
  return {
    id: job.id,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    targetName: job.review.targetName,
    topic: job.review.input.topic,
    partition: job.review.input.partition,
    status: job.status === "running" ? "interrupted" : job.status,
    cleanup: job.cleanup,
    total: job.review.batch.records.length,
    unsent:
      job.review.batch.records.length - job.outcomes.length - (job.pendingIndex === null ? 0 : 1),
    outcomes: structuredClone(job.outcomes),
    uncertainIndex: job.pendingIndex,
  };
}
export function parseRepairJobSummary(value: unknown): RepairJobSummary {
  const s = record(value, "repairSummary");
  exactKeys(
    s,
    [
      "id",
      "createdAt",
      "updatedAt",
      "targetName",
      "topic",
      "partition",
      "status",
      "cleanup",
      "total",
      "unsent",
      "outcomes",
      "uncertainIndex",
    ],
    "repairSummary",
  );
  if (!Array.isArray(s.outcomes) || s.outcomes.length > 50)
    throw new Error("Invalid repair summary.");
  const outcomes = s.outcomes.map(parseKafkaWriteOutcome),
    total = nonNegativeInteger(s.total, "total"),
    unsent = nonNegativeInteger(s.unsent, "unsent"),
    uncertainIndex =
      s.uncertainIndex === null ? null : nonNegativeInteger(s.uncertainIndex, "uncertainIndex");
  if (
    total < 1 ||
    total > 50 ||
    unsent + outcomes.length + (uncertainIndex === null ? 0 : 1) !== total ||
    (uncertainIndex !== null && uncertainIndex !== outcomes.length)
  )
    throw new Error("Inconsistent repair summary.");
  return {
    id: text(s.id, "id", 128),
    createdAt: text(s.createdAt, "createdAt", 64),
    updatedAt: text(s.updatedAt, "updatedAt", 64),
    targetName: text(s.targetName, "targetName", 256),
    topic: text(s.topic, "topic", 249),
    partition: nonNegativeInteger(s.partition, "partition"),
    status: declaredValue(
      s.status,
      ["running", "interrupted", "stopped", "complete"] as const,
      "status",
    ),
    cleanup: declaredValue(s.cleanup, ["pending", "complete", "unavailable"] as const, "cleanup"),
    total,
    unsent,
    outcomes,
    uncertainIndex,
  };
}
