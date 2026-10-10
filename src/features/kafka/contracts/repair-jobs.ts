import { parseRecordReplayReview, type RecordReplayReview } from "./record-replay";
import { parseKafkaWriteOutcome, type KafkaWriteOutcome } from "./reviewed-writes";
import { parseRepairFinding, type RepairFinding } from "./repair-recovery";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

export const REPAIR_JOB_LIMITS = Object.freeze({
  jobs: 32,
  fileBytes: 4 * 1_048_576,
  findings: 128,
});
export interface RepairJob {
  readonly id: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly review: RecordReplayReview;
  readonly outcomes: readonly KafkaWriteOutcome[];
  readonly pendingIndex: number | null;
  readonly status: "running" | "stopped" | "complete";
  readonly cleanup: "pending" | "complete" | "unavailable";
  readonly revision: number;
  readonly parentJobId: string | null;
  readonly continuationId: string | null;
  readonly findings: readonly RepairFinding[];
}
export interface RepairJournalDocument {
  readonly schemaVersion: 2;
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
  readonly revision: number;
  readonly parentJobId: string | null;
  readonly continuationId: string | null;
  readonly findings: readonly RepairFinding[];
  readonly targetProfile: RecordReplayReview["input"]["targetProfile"];
  readonly canContinue: boolean;
  readonly canArchive: boolean;
}
export function parseRepairJournalDocument(value: unknown): RepairJournalDocument {
  const d = record(value, "repairJournal");
  exactKeys(d, ["schemaVersion", "jobs"], "repairJournal");
  if (
    (d.schemaVersion !== 1 && d.schemaVersion !== 2) ||
    !Array.isArray(d.jobs) ||
    d.jobs.length > REPAIR_JOB_LIMITS.jobs
  )
    throw new Error("Unsupported or oversized repair journal.");
  const jobs = d.jobs.map((value: unknown): RepairJob => {
    const j = record(value, "repairJob");
    exactKeys(
      j,
      [
        "id",
        "createdAt",
        "updatedAt",
        "review",
        "outcomes",
        "pendingIndex",
        "status",
        "cleanup",
        ...(d.schemaVersion === 2 ? ["revision", "parentJobId", "continuationId", "findings"] : []),
      ],
      "repairJob",
    );
    const review = parseRecordReplayReview(j.review);
    if (!Array.isArray(j.outcomes) || j.outcomes.length > review.batch.records.length)
      throw new Error("Invalid repair receipts.");
    const outcomes = j.outcomes.map(parseKafkaWriteOutcome);
    if (
      review.input.records.length !== review.batch.records.length ||
      review.input.topic !== review.batch.topic ||
      review.input.partition !== review.batch.partition
    )
      throw new Error("Repair inputs differ from the reviewed batch.");
    const findings =
      d.schemaVersion === 1 ? [] : parseFindings(j.findings, review.batch.records.length);
    const revision = d.schemaVersion === 1 ? 1 : journalRevision(j.revision);
    const parentJobId =
      d.schemaVersion === 1 || j.parentJobId === null
        ? null
        : text(j.parentJobId, "parentJobId", 128);
    const continuationId =
      d.schemaVersion === 1 || j.continuationId === null
        ? null
        : text(j.continuationId, "continuationId", 128);
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
      revision,
      parentJobId,
      continuationId,
      findings,
    };
  });
  if (new Set(jobs.map((j) => j.id)).size !== jobs.length) throw new Error("Duplicate repair job.");
  validateRepairLinks(jobs);
  const document = { schemaVersion: 2 as const, jobs };
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
    revision: job.revision,
    parentJobId: job.parentJobId,
    continuationId: job.continuationId,
    findings: structuredClone(job.findings),
    targetProfile: structuredClone(job.review.input.targetProfile),
    canContinue:
      job.continuationId === null &&
      job.review.batch.records.length > job.outcomes.length + (job.pendingIndex === null ? 0 : 1),
    canArchive: false,
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
      "revision",
      "parentJobId",
      "continuationId",
      "findings",
      "targetProfile",
      "canContinue",
      "canArchive",
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
  if (
    (flag(s.canContinue) &&
      (s.continuationId !== null || unsent === 0 || s.status === "running")) ||
    (flag(s.canArchive) &&
      (s.parentJobId !== null ||
        uncertainIndex !== null ||
        s.status === "running" ||
        outcomes.some((o) => o.state === "unknown")))
  )
    throw new Error("Inconsistent repair action capability.");
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
    revision: journalRevision(s.revision),
    parentJobId: s.parentJobId === null ? null : text(s.parentJobId, "parentJobId", 128),
    continuationId:
      s.continuationId === null ? null : text(s.continuationId, "continuationId", 128),
    findings: parseFindings(s.findings, total),
    targetProfile: parseTargetProfile(s.targetProfile),
    canContinue: flag(s.canContinue),
    canArchive: flag(s.canArchive),
  };
}

function journalRevision(value: unknown): number {
  const revision = nonNegativeInteger(value, "revision");
  if (revision < 1) throw new Error("Invalid repair revision.");
  return revision;
}
function flag(value: unknown): boolean {
  if (typeof value !== "boolean") throw new Error("Invalid repair action capability.");
  return value;
}
function parseTargetProfile(value: unknown): RepairJobSummary["targetProfile"] {
  if (value === null) return null;
  const p = record(value, "targetProfile");
  exactKeys(p, ["id", "revision"], "targetProfile");
  return { id: text(p.id, "id", 128), revision: journalRevision(p.revision) };
}
function parseFindings(value: unknown, total: number): readonly RepairFinding[] {
  if (!Array.isArray(value) || value.length > REPAIR_JOB_LIMITS.findings)
    throw new Error("Repair observation history exceeds its bound.");
  const findings = value.map(parseRepairFinding);
  if (
    findings.some((f) => f.recordIndex >= total) ||
    new Set(findings.map((f) => f.id)).size !== findings.length
  )
    throw new Error("Invalid repair observation identity.");
  return findings;
}
function validateRepairLinks(jobs: readonly RepairJob[]): void {
  const byId = new Map(jobs.map((j) => [j.id, j]));
  for (const job of jobs) {
    if (
      (job.parentJobId !== null && byId.get(job.parentJobId)?.continuationId !== job.id) ||
      (job.continuationId !== null && byId.get(job.continuationId)?.parentJobId !== job.id)
    )
      throw new Error("Broken repair continuation link.");
    const visited = new Set<string>();
    let current: RepairJob | undefined = job;
    while (current !== undefined) {
      if (visited.has(current.id)) throw new Error("Cyclic repair continuation link.");
      visited.add(current.id);
      current = current.parentJobId === null ? undefined : byId.get(current.parentJobId);
    }
  }
}
