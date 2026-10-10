import { parseRepairJobSummary, REPAIR_JOB_LIMITS } from "./repair-jobs";
import type { HostCommand, HostCommandResponse } from "./types";
import { record, exactKeys, text, declaredValue } from "./validation-primitives";
import {
  parseRecordReplayInput,
  parseRecordReplayReview,
  parseRecordReplayOutcome,
} from "./record-replay";
import {
  parseRepairContinuationInput,
  parseRepairReconciliationInput,
  parseRepairArchiveInput,
  parseRepairContinuationReview,
  parseRepairFinding,
} from "./repair-recovery";
export function parseReplayCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "records.repair.review")
    return { command, id, version, payload: parseRepairContinuationInput(value) };
  if (command === "records.repair.reconcile")
    return { command, id, version, payload: parseRepairReconciliationInput(value) };
  if (command === "records.repair.archive")
    return { command, id, version, payload: parseRepairArchiveInput(value) };
  if (command === "records.repair.list") {
    exactKeys(record(value, "repairList"), [], "repairList");
    return { command, id, version, payload: {} };
  }
  if (command === "records.replay.review")
    return { command, id, version, payload: parseRecordReplayInput(value) };
  if (command === "records.replay.apply") {
    const p = record(value, "replayApply");
    exactKeys(p, ["planId", "confirmation"], "replayApply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(p.planId, "planId", 128),
        confirmation: text(p.confirmation, "confirmation", 1024),
      },
    };
  }
  if (command === "records.replay.cancel") {
    const p = record(value, "replayCancel");
    exactKeys(p, ["planId"], "replayCancel");
    return { command, id, version, payload: { planId: text(p.planId, "planId", 128) } };
  }
  return undefined;
}
export function parseReplayResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "records.repair.review") {
    exactKeys(result, ["correlationId", "continuation"], "repairReview");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        continuation: parseRepairContinuationReview(result.continuation),
      },
    };
  }
  if (command === "records.repair.reconcile") {
    exactKeys(result, ["correlationId", "finding"], "repairReconcile");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        finding: parseRepairFinding(result.finding),
      },
    };
  }
  if (command === "records.repair.list") {
    exactKeys(result, ["correlationId", "durability", "jobs"], "repairList");
    if (!Array.isArray(result.jobs) || result.jobs.length > REPAIR_JOB_LIMITS.jobs)
      throw new Error("Invalid repair history.");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        durability: declaredValue(
          result.durability,
          ["durable", "session", "unavailable"] as const,
          "durability",
        ),
        jobs: result.jobs.map(parseRepairJobSummary),
      },
    };
  }
  if (command === "records.replay.review") {
    exactKeys(result, ["correlationId", "review"], "replayResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseRecordReplayReview(result.review),
      },
    };
  }
  if (command === "records.replay.apply") {
    exactKeys(result, ["correlationId", "outcome"], "replayResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseRecordReplayOutcome(result.outcome),
      },
    };
  }
  return undefined;
}
