import type { HostCommand, HostCommandResponse } from "./types";
import { record, exactKeys, text } from "./validation-primitives";
import {
  parseRecordReplayInput,
  parseRecordReplayReview,
  parseRecordReplayOutcome,
} from "./record-replay";
export function parseReplayCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
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
