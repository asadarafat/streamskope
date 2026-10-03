import { parseAclReviewCommand, parseAclReviewResponse } from "./acl-review-protocol";
import { parseReplayCommand, parseReplayResponse } from "./replay-protocol";
import type { HostCommand, HostCommandResponse } from "./types";
import { record, exactKeys, text } from "./validation-primitives";
import {
  parseOffsetResetInput,
  parseOffsetResetReview,
  parseOffsetResetOutcome,
} from "./offset-reset";

function parseOffsetResetCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "consumerGroups.reset.review")
    return { command, id, version, payload: parseOffsetResetInput(value) };
  if (command === "consumerGroups.reset.apply") {
    const p = record(value, "resetApply");
    exactKeys(p, ["planId", "confirmation"], "resetApply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(p.planId, "planId", 128),
        confirmation: text(p.confirmation, "confirmation", 512),
      },
    };
  }
  return undefined;
}
function parseOffsetResetResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "consumerGroups.reset.review") {
    exactKeys(result, ["correlationId", "review"], "resetResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseOffsetResetReview(result.review),
      },
    };
  }
  if (command === "consumerGroups.reset.apply") {
    exactKeys(result, ["correlationId", "outcome"], "resetResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseOffsetResetOutcome(result.outcome),
      },
    };
  }
  return undefined;
}

export function parseRecoveryCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  return (
    parseOffsetResetCommand(command, id, value, version) ??
    parseReplayCommand(command, id, value, version) ??
    parseAclReviewCommand(command, id, value, version)
  );
}
export function parseRecoveryResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  return (
    parseOffsetResetResponse(command, id, result, version) ??
    parseReplayResponse(command, id, result, version) ??
    parseAclReviewResponse(command, id, result, version)
  );
}
