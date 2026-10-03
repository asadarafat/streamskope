import type { HostCommand, HostCommandResponse } from "./types";
import { record, exactKeys, text } from "./validation-primitives";
import {
  parseAclChangeInput,
  parseAclChangeReview,
  parseTopicAccessInput,
  parseTopicAccessExplanation,
} from "./acl-review";
import { parseKafkaWriteOutcome } from "./reviewed-writes";

export function parseAclReviewCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "acls.access.explain")
    return { command, id, version, payload: parseTopicAccessInput(value) };
  if (command === "acls.change.review")
    return { command, id, version, payload: parseAclChangeInput(value) };
  if (command === "acls.change.apply") {
    const p = record(value, "aclApply");
    exactKeys(p, ["planId", "confirmation"], "aclApply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(p.planId, "planId", 128),
        confirmation: text(p.confirmation, "confirmation", 8192),
      },
    };
  }
  return undefined;
}
export function parseAclReviewResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "acls.access.explain") {
    exactKeys(result, ["correlationId", "explanation"], "aclResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        explanation: parseTopicAccessExplanation(result.explanation),
      },
    };
  }
  if (command === "acls.change.review") {
    exactKeys(result, ["correlationId", "review"], "aclResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseAclChangeReview(result.review),
      },
    };
  }
  if (command === "acls.change.apply") {
    exactKeys(result, ["correlationId", "outcome"], "aclResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseKafkaWriteOutcome(result.outcome),
      },
    };
  }
  return undefined;
}
