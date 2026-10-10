import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import {
  parseSchemaPolicyInput,
  parseSchemaPolicyReview,
  parseSchemaPolicyOutcome,
  parseSchemaPolicyBaseline,
  type SchemaPolicyInput,
} from "./schema-policy";
import { exactKeys, record, text } from "./validation-primitives";
export type SchemaPolicyCommand = HostCommandBase &
  (
    | { readonly command: "schemas.policy.load"; readonly payload: { readonly subject: string } }
    | { readonly command: "schemas.policy.review"; readonly payload: SchemaPolicyInput }
    | {
        readonly command: "schemas.policy.apply";
        readonly payload: { readonly planId: string; readonly confirmation: string };
      }
  );
export function parseSchemaPolicyCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "schemas.policy.review")
    return { command, id, version, payload: parseSchemaPolicyInput(value) };
  if (command === "schemas.policy.load") {
    const p = record(value, "payload");
    exactKeys(p, ["subject"], "payload");
    return { command, id, version, payload: { subject: text(p.subject, "payload.subject", 512) } };
  }
  if (command === "schemas.policy.apply") {
    const p = record(value, "payload");
    exactKeys(p, ["planId", "confirmation"], "payload");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(p.planId, "payload.planId", 128),
        confirmation: text(p.confirmation, "payload.confirmation", 512),
      },
    };
  }
  return undefined;
}
export function parseSchemaPolicyResponse(
  command: HostCommand["command"],
  id: string,
  value: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "schemas.policy.load") {
    exactKeys(value, ["correlationId", "baseline"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(value.correlationId, "correlationId", 128),
        baseline: parseSchemaPolicyBaseline(value.baseline),
      },
    };
  }
  if (command === "schemas.policy.review") {
    exactKeys(value, ["correlationId", "review"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(value.correlationId, "correlationId", 128),
        review: parseSchemaPolicyReview(value.review),
      },
    };
  }
  if (command === "schemas.policy.apply") {
    exactKeys(value, ["correlationId", "outcome"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(value.correlationId, "correlationId", 128),
        outcome: parseSchemaPolicyOutcome(value.outcome),
      },
    };
  }
  return undefined;
}
