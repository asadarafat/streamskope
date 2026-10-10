import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import {
  parseSchemaChangeInput,
  parseSchemaChangeOutcome,
  parseSchemaChangeReview,
  type SchemaChangeInput,
} from "./schema-changes";
import { exactKeys, record, text } from "./validation-primitives";
export type SchemaChangeCommand = HostCommandBase &
  (
    | { readonly command: "schemas.change.review"; readonly payload: SchemaChangeInput }
    | {
        readonly command: "schemas.change.apply";
        readonly payload: { readonly planId: string; readonly confirmation: string };
      }
  );
export function parseSchemaChangeCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "schemas.change.review")
    return { command, id, version, payload: parseSchemaChangeInput(value) };
  if (command === "schemas.change.apply") {
    const payload = record(value, "payload");
    exactKeys(payload, ["planId", "confirmation"], "payload");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(payload.planId, "payload.planId", 128),
        confirmation: text(payload.confirmation, "payload.confirmation", 512),
      },
    };
  }
  return undefined;
}
export function parseSchemaChangeResponse(
  command: HostCommand["command"],
  id: string,
  value: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "schemas.change.review") {
    exactKeys(value, ["correlationId", "review"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(value.correlationId, "correlationId", 128),
        review: parseSchemaChangeReview(value.review),
      },
    };
  }
  if (command === "schemas.change.apply") {
    exactKeys(value, ["correlationId", "outcome"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(value.correlationId, "correlationId", 128),
        outcome: parseSchemaChangeOutcome(value.outcome),
      },
    };
  }
  return undefined;
}
