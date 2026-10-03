import type { HostCommand, HostCommandResponse } from "./types";
import { record, exactKeys, text, emptyRecord } from "./validation-primitives";
import {
  connectName,
  parseConnectInput,
  parseConnectInventory,
  parseConnectDetail,
  parseConnectValidation,
  parseConnectReview,
  parseConnectOutcome,
} from "./connect";
export function parseConnectCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "connect.list")
    return { command, id, version, payload: emptyRecord(value, "payload") };
  if (command === "connect.load") {
    const p = record(value, "payload");
    exactKeys(p, ["name"], "payload");
    return { command, id, version, payload: { name: connectName(p.name) } };
  }
  if (command === "connect.validate" || command === "connect.review")
    return { command, id, version, payload: parseConnectInput(value) };
  if (command === "connect.apply") {
    const p = record(value, "payload");
    exactKeys(p, ["planId", "confirmation"], "payload");
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
export function parseConnectResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (!command.startsWith("connect.")) return undefined;
  const correlationId = text(result.correlationId, "correlationId", 128);
  switch (command) {
    case "connect.list":
      exactKeys(result, ["correlationId", "inventory"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, inventory: parseConnectInventory(result.inventory) },
      };
    case "connect.load":
      exactKeys(result, ["correlationId", "detail"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, detail: parseConnectDetail(result.detail) },
      };
    case "connect.validate":
      exactKeys(result, ["correlationId", "validation"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, validation: parseConnectValidation(result.validation) },
      };
    case "connect.review":
      exactKeys(result, ["correlationId", "review"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, review: parseConnectReview(result.review) },
      };
    case "connect.apply":
      exactKeys(result, ["correlationId", "outcome"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, outcome: parseConnectOutcome(result.outcome) },
      };
    default:
      return undefined;
  }
}
