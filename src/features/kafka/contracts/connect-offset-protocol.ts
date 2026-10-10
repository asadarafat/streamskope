import type { HostCommand, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { record, exactKeys, text } from "./validation-primitives";
import { connectName } from "./connect";
import {
  parseConnectOffsetsInput,
  parseConnectOffsetsSnapshot,
  parseConnectOffsetsReview,
  parseConnectOffsetsOutcome,
} from "./connect-offsets";

export function parseConnectOffsetsCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "connect.offsets.review")
    return { command, id, version, payload: parseConnectOffsetsInput(value) };
  if (command === "connect.offsets.inspect") {
    const p = record(value, "payload");
    exactKeys(p, ["name"], "payload");
    return { command, id, version, payload: { name: connectName(p.name) } };
  }
  if (command === "connect.offsets.apply") {
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
export function parseConnectOffsetsResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (!command.startsWith("connect.offsets.")) return undefined;
  const correlationId = text(result.correlationId, "correlationId", 128);
  switch (command) {
    case "connect.offsets.inspect":
      exactKeys(result, ["correlationId", "snapshot"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, snapshot: parseConnectOffsetsSnapshot(result.snapshot) },
      };
    case "connect.offsets.review":
      exactKeys(result, ["correlationId", "review"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, review: parseConnectOffsetsReview(result.review) },
      };
    case "connect.offsets.apply":
      exactKeys(result, ["correlationId", "outcome"], "result");
      return {
        command,
        id,
        version,
        ok: true,
        result: { correlationId, outcome: parseConnectOffsetsOutcome(result.outcome) },
      };
    default:
      return undefined;
  }
}
export function assertConnectOffsetsResponse(
  response: HostCommandResponse,
  command: HostCommand,
): void {
  if (!response.ok) return;
  if (
    response.command === "connect.offsets.apply" &&
    command.command === "connect.offsets.apply" &&
    (response.result.outcome.planId !== command.payload.planId ||
      response.result.outcome.confirmation !== command.payload.confirmation)
  )
    throw new HostContractValidationError(
      "outcome",
      "must retain the original plan and confirmation",
    );
  if (
    response.command === "connect.offsets.inspect" &&
    command.command === "connect.offsets.inspect" &&
    response.result.snapshot.name !== command.payload.name
  )
    throw new HostContractValidationError("snapshot.name", "must match the requested connector");
  if (
    response.command === "connect.offsets.review" &&
    command.command === "connect.offsets.review" &&
    JSON.stringify(response.result.review.input) !==
      JSON.stringify(parseConnectOffsetsInput(command.payload))
  )
    throw new HostContractValidationError(
      "review.input",
      "must match the exact submitted selection",
    );
  if (
    response.command === "connect.offsets.apply" &&
    command.command === "connect.offsets.apply" &&
    response.result.outcome.observed !== null &&
    `${command.payload.confirmation.split(" ")[0]} OFFSETS ${response.result.outcome.observed.name}` !==
      command.payload.confirmation
  )
    throw new HostContractValidationError(
      "outcome.observed.name",
      "must match the confirmed connector",
    );
}
