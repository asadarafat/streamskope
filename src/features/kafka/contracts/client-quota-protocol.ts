import type { HostCommand, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { record, exactKeys, text } from "./validation-primitives";
import {
  parseClientQuotaEntity,
  parseClientQuotaInput,
  parseClientQuotaSnapshot,
  parseClientQuotaReview,
  parseClientQuotaOutcome,
  sameClientQuotaEntity,
} from "./client-quotas";

export function assertClientQuotaResponse(
  response: HostCommandResponse,
  command: HostCommand,
): void {
  if (!response.ok) return;
  if (
    response.command === "quotas.inspect" &&
    command.command === "quotas.inspect" &&
    !sameClientQuotaEntity(
      response.result.snapshot.entity,
      parseClientQuotaEntity(command.payload.entity),
    )
  )
    throw new HostContractValidationError(
      "snapshot.entity",
      "must match the submitted exact quota entity",
    );
  if (
    response.command === "quotas.change.review" &&
    command.command === "quotas.change.review" &&
    JSON.stringify(response.result.review.input) !==
      JSON.stringify(parseClientQuotaInput(command.payload))
  )
    throw new HostContractValidationError(
      "review.input",
      "must match the submitted exact quota changes",
    );
}
export function parseClientQuotaCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "quotas.inspect") {
    const v = record(value, "quotaInspect");
    exactKeys(v, ["entity"], "quotaInspect");
    return { command, id, version, payload: { entity: parseClientQuotaEntity(v.entity) } };
  }
  if (command === "quotas.change.review")
    return { command, id, version, payload: parseClientQuotaInput(value) };
  if (command === "quotas.change.apply") {
    const v = record(value, "quotaApply");
    exactKeys(v, ["planId", "confirmation"], "quotaApply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(v.planId, "planId", 128),
        confirmation: text(v.confirmation, "confirmation", 1024),
      },
    };
  }
  return undefined;
}
export function parseClientQuotaResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "quotas.inspect") {
    exactKeys(result, ["correlationId", "snapshot"], "quotaResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        snapshot: parseClientQuotaSnapshot(result.snapshot),
      },
    };
  }
  if (command === "quotas.change.review") {
    exactKeys(result, ["correlationId", "review"], "quotaResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseClientQuotaReview(result.review),
      },
    };
  }
  if (command === "quotas.change.apply") {
    exactKeys(result, ["correlationId", "outcome"], "quotaResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseClientQuotaOutcome(result.outcome),
      },
    };
  }
  return undefined;
}
