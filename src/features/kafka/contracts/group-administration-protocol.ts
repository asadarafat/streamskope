import type { HostCommand, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { exactKeys, record, text } from "./validation-primitives";
import {
  parseGroupAdministrationInput,
  parseGroupAdministrationReview,
  parseGroupAdministrationOutcome,
} from "./group-administration";
import { parseOffsetResetRequest } from "./offset-reset";

export function assertGroupAdministrationResponse(
  response: HostCommandResponse,
  command: HostCommand,
): void {
  if (!response.ok) return;
  if (
    response.command === "consumerGroups.delete.review" &&
    command.command === "consumerGroups.delete.review" &&
    response.result.review.baseline.groupId !== command.payload.groupId
  )
    throw new HostContractValidationError(
      "review.baseline.groupId",
      "must match the submitted group deletion",
    );
  if (
    response.command === "consumerGroups.reset.review" &&
    command.command === "consumerGroups.reset.review"
  ) {
    const submitted = parseOffsetResetRequest(command.payload),
      review = response.result.review;
    const actual = "position" in submitted ? review.selection : review.input;
    if (JSON.stringify(actual) !== JSON.stringify(submitted))
      throw new HostContractValidationError(
        "review.input",
        "must match the submitted partitions and reset selector",
      );
  }
}
export function parseGroupAdministrationCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "consumerGroups.delete.review")
    return { command, id, version, payload: parseGroupAdministrationInput(value) };
  if (command === "consumerGroups.delete.apply") {
    const v = record(value, "groupApply");
    exactKeys(v, ["planId", "confirmation"], "groupApply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(v.planId, "planId", 128),
        confirmation: text(v.confirmation, "confirmation", 524),
      },
    };
  }
  return undefined;
}
export function parseGroupAdministrationResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "consumerGroups.delete.review") {
    exactKeys(result, ["correlationId", "review"], "groupResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseGroupAdministrationReview(result.review),
      },
    };
  }
  if (command === "consumerGroups.delete.apply") {
    exactKeys(result, ["correlationId", "outcome"], "groupResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseGroupAdministrationOutcome(result.outcome),
      },
    };
  }
  return undefined;
}
