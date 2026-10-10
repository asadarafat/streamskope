import type { HostCommand, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { exactKeys, record, text } from "./validation-primitives";
import {
  parseTopicAdministrationInput,
  parseTopicAdministrationReview,
  parseTopicAdministrationOutcome,
} from "./topic-administration";

export function assertTopicAdministrationResponse(
  response: HostCommandResponse,
  command: HostCommand,
): void {
  if (
    response.ok &&
    response.command === "topics.change.review" &&
    command.command === "topics.change.review" &&
    JSON.stringify(response.result.review.input) !==
      JSON.stringify(parseTopicAdministrationInput(command.payload))
  )
    throw new HostContractValidationError(
      "response.result.review.input",
      "must match the submitted topic change",
    );
}

export function parseTopicAdministrationCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "topics.change.review")
    return { command, id, version, payload: parseTopicAdministrationInput(value) };
  if (command === "topics.change.apply") {
    const v = record(value, "topicApply");
    exactKeys(v, ["planId", "confirmation"], "topicApply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(v.planId, "planId", 128),
        confirmation: text(v.confirmation, "confirmation", 512),
      },
    };
  }
  return undefined;
}
export function parseTopicAdministrationResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "topics.change.review") {
    exactKeys(result, ["correlationId", "review"], "topicResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseTopicAdministrationReview(result.review),
      },
    };
  }
  if (command === "topics.change.apply") {
    exactKeys(result, ["correlationId", "outcome"], "topicResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseTopicAdministrationOutcome(result.outcome),
      },
    };
  }
  return undefined;
}
