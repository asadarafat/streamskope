import type { HostCommand, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { sameKafkaRecordLocator } from "./record-locator";
import { assertTopicCatalogResponse } from "./topic-catalog-protocol";
import { assertTopicAdministrationResponse } from "./topic-administration-protocol";
import { assertGroupAdministrationResponse } from "./group-administration-protocol";
import { assertClientQuotaResponse } from "./client-quota-protocol";
import { assertConnectResponse } from "./connect-protocol";

/** Bind parsed replies to the submitted operation and its exact reviewed scope. */
export function assertHostResponseCorrelation(
  response: HostCommandResponse,
  command: HostCommand,
): void {
  if (response.id !== command.id || response.command !== command.command) {
    throw new HostContractValidationError(
      "response",
      "must match the submitted command identifier and name",
    );
  }
  if (
    response.ok &&
    response.command === "records.locator.load" &&
    command.command === "records.locator.load" &&
    (response.result.outcome.requestId !== command.payload.requestId ||
      !sameKafkaRecordLocator(response.result.outcome.locator, command.payload.locator))
  )
    throw new HostContractValidationError(
      "response.result.outcome",
      "must match the submitted record reload and saved position",
    );
  if (
    response.ok &&
    response.command === "records.locator.cancel" &&
    command.command === "records.locator.cancel" &&
    response.result.requestId !== command.payload.requestId
  )
    throw new HostContractValidationError(
      "response.result.requestId",
      "must match the submitted record cancellation",
    );
  assertTopicCatalogResponse(response, command);
  assertTopicAdministrationResponse(response, command);
  assertGroupAdministrationResponse(response, command);
  assertClientQuotaResponse(response, command);
  assertConnectResponse(response, command);
}
