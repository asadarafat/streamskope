import { parseRecordReadId } from "./finite-record-validation";
import { utf8ByteLength } from "./message-limits";
import { parseKafkaMessage } from "./message-validation";
import {
  KAFKA_RECORD_LOCATOR_LIMITS,
  KAFKA_RECORD_LOCATOR_REASONS,
  kafkaRecordLocator,
  parseKafkaRecordLocator,
  parseKafkaRecordLocatorLoadInput,
  sameKafkaRecordLocator,
  type KafkaRecordLocatorLoadInput,
  type KafkaRecordLocatorOutcome,
} from "./record-locator";
import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { declaredValue, exactKeys, record, text } from "./validation-primitives";

export type RecordLocatorCommand = HostCommandBase &
  (
    | { readonly command: "records.locator.load"; readonly payload: KafkaRecordLocatorLoadInput }
    | {
        readonly command: "records.locator.cancel";
        readonly payload: { readonly requestId: string };
      }
  );
export interface RecordLocatorResults {
  readonly "records.locator.load": {
    readonly correlationId: string;
    readonly outcome: KafkaRecordLocatorOutcome;
  };
  readonly "records.locator.cancel": {
    readonly correlationId: string;
    readonly requestId: string;
    readonly stopped: true;
  };
}
export function parseKafkaRecordLocatorOutcome(value: unknown): KafkaRecordLocatorOutcome {
  const input = record(value, "locatorOutcome");
  const state = declaredValue(
    input.state,
    ["loaded", ...KAFKA_RECORD_LOCATOR_REASONS],
    "locatorOutcome.state",
  );
  exactKeys(
    input,
    ["requestId", "locator", "state", state === "loaded" ? "message" : "detail"],
    "locatorOutcome",
  );
  if (utf8ByteLength(JSON.stringify(value)) > KAFKA_RECORD_LOCATOR_LIMITS.responseBytes)
    throw new HostContractValidationError(
      "locatorOutcome",
      "exceeds the bounded record response size",
    );
  const common = {
    requestId: parseRecordReadId(input.requestId, "locatorOutcome.requestId"),
    locator: parseKafkaRecordLocator(input.locator, "locatorOutcome.locator"),
  };
  if (state !== "loaded")
    return { ...common, state, detail: text(input.detail, "locatorOutcome.detail", 512) };
  const message = parseKafkaMessage(input.message, "locatorOutcome.message");
  const actual = kafkaRecordLocator(message);
  if (actual === null || !sameKafkaRecordLocator(actual, common.locator))
    throw new HostContractValidationError(
      "locatorOutcome.message",
      "loaded record must match its verified saved position and history",
    );
  return { ...common, state, message };
}
export function parseRecordLocatorCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "records.locator.load")
    return { command, id, version, payload: parseKafkaRecordLocatorLoadInput(value) };
  if (command !== "records.locator.cancel") return undefined;
  const payload = record(value, "locatorCancel");
  exactKeys(payload, ["requestId"], "locatorCancel");
  return {
    command,
    id,
    version,
    payload: { requestId: parseRecordReadId(payload.requestId, "locatorCancel.requestId") },
  };
}
export function parseRecordLocatorResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command === "records.locator.load") {
    exactKeys(result, ["correlationId", "outcome"], "locatorResult");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "locatorResult.correlationId", 128),
        outcome: parseKafkaRecordLocatorOutcome(result.outcome),
      },
    };
  }
  if (command !== "records.locator.cancel") return undefined;
  exactKeys(result, ["correlationId", "requestId", "stopped"], "locatorCancelResult");
  if (result.stopped !== true)
    throw new HostContractValidationError(
      "locatorCancelResult.stopped",
      "requires confirmed cleanup",
    );
  return {
    command,
    id,
    version,
    ok: true,
    result: {
      correlationId: text(result.correlationId, "locatorCancelResult.correlationId", 128),
      requestId: parseRecordReadId(result.requestId, "locatorCancelResult.requestId"),
      stopped: true,
    },
  };
}
