import {
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  type HostError,
} from "../../../src/features/kafka/contracts";
import { HostContractValidationError } from "../../../src/features/kafka/contracts/validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  optionalText,
  record,
  text,
  truth,
} from "../../../src/features/kafka/contracts/validation-primitives";

import {
  EDA_CAPTURE_COMMANDS,
  EDA_PROTOCOL_VERSION,
  type EdaCaptureCommand,
  type EdaCaptureCommandResponse,
  type EdaCaptureHostEvent,
} from "./types";
import {
  parseEdaCaptureHostCommand,
  parseEdaCaptureHostEvent,
  parseEdaCaptureHostResponse,
} from "./eda-capture-protocol";
function parseHostError(value: unknown, path: string): HostError {
  const error = record(value, path);
  exactKeys(
    error,
    [
      "activeStateChanged",
      "code",
      "correlationId",
      "recovery",
      "retryable",
      "stage",
      "summary",
      "target",
    ],
    path,
  );
  const target = optionalText(error, "target", path, 2_048);
  const base = {
    activeStateChanged: truth(error.activeStateChanged, `${path}.activeStateChanged`),
    code: declaredValue(error.code, HOST_ERROR_CODES, `${path}.code`),
    correlationId: text(error.correlationId, `${path}.correlationId`, 128),
    recovery: text(error.recovery, `${path}.recovery`, 2_048),
    retryable: truth(error.retryable, `${path}.retryable`),
    stage: declaredValue(error.stage, HOST_ERROR_STAGES, `${path}.stage`),
    summary: text(error.summary, `${path}.summary`, 2_048),
  };
  return target === undefined ? base : { ...base, target };
}

function version(value: unknown, path: string): 1 {
  if (value !== EDA_PROTOCOL_VERSION)
    throw new HostContractValidationError(path, "unsupported EDA plugin protocol version");
  return EDA_PROTOCOL_VERSION;
}
export function parseEdaCaptureCommand(value: unknown): EdaCaptureCommand {
  const input = record(value, "command");
  exactKeys(input, ["command", "id", "payload", "version"], "command");
  const name = declaredValue(input.command, EDA_CAPTURE_COMMANDS, "command.command");
  const command = parseEdaCaptureHostCommand(
    name,
    text(input.id, "command.id", 128),
    input.payload,
    version(input.version, "command.version"),
  );
  if (command === undefined)
    throw new HostContractValidationError("command.command", "unknown EDA command");
  return command;
}
export function parseEdaCaptureResponse(value: unknown): EdaCaptureCommandResponse {
  const input = record(value, "response");
  const ok = truth(input.ok, "response.ok");
  exactKeys(input, ["command", "id", "version", "ok", ok ? "result" : "error"], "response");
  const command = declaredValue(input.command, EDA_CAPTURE_COMMANDS, "response.command");
  const id = text(input.id, "response.id", 128);
  const protocol = version(input.version, "response.version");
  if (!ok)
    return {
      command,
      id,
      version: protocol,
      ok: false,
      error: parseHostError(input.error, "response.error"),
    };
  if (command === "edaCapture.cancel") {
    const result = record(input.result, "response.result");
    exactKeys(result, ["correlationId"], "response.result");
    return {
      command,
      id,
      version: protocol,
      ok: true,
      result: { correlationId: text(result.correlationId, "response.result.correlationId", 128) },
    };
  }
  const response = parseEdaCaptureHostResponse(command, id, input.result, protocol);
  if (response === undefined)
    throw new HostContractValidationError("response.command", "unknown EDA response");
  return response;
}
export function parseEdaCaptureEvent(value: unknown): EdaCaptureHostEvent {
  const input = record(value, "event");
  exactKeys(input, ["event", "payload", "sequence", "version"], "event");
  const event = parseEdaCaptureHostEvent(
    declaredValue(input.event, ["edaCapture.progress"], "event.event"),
    input.payload,
    nonNegativeInteger(input.sequence, "event.sequence"),
    version(input.version, "event.version"),
  );
  if (event === undefined)
    throw new HostContractValidationError("event.event", "unknown EDA event");
  return event;
}
export {
  parseEdaCaptureCommand as parseHostCommand,
  parseEdaCaptureResponse as parseHostCommandResponse,
  parseEdaCaptureEvent as parseHostEvent,
};

export { parseEdaCaptureResponse as parseEdaCaptureCommandResponse };
