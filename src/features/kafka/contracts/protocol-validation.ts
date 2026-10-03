import { HOST_PROTOCOL_VERSION, type HostEvent } from "./types";
import {
  declaredValue,
  exactKeys,
  optionalText,
  type UnknownRecord,
} from "./validation-primitives";
import { HostContractValidationError } from "./validation-error";

export function parseProtocolVersion(value: unknown, path: string): typeof HOST_PROTOCOL_VERSION {
  if (value !== HOST_PROTOCOL_VERSION) {
    throw new HostContractValidationError(path, `must equal ${HOST_PROTOCOL_VERSION}`);
  }
  return HOST_PROTOCOL_VERSION;
}

export function parseBackendAvailability(
  payload: UnknownRecord,
): Extract<HostEvent, { event: "backend.availability" }>["payload"] {
  exactKeys(payload, ["recovery", "state"], "event.payload");
  const recovery = optionalText(payload, "recovery", "event.payload", 2048);
  return {
    state: declaredValue(payload.state, ["ready", "unavailable"], "event.payload.state"),
    ...(recovery === undefined ? {} : { recovery }),
  };
}
