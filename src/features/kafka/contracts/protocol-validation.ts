import { HOST_PROTOCOL_VERSION } from "./types";
import { HostContractValidationError } from "./validation-error";

export function parseProtocolVersion(value: unknown, path: string): typeof HOST_PROTOCOL_VERSION {
  if (value !== HOST_PROTOCOL_VERSION) {
    throw new HostContractValidationError(path, `must equal ${HOST_PROTOCOL_VERSION}`);
  }
  return HOST_PROTOCOL_VERSION;
}
