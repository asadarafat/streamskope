import { REMOTE_TRUST_ACQUISITION_LIMITS } from "./remote-trust-types";
import { HTTPS_TRUST_LIMITS } from "./https-trust-types";
import { parseHttpsUrl } from "./https-trust-validation";
import { boundedText, declaredValue, exactKeys, record } from "./validation-primitives";
import { HostContractValidationError } from "./validation-error";

/** Safe preferences only. API authentication secrets and CA bytes are never part of a binding. */
export interface HttpsProfileAccess {
  readonly host: string;
  readonly username: string;
  readonly tls: "system" | "custom";
}

export function parseHttpsProfileAccess(value: unknown, path: string): HttpsProfileAccess {
  const input = record(value, path);
  exactKeys(input, ["host", "username", "tls"], path);
  const host = boundedText(
    input.host,
    `${path}.host`,
    REMOTE_TRUST_ACQUISITION_LIMITS.hostCharacters,
  );
  if (host !== "") {
    if (/[/@?#\\\s\p{Cc}]/u.test(host))
      throw new HostContractValidationError(
        `${path}.host`,
        "must be a hostname or IP address, not a URL",
      );
    parseHttpsUrl(
      `https://${host.includes(":") && !host.startsWith("[") ? `[${host}]` : host}`,
      `${path}.host`,
    );
  }
  const username = boundedText(
    input.username,
    `${path}.username`,
    HTTPS_TRUST_LIMITS.valueCharacters,
  );
  if (/[:\p{Cc}]/u.test(username))
    throw new HostContractValidationError(
      `${path}.username`,
      "must not contain colons or control characters",
    );
  return { host, username, tls: declaredValue(input.tls, ["system", "custom"], `${path}.tls`) };
}
