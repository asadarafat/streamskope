import {
  REMOTE_TRUST_ACQUISITION_LIMITS,
  type RemoteSshEndpointInput,
  type AcceptedSshIdentity,
} from "./remote-trust-types";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export interface RemoteSshAccess extends RemoteSshEndpointInput {
  readonly username: string;
  readonly authentication: "password" | "private-key" | "agent";
}

export function parseRemoteSshHostKeyFingerprint(value: unknown, path: string): string {
  const fingerprint = text(value, path, REMOTE_TRUST_ACQUISITION_LIMITS.fingerprintCharacters);
  if (!/^SHA256:[A-Za-z0-9+/]{43}$/u.test(fingerprint))
    throw new HostContractValidationError(path, "must be an exact SHA256 OpenSSH fingerprint");
  return fingerprint;
}

export function parseAcceptedSshIdentity(value: unknown, path: string): AcceptedSshIdentity {
  const input = record(value, path);
  exactKeys(input, ["host", "port", "fingerprint"], path);
  return {
    ...parseRemoteSshEndpoint({ host: input.host, port: input.port }, path),
    fingerprint: parseRemoteSshHostKeyFingerprint(input.fingerprint, `${path}.fingerprint`),
  };
}

export function parseRemoteSshEndpoint(value: unknown, path: string): RemoteSshEndpointInput {
  const target = record(value, path);
  exactKeys(target, ["host", "port"], path);
  const host = text(target.host, `${path}.host`, REMOTE_TRUST_ACQUISITION_LIMITS.hostCharacters);
  if (host.trim() !== host || /[\s\p{Cc}]/u.test(host)) {
    throw new HostContractValidationError(
      `${path}.host`,
      "must not contain whitespace or control characters",
    );
  }
  return {
    host,
    port: positiveBoundedInteger(
      target.port,
      `${path}.port`,
      REMOTE_TRUST_ACQUISITION_LIMITS.portMaximum,
    ),
  };
}

export function parseRemoteSshUsername(value: unknown, path: string): string {
  const username = text(value, path, REMOTE_TRUST_ACQUISITION_LIMITS.usernameCharacters);
  if (username.trim() !== username || /[\s\p{Cc}]/u.test(username))
    throw new HostContractValidationError(
      path,
      "must not contain whitespace or control characters",
    );
  return username;
}

export function parseRemoteSshAccess(value: unknown, path: string): RemoteSshAccess {
  const input = record(value, path);
  exactKeys(input, ["host", "port", "username", "authentication"], path);
  return {
    ...parseRemoteSshEndpoint({ host: input.host, port: input.port }, path),
    username: parseRemoteSshUsername(input.username, `${path}.username`),
    authentication: declaredValue(
      input.authentication,
      ["password", "private-key", "agent"],
      `${path}.authentication`,
    ),
  };
}
