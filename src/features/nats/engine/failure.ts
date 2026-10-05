import {
  AuthorizationError,
  ClosedConnectionError,
  ConnectionError,
  PermissionViolationError,
  TimeoutError,
  UserAuthenticationExpiredError,
} from "@nats-io/transport-node";

import type { NatsFailureCode, NatsSafeFailure } from "../contracts";
import { NatsOperationError } from "../application/failure";

const failures = {
  authentication: {
    code: "authentication",
    summary: "NATS rejected the supplied authentication.",
    recovery: "Verify the profile token and the server authentication policy.",
  },
  tls: {
    code: "tls",
    summary: "NATS certificate verification failed.",
    recovery: "Verify the server hostname and the CA that issued its certificate.",
  },
  permission: {
    code: "permission",
    summary: "NATS denied the requested subscription.",
    recovery: "Verify that the profile is allowed to subscribe to this subject.",
  },
  connection: {
    code: "connection",
    summary: "The NATS connection is unavailable.",
    recovery: "Check the server address and connection, then reconnect explicitly.",
  },
  timeout: {
    code: "timeout",
    summary: "NATS did not confirm the operation before its deadline.",
    recovery: "Check the server connection, then retry after cleanup completes.",
  },
  cancelled: {
    code: "cancelled",
    summary: "The NATS operation was cancelled.",
  },
  cleanup: {
    code: "cleanup",
    summary: "NATS resource cleanup could not be confirmed.",
    recovery: "Restart StreamSkope before starting another subscription.",
  },
  "not-connected": {
    code: "not-connected",
    summary: "Connect a NATS profile before starting a subscription.",
  },
  unavailable: {
    code: "unavailable",
    summary: "The NATS engine is unavailable.",
    recovery: "Restart StreamSkope before starting another subscription.",
  },
  validation: {
    code: "validation",
    summary: "The NATS connection or subject is invalid.",
  },
} satisfies Partial<Record<NatsFailureCode, NatsSafeFailure>>;

/** Causes remain host-owned; only the safe failure object crosses presentation. */
export class NatsEngineFailure extends NatsOperationError {
  constructor(failure: NatsSafeFailure, cause?: unknown) {
    super(failure, { cause });
    this.name = "NatsEngineFailure";
  }
}

export function natsEngineFailure(code: keyof typeof failures, cause?: unknown): NatsEngineFailure {
  return new NatsEngineFailure(failures[code], cause);
}

const tlsCodes = new Set([
  "CERT_HAS_EXPIRED",
  "CERT_NOT_YET_VALID",
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "ERR_TLS_CERT_ALTNAME_INVALID",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "ERR_TLS_CERT_SIGNATURE_ALGORITHM_UNSUPPORTED",
  "ERR_SSL_WRONG_VERSION_NUMBER",
]);

/** Classify supported SDK errors and finite Node TLS codes, never server text. */
export function normalizeNatsEngineFailure(error: unknown): NatsEngineFailure {
  try {
    return classifyNatsEngineFailure(error);
  } catch {
    // Exotic external error properties must not throw from an SDK callback.
    return natsEngineFailure("connection", error);
  }
}

function classifyNatsEngineFailure(error: unknown): NatsEngineFailure {
  if (error instanceof NatsEngineFailure) return error;
  const pending: unknown[] = [error];
  const seen = new Set<unknown>();
  while (pending.length > 0 && seen.size < 16) {
    const entry = pending.shift();
    if (entry === undefined || seen.has(entry)) continue;
    seen.add(entry);
    if (entry instanceof PermissionViolationError) return natsEngineFailure("permission", error);
    if (entry instanceof AuthorizationError || entry instanceof UserAuthenticationExpiredError)
      return natsEngineFailure("authentication", error);
    if (entry instanceof TimeoutError) return natsEngineFailure("timeout", error);
    if (entry !== null && typeof entry === "object") {
      if ("code" in entry && typeof entry.code === "string" && tlsCodes.has(entry.code))
        return natsEngineFailure("tls", error);
      if ("cause" in entry) pending.push(entry.cause);
    }
  }
  return natsEngineFailure("connection", error);
}

export function safeNatsFailure(error: unknown): NatsSafeFailure {
  return { ...normalizeNatsEngineFailure(error).failure };
}

export function subscriptionClosureFailure(error: Error | void): NatsEngineFailure | undefined {
  if (error === undefined) return undefined;
  if (error instanceof PermissionViolationError) return normalizeNatsEngineFailure(error);
  if (error instanceof ClosedConnectionError || error instanceof ConnectionError)
    return natsEngineFailure("connection", error);
  return normalizeNatsEngineFailure(error);
}
