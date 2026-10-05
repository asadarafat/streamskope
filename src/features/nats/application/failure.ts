import type { NatsSafeFailure } from "../contracts";

import { NatsProfileError } from "./profile-types";

/** Trusted provider-owned failure; SDK text is never a presentation message. */
export class NatsOperationError extends Error {
  readonly failure: NatsSafeFailure;
  constructor(failure: NatsSafeFailure, options?: ErrorOptions) {
    super(failure.summary, options);
    this.name = "NatsOperationError";
    this.failure = { ...failure };
  }
}
export function natsCancelled(): NatsOperationError {
  return new NatsOperationError({
    code: "cancelled",
    summary: "The NATS operation was cancelled.",
  });
}
export function natsCleanupFailure(cause?: unknown): NatsOperationError {
  return new NatsOperationError(
    {
      code: "cleanup",
      summary: "NATS resource cleanup could not be confirmed.",
      recovery: "Restart StreamSkope before starting another subscription.",
    },
    { cause },
  );
}

export function safeNatsOperationFailure(error: unknown): NatsSafeFailure {
  if (error instanceof NatsOperationError) return { ...error.failure };
  if (error instanceof NatsProfileError)
    return {
      code: error.code,
      summary: error.summary,
      ...(error.recovery === undefined ? {} : { recovery: error.recovery }),
    };
  return {
    code: "unavailable",
    summary: "The NATS operation could not be completed.",
    recovery: "Check the connection and retry after cleanup completes.",
  };
}
