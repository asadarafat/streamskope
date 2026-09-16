import { ConnectionAttemptSupersededError } from "./session-errors";
import type { KafkaActiveConnection } from "./types";

export async function closeCancelledConnection(
  connection: KafkaActiveConnection,
  signal: AbortSignal,
): Promise<void> {
  if (!signal.aborted) return;
  try {
    await connection.close();
  } catch (cleanup) {
    throw new AggregateError([signal.reason, cleanup], "Cancelled connection cleanup failed.", {
      cause: cleanup,
    });
  }
  signal.throwIfAborted();
}

export function abortSignals(
  ownedSignal: AbortSignal,
  externalSignal: AbortSignal | undefined,
): AbortSignal {
  return externalSignal === undefined
    ? ownedSignal
    : AbortSignal.any([ownedSignal, externalSignal]);
}

export function cleanupFailures(
  results: readonly PromiseSettledResult<unknown>[],
): readonly unknown[] {
  return results.flatMap((result) =>
    result.status === "rejected" &&
    result.reason instanceof ConnectionAttemptSupersededError &&
    result.reason.cleanupFailure !== undefined
      ? [result.reason.cleanupFailure]
      : [],
  );
}

export function ownedCleanupFailure(error: unknown): unknown {
  return error !== null && typeof error === "object" && "cleanupCause" in error
    ? error.cleanupCause
    : undefined;
}
