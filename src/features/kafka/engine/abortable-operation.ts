import { observeAbortableOperation } from "../../../platform/providers/operation-ownership";

import { normalizeKafkaError } from "./failure";

/** Kafka retains its error policy while sharing observed cancellation mechanics. */
export function abortableOperation<T>(
  operation: Promise<T>,
  signal: AbortSignal,
  createAbortError: () => Error,
): Promise<T> {
  return observeAbortableOperation(operation, signal, createAbortError, normalizeKafkaError);
}
