import { observeAbortableOperation } from "../../../platform/providers/operation-ownership";

import { natsEngineFailure, normalizeNatsEngineFailure } from "./failure";

/** A waiting deadline never removes the actual operation from its owner's barrier. */
export async function waitForNatsOperation<T>(
  operation: Promise<T>,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<T> {
  const deadline = new AbortController();
  const timer = setTimeout(() => deadline.abort(), timeoutMs);
  try {
    return await observeAbortableOperation(
      operation,
      signal === undefined ? deadline.signal : AbortSignal.any([signal, deadline.signal]),
      () => natsEngineFailure(signal?.aborted === true ? "cancelled" : "timeout"),
      normalizeNatsEngineFailure,
    );
  } finally {
    clearTimeout(timer);
  }
}
