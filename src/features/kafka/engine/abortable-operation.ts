import { normalizeKafkaError } from "./failure";

/** Observe the started driver promise even when cancellation has already won. */
export function abortableOperation<T>(
  operation: Promise<T>,
  signal: AbortSignal,
  createAbortError: () => Error,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      signal.removeEventListener("abort", onAbort);
      reject(createAbortError());
    };
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(normalizeKafkaError(error));
      },
    );
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  });
}
