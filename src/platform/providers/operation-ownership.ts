/** Owns admitted commands until their complete facade response has settled. */
export class ProviderCommandAdmission {
  private acceptingExternal = true;
  private readonly operations = new Set<Promise<unknown>>();

  accepts(internal: boolean): boolean {
    return internal || this.acceptingExternal;
  }

  close(): void {
    this.acceptingExternal = false;
  }

  track<Value>(run: () => Promise<Value>): Promise<Value> {
    let resolve!: (value: Value) => void;
    let reject!: (reason: unknown) => void;
    const operation = new Promise<Value>((complete, fail) => {
      resolve = complete;
      reject = fail;
    });
    // Publish ownership before dispatch, which can synchronously notify subscribers.
    this.operations.add(operation);
    operation.then(
      () => this.operations.delete(operation),
      () => this.operations.delete(operation),
    );
    try {
      run().then(resolve, reject);
    } catch (error) {
      reject(error);
    }
    return operation;
  }

  async idle(): Promise<void> {
    while (this.operations.size > 0) await Promise.allSettled([...this.operations]);
  }
}

/** Serializes one owner's mutations and lets later work proceed after a rejected operation. */
export class SerialMutationQueue {
  private tail: Promise<void> = Promise.resolve();

  enqueue<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const result = this.tail.then(() => {
      signal?.throwIfAborted();
      return operation();
    });
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

/** Observe real work before evaluating cancellation; normalization stays provider-owned. */
export function observeAbortableOperation<T>(
  operation: Promise<T>,
  signal: AbortSignal,
  createAbortError: () => Error,
  normalizeError: (error: unknown) => Error,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      signal.removeEventListener("abort", onAbort);
      try {
        reject(createAbortError());
      } catch (error) {
        reject(
          error instanceof Error
            ? error
            : new Error("Provider cancellation failed.", { cause: error }),
        );
      }
    };
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        try {
          reject(normalizeError(error));
        } catch (failure) {
          reject(
            failure instanceof Error
              ? failure
              : new Error("Provider failure normalization failed.", { cause: failure }),
          );
        }
      },
    );
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  });
}
