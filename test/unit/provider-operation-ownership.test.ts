import { describe, expect, it } from "vitest";

import { observeAbortableOperation } from "../../src/platform/providers/operation-ownership";

describe("shared observation retains real rejection ownership", () => {
  it("settles the caller if provider failure normalization itself throws", async () => {
    const signal = new AbortController().signal;
    const normalizationFailure = new Error("normalization failed");
    await expect(
      observeAbortableOperation(
        Promise.reject(new Error("driver failure")),
        signal,
        () => new Error("cancelled"),
        () => {
          throw normalizationFailure;
        },
      ),
    ).rejects.toBe(normalizationFailure);
  });
  it("settles asynchronous cancellation if its failure factory throws and still observes late driver rejection", async () => {
    const cancellation = new AbortController();
    let failDriver = (_error: Error): void => undefined;
    const operation = new Promise<never>((_resolve, reject) => {
      failDriver = reject;
    });
    const factoryFailure = new Error("factory failed");
    const observed = observeAbortableOperation(
      operation,
      cancellation.signal,
      () => {
        throw factoryFailure;
      },
      (error) => (error instanceof Error ? error : new Error("driver failed")),
    );
    const assertion = expect(observed).rejects.toBe(factoryFailure);
    cancellation.abort();
    failDriver(new Error("late driver failure"));
    await assertion;
  });
});
