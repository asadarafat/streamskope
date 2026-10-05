import { describe, expect, it, vi } from "vitest";

import {
  FeatureLifecycle,
  type FeatureLifecycleParticipant,
} from "../../src/features/kafka/facade/feature-lifecycle";

function deferred(): { readonly promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

describe("feature lifecycle coordination", () => {
  it("invalidates synchronously in registration order without waiting for asynchronous cleanup", async () => {
    const cleanup = deferred();
    const calls: string[] = [];
    const drain = vi.fn(() => undefined);
    const lifecycle = new FeatureLifecycle([
      {
        owner: "First",
        invalidate: (reason): Promise<void> => {
          calls.push(`first:${reason}`);
          return cleanup.promise;
        },
        drain,
      },
      {
        owner: "Second",
        invalidate: (reason): void => {
          calls.push(`second:${reason}`);
        },
      },
    ]);

    expect(lifecycle.invalidate()).toBeUndefined();
    expect(calls).toEqual(["first:connection-change", "second:connection-change"]);
    expect(drain).not.toHaveBeenCalled();
    cleanup.resolve();
    await cleanup.promise;
  });

  it("invalidates every owner before reporting safe synchronous failures and handles detached rejections", async () => {
    const rejected = Promise.reject(new Error("private-asynchronous-cancellation-token"));
    const last = vi.fn(() => undefined);
    const lifecycle = new FeatureLifecycle([
      {
        owner: "Throwing",
        invalidate: (): never => {
          throw new Error("private-synchronous-cancellation-token");
        },
      },
      { owner: "Rejecting", invalidate: (): Promise<void> => rejected },
      { owner: "Last", invalidate: last },
    ]);

    let failure: unknown;
    try {
      lifecycle.invalidate();
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(AggregateError);
    if (!(failure instanceof AggregateError))
      throw new Error("Expected aggregate invalidation failure.");
    expect(failure.message).toBe("Kafka connection state invalidation failed.");
    expect(failure.errors.map((error: Error) => error.message)).toEqual([
      "Throwing invalidation failed.",
    ]);
    expect(failure.cause).toBeUndefined();
    expect(failure.errors.every((error: Error) => error.cause === undefined)).toBe(true);
    expect(failure.errors.map(String).join(" ")).not.toContain("private-");
    expect(last).toHaveBeenCalledExactlyOnceWith("connection-change");
    // Crossing a turn also exposes an unhandled detached rejection to the test runner.
    await new Promise<void>((resolve) => setImmediate(resolve));
  });

  it("starts every invalidation before any drain, even when an owner throws synchronously", async () => {
    const calls: string[] = [];
    const lifecycle = new FeatureLifecycle([
      {
        owner: "Throwing",
        invalidate: (reason): never => {
          calls.push(`throwing:${reason}`);
          throw new Error("private-cancellation-token");
        },
        drain: (): void => {
          calls.push("throwing:drain");
        },
      },
      {
        owner: "Other",
        invalidate: (reason): void => {
          calls.push(`other:${reason}`);
        },
        drain: (): void => {
          calls.push("other:drain");
        },
      },
    ]);

    const shutdown = lifecycle.shutdown();
    expect(calls).toEqual(["throwing:shutdown", "other:shutdown", "throwing:drain", "other:drain"]);
    await expect(shutdown).rejects.toMatchObject({
      message: "Kafka application resources did not close cleanly.",
      errors: [expect.objectContaining({ message: "Throwing cleanup failed." })],
    });
  });

  it("waits for pending invalidation and drain work after an earlier rejection", async () => {
    const invalidation = deferred();
    const drain = deferred();
    const settled = vi.fn();
    const lifecycle = new FeatureLifecycle([
      {
        owner: "Rejecting",
        invalidate: (): Promise<void> => Promise.reject(new Error("private-rejection-token")),
      },
      { owner: "Invalidating", invalidate: (): Promise<void> => invalidation.promise },
      { owner: "Draining", drain: (): Promise<void> => drain.promise },
    ]);
    const result = lifecycle.shutdown().then(
      () => {
        settled();
        return undefined;
      },
      (error: unknown) => {
        settled();
        return error;
      },
    );

    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(settled).not.toHaveBeenCalled();
    invalidation.resolve();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(settled).not.toHaveBeenCalled();
    drain.resolve();
    const failure = await result;
    expect(failure).toBeInstanceOf(AggregateError);
    expect(settled).toHaveBeenCalledOnce();
  });

  it("supports owners with only invalidation or only draining and leaves shutdown policy to callbacks", async () => {
    const invalidation = vi.fn(() => undefined);
    const drain = vi.fn(() => undefined);
    const consumption = vi.fn((reason: string) => {
      if (reason === "connection-change") invalidation();
    });
    const lifecycle = new FeatureLifecycle([
      { owner: "Consumption", invalidate: consumption },
      { owner: "Cache", invalidate: invalidation },
      { owner: "Session", drain },
    ]);

    await expect(lifecycle.shutdown()).resolves.toBeUndefined();
    expect(consumption).toHaveBeenCalledExactlyOnceWith("shutdown");
    expect(invalidation).toHaveBeenCalledExactlyOnceWith("shutdown");
    expect(drain).toHaveBeenCalledExactlyOnceWith();
  });

  it("reports only safe owner messages for synchronous and asynchronous failures", async () => {
    const lifecycle = new FeatureLifecycle([
      {
        owner: "Throwing",
        invalidate: (): never => {
          throw new Error("private-synchronous-token");
        },
      },
      {
        owner: "Rejecting",
        drain: (): Promise<void> => Promise.reject(new Error("private-asynchronous-token")),
      },
      {
        owner: "Both",
        invalidate: (): Promise<void> => Promise.reject(new Error("private-invalidation-token")),
        drain: (): never => {
          throw new Error("private-drain-token");
        },
      },
    ]);

    const failure: unknown = await lifecycle.shutdown().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    if (!(failure instanceof AggregateError))
      throw new Error("Expected aggregate cleanup failure.");
    expect(failure.message).toBe("Kafka application resources did not close cleanly.");
    expect(failure.errors.map((error: Error) => error.message)).toEqual([
      "Throwing cleanup failed.",
      "Both cleanup failed.",
      "Rejecting cleanup failed.",
      "Both cleanup failed.",
    ]);
    expect(failure.cause).toBeUndefined();
    expect(failure.errors.every((error: Error) => error.cause === undefined)).toBe(true);
    expect(JSON.stringify(failure)).not.toContain("private-");
    expect(String(failure)).not.toContain("private-");
    expect(failure.errors.map(String).join(" ")).not.toContain("private-");
  });

  it("keeps a fixed snapshot of its registrations", async () => {
    const invalidate = vi.fn(() => undefined);
    const replacement = vi.fn(() => undefined);
    const participant = { owner: "Original", invalidate };
    const participants: FeatureLifecycleParticipant[] = [participant];
    const lifecycle = new FeatureLifecycle(participants);
    participant.owner = "Changed";
    participant.invalidate = replacement;
    participants.push({ owner: "Added", invalidate: replacement });

    await lifecycle.shutdown();
    expect(invalidate).toHaveBeenCalledExactlyOnceWith("shutdown");
    expect(replacement).not.toHaveBeenCalled();
  });
});
