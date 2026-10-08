import { expect, it, vi } from "vitest";

import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import { KafkaProfileService } from "../../src/features/kafka/application/profile-service";
import { SavedReplayDestinations } from "../../src/features/kafka/application/replay-destination";
import type { KafkaWriteOutcome } from "../../src/features/kafka/contracts/reviewed-writes";
import { ownedCleanupFailure } from "../../src/features/kafka/application/session-lifecycle";
import {
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (error: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

it.each([false, true])(
  "awaits and identifies failed cleanup of a destination that opens after cancellation; undefined rejection: %s",
  async (undefinedRejection) => {
    const profiles = new KafkaProfileService(
      new InMemoryKafkaProfileStore({
        durability: "session",
        protection: "memory",
        state: "ready",
      }),
      {
        decode: (): Promise<never> =>
          Promise.reject(new Error("Plaintext profiles do not decode trust.")),
      },
      { createId: (): string => "destination" },
    );
    await profiles.create({
      name: "Destination",
      brokers: ["localhost:9092"],
      transport: "plaintext",
    });
    const connection = new RecordingActiveConnection();
    const opened = deferred<RecordingActiveConnection>();
    const closed = deferred<void>();
    const close = vi.spyOn(connection, "close").mockImplementation(() => closed.promise);
    const port = new RecordingConnectionPort();
    const started = deferred<void>();
    port.openOperations.push(() => {
      started.resolve();
      return opened.promise;
    });
    const controller = new AbortController();
    const operation = new SavedReplayDestinations(profiles, port).openReviewed(
      "destination",
      1,
      controller.signal,
    );
    const result = operation.catch((error: unknown): unknown => error);
    await started.promise;
    controller.abort();
    opened.resolve(connection);
    await vi.waitFor(() => expect(close).toHaveBeenCalledOnce());
    let settled = false;
    void result.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    const failure = undefinedRejection ? undefined : new Error("fixture cleanup failure");
    closed.reject(failure);
    const error = await result;
    expect(error).toBeInstanceOf(Error);
    if (failure === undefined) {
      expect(ownedCleanupFailure(error)).toBeInstanceOf(Error);
      expect(ownedCleanupFailure(error)).toHaveProperty("cause", undefined);
    } else {
      expect(ownedCleanupFailure(error)).toBe(failure);
      expect(String(error)).not.toContain(failure.message);
    }
  },
);

it("projects only reviewed authority, keeps the opening deadline separate, and revokes immediately on coalesced close", async () => {
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore({ durability: "session", protection: "memory", state: "ready" }),
    { decode: (): Promise<never> => Promise.reject(new Error("No trust decoding expected.")) },
    { createId: (): string => "destination" },
  );
  await profiles.create({
    name: "Destination",
    brokers: ["localhost:9092"],
    transport: "plaintext",
  });
  const receipt: KafkaWriteOutcome = {
    state: "acknowledged",
    detail: "Accepted",
    receipt: { topic: "events", partition: 0, offset: "1" },
    verification: "unavailable",
  };
  const send = vi.fn(() => Promise.resolve(receipt));
  const connection = Object.assign(new RecordingActiveConnection(), {
    reviewWrite: (): Promise<void> => Promise.resolve(),
    applyWrite: send,
  });
  const closed = deferred<void>();
  const close = vi.spyOn(connection, "close").mockImplementation(() => closed.promise);
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(connection));
  const controller = new AbortController();
  const target = await new SavedReplayDestinations(profiles, port).openReviewed(
    "destination",
    1,
    controller.signal,
  );
  expect(Object.keys(target).sort()).toEqual(["close", "scope"]);
  expect(Object.keys(target.scope).sort()).toEqual([
    "connectionName",
    "isCurrent",
    "reviewWrite",
    "tryDispatchWrite",
  ]);
  controller.abort(new Error("Opening deadline elapsed after successful admission."));
  expect(target.scope.isCurrent()).toBe(true);
  const first = target.close();
  const second = target.close();
  expect(second).toBe(first);
  expect(target.scope.isCurrent()).toBe(false);
  expect(
    target.scope.tryDispatchWrite!({
      kind: "topic",
      topic: "events",
      partitions: 1,
      replicationFactor: 1,
      configs: [],
    }),
  ).toEqual({ started: false });
  await vi.waitFor(() => expect(close).toHaveBeenCalledOnce());
  closed.resolve();
  await first;
  expect(send).not.toHaveBeenCalled();
});

it("revokes a reviewed destination when its saved profile disappears", async () => {
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore({ durability: "session", protection: "memory", state: "ready" }),
    { decode: (): Promise<never> => Promise.reject(new Error("No trust decoding expected.")) },
    { createId: (): string => "destination" },
  );
  await profiles.create({
    name: "Destination",
    brokers: ["localhost:9092"],
    transport: "plaintext",
  });
  const connection = new RecordingActiveConnection();
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(connection));
  const target = await new SavedReplayDestinations(profiles, port).openReviewed(
    "destination",
    1,
    new AbortController().signal,
  );
  expect(target.scope.isCurrent()).toBe(true);
  await profiles.delete("destination");
  expect(target.scope.isCurrent()).toBe(false);
  await target.close();
});
