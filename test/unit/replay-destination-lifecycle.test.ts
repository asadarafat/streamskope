import { expect, it, vi } from "vitest";

import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import { KafkaProfileService } from "../../src/features/kafka/application/profile-service";
import { SavedReplayDestinations } from "../../src/features/kafka/application/replay-destination";
import { ownedCleanupFailure } from "../../src/features/kafka/application/session-lifecycle";
import {
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (error: Error) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

it("awaits and identifies failed cleanup of a destination that opens after cancellation", async () => {
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore({ durability: "session", protection: "memory", state: "ready" }),
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
  const operation = new SavedReplayDestinations(profiles, port).open(
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
  const failure = new Error("fixture cleanup failure");
  closed.reject(failure);
  const error = await result;
  expect(error).toBeInstanceOf(Error);
  expect(ownedCleanupFailure(error)).toBe(failure);
  expect(String(error)).not.toContain(failure.message);
});
