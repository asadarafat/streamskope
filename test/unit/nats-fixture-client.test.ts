import { afterEach, expect, it, vi } from "vitest";

import { boundedNatsOperation } from "../../tools/dev/nats-fixture/client";

afterEach(() => {
  vi.useRealTimers();
});

it("bounds an SDK promise which never receives PONG and clears its timer", async () => {
  vi.useFakeTimers();
  const work = boundedNatsOperation(new Promise<never>(() => undefined), 1_000);
  const failure = expect(work).rejects.toThrow("operation timed out");
  await vi.advanceTimersByTimeAsync(1_000);
  await failure;
  expect(vi.getTimerCount()).toBe(0);
});

it("observes a late SDK rejection after timeout", async () => {
  vi.useFakeTimers();
  let rejectLate: (error: Error) => void = () => undefined;
  const sdk = new Promise<never>((_resolve, reject) => {
    rejectLate = reject;
  });
  const failure = expect(boundedNatsOperation(sdk, 1_000)).rejects.toThrow("operation timed out");
  await vi.advanceTimersByTimeAsync(1_000);
  await failure;
  rejectLate(new Error("late SDK failure"));
  await vi.advanceTimersByTimeAsync(1);
  expect(vi.getTimerCount()).toBe(0);
});

it("returns the early SDK result without leaving a deadline timer", async () => {
  vi.useFakeTimers();
  await expect(boundedNatsOperation(Promise.resolve("pong"), 1_000)).resolves.toBe("pong");
  expect(vi.getTimerCount()).toBe(0);
});
