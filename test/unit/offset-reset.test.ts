import { expect, it, vi } from "vitest";

import { OffsetResetService } from "../../src/features/kafka/application/offset-reset-service";
import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import {
  parseOffsetResetInput,
  parseOffsetResetReview,
  type OffsetResetSnapshot,
  type OffsetResetResult,
} from "../../src/features/kafka/contracts/offset-reset";
import { parseHostCommand, HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const input = {
  groupId: "recovery",
  targets: [
    { topic: "events", partition: 0, offset: "2" },
    { topic: "events", partition: 1, offset: "3" },
  ],
};

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: Error): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((complete, fail) => {
    resolve = complete;
    reject = fail;
  });
  return { promise, resolve, reject };
}
function fixture(): {
  service: OffsetResetService;
  snapshot: OffsetResetSnapshot;
  read: ReturnType<typeof vi.fn<() => Promise<OffsetResetSnapshot>>>;
  send: ReturnType<
    typeof vi.fn<
      (group: string, target: (typeof input.targets)[number]) => Promise<OffsetResetResult>
    >
  >;
  change(): void;
  expire(): void;
} {
  let generation = 0,
    now = 0;
  const snapshot: OffsetResetSnapshot = {
    inactive: true,
    state: "Empty",
    groupRead: "allowed",
    partitions: input.targets.map((t) => ({
      ...t,
      before: "8",
      low: "0",
      high: "10",
      replayUpperBound: String(8 - Number(t.offset)),
    })),
  };
  const send = vi.fn(
    (_group: string, target: (typeof input.targets)[number]): Promise<OffsetResetResult> => {
      const index = snapshot.partitions.findIndex((p) => p.partition === target.partition);
      Object.assign(snapshot.partitions[index]!, { before: target.offset });
      return Promise.resolve({
        ...target,
        state: "acknowledged",
        observed: target.offset,
        verified: true,
      });
    },
  );
  const read = vi.fn((): Promise<OffsetResetSnapshot> =>
    Promise.resolve(structuredClone(snapshot)),
  );
  const connection = Object.assign(new RecordingActiveConnection(), {
    offsetResetSnapshot: read,
    resetGroupOffset: send,
  });
  const scopes = new KafkaConnectionScopes(() => ({
    connection,
    generation,
    connectionName: "Fixture",
  }));
  return {
    snapshot,
    read,
    send,
    service: new OffsetResetService(
      () => scopes.offsetReset(),
      () => now,
    ),
    change(): void {
      generation++;
    },
    expire(): void {
      now = 120_001;
    },
  };
}
it("previews exact before/after offsets without committing, then coalesces confirmed reset and preserves per-partition receipts", async () => {
  const f = fixture();
  const review = await f.service.review(input);
  expect(f.send).not.toHaveBeenCalled();
  expect(parseOffsetResetReview(review)).toEqual(review);
  expect(review.baseline.partitions.map((p) => p.replayUpperBound)).toEqual(["6", "5"]);
  await expect(f.service.apply(review.planId, "other-group")).rejects.toThrow("exact");
  const [first, duplicate] = await Promise.all([
    f.service.apply(review.planId, input.groupId),
    f.service.apply(review.planId, input.groupId),
  ]);
  expect(first).toEqual(duplicate);
  expect(first.partitions.map((p) => p.state)).toEqual(["acknowledged", "acknowledged"]);
  expect(f.send).toHaveBeenCalledTimes(2);
});
it.each(["active", "stale", "denied", "retention"])(
  "refuses %s inputs before mutation",
  async (scenario) => {
    const f = fixture();
    const review = await f.service.review(input);
    if (scenario === "active") Object.assign(f.snapshot, { inactive: false, state: "Stable" });
    if (scenario === "stale") Object.assign(f.snapshot.partitions[1]!, { before: "9" });
    if (scenario === "denied") Object.assign(f.snapshot, { groupRead: "denied" });
    if (scenario === "retention") Object.assign(f.snapshot.partitions[0]!, { low: "4" });
    expect(
      (await f.service.apply(review.planId, input.groupId)).partitions.every(
        (p) => p.state === "unsent",
      ),
    ).toBe(true);
    expect(f.send).not.toHaveBeenCalled();
  },
);
it.each(["change", "expire"] as const)("rejects %s plans", async (action) => {
  const f = fixture();
  const review = await f.service.review(input);
  f[action]();
  await expect(f.service.apply(review.planId, input.groupId)).rejects.toThrow();
  expect(f.send).not.toHaveBeenCalled();
});
it("does not dispatch when the revalidation itself crosses the execution deadline", async () => {
  const f = fixture();
  let now = 0,
    reads = 0;
  const connection = Object.assign(new RecordingActiveConnection(), {
    offsetResetSnapshot: (): Promise<OffsetResetSnapshot> => {
      if (++reads > 1) now = 60_000;
      return Promise.resolve(structuredClone(f.snapshot));
    },
    resetGroupOffset: f.send,
  });
  const scopes = new KafkaConnectionScopes(() => ({
    connection,
    generation: 1,
    connectionName: "Deadline fixture",
  }));
  const service = new OffsetResetService(
    () => scopes.offsetReset(),
    () => now,
  );
  const review = await service.review(input);
  const outcome = await service.apply(review.planId, input.groupId);
  expect(outcome.partitions.every((p) => p.state === "unsent")).toBe(true);
  expect(outcome.detail).toContain("60-second");
  expect(f.send).not.toHaveBeenCalled();
});
it("stops after unknown dispatch and never repeats it when the same plan is confirmed", async () => {
  const f = fixture();
  f.send.mockRejectedValueOnce(new Error("Lost response"));
  const review = await f.service.review(input);
  const result = await f.service.apply(review.planId, input.groupId);
  expect(result.partitions.map((p) => p.state)).toEqual(["unknown", "unsent"]);
  expect(await f.service.apply(review.planId, input.groupId)).toEqual(result);
  expect(f.send).toHaveBeenCalledTimes(1);
});
it("retains acknowledged but unverified positions and stops the remainder", async () => {
  const f = fixture();
  f.send.mockResolvedValueOnce({
    ...input.targets[0]!,
    state: "acknowledged",
    observed: null,
    verified: false,
  });
  const review = await f.service.review(input);
  const result = await f.service.apply(review.planId, input.groupId);
  expect(result.partitions.map((p) => p.state)).toEqual(["acknowledged", "unsent"]);
});
it("validates bounds, exact keys and duplicate partitions on the host boundary", () => {
  expect(
    parseHostCommand({
      command: "consumerGroups.reset.review",
      id: "x",
      version: HOST_PROTOCOL_VERSION,
      payload: input,
    }).payload,
  ).toEqual(input);
  for (const offset of ["-1", "1.1", "01", "9223372036854775808"])
    expect(() =>
      parseOffsetResetInput({ ...input, targets: [{ ...input.targets[0], offset }] }),
    ).toThrow();
  expect(() =>
    parseOffsetResetInput({ ...input, targets: [input.targets[0], input.targets[0]] }),
  ).toThrow();
});

it("rejects a reset review completed after same-adapter reconnection", async () => {
  const f = fixture();
  const reading = deferred<OffsetResetSnapshot>();
  f.read.mockReturnValueOnce(reading.promise);
  const pending = f.service.review(input);
  const rejected = expect(pending).rejects.toThrow(/connection changed/iu);
  f.change();
  reading.resolve(f.snapshot);
  await rejected;
  expect(f.send).not.toHaveBeenCalled();
});

it("leaves every partition unsent when reset revalidation completes after reconnection", async () => {
  const f = fixture();
  const review = await f.service.review(input);
  const reading = deferred<OffsetResetSnapshot>();
  f.read.mockReturnValueOnce(reading.promise);
  const pending = f.service.apply(review.planId, input.groupId);
  await vi.waitFor(() => expect(f.read).toHaveBeenCalledTimes(2));
  f.change();
  reading.resolve(f.snapshot);
  expect((await pending).partitions.map((p) => p.state)).toEqual(["unsent", "unsent"]);
  expect(f.service.apply(review.planId, input.groupId)).toBe(pending);
  expect(f.send).not.toHaveBeenCalled();
});

it.each(["acknowledged", "unknown"] as const)(
  "retains an admitted partition's %s result after reconnection and leaves later partitions unsent",
  async (state) => {
    const f = fixture();
    const review = await f.service.review(input);
    const writing = deferred<OffsetResetResult>();
    f.send.mockReturnValueOnce(writing.promise);
    const pending = f.service.apply(review.planId, input.groupId);
    await vi.waitFor(() => expect(f.send).toHaveBeenCalledOnce());
    f.change();
    f.expire();
    if (state === "acknowledged")
      writing.resolve({
        ...input.targets[0]!,
        state,
        observed: input.targets[0]!.offset,
        verified: true,
      });
    else writing.reject(new Error("Acknowledgement lost"));
    expect((await pending).partitions.map((p) => p.state)).toEqual([state, "unsent"]);
    expect(f.service.apply(review.planId, input.groupId)).toBe(pending);
    expect(f.send).toHaveBeenCalledOnce();
    expect(f.read).toHaveBeenCalledTimes(2);
  },
);
