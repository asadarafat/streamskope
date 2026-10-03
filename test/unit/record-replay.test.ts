import { afterEach, expect, it, vi } from "vitest";

import { RecordReplayService } from "../../src/features/kafka/application/record-replay-service";
import {
  parseRecordReplayInput,
  parseRecordReplayReview,
  parseRecordReplayOutcome,
  replayBatch,
  replayConfirmation,
  transformReplayRecord,
  UNCHANGED_REPLAY_TRANSFORM,
  type RecordReplayInput,
} from "../../src/features/kafka/contracts/record-replay";
import type { KafkaWriteOutcome } from "../../src/features/kafka/contracts/reviewed-writes";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const original = {
  state: "complete",
  encoding: "base64",
  key: "",
  value: null,
  headers: [
    { key: "aA==", value: null },
    { key: "aA==", value: "" },
  ],
} as const;
const input: RecordReplayInput = {
  topic: "destination",
  partition: 0,
  ratePerSecond: 10,
  targetProfile: { id: "target", revision: 3 },
  records: [0, 1, 2].map((index) => ({
    topic: "source",
    partition: 1,
    offset: String(index),
    timestampMs: "1000",
    original,
  })),
  transform: UNCHANGED_REPLAY_TRANSFORM,
};
const receipt: KafkaWriteOutcome = {
  state: "acknowledged",
  detail: "Accepted",
  receipt: { topic: "destination", partition: 0, offset: "9" },
  verification: "verified",
};
const services: RecordReplayService[] = [];
afterEach(async () => {
  for (const service of services.splice(0)) await service.invalidate();
  vi.useRealTimers();
});
function fixture(): {
  service: RecordReplayService;
  send: ReturnType<typeof vi.fn<(value: unknown) => Promise<KafkaWriteOutcome>>>;
  close: ReturnType<typeof vi.fn<() => Promise<void>>>;
  validate: ReturnType<
    typeof vi.fn<() => Promise<{ clusterId: string; topicId: string; partitions: number }>>
  >;
  changeSource(): void;
  changeProfile(): void;
} {
  let generation = 0,
    valid = true;
  const source = new RecordingActiveConnection();
  const send = vi.fn((_value: unknown) => Promise.resolve(receipt));
  const close = vi.fn(() => Promise.resolve());
  const validate = vi.fn(() =>
    Promise.resolve({ clusterId: "cluster-target", topicId: "topic-target", partitions: 1 }),
  );
  const target = Object.assign(new RecordingActiveConnection(), {
    reviewWrite: validate,
    applyWrite: send,
  });
  const service = new RecordReplayService(
    () => ({ connection: source, generation, connectionName: "Source" }),
    {
      open: (): Promise<
        import("../../src/features/kafka/application/replay-destination").ReplayDestination
      > => Promise.resolve({ connection: target, name: "Target", current: () => valid, close }),
    },
  );
  services.push(service);
  return {
    service,
    send,
    close,
    validate,
    changeSource(): void {
      generation++;
    },
    changeProfile(): void {
      valid = false;
    },
  };
}
it("preserves null versus empty, tombstones and ordered duplicate headers while applying explicit transforms", () => {
  expect(transformReplayRecord(original, UNCHANGED_REPLAY_TRANSFORM)).toEqual(original);
  expect(
    transformReplayRecord(original, {
      ...UNCHANGED_REPLAY_TRANSFORM,
      key: { value: null },
      appendHeaders: [{ key: "aA==", value: "dmFsdWU=" }],
      valueText: { search: "any", replacement: "never revive tombstones" },
    }),
  ).toEqual({
    ...original,
    key: null,
    headers: [...original.headers, { key: "aA==", value: "dmFsdWU=" }],
  });
  expect(
    transformReplayRecord(
      { ...original, value: "YSBh" },
      {
        ...UNCHANGED_REPLAY_TRANSFORM,
        removeHeaders: ["aA=="],
        valueText: { search: "a", replacement: "b" },
      },
    ),
  ).toEqual({ ...original, value: "YiBi", headers: [] });
  expect(() =>
    transformReplayRecord(
      { ...original, value: "/w==" },
      { ...UNCHANGED_REPLAY_TRANSFORM, valueText: { search: "a", replacement: "b" } },
    ),
  ).toThrow();
});
it("bounds originals, transformed expansion, rate and repeated source identities", () => {
  expect(parseRecordReplayInput(input)).toEqual(input);
  expect(() =>
    parseRecordReplayInput({ ...input, records: [input.records[0], input.records[0]] }),
  ).toThrow();
  expect(() => parseRecordReplayInput({ ...input, ratePerSecond: 11 })).toThrow();
  expect(() =>
    replayBatch({
      ...input,
      records: [
        {
          ...input.records[0]!,
          original: { ...original, value: Buffer.from("a".repeat(1000)).toString("base64") },
        },
      ],
      transform: {
        ...UNCHANGED_REPLAY_TRANSFORM,
        valueText: { search: "a", replacement: "b".repeat(1024) },
      },
    }),
  ).toThrow();
});
it("reviews without writes, freezes original bytes and timestamps, coalesces confirmation and closes its isolated target", async () => {
  const f = fixture();
  const selected = structuredClone(input);
  const review = await f.service.review(selected);
  expect(parseRecordReplayReview(review)).toEqual(review);
  expect(f.send).not.toHaveBeenCalled();
  Object.assign(selected.records[0]!.original, { value: "ZXZpbA==" });
  await expect(f.service.apply(review.planId, "wrong destination")).rejects.toThrow("exact");
  const [result, duplicate] = await Promise.all([
    f.service.apply(review.planId, replayConfirmation(review)),
    f.service.apply(review.planId, replayConfirmation(review)),
  ]);
  expect(result).toEqual(duplicate);
  expect(parseRecordReplayOutcome(result)).toEqual(result);
  expect(result).toMatchObject({
    stopReason: "complete",
    total: 3,
    unsent: 0,
    cleanup: "complete",
  });
  expect(f.send).toHaveBeenCalledTimes(3);
  expect(f.send.mock.calls[0]?.[0]).toMatchObject({ record: original, timestamp: "1000" });
  expect(f.close).toHaveBeenCalledTimes(1);
});
it.each(["changeSource", "changeProfile"] as const)(
  "rejects %s before dispatch and closes the isolated context on invalidation",
  async (change) => {
    const f = fixture();
    const review = await f.service.review(input);
    f[change]();
    await expect(f.service.apply(review.planId, replayConfirmation(review))).rejects.toThrow(
      "stale",
    );
    await f.service.invalidate();
    expect(f.send).not.toHaveBeenCalled();
    expect(f.close).toHaveBeenCalledTimes(1);
  },
);
it("detects topic recreation on the destination before sending any record", async () => {
  const f = fixture();
  const review = await f.service.review(input);
  f.validate.mockResolvedValue({
    clusterId: "cluster-target",
    topicId: "recreated",
    partitions: 1,
  });
  expect(await f.service.apply(review.planId, replayConfirmation(review))).toMatchObject({
    stopReason: "destination-changed",
    unsent: 3,
    outcomes: [],
  });
  expect(f.send).not.toHaveBeenCalled();
});
it("settles an in-flight acknowledgement after cancellation and accounts for all unsent records", async () => {
  const f = fixture();
  let finish!: (value: KafkaWriteOutcome) => void;
  f.send.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const review = await f.service.review(input),
    pending = f.service.apply(review.planId, replayConfirmation(review));
  await vi.waitFor(() => expect(f.send).toHaveBeenCalledTimes(1));
  const cancelled = f.service.cancel(review.planId);
  expect(f.close).not.toHaveBeenCalled();
  finish(receipt);
  expect(await pending).toMatchObject({
    total: 3,
    unsent: 2,
    outcomes: [receipt],
    stopReason: "cancelled",
  });
  await cancelled;
  expect(f.close).toHaveBeenCalledTimes(1);
});
it("retains partial acknowledgements and uncertainty without retry, including cleanup failure", async () => {
  const f = fixture();
  f.send.mockResolvedValueOnce(receipt).mockRejectedValueOnce(new Error("Transport lost"));
  f.close.mockRejectedValue(new Error("Cleanup unavailable"));
  const review = await f.service.review(input);
  const result = await f.service.apply(review.planId, replayConfirmation(review));
  expect(result).toMatchObject({
    unsent: 1,
    stopReason: "write-failed",
    cleanup: "unavailable",
    outcomes: [{ state: "acknowledged" }, { state: "unknown" }],
  });
  expect(await f.service.apply(review.planId, replayConfirmation(review))).toEqual(result);
  expect(f.send).toHaveBeenCalledTimes(2);
});
it("expires unused reviews and closes isolated clients without an operator action", async () => {
  vi.useFakeTimers();
  const f = fixture();
  const review = await f.service.review(input);
  await vi.advanceTimersByTimeAsync(120_001);
  expect(f.close).toHaveBeenCalledTimes(1);
  await expect(f.service.apply(review.planId, replayConfirmation(review))).rejects.toThrow("stale");
  expect(f.send).not.toHaveBeenCalled();
});
it("closes an isolated target when destination review fails", async () => {
  const f = fixture();
  f.validate.mockRejectedValue(new Error("Denied"));
  await expect(f.service.review(input)).rejects.toThrow("Denied");
  expect(f.close).toHaveBeenCalledTimes(1);
  expect(f.send).not.toHaveBeenCalled();
});
