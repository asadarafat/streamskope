import { expect, it, vi } from "vitest";

import { RecordBatchService } from "../../src/features/kafka/application/record-batch-service";
import type { RecordBatchInput } from "../../src/features/kafka/contracts/schema-samples";
import type { KafkaWriteOutcome } from "../../src/features/kafka/contracts/reviewed-writes";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const acknowledged: KafkaWriteOutcome = {
  state: "acknowledged",
  detail: "Accepted",
  receipt: { topic: "events", partition: 0, offset: "1" },
  verification: "unavailable",
};
const batch: RecordBatchInput = {
  topic: "events",
  partition: 0,
  ratePerSecond: 5,
  records: Array.from({ length: 3 }, () => ({
    state: "complete",
    encoding: "base64",
    key: null,
    value: "e30=",
    headers: [],
  })),
};
function setup(): {
  service: RecordBatchService;
  send: ReturnType<typeof vi.fn<() => Promise<KafkaWriteOutcome>>>;
  change: () => void;
  advance: () => void;
  delays: number[];
} {
  const send = vi.fn(() => Promise.resolve(acknowledged));
  const connection = Object.assign(new RecordingActiveConnection(), {
    reviewWrite: (): Promise<void> => Promise.resolve(),
    applyWrite: send,
  });
  let generation = 1;
  let now = 0;
  const delays: number[] = [];
  const service = new RecordBatchService(
    () => ({ connection, generation, connectionName: "Fixture" }),
    () => now,
    (delay): Promise<void> => {
      delays.push(delay);
      return Promise.resolve();
    },
  );
  return {
    service,
    send,
    change: (): void => {
      generation++;
    },
    advance: (): void => {
      now += 120_001;
    },
    delays,
  };
}
it("reviews without writes, pins all records and coalesces repeated confirmation with a rate ceiling", async () => {
  const fixture = setup();
  const review = await fixture.service.review(batch);
  expect(fixture.send).not.toHaveBeenCalled();
  const [first, repeated] = await Promise.all([
    fixture.service.apply(review.planId),
    fixture.service.apply(review.planId),
  ]);
  expect(first).toEqual(repeated);
  expect(first).toMatchObject({ total: 3, unsent: 0, stopReason: "complete" });
  expect(fixture.send).toHaveBeenCalledTimes(3);
  expect(fixture.delays).toEqual([200, 200]);
  await fixture.service.apply(review.planId);
  expect(fixture.send).toHaveBeenCalledTimes(3);
});
it("cancels during an in-flight send and accounts for acknowledgement plus all unsent records", async () => {
  const fixture = setup();
  let complete!: (value: KafkaWriteOutcome) => void;
  fixture.send.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        complete = resolve;
      }),
  );
  const review = await fixture.service.review(batch);
  const pending = fixture.service.apply(review.planId);
  await vi.waitFor(() => expect(fixture.send).toHaveBeenCalledTimes(1));
  fixture.service.cancel(review.planId);
  complete(acknowledged);
  expect(await pending).toMatchObject({
    total: 3,
    outcomes: [acknowledged],
    unsent: 2,
    stopReason: "cancelled",
  });
  expect(fixture.send).toHaveBeenCalledTimes(1);
});
it("stops on uncertainty, expired or stale reviews and connection changes without resending", async () => {
  const fixture = setup();
  fixture.send.mockRejectedValueOnce(new Error("transport lost"));
  const review = await fixture.service.review(batch);
  const outcome = await fixture.service.apply(review.planId);
  expect(outcome).toMatchObject({
    unsent: 2,
    stopReason: "write-failed",
    outcomes: [{ state: "unknown" }],
  });
  expect(fixture.send).toHaveBeenCalledTimes(1);
  const stale = await fixture.service.review(batch);
  fixture.change();
  await expect(fixture.service.apply(stale.planId)).rejects.toThrow();
  const expired = await fixture.service.review(batch);
  fixture.advance();
  await expect(fixture.service.apply(expired.planId)).rejects.toThrow();
  const changeDuring = await fixture.service.review(batch);
  fixture.send.mockImplementationOnce(() => {
    fixture.change();
    return Promise.resolve(acknowledged);
  });
  expect(await fixture.service.apply(changeDuring.planId)).toMatchObject({
    unsent: 2,
    stopReason: "connection-changed",
  });
});

it("stops at the duration budget after an acknowledged record and before another dispatch", async () => {
  const fixture = setup();
  fixture.send.mockImplementationOnce(() => {
    fixture.advance();
    return Promise.resolve(acknowledged);
  });
  const review = await fixture.service.review(batch);
  expect(await fixture.service.apply(review.planId)).toMatchObject({
    unsent: 2,
    stopReason: "deadline",
  });
  expect(fixture.send).toHaveBeenCalledTimes(1);
});
