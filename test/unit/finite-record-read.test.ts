import { expect, it, vi } from "vitest";

import type { KafkaMessage, KafkaReadCoverage } from "../../src/features/kafka/contracts";
import { FINITE_RECORD_READ_LIMITS } from "../../src/features/kafka/contracts/finite-record-read";
import { FiniteRecordRead } from "../../src/features/kafka/application/finite-record-read";
import type { KafkaReadCheckpoint } from "../../src/features/kafka/application/read-checkpoint";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";

function record(offset: number): KafkaMessage {
  return {
    id: String(offset),
    topic: "events",
    partition: 0,
    offset: String(offset),
    timestamp: "2026-10-09T10:00:00.000Z",
    key: null,
    payload: null,
    preview: "",
    headers: {},
    originalByteSize: 0,
    truncated: false,
  };
}
class Reader implements KafkaMessageStream {
  next = 0;
  accepted = 0;
  closed = false;
  constructor(readonly end: number) {}
  readonly close = vi.fn((): Promise<void> => {
    this.closed = true;
    return Promise.resolve();
  });
  readonly acknowledge = vi.fn((message: KafkaMessage): void => {
    this.accepted = Number(message.offset) + 1;
  });
  coverage(): KafkaReadCoverage {
    return {
      reason: this.next === this.end ? "range-complete" : this.closed ? "cancelled" : "reading",
      scannedRecords: this.next,
      scannedBytes: this.next,
      matchedRecords: this.next,
      unavailableRecords: 0,
      partitions: [
        {
          partition: 0,
          startOffset: "0",
          endOffset: String(this.end),
          nextOffset: String(this.next),
        },
      ],
    };
  }
  checkpoint(): KafkaReadCheckpoint {
    return {
      clusterId: "cluster",
      topicId: "topic",
      partitionCount: 1,
      coverage: {
        ...this.coverage(),
        reason:
          this.accepted === this.end ? "range-complete" : this.closed ? "cancelled" : "reading",
        partitions: [
          {
            partition: 0,
            startOffset: "0",
            endOffset: String(this.end),
            nextOffset: String(this.accepted),
          },
        ],
      },
    };
  }
  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    while (!this.closed && this.next < this.end) {
      await Promise.resolve();
      yield record(this.next++);
    }
  }
}
function fixture(total = 3): {
  reader: Reader;
  read: FiniteRecordRead;
  authority: AbortController;
} {
  const reader = new Reader(total);
  const authority = new AbortController();
  const read = new FiniteRecordRead({
    scope: {
      connectionName: "fixture",
      isCurrent: (): boolean => true,
      openMessageStream: (): Promise<KafkaMessageStream> => Promise.resolve(reader),
    },
    input: {
      topic: "events",
      range: { mode: "earliest" },
      search: { key: "", value: "", offset: "", timestamp: "", partition: null },
      maxRecords: 10_000,
    },
    limits: FINITE_RECORD_READ_LIMITS,
    deadlineAt: Date.now() + 60_000,
    authority: authority.signal,
    assertCurrent: (): void => undefined,
    changed: (): void => undefined,
  });
  return { reader, read, authority };
}

it("yields a buffered synchronous analysis to cancellation while retaining exact accepted coverage", async () => {
  const { reader, read } = fixture(1_000);
  const stop = setTimeout(() => read.stop("cancelled"), 0);
  try {
    const result = await read.run(() => Promise.resolve("committed"));
    expect(result.reason).toBe("cancelled");
    expect(result.counts.acceptedRecords).toBeGreaterThan(0);
    expect(result.counts.acceptedRecords).toBeLessThanOrEqual(128);
    expect(reader.acknowledge).toHaveBeenCalledTimes(result.counts.acceptedRecords);
    expect(result.coverage?.partitions[0]?.nextOffset).toBe(String(result.counts.acceptedRecords));
    expect(reader.close).toHaveBeenCalledOnce();
  } finally {
    clearTimeout(stop);
    await read.idle();
  }
});

it("refuses a consumer limit without acknowledging or advancing past the refused record", async () => {
  const { reader, read } = fixture();
  const result = await read.run((message) =>
    Promise.resolve(message.offset === "0" ? "committed" : "limit"),
  );
  expect(result).toMatchObject({
    reason: "consumer-limit",
    counts: { acceptedRecords: 1, scannedRecords: 2 },
    coverage: { partitions: [{ nextOffset: "1", endOffset: "3" }] },
  });
  expect(reader.acknowledge).toHaveBeenCalledOnce();
  expect(reader.close).toHaveBeenCalledOnce();
});

it("does not convert a consumer rejection during soft cancellation into a confirmed partial result", async () => {
  const { reader, read } = fixture();
  const failure = new Error("consumer write failed");
  await expect(
    read.run(() => {
      read.stop("cancelled");
      return Promise.reject(failure);
    }),
  ).rejects.toMatchObject({ kind: "consumer", cause: failure, cleanupDebt: null });
  expect(reader.acknowledge).not.toHaveBeenCalled();
  expect(reader.close).toHaveBeenCalledOnce();
  await expect(read.idle()).resolves.toBeUndefined();
});

it("revokes before acknowledging a consumer that settles after authority was withdrawn", async () => {
  const { reader, read, authority } = fixture();
  await expect(
    read.run(() => {
      authority.abort();
      return Promise.resolve("committed");
    }),
  ).rejects.toMatchObject({ kind: "revoked" });
  expect(reader.acknowledge).not.toHaveBeenCalled();
  expect(reader.close).toHaveBeenCalledOnce();
});

it("retains original-reader cleanup debt until the actual close retry succeeds", async () => {
  const { reader, read } = fixture();
  reader.close.mockRejectedValue(new Error("close failed"));
  await expect(read.run(() => Promise.resolve("committed"))).rejects.toMatchObject({
    kind: "cleanup",
    cleanupDebt: "reader-close",
  });
  await expect(read.idle()).rejects.toMatchObject({ kind: "cleanup" });
  reader.close.mockResolvedValue();
  await read.retryCleanup();
  await expect(read.idle()).resolves.toBeUndefined();
  expect(reader.close).toHaveBeenCalledTimes(2);
});

it("returns detached snapshots and prevents accidental replay by a second run", async () => {
  const { read } = fixture();
  await read.run(() => Promise.resolve("committed"));
  const snapshot = read.snapshot();
  Object.assign(snapshot.counts, { acceptedRecords: 90 });
  Object.assign(snapshot.coverage!.partitions[0]!, { nextOffset: "90" });
  expect(read.snapshot()).toMatchObject({
    counts: { acceptedRecords: 3 },
    coverage: { partitions: [{ nextOffset: "3" }] },
  });
  expect(() => read.run(() => Promise.resolve("committed"))).toThrow("only run once");
});

it("refreshes explicit status from raw coverage without requiring periodic notifications", async () => {
  const { read } = fixture();
  let observed: ReturnType<FiniteRecordRead["snapshot"]> | undefined;
  await read.run((message) => {
    if (message.offset === "1") observed = read.snapshot();
    return Promise.resolve("committed");
  });
  expect(observed).toMatchObject({
    counts: { acceptedRecords: 1, scannedRecords: 2 },
    coverage: { matchedRecords: 1, scannedRecords: 2, partitions: [{ nextOffset: "1" }] },
  });
});
