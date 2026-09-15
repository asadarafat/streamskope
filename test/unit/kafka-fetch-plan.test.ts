import { describe, expect, it } from "vitest";

import type { KafkaFetchRequest } from "../../src/kafka/contracts";
import {
  KAFKA_EARLIEST_OFFSET_TIMESTAMP,
  KAFKA_LATEST_OFFSET_TIMESTAMP,
  resolveKafkaFetchPlan,
  type KafkaOffsetLookup,
} from "../../src/kafka/engine/fetch-plan";

class RecordingOffsetLookup implements KafkaOffsetLookup {
  readonly calls: Array<{ readonly timestamp: bigint; readonly topic: string }> = [];

  constructor(private readonly resolve: (timestamp: bigint) => readonly bigint[]) {}

  listTopicOffsets(topic: string, timestamp: bigint): Promise<readonly bigint[]> {
    this.calls.push({ timestamp, topic });
    return Promise.resolve(this.resolve(timestamp));
  }
}

function entries(
  offsets: ReadonlyMap<number, bigint> | null,
): readonly (readonly [number, bigint])[] {
  return offsets === null ? [] : [...offsets.entries()];
}

describe("Kafka fetch offset planning", () => {
  it("bounds a First N plan from retained low offsets to the starting snapshot", async () => {
    const lookup = new RecordingOffsetLookup((timestamp) => {
      if (timestamp === KAFKA_EARLIEST_OFFSET_TIMESTAMP) {
        return [2n, 10n];
      }
      if (timestamp === KAFKA_LATEST_OFFSET_TIMESTAMP) {
        return [8n, 15n];
      }
      throw new Error(`Unexpected timestamp ${String(timestamp)}.`);
    });
    const request: KafkaFetchRequest = {
      maxMessages: 3,
      mode: "earliest",
      topic: "orders",
    };

    const plan = await resolveKafkaFetchPlan(lookup, request, 1_722_000_000_000);

    expect(plan).toMatchObject({
      continuous: false,
      maxMessages: 3,
      request,
    });
    expect(entries(plan.startOffsets)).toEqual([
      [0, 2n],
      [1, 10n],
    ]);
    expect(entries(plan.endOffsets)).toEqual([
      [0, 5n],
      [1, 13n],
    ]);
    expect(lookup.calls).toEqual([
      { timestamp: KAFKA_EARLIEST_OFFSET_TIMESTAMP, topic: "orders" },
      { timestamp: KAFKA_LATEST_OFFSET_TIMESTAMP, topic: "orders" },
    ]);
  });

  it("clamps a half-open time window to retained topic offsets", async () => {
    const start = 1_722_000_000_000n;
    const end = start + 120_000n;
    const lookup = new RecordingOffsetLookup((timestamp) => {
      if (timestamp === KAFKA_EARLIEST_OFFSET_TIMESTAMP) {
        return [2n, 10n];
      }
      if (timestamp === KAFKA_LATEST_OFFSET_TIMESTAMP) {
        return [20n, 15n];
      }
      if (timestamp === start) {
        return [4n, -1n];
      }
      if (timestamp === end) {
        return [9n, -1n];
      }
      throw new Error(`Unexpected timestamp ${String(timestamp)}.`);
    });
    const request: KafkaFetchRequest = {
      endTimeMs: Number(end),
      maxMessages: 3,
      mode: "time-window",
      startTimeMs: Number(start),
      topic: "orders",
    };

    const plan = await resolveKafkaFetchPlan(lookup, request, Number(end));

    expect(entries(plan.startOffsets)).toEqual([
      [0, 4n],
      [1, 15n],
    ]);
    expect(entries(plan.endOffsets)).toEqual([
      [0, 7n],
      [1, 15n],
    ]);
    expect(plan.continuous).toBe(false);
  });

  it("uses bounded timestamp probes and per-partition candidate spans for recent modes", async () => {
    const now = 1_722_000_000_000;
    const lookup = new RecordingOffsetLookup((timestamp) => {
      if (timestamp === KAFKA_EARLIEST_OFFSET_TIMESTAMP) {
        return [0n, 0n];
      }
      if (timestamp === KAFKA_LATEST_OFFSET_TIMESTAMP) {
        return [100n, 200n];
      }
      return timestamp >= BigInt(now - 60 * 60 * 1_000) ? [98n, 199n] : [92n, 194n];
    });
    const request: KafkaFetchRequest = {
      maxMessages: 5,
      mode: "newest",
      topic: "orders",
    };

    const plan = await resolveKafkaFetchPlan(lookup, request, now);

    expect(plan.continuous).toBe(false);
    expect(entries(plan.endOffsets)).toEqual([
      [0, 100n],
      [1, 200n],
    ]);
    for (const [partition, startOffset] of plan.startOffsets) {
      const endOffset = plan.endOffsets?.get(partition);
      expect(endOffset).toBeDefined();
      expect((endOffset ?? startOffset) - startOffset).toBeLessThanOrEqual(5n);
    }
    expect(lookup.calls.some(({ timestamp }) => timestamp >= 0n)).toBe(true);
    expect(lookup.calls.length).toBeLessThanOrEqual(32);
  });

  it("keeps a recent Tail plan continuous while bounding its initial candidates", async () => {
    const lookup = new RecordingOffsetLookup((timestamp) => {
      if (timestamp === KAFKA_EARLIEST_OFFSET_TIMESTAMP) {
        return [0n];
      }
      if (timestamp === KAFKA_LATEST_OFFSET_TIMESTAMP) {
        return [100n];
      }
      return [95n];
    });

    const plan = await resolveKafkaFetchPlan(
      lookup,
      {
        maxMessages: 5,
        mode: "tail",
        topic: "orders",
      },
      1_722_000_000_000,
    );

    expect(plan.continuous).toBe(true);
    expect(plan.endOffsets).toBeNull();
    expect(entries(plan.startOffsets)).toEqual([[0, 95n]]);
  });

  it("rejects inconsistent broker offset maps instead of opening an invalid range", async () => {
    const lookup = new RecordingOffsetLookup((timestamp) =>
      timestamp === KAFKA_EARLIEST_OFFSET_TIMESTAMP ? [5n] : [4n],
    );

    await expect(
      resolveKafkaFetchPlan(
        lookup,
        {
          maxMessages: 5,
          mode: "earliest",
          topic: "orders",
        },
        1_722_000_000_000,
      ),
    ).rejects.toThrow("inconsistent");
  });
});
