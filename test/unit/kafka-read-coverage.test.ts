import { describe, expect, it } from "vitest";

import {
  KAFKA_QUERY_LIMITS,
  parseKafkaFetchRequest,
  parseKafkaReadCoverage,
} from "../../src/features/kafka/contracts";
import { KafkaReadTracker } from "../../src/features/kafka/engine/read-coverage";
import type { KafkaRawMessage } from "../../src/features/kafka/engine/types";

const search = { key: "", value: "needle", timestamp: "", offset: "", partition: null };
const raw = (offset: number, value: string | Buffer = "hay", partition = 0): KafkaRawMessage => ({
  offset: BigInt(offset),
  partition,
  topic: "orders",
  timestamp: 1000n,
  headers: new Map(),
  value: Buffer.isBuffer(value) ? value : Buffer.from(value),
});
function tracker(end = 3n): KafkaReadTracker {
  return new KafkaReadTracker({
    continuous: false,
    maxMessages: 2,
    startOffsets: new Map([[0, 0n]]),
    endOffsets: new Map([[0, end]]),
    request: { mode: "earliest", maxMessages: 2, topic: "orders", search },
  });
}

describe("bounded broker search coverage", () => {
  it("searches beyond the result limit and proves the selected offset range separately", () => {
    const read = tracker();
    expect([raw(0), raw(1), raw(2, "Needle")].map((record) => read.accept(record))).toEqual([
      false,
      false,
      true,
    ]);
    expect(read.snapshot()).toMatchObject({
      reason: "range-complete",
      scannedRecords: 3,
      matchedRecords: 1,
      partitions: [{ startOffset: "0", endOffset: "3", nextOffset: "3" }],
    });
    expect(parseKafkaReadCoverage(read.snapshot(), "coverage")).toEqual(read.snapshot());
  });
  it("distinguishes a full result page from an exhausted range", () => {
    const read = tracker();
    read.accept(raw(0, "needle"));
    read.accept(raw(1, "needle"));
    expect(read.snapshot()).toMatchObject({ reason: "result-limit", scannedRecords: 2 });
    expect(read.accept(raw(2, "needle"))).toBe(false);
  });
  it("reports fetch exhaustion and cancellation without inventing coverage", () => {
    for (const reason of ["fetch-limit", "cancelled", "deadline", "failed"] as const) {
      const read = tracker();
      read.accept(raw(0));
      read.finish(reason);
      expect(read.snapshot()).toMatchObject({
        reason,
        matchedRecords: 0,
        partitions: [{ nextOffset: "1" }],
      });
    }
  });
  it("requires every partition to be traversed and ignores duplicates and unrelated records", () => {
    const read = new KafkaReadTracker({
      continuous: false,
      maxMessages: 10,
      startOffsets: new Map([
        [0, 1n],
        [1, 2n],
      ]),
      endOffsets: new Map([
        [0, 2n],
        [1, 4n],
      ]),
      request: { mode: "earliest", maxMessages: 10, topic: "orders" },
    });
    read.accept(raw(1));
    read.accept(raw(1));
    read.accept({ ...raw(3, "hay", 1), topic: "other" });
    expect(read.snapshot().reason).toBe("reading");
    // Offset holes are allowed; a later record establishes traversal through a compacted gap.
    read.accept(raw(3, "hay", 1));
    expect(read.snapshot()).toMatchObject({ reason: "range-complete", scannedRecords: 2 });
  });
  it("bounds scanning even when nothing matches", () => {
    const read = tracker(50_000n);
    for (let index = 0; index <= KAFKA_QUERY_LIMITS.scanRecords; index += 1)
      read.accept(raw(index));
    expect(read.snapshot()).toMatchObject({
      reason: "scan-limit",
      scannedRecords: KAFKA_QUERY_LIMITS.scanRecords,
      matchedRecords: 0,
    });
  });
  it("reports unsearchable large payloads and enforces a byte budget without decoding them", () => {
    const read = tracker(100n);
    const value = Buffer.alloc(2 * 1_048_576);
    for (let index = 0; index < 20; index += 1) read.accept(raw(index, value));
    expect(read.snapshot()).toMatchObject({
      reason: "byte-limit",
      scannedRecords: 16,
      unavailableRecords: 16,
      matchedRecords: 0,
      scannedBytes: KAFKA_QUERY_LIMITS.scanBytes,
    });
  });
  it("rejects unbounded search and false coverage at the host boundary", () => {
    expect(() =>
      parseKafkaFetchRequest({ topic: "orders", mode: "tail", maxMessages: 1, search }),
    ).toThrow("finite");
    expect(() =>
      parseKafkaFetchRequest({
        topic: "orders",
        mode: "earliest",
        maxMessages: 1,
        search: { ...search, value: "x".repeat(257) },
      }),
    ).toThrow("256");
    const result = tracker().snapshot();
    expect(() =>
      parseKafkaReadCoverage({ ...result, reason: "range-complete" }, "coverage"),
    ).toThrow("inconsistent");
    expect(() =>
      parseKafkaReadCoverage({ ...result, password: "not-allowed" }, "coverage"),
    ).toThrow("not declared");
  });
});
