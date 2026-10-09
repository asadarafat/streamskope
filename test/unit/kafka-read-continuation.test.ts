import { describe, expect, it } from "vitest";
import { KafkaReadContinuations } from "../../src/features/kafka/application/read-continuation";
import type { KafkaReadCheckpoint } from "../../src/features/kafka/application/read-checkpoint";
import {
  KAFKA_CONTINUATION_LIMITS,
  parseKafkaContinuationInput,
  parseKafkaSearchProgress,
} from "../../src/features/kafka/contracts/query-search";

const request = { topic: "events", mode: "earliest", maxMessages: 10 } as const;
function checkpoint(start = "0", next = "10", end = "30"): KafkaReadCheckpoint {
  return {
    clusterId: "cluster-a",
    topicId: "topic-a",
    partitionCount: 1,
    coverage: {
      reason: next === end ? "range-complete" : "result-limit",
      scannedRecords: 10,
      scannedBytes: 200,
      matchedRecords: 10,
      unavailableRecords: 0,
      partitions: [{ partition: 0, startOffset: start, nextOffset: next, endOffset: end }],
    },
  };
}

describe("host-owned read continuation", () => {
  it("consumes tokens once and independently accumulates page counters with original bounds", () => {
    let serial = 0;
    const owner = new KafkaReadContinuations(
      () => 1000,
      () => `token-${++serial}`,
    );
    const first = owner.finish(request, "binding", checkpoint(), 0);
    const previous = owner.take(first.continuation!.id, "binding");
    expect(() => owner.take(first.continuation!.id, "binding")).toThrow("already used");
    const second = owner.finish(request, "binding", checkpoint("10", "20"), 0, previous);
    expect(second).toMatchObject({
      pass: 2,
      scannedRecords: 20,
      matchedRecords: 20,
      scannedBytes: 400,
    });
    expect(owner.coverage(checkpoint("10", "20").coverage, previous)).toMatchObject({
      scannedRecords: 10,
      partitions: [{ startOffset: "0", nextOffset: "20", endOffset: "30" }],
    });
    const last = owner.take(second.continuation!.id, "binding");
    expect(owner.finish(request, "binding", checkpoint("20", "30"), 0, last)).toMatchObject({
      pass: 3,
      scannedRecords: 30,
      continuation: null,
    });
  });

  it("rejects changed interpretation, expiry and invalidation without retaining record bytes", () => {
    let time = 1000;
    const owner = new KafkaReadContinuations(() => time);
    const first = owner.finish(request, "codecs-and-masking-a", checkpoint(), 0);
    expect(() => owner.take(first.continuation!.id, "codecs-and-masking-b")).toThrow();
    expect(() => owner.take(first.continuation!.id, "codecs-and-masking-a")).toThrow();
    const second = owner.finish(request, "binding", checkpoint(), 0);
    time += KAFKA_CONTINUATION_LIMITS.lifetimeMs;
    expect(() => owner.take(second.continuation!.id, "binding")).toThrow();
    const third = owner.finish(request, "binding", checkpoint(), 0);
    owner.invalidate();
    expect(() => owner.take(third.continuation!.id, "binding")).toThrow();
  });

  it("keeps the current capability when an unrelated token is presented", () => {
    const owner = new KafkaReadContinuations();
    const first = owner.finish(request, "binding", checkpoint(), 0);
    expect(() => owner.take("foreign", "binding")).toThrow();
    expect(owner.take(first.continuation!.id, "binding").request).toEqual(request);
  });

  it.each(["deadline", "scan-limit", "byte-limit", "fetch-limit", "cancelled"] as const)(
    "continues a confirmed %s boundary that made progress",
    (reason) => {
      const owner = new KafkaReadContinuations();
      const value = checkpoint();
      expect(
        owner.finish(request, "binding", { ...value, coverage: { ...value.coverage, reason } }, 0)
          .continuation,
      ).not.toBeNull();
    },
  );

  it.each(["failed", "reading"] as const)("does not authorize continuation for %s", (reason) => {
    const owner = new KafkaReadContinuations();
    const value = checkpoint();
    expect(
      owner.finish(request, "binding", { ...value, coverage: { ...value.coverage, reason } }, 0)
        .continuation,
    ).toBeNull();
  });

  it("refuses dropped output, non-advancing cursors and continuous reads", () => {
    const owner = new KafkaReadContinuations();
    expect(owner.finish(request, "binding", checkpoint(), 1).continuation).toBeNull();
    expect(owner.finish(request, "binding", checkpoint("0", "0"), 0).continuation).toBeNull();
    expect(
      owner.finish({ ...request, mode: "tail" }, "binding", checkpoint(), 0).continuation,
    ).toBeNull();
  });

  it("retains exact int64 offset strings without Number rounding", () => {
    const owner = new KafkaReadContinuations();
    const original = checkpoint(
      "9223372036854775000",
      "9223372036854775001",
      "9223372036854775807",
    );
    const result = owner.finish(request, "binding", original, 0);
    expect(owner.take(result.continuation!.id, "binding").checkpoint).toEqual(original);
  });
});

describe("continuation wire boundary", () => {
  it("rejects renderer-supplied offsets or mismatched cumulative evidence", () => {
    expect(() =>
      parseKafkaContinuationInput({ continuationId: "token", partitions: [] }, "request"),
    ).toThrow();
    expect(() => parseKafkaContinuationInput({ continuationId: "" }, "request")).toThrow();
    const owner = new KafkaReadContinuations();
    const progress = owner.finish(request, "binding", checkpoint(), 0);
    expect(parseKafkaSearchProgress(progress, "progress")).toEqual(progress);
    expect(() =>
      parseKafkaSearchProgress({ ...progress, scannedRecords: 0 }, "progress"),
    ).toThrow();
    expect(() => parseKafkaSearchProgress({ ...progress, pass: 0 }, "progress")).toThrow();
    expect(() =>
      parseKafkaSearchProgress(
        { ...progress, continuation: { id: "a", expiresAt: "tomorrow" } },
        "progress",
      ),
    ).toThrow();
  });
});
