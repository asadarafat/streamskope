import { describe, expect, it } from "vitest";

import type { KafkaStreamMonitorSnapshot } from "../../src/features/kafka/contracts";
import {
  monitorRateSamples,
  monitorStatus,
  monitorWindow,
  scopedHostHistory,
} from "../../src/features/kafka/ui/stream-monitor-presentation";

const now = Date.parse("2026-07-26T12:00:30.000Z");
const stamp = (seconds: number): string => new Date(now - seconds * 1000).toISOString();
const snapshot: KafkaStreamMonitorSnapshot = {
  operationId: "new",
  connectionName: "local",
  request: { topic: "orders", mode: "tail", maxMessages: 100 },
  state: "streaming",
  status: "nominal",
  sampledAt: stamp(0),
  queue: null,
  delivery: {
    batchCount: 1,
    batchSize: 200,
    publishedMessages: 20,
    historySamples: 50,
    intervalMs: 20,
    lastBatchMessages: 20,
    messagesPerSecond: 20,
    rateSampledAt: stamp(2),
    rateWindowMs: 1000,
    publicationDurationMs: 1,
    publicationSampledAt: stamp(2),
    queueWaitMs: 2,
    queueWaitSampledAt: stamp(2),
    receivedMessages: 20,
    tuningSource: "confirmed",
  },
};
describe("monitor evidence presentation", () => {
  it("deduplicates repeated rate evidence by measurement time, not sample envelope time", () => {
    const rate = monitorRateSamples(
      [{ ...snapshot, sampledAt: stamp(1) }, snapshot],
      [stamp(60), stamp(0)],
    );
    expect(rate).toEqual([{ sampledAt: stamp(2), value: 20 }]);
  });
  it("discards identically shaped request history owned by another operation", () => {
    expect(scopedHostHistory(snapshot, [{ ...snapshot, operationId: "old" }])).toEqual([snapshot]);
  });
  it("freezes a stale or terminal chart on the last host sample", () => {
    const stale = { ...snapshot, sampledAt: stamp(20) };
    expect(monitorStatus(stale, now).stale).toBe(true);
    expect(monitorWindow(stale, now, 60)).toEqual([stamp(80), stamp(20)]);
    expect(monitorWindow({ ...snapshot, state: "stopped", sampledAt: stamp(2) }, now, 60)).toEqual([
      stamp(62),
      stamp(2),
    ]);
  });
  it("describes the byte watermark as approaching capacity", () => {
    const pressured = {
      ...snapshot,
      queue: {
        capacityBytes: 16_777_216,
        capacityMessages: 1000,
        currentBytes: 15_728_640,
        currentMessages: 15,
        droppedMessages: 0,
        droppedPerSecond: 0,
        droppedSincePrevious: 0,
        oldestMessageAgeMs: 10,
        peakBytes: 15_728_640,
        peakMessages: 15,
        pressureReasons: ["byte-capacity"] as const,
        dropReasons: { countCapacity: 0, byteCapacity: 0, oversized: 0, terminalDiscarded: 0 },
      },
    };
    expect(monitorStatus(pressured, now).explanation).toBe("Byte capacity nearly reached");
  });
  it("does not treat document FPS availability as host health", () => {
    expect(monitorStatus(snapshot, now).label).toBe("Delivering");
  });
});
