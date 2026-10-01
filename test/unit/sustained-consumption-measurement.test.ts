import { describe, expect, it } from "vitest";

import {
  KAFKA_MESSAGE_LIMITS,
  KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
} from "../../src/features/kafka/contracts";
import { measureSustainedConsumption } from "../support/sustained-consumption-measurement";

describe("sustained consumption measurement", () => {
  it("keeps host scheduling, renderer retention and monitor history bounded", async () => {
    const evidence = await measureSustainedConsumption({ messageCount: 2_000 });

    expect(evidence).toMatchObject({
      acceptedMessages: 2_000,
      maximumConcurrentFlushes: 1,
      retainedMessages: KAFKA_MESSAGE_LIMITS.retainedMessages,
      stopState: "succeeded",
    });
    expect(evidence.maximumHostQueueMessages).toBeLessThanOrEqual(
      KAFKA_MESSAGE_LIMITS.queuedMessages,
    );
    expect(evidence.monitorHistorySamples).toBeLessThanOrEqual(KAFKA_STREAM_MONITOR_HISTORY_LIMIT);
    expect(evidence.pendingFlushesAfterStop).toBe(0);
  });
});
