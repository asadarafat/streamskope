import { describe, expect, it } from "vitest";

import { KAFKA_MESSAGE_LIMITS } from "../../src/features/kafka/contracts";
import { measureRendererMessageRetention } from "../support/renderer-message-retention-measurement";

describe("renderer message retention measurement", () => {
  it("delivers ten thousand independent records through production reduction", () => {
    const evidence = measureRendererMessageRetention({
      collectHeapBytes: () => 10_000_000,
      forceGarbageCollection: () => undefined,
    });

    expect(evidence).toMatchObject({
      deliveredMessages: 10_000,
      heapDeltaBytes: 0,
      rendererEvictions: 9_000,
      retainedMessages: KAFKA_MESSAGE_LIMITS.retainedMessages,
    });
    expect(evidence.retainedBytes).toBeLessThanOrEqual(KAFKA_MESSAGE_LIMITS.retainedBytes);
    expect(evidence.elapsedCpuMs).toBeGreaterThanOrEqual(0);
  });
});
