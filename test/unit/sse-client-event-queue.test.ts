import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  kafkaMessageRetainedBytes,
  type HostEvent,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
} from "../../src/features/kafka/contracts";
import {
  AccountedProviderEventQueue,
  type ProviderDeliveryQueue,
} from "../../src/platform/node/accounted-provider-event-queue";
import { createKafkaIpcDeliveryPolicy } from "../../src/platform/node/kafka-delivery-policy";
import { SseClientEventQueue } from "../../src/platform/node/kafka-sse-event-queue";

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

const readyCapability = {
  applicableRules: 0,
  omittedRules: 0,
  state: "ready",
} as const;

function message(
  offset: number,
  payload = "data",
  ruleEvaluation: KafkaLiveRuleEvaluation = evaluated,
): KafkaExploredMessage {
  return {
    headers: {},
    id: `test:0:${offset}`,
    key: null,
    offset: String(offset),
    originalByteSize: Buffer.byteLength(payload),
    partition: 0,
    payload,
    preview: payload,
    ruleEvaluation,
    timestamp: "2026-07-25T15:00:00.000Z",
    topic: "test",
    truncated: false,
  };
}

function batch(sequence: number, messages: readonly KafkaExploredMessage[]): HostEvent {
  return {
    event: "messages.batch",
    payload: {
      droppedMessages: 0,
      messages,
      topic: "test",
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function take(queue: ProviderDeliveryQueue<HostEvent>): HostEvent | undefined {
  const next = queue.begin();
  if (next.kind === "failure") throw new Error(next.reason);
  if (next.kind === "empty") return undefined;
  next.lease.complete();
  return next.lease.event;
}

describe("development-host SSE client event queue", () => {
  it("retains the newest records within the configured count and reports oldest-record loss", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: 1_024,
      maxMessages: 3,
    });

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBeUndefined();
    expect(queue.enqueue(batch(2, [message(3), message(4)]))).toBeUndefined();

    expect(queue.costs.records).toBe(3);
    expect(take(queue)).toMatchObject({
      event: "messages.batch",
      payload: {
        droppedMessages: 1,
        messages: [{ offset: "2" }],
      },
    });
    expect(take(queue)).toMatchObject({
      event: "messages.batch",
      payload: {
        droppedMessages: 1,
        messages: [{ offset: "3" }, { offset: "4" }],
      },
    });
  });

  it("retains the newest records within the configured UTF-8 byte bound", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: kafkaMessageRetainedBytes(message(2, "bbb")),
      maxMessages: 8,
    });

    expect(queue.enqueue(batch(1, [message(1, "åå"), message(2, "bbb")]))).toBeUndefined();

    expect(queue.costs.recordBytes).toBe(kafkaMessageRetainedBytes(message(2, "bbb")));
    expect(take(queue)).toMatchObject({
      payload: {
        droppedMessages: 1,
        messages: [{ offset: "2" }],
      },
    });
  });

  it("rejects non-message event overflow instead of silently losing operational state", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 1,
      maxMessageBytes: 1_024,
      maxMessages: 10,
    });
    const ready: HostEvent = {
      event: "backend.availability",
      payload: { state: "ready" },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    };
    const connected: HostEvent = {
      event: "connection.state",
      payload: { connectionName: "Local aio", state: "connected" },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };

    expect(queue.enqueue(ready)).toBeUndefined();
    expect(queue.enqueue(connected)).toBe("event-limit");
    expect(take(queue)).toEqual(ready);
  });

  it("adds transport loss to a later consumption-state counter", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: 1_024,
      maxMessages: 1,
    });
    const state: HostEvent = {
      event: "consumption.state",
      payload: {
        droppedMessages: 4,
        receivedMessages: 0,
        request: {
          maxMessages: 1_000,
          mode: "tail",
          topic: "test",
        },
        ruleEvaluation: readyCapability,
        state: "streaming",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBeUndefined();
    expect(queue.enqueue(state)).toBeUndefined();
    expect(take(queue)).toMatchObject({
      payload: { droppedMessages: 1, messages: [{ offset: "2" }] },
    });
    expect(take(queue)).toMatchObject({
      event: "consumption.state",
      payload: { droppedMessages: 5 },
    });
  });

  it("does not carry transport loss into a new consumption generation", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: 1_024,
      maxMessages: 1,
    });
    const loading: HostEvent = {
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 0,
        request: {
          maxMessages: 1_000,
          mode: "tail",
          topic: "next-topic",
        },
        ruleEvaluation: readyCapability,
        state: "loading",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBeUndefined();
    expect(queue.enqueue(loading)).toBeUndefined();
    expect(take(queue)).toEqual(loading);
    expect(take(queue)).toBeUndefined();
  });

  it("preserves old terminal loss evidence when loading replaces unsent records", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: 1_024,
      maxMessages: 1,
    });
    const terminal: HostEvent = {
      event: "consumption.state",
      payload: {
        droppedMessages: 4,
        receivedMessages: 2,
        request: { maxMessages: 1_000, mode: "tail", topic: "test" },
        ruleEvaluation: readyCapability,
        state: "stopped",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };
    const loading: HostEvent = {
      ...terminal,
      payload: { ...terminal.payload, droppedMessages: 0, receivedMessages: 0, state: "loading" },
      sequence: 3,
    };

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBeUndefined();
    expect(queue.enqueue(terminal)).toBeUndefined();
    expect(queue.enqueue(loading)).toBeUndefined();

    expect(take(queue)).toMatchObject({
      event: "consumption.state",
      payload: { droppedMessages: 6, state: "stopped" },
    });
    expect(take(queue)).toEqual(loading);
    expect(terminal.payload.droppedMessages).toBe(4);
  });

  it("counts bounded live-rule evidence when evicting the oldest SSE record", () => {
    const evidenceHeavy: KafkaLiveRuleEvaluation = {
      ...evaluated,
      errorCount: 1,
      errors: [{ diagnostic: "x".repeat(512), name: "Evidence heavy" }],
      evaluatedRules: 1,
      state: "partial",
    };
    const newest = message(2, "");
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: kafkaMessageRetainedBytes(newest),
      maxMessages: 8,
    });

    expect(queue.enqueue(batch(1, [message(1, "", evidenceHeavy), newest]))).toBeUndefined();

    expect(queue.costs.recordBytes).toBe(kafkaMessageRetainedBytes(newest));
    expect(take(queue)).toMatchObject({
      payload: {
        droppedMessages: 1,
        messages: [{ offset: "2" }],
      },
    });
  });

  it("keeps unsent Kafka records across IPC loading while only IPC replaces observations", () => {
    const observation = (sequence: number): HostEvent => ({
      event: "streamMetrics.changed",
      sequence,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        operationId: "test-operation",
        connectionName: null,
        delivery: null,
        queue: null,
        request: null,
        sampledAt: null,
        state: "unavailable",
        status: "unavailable",
      },
    });
    const loading: HostEvent = {
      event: "consumption.state",
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        droppedMessages: 0,
        receivedMessages: 0,
        request: { maxMessages: 1_000, mode: "tail", topic: "next" },
        ruleEvaluation: readyCapability,
        state: "loading",
      },
    };
    const connected: HostEvent = {
      event: "connection.state",
      sequence: 4,
      version: HOST_PROTOCOL_VERSION,
      payload: { connectionName: "Local Kafka", state: "connected" },
    };
    const ipc = new AccountedProviderEventQueue({
      limits: { maxEvents: 8, maxSerializedBytes: 8 * 1024 * 1024, maxRecords: 1_000 },
      overflow: "reject",
      policy: createKafkaIpcDeliveryPolicy(),
    });
    for (const event of [
      batch(1, [message(1)]),
      loading,
      observation(3),
      connected,
      observation(5),
    ])
      expect(ipc.enqueue(event)).toBeUndefined();
    expect(ipc.costs.records).toBe(1);
    expect([
      take(ipc)?.sequence,
      take(ipc)?.sequence,
      take(ipc)?.sequence,
      take(ipc)?.sequence,
    ]).toEqual([1, 2, 4, 5]);
    const sse = new SseClientEventQueue({ maxEvents: 8, maxMessageBytes: 1_024, maxMessages: 1 });
    expect(sse.enqueue(observation(3))).toBeUndefined();
    expect(sse.enqueue(observation(5))).toBeUndefined();
    expect([take(sse)?.sequence, take(sse)?.sequence]).toEqual([3, 5]);
  });

  it("rejects unsafe Kafka loss evidence before beginning an event write", () => {
    const queue = new SseClientEventQueue({ maxEvents: 8, maxMessageBytes: 1_024, maxMessages: 1 });
    const terminal: HostEvent = {
      event: "consumption.state",
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        droppedMessages: Number.MAX_SAFE_INTEGER,
        receivedMessages: 2,
        request: { maxMessages: 1_000, mode: "tail", topic: "test" },
        ruleEvaluation: readyCapability,
        state: "stopped",
      },
    };
    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBeUndefined();
    expect(queue.enqueue(terminal)).toBeUndefined();
    take(queue);
    expect(queue.begin()).toEqual({ kind: "failure", reason: "event-validation" });
    expect(terminal.payload.droppedMessages).toBe(Number.MAX_SAFE_INTEGER);
    queue.close();
    expect(queue.costs.events).toBe(0);
  });
});
