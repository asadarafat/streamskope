import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  kafkaMessageRetainedBytes,
  type HostEvent,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
} from "../../src/kafka/contracts";
import { SseClientEventQueue } from "../../src/platform/dev-host/sse-client-event-queue";

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

describe("development-host SSE client event queue", () => {
  it("retains the newest records within the configured count and reports oldest-record loss", () => {
    const queue = new SseClientEventQueue({
      maxEvents: 8,
      maxMessageBytes: 1_024,
      maxMessages: 3,
    });

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBe(true);
    expect(queue.enqueue(batch(2, [message(3), message(4)]))).toBe(true);

    expect(queue.queuedMessages).toBe(3);
    expect(queue.dequeue()).toMatchObject({
      event: "messages.batch",
      payload: {
        droppedMessages: 1,
        messages: [{ offset: "2" }],
      },
    });
    expect(queue.dequeue()).toMatchObject({
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

    expect(queue.enqueue(batch(1, [message(1, "åå"), message(2, "bbb")]))).toBe(true);

    expect(queue.queuedMessageBytes).toBe(kafkaMessageRetainedBytes(message(2, "bbb")));
    expect(queue.dequeue()).toMatchObject({
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

    expect(queue.enqueue(ready)).toBe(true);
    expect(queue.enqueue(connected)).toBe(false);
    expect(queue.dequeue()).toEqual(ready);
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

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBe(true);
    expect(queue.enqueue(state)).toBe(true);
    expect(queue.dequeue()).toMatchObject({
      payload: { droppedMessages: 1, messages: [{ offset: "2" }] },
    });
    expect(queue.dequeue()).toMatchObject({
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

    expect(queue.enqueue(batch(1, [message(1), message(2)]))).toBe(true);
    expect(queue.enqueue(loading)).toBe(true);
    expect(queue.dequeue()).toEqual(loading);
    expect(queue.dequeue()).toBeUndefined();
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

    expect(queue.enqueue(batch(1, [message(1, "", evidenceHeavy), newest]))).toBe(true);

    expect(queue.queuedMessageBytes).toBe(kafkaMessageRetainedBytes(newest));
    expect(queue.dequeue()).toMatchObject({
      payload: {
        droppedMessages: 1,
        messages: [{ offset: "2" }],
      },
    });
  });
});
