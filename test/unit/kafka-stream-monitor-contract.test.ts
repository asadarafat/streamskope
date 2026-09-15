import { describe, expect, it } from "vitest";

import {
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
  KAFKA_STREAM_MONITOR_STATES,
  KAFKA_STREAM_MONITOR_STATUSES,
  HostContractValidationError,
  parseHostEvent,
} from "../../src/kafka/contracts";

const request = {
  maxMessages: 100,
  mode: "tail",
  topic: "orders.events",
} as const;

const snapshot = {
  connectionName: "local-aio",
  delivery: {
    batchCount: 2,
    batchSize: 50,
    deliveredMessages: 3,
    historySamples: 25,
    intervalMs: 100,
    lastBatchMessages: 1,
    messagesPerSecond: 30,
    publicationDurationMs: 0.4,
    queueWaitMs: 1.2,
    receivedMessages: 4,
    tuningSource: "confirmed",
  },
  queue: {
    capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
    capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
    currentBytes: 0,
    currentMessages: 0,
    droppedMessages: 1,
    droppedPerSecond: 10,
    droppedSincePrevious: 1,
    peakBytes: 300,
    peakMessages: 3,
  },
  request,
  sampledAt: "2026-07-26T09:00:00.000Z",
  state: "streaming",
  status: "backpressure",
} as const;

function event(payload: unknown): unknown {
  return {
    event: "streamMetrics.changed",
    payload,
    sequence: 42,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka stream-monitor contract", () => {
  it("declares the additive bounded protocol vocabulary", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_EVENTS).toContain("streamMetrics.changed");
    expect(KAFKA_STREAM_MONITOR_HISTORY_LIMIT).toBe(400);
    expect(KAFKA_STREAM_MONITOR_STATES).toEqual([
      "unavailable",
      "loading",
      "fetching",
      "streaming",
      "complete",
      "stopped",
      "empty",
      "failed",
      "stale",
    ]);
    expect(KAFKA_STREAM_MONITOR_STATUSES).toEqual([
      "unavailable",
      "idle",
      "nominal",
      "backpressure",
      "degraded",
      "stale",
    ]);
  });

  it("parses exact aggregate queue, delivery, ownership and sampling evidence", () => {
    expect(parseHostEvent(event(snapshot))).toEqual(event(snapshot));

    const nominal = {
      ...snapshot,
      delivery: {
        ...snapshot.delivery,
        deliveredMessages: 4,
        receivedMessages: 4,
      },
      queue: {
        ...snapshot.queue,
        droppedMessages: 0,
        droppedPerSecond: 0,
        droppedSincePrevious: 0,
      },
      status: "nominal",
    } as const;
    expect(parseHostEvent(event(nominal))).toEqual(event(nominal));
  });

  it("rejects undeclared tuning sources and mismatched effective count bounds", () => {
    expect(() =>
      parseHostEvent(
        event({
          ...snapshot,
          delivery: {
            ...snapshot.delivery,
            tuningSource: "guessed",
          },
        }),
      ),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostEvent(
        event({
          ...snapshot,
          delivery: {
            ...snapshot.delivery,
            batchSize: 10,
            lastBatchMessages: 11,
          },
        }),
      ),
    ).toThrow(HostContractValidationError);
  });

  it("preserves unavailable measurements as null and stale evidence as stale", () => {
    const unavailable = {
      connectionName: null,
      delivery: null,
      queue: null,
      request: null,
      sampledAt: null,
      state: "unavailable",
      status: "unavailable",
    } as const;
    expect(parseHostEvent(event(unavailable))).toEqual(event(unavailable));

    expect(
      parseHostEvent(
        event({
          ...snapshot,
          state: "stale",
          status: "stale",
        }),
      ),
    ).toMatchObject({
      payload: {
        delivery: { messagesPerSecond: 30 },
        queue: { droppedPerSecond: 10 },
        state: "stale",
        status: "stale",
      },
    });

    expect(
      parseHostEvent(
        event({
          ...snapshot,
          delivery: {
            ...snapshot.delivery,
            messagesPerSecond: null,
            publicationDurationMs: null,
            queueWaitMs: null,
          },
          queue: {
            ...snapshot.queue,
            droppedPerSecond: null,
          },
        }),
      ),
    ).toMatchObject({
      payload: {
        delivery: {
          messagesPerSecond: null,
          publicationDurationMs: null,
          queueWaitMs: null,
        },
        queue: { droppedPerSecond: null },
      },
    });
  });

  it.each([
    [
      "negative queue depth",
      {
        ...snapshot,
        queue: { ...snapshot.queue, currentMessages: -1 },
      },
    ],
    [
      "queue depth above its peak",
      {
        ...snapshot,
        queue: { ...snapshot.queue, currentMessages: 4, peakMessages: 3 },
      },
    ],
    [
      "queue peak above capacity",
      {
        ...snapshot,
        queue: {
          ...snapshot.queue,
          peakMessages: KAFKA_MESSAGE_LIMITS.queuedMessages + 1,
        },
      },
    ],
    [
      "wrong canonical capacity",
      {
        ...snapshot,
        queue: {
          ...snapshot.queue,
          capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes - 1,
        },
      },
    ],
    [
      "drop delta above cumulative drops",
      {
        ...snapshot,
        queue: {
          ...snapshot.queue,
          droppedMessages: 1,
          droppedSincePrevious: 2,
        },
      },
    ],
    [
      "non-finite rate",
      {
        ...snapshot,
        delivery: { ...snapshot.delivery, messagesPerSecond: Number.NaN },
      },
    ],
    [
      "inconsistent received total",
      {
        ...snapshot,
        delivery: { ...snapshot.delivery, receivedMessages: 5 },
      },
    ],
    [
      "last batch above the canonical batch limit",
      {
        ...snapshot,
        delivery: {
          ...snapshot.delivery,
          lastBatchMessages: KAFKA_MESSAGE_LIMITS.batchMessages + 1,
        },
      },
    ],
    [
      "batch count without delivered messages",
      {
        ...snapshot,
        delivery: {
          ...snapshot.delivery,
          batchCount: 0,
        },
      },
    ],
    [
      "nominal status with confirmed drops",
      {
        ...snapshot,
        status: "nominal",
      },
    ],
    [
      "stale lifecycle without stale status",
      {
        ...snapshot,
        state: "stale",
      },
    ],
    [
      "unavailable lifecycle with current evidence",
      {
        ...snapshot,
        state: "unavailable",
        status: "unavailable",
      },
    ],
    [
      "message content in aggregate evidence",
      {
        ...snapshot,
        queue: {
          ...snapshot.queue,
          messagePayload: '{"secret":true}',
        },
      },
    ],
    [
      "undeclared root field",
      {
        ...snapshot,
        consumerGroup: "streamskope",
      },
    ],
  ])("rejects %s", (_label, payload) => {
    expect(() => parseHostEvent(event(payload))).toThrow(HostContractValidationError);
  });

  it("rejects a non-canonical sample timestamp and the prior protocol version", () => {
    expect(() =>
      parseHostEvent(
        event({
          ...snapshot,
          sampledAt: "July 26, 2026",
        }),
      ),
    ).toThrow(HostContractValidationError);

    expect(() =>
      parseHostEvent({
        event: "streamMetrics.changed",
        payload: snapshot,
        sequence: 42,
        version: 9,
      }),
    ).toThrow(HostContractValidationError);
  });
});
