import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
  type HostEvent,
  type KafkaFetchRequest,
  type KafkaStreamMonitorSnapshot,
} from "../../src/kafka/contracts";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/kafka/ui/state";

const firstRequest: KafkaFetchRequest = {
  maxMessages: 100,
  mode: "tail",
  topic: "orders.events",
};

function snapshot(
  sample: number,
  request: KafkaFetchRequest = firstRequest,
): KafkaStreamMonitorSnapshot {
  const deliveredMessages = sample + 1;
  return {
    connectionName: "local-aio",
    delivery: {
      batchCount: deliveredMessages,
      batchSize: 200,
      deliveredMessages,
      historySamples: 50,
      intervalMs: 20,
      lastBatchMessages: 1,
      messagesPerSecond: deliveredMessages * 10,
      publicationDurationMs: 0.5,
      queueWaitMs: 1,
      receivedMessages: deliveredMessages,
      tuningSource: "confirmed",
    },
    queue: {
      capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
      capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
      currentBytes: 0,
      currentMessages: 0,
      droppedMessages: 0,
      droppedPerSecond: 0,
      droppedSincePrevious: 0,
      peakBytes: 128,
      peakMessages: 1,
    },
    request,
    sampledAt: new Date(Date.UTC(2026, 6, 26, 9, 0, sample)).toISOString(),
    state: "streaming",
    status: "nominal",
  };
}

function monitorEvent(
  sequence: number,
  payload: KafkaStreamMonitorSnapshot,
): Extract<HostEvent, { readonly event: "streamMetrics.changed" }> {
  return {
    event: "streamMetrics.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka stream-monitor UI state", () => {
  it("starts unavailable with no manufactured sample or history", () => {
    expect(initialKafkaUiState.streamMonitor).toEqual({
      current: {
        connectionName: null,
        delivery: null,
        queue: null,
        request: null,
        sampledAt: null,
        state: "unavailable",
        status: "unavailable",
      },
      history: [],
    });
  });

  it("retains only the newest samples within the operation's effective history", () => {
    let state = initialKafkaUiState;
    for (let index = 0; index <= KAFKA_STREAM_MONITOR_HISTORY_LIMIT; index += 1) {
      state = reduceKafkaHostEvent(state, monitorEvent(index + 1, snapshot(index)));
    }

    expect(state.streamMonitor.current).toEqual(snapshot(KAFKA_STREAM_MONITOR_HISTORY_LIMIT));
    expect(state.streamMonitor.history).toHaveLength(50);
    expect(state.streamMonitor.history[0]).toEqual(
      snapshot(KAFKA_STREAM_MONITOR_HISTORY_LIMIT - 49),
    );
    expect(state.streamMonitor.history.at(-1)).toEqual(
      snapshot(KAFKA_STREAM_MONITOR_HISTORY_LIMIT),
    );
  });

  it("resets history when a newer request becomes authoritative", () => {
    const first = reduceKafkaHostEvent(initialKafkaUiState, monitorEvent(1, snapshot(0)));
    const replacementRequest: KafkaFetchRequest = {
      maxMessages: 10,
      mode: "newest",
      topic: "payments.events",
    };
    const replacement = snapshot(1, replacementRequest);
    const state = reduceKafkaHostEvent(first, monitorEvent(2, replacement));

    expect(state.streamMonitor).toEqual({
      current: replacement,
      history: [replacement],
    });
  });

  it("marks retained evidence stale on disconnect without fabricating a sample", () => {
    const current = snapshot(0);
    const measured = reduceKafkaHostEvent(initialKafkaUiState, monitorEvent(1, current));
    const state = reduceKafkaHostEvent(measured, {
      event: "connection.state",
      payload: {
        connectionName: null,
        state: "disconnected",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.streamMonitor.current).toEqual({
      ...current,
      state: "stale",
      status: "stale",
    });
    expect(state.streamMonitor.history).toEqual([current]);
  });

  it("clears prior ownership when another connection becomes current", () => {
    const measured = reduceKafkaHostEvent(initialKafkaUiState, monitorEvent(1, snapshot(0)));
    const state = reduceKafkaHostEvent(measured, {
      event: "connection.state",
      payload: {
        connectionName: "other-cluster",
        state: "connected",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    expect(state.streamMonitor).toEqual(initialKafkaUiState.streamMonitor);
  });

  it("retains a terminal snapshot and ignores late lower sequences", () => {
    const terminal: KafkaStreamMonitorSnapshot = {
      ...snapshot(2),
      state: "complete",
    };
    const complete = reduceKafkaHostEvent(initialKafkaUiState, monitorEvent(5, terminal));
    const late = reduceKafkaHostEvent(complete, monitorEvent(4, snapshot(3)));

    expect(complete.streamMonitor.current).toEqual(terminal);
    expect(late).toBe(complete);
  });
});
