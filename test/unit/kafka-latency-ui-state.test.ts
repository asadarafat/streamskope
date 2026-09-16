import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type KafkaLatencyHistorySnapshot,
  type KafkaLatencyProbeEvidence,
  type KafkaLatencySnapshot,
} from "../../src/features/kafka/contracts";
import { initialKafkaUiState, reduceKafkaUiState } from "../../src/features/kafka/ui/state";

const request = {
  acknowledgements: -1,
  messageCount: 1,
  timeoutMs: 10_000,
  topic: "orders.events",
} as const;

const evidence: KafkaLatencyProbeEvidence = {
  acknowledgements: -1,
  completedAt: "2026-07-25T16:00:01.000Z",
  connection: { endpoint: "127.0.0.1:19093", name: "local-aio" },
  endToEnd: { averageMs: 5, p95Ms: 5, samples: 1 },
  fetch: {
    perBroker: [
      {
        broker: "kafka-1:9093",
        nodeId: 1,
        summary: { averageMs: 2, p95Ms: 2, samples: 1 },
      },
    ],
    summary: { averageMs: 2, p95Ms: 2, samples: 1 },
  },
  issues: [],
  network: {
    endpoint: "127.0.0.1:19093",
    tcpConnectMs: 1,
    tlsHandshakeMs: 2,
  },
  observedMessages: 1,
  producer: {
    semantics: "acknowledged",
    summary: { averageMs: 3, p95Ms: 3, samples: 1 },
  },
  requestedMessages: 1,
  runId: "run-123",
  sampleIds: ["sample-1"],
  schema: "streamskope.kafka-latency.v1",
  startedAt: "2026-07-25T16:00:00.000Z",
  topic: "orders.events",
};

function event(payload: KafkaLatencySnapshot, sequence: number): HostEvent {
  return {
    event: "latency.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function historyEvent(payload: KafkaLatencyHistorySnapshot, sequence: number): HostEvent {
  return {
    event: "latency.history.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka latency UI state", () => {
  it("starts unavailable and accepts host-owned running, ready and partial truth", () => {
    expect(initialKafkaUiState.latency).toEqual({
      evidence: null,
      request: null,
      state: "unavailable",
    });
    const running = reduceKafkaUiState(initialKafkaUiState, {
      event: event({ evidence: null, request, state: "running" }, 1),
      type: "host.event",
    });
    expect(running.latency).toEqual({ evidence: null, request, state: "running" });

    const ready = reduceKafkaUiState(running, {
      event: event({ evidence, request: null, state: "ready" }, 2),
      type: "host.event",
    });
    expect(ready.latency).toMatchObject({
      evidence: { runId: "run-123" },
      state: "ready",
    });

    const partialEvidence = {
      ...evidence,
      issues: [
        {
          recovery: "Verify TLS trust.",
          stage: "tls" as const,
          summary: "TLS evidence is unavailable.",
        },
      ],
      network: { ...evidence.network, tlsHandshakeMs: null },
    };
    const partial = reduceKafkaUiState(ready, {
      event: event({ evidence: partialEvidence, request: null, state: "partial" }, 3),
      type: "host.event",
    });
    expect(partial.latency).toMatchObject({
      evidence: {
        issues: [{ stage: "tls" }],
        network: { tlsHandshakeMs: null },
      },
      state: "partial",
    });
  });

  it("invalidates current evidence on connection replacement and ignores late events", () => {
    const withHistory = reduceKafkaUiState(initialKafkaUiState, {
      event: historyEvent(
        {
          connectionName: "local-aio",
          entries: [
            {
              acknowledgements: -1,
              completedAt: evidence.completedAt,
              endToEnd: { averageMs: 5, p95Ms: 5 },
              fetch: { averageMs: 2, p95Ms: 2 },
              issueCount: 0,
              observedMessages: 1,
              producer: { averageMs: 3, p95Ms: 3 },
              requestedMessages: 1,
              runId: evidence.runId,
              state: "ready",
              topic: evidence.topic,
            },
          ],
        },
        3,
      ),
      type: "host.event",
    });
    const ready = reduceKafkaUiState(withHistory, {
      event: event({ evidence, request: null, state: "ready" }, 4),
      type: "host.event",
    });
    expect(ready.latencyHistory.entries).toHaveLength(1);
    const connecting = reduceKafkaUiState(ready, {
      event: {
        event: "connection.state",
        payload: { connectionName: "replacement", state: "connecting" },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      },
      type: "host.event",
    });
    expect(connecting.latency).toEqual(initialKafkaUiState.latency);
    expect(connecting.latencyHistory).toEqual(initialKafkaUiState.latencyHistory);

    const late = reduceKafkaUiState(connecting, {
      event: event({ evidence, request: null, state: "ready" }, 4),
      type: "host.event",
    });
    expect(late).toBe(connecting);
  });
});
