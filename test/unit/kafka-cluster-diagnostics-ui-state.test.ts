import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostError,
  type HostEvent,
  type KafkaClusterDiagnosticsSnapshot,
} from "../../src/features/kafka/contracts";
import { initialKafkaUiState, reduceKafkaUiState } from "../../src/features/kafka/ui/state";

const profile = {
  brokers: ["127.0.0.1:19093"],
  id: "profile-local",
  name: "Local validation",
} as const;

const document = {
  cluster: {
    brokers: [{ host: "kafka", nodeId: 1, port: 9093, rack: null }],
    clusterId: "fixture-cluster",
    configuration: [],
    configurationSourceBrokerId: 1,
    controllerId: 1,
  },
  endpoint: "127.0.0.1:19093",
  fetchedAt: "2026-07-25T13:00:00.000Z",
  profile,
} as const;

const timeout: HostError = {
  activeStateChanged: false,
  code: "TIMEOUT",
  correlationId: "cluster-correlation",
  recovery: "Retry cluster details.",
  retryable: true,
  stage: "broker",
  summary: "Kafka broker metadata access timed out.",
};

function event(payload: KafkaClusterDiagnosticsSnapshot, sequence: number): HostEvent {
  return {
    event: "clusterDetails.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka cluster-diagnostics UI state", () => {
  it("starts unavailable and accepts loading, ready and stale host truth", () => {
    expect(initialKafkaUiState.clusterDiagnostics).toEqual({
      cluster: null,
      endpoint: null,
      fetchedAt: null,
      profile: null,
      state: "unavailable",
    });

    const loading = reduceKafkaUiState(initialKafkaUiState, {
      event: event(
        {
          cluster: null,
          endpoint: document.endpoint,
          fetchedAt: null,
          profile,
          state: "loading",
        },
        1,
      ),
      type: "host.event",
    });
    expect(loading.clusterDiagnostics.state).toBe("loading");

    const ready = reduceKafkaUiState(loading, {
      event: event({ ...document, state: "ready" }, 2),
      type: "host.event",
    });
    expect(ready.clusterDiagnostics).toMatchObject({
      cluster: { clusterId: "fixture-cluster" },
      state: "ready",
    });

    const stale = reduceKafkaUiState(ready, {
      event: event({ ...document, error: timeout, state: "stale" }, 3),
      type: "host.event",
    });
    expect(stale.clusterDiagnostics).toMatchObject({
      error: { code: "TIMEOUT" },
      state: "stale",
    });
  });

  it("clears cluster context on connection replacement and ignores obsolete events", () => {
    const ready = reduceKafkaUiState(initialKafkaUiState, {
      event: event({ ...document, state: "ready" }, 4),
      type: "host.event",
    });
    const disconnected = reduceKafkaUiState(ready, {
      event: {
        event: "connection.state",
        payload: {
          connectionName: profile.name,
          state: "disconnecting",
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      },
      type: "host.event",
    });
    expect(disconnected.clusterDiagnostics).toEqual(initialKafkaUiState.clusterDiagnostics);

    const obsolete = reduceKafkaUiState(disconnected, {
      event: event({ ...document, state: "ready" }, 4),
      type: "host.event",
    });
    expect(obsolete).toBe(disconnected);
  });
});
