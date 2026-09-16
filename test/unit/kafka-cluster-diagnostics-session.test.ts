import { describe, expect, it } from "vitest";

import type {
  KafkaConfigurationEntry,
  KafkaFetchRequest,
  KafkaLatencyProbeRequest,
  KafkaMessage,
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
  SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaApplicationSession,
  type KafkaActiveConnection,
  type KafkaClusterMetadata,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaLatencyProbeMeasurement,
  type KafkaMessageStream,
} from "../../src/features/kafka/application";

const connectionInput: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

const metadata: KafkaClusterMetadata = {
  brokers: [{ host: "kafka", nodeId: 1, port: 9093, rack: null }],
  clusterId: "fixture-cluster",
  controllerId: 1,
};

const brokerEntry: KafkaConfigurationEntry = {
  documentation: null,
  isDefault: true,
  isSensitive: false,
  name: "num.partitions",
  readOnly: false,
  source: "default",
  synonyms: [],
  type: "int",
  value: "1",
};

const latencyRequest: KafkaLatencyProbeRequest = {
  acknowledgements: -1,
  messageCount: 1,
  timeoutMs: 10_000,
  topic: "orders.events",
};

const latencyMeasurement: KafkaLatencyProbeMeasurement = {
  endToEndDurationsMs: [4],
  fetchSamples: [{ broker: "kafka:9093", durationMs: 2, nodeId: 1 }],
  issues: [],
  network: {
    endpoint: "localhost:19093",
    tcpConnectMs: 1,
    tlsHandshakeMs: 2,
  },
  observedSampleIds: ["sample-1"],
  producerDurationsMs: [3],
};

class EmptyMessageStream implements KafkaMessageStream {
  close(): Promise<void> {
    return Promise.resolve();
  }

  [Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    return {
      next: (): Promise<IteratorResult<KafkaMessage>> =>
        Promise.resolve({ done: true, value: undefined }),
    };
  }
}

class RecordingConnection implements KafkaActiveConnection {
  readonly brokerConfigCalls: Array<{ brokerId: number; signal?: AbortSignal }> = [];
  readonly metadataCalls: Array<AbortSignal | undefined> = [];
  readonly latencyCalls: Array<{ signal: AbortSignal; runId: string }> = [];
  metadataOperation: (signal: AbortSignal | undefined) => Promise<KafkaClusterMetadata> = () =>
    Promise.resolve(metadata);
  latencyOperation: (signal: AbortSignal) => Promise<KafkaLatencyProbeMeasurement> = () =>
    Promise.resolve(latencyMeasurement);

  alterTopicConfiguration(
    _topic: string,
    _changes: readonly KafkaTopicConfigurationChange[],
    _validateOnly: boolean,
    _signal?: AbortSignal,
  ): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  describeBrokerConfiguration(
    brokerId: number,
    signal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]> {
    this.brokerConfigCalls.push({
      brokerId,
      ...(signal === undefined ? {} : { signal }),
    });
    return Promise.resolve([brokerEntry]);
  }

  describeClusterMetadata(signal?: AbortSignal): Promise<KafkaClusterMetadata> {
    this.metadataCalls.push(signal);
    return this.metadataOperation(signal);
  }

  describeTopicConfiguration(
    _topic: string,
    _signal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]> {
    return Promise.resolve([]);
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve([]);
  }

  openMessageStream(
    _request: KafkaFetchRequest,
    _signal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    return Promise.resolve(new EmptyMessageStream());
  }

  runLatencyProbe(
    _request: KafkaLatencyProbeRequest,
    runId: string,
    signal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement> {
    this.latencyCalls.push({ runId, signal });
    return this.latencyOperation(signal);
  }
}

class ImmediateConnectionPort implements KafkaConnectionPort {
  constructor(private readonly connection: RecordingConnection) {}

  openConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    return Promise.resolve(this.connection);
  }

  testConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 0 });
  }
}

class SequencedConnectionPort implements KafkaConnectionPort {
  readonly openCalls: SecureConnectionInput[] = [];

  constructor(private readonly connections: readonly RecordingConnection[]) {}

  openConnection(
    connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    this.openCalls.push(connection);
    const opened = this.connections[this.openCalls.length - 1];
    return opened === undefined
      ? Promise.reject(new Error("The connection fixture was exhausted."))
      : Promise.resolve(opened);
  }

  testConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 0 });
  }
}

async function connectedSession(connection: RecordingConnection): Promise<KafkaApplicationSession> {
  const session = new KafkaApplicationSession(new ImmediateConnectionPort(connection));
  await session.connect(connectionInput);
  return session;
}

describe("Kafka application cluster-diagnostics lifecycle", () => {
  it("forwards metadata and one broker configuration through the active connection", async () => {
    const connection = new RecordingConnection();
    const session = await connectedSession(connection);

    await expect(session.describeClusterMetadata()).resolves.toEqual(metadata);
    await expect(session.describeBrokerConfiguration(1)).resolves.toEqual([brokerEntry]);

    expect(connection.metadataCalls).toHaveLength(1);
    expect(connection.brokerConfigCalls).toMatchObject([{ brokerId: 1 }]);
  });

  it("cancels an obsolete metadata request before a newer refresh starts", async () => {
    const connection = new RecordingConnection();
    connection.metadataOperation = (signal): Promise<KafkaClusterMetadata> =>
      new Promise((_resolve, reject) => {
        signal?.addEventListener(
          "abort",
          () => {
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      });
    const session = await connectedSession(connection);

    const obsolete = session.describeClusterMetadata();
    connection.metadataOperation = (): Promise<KafkaClusterMetadata> => Promise.resolve(metadata);
    const current = session.describeClusterMetadata();

    await expect(obsolete).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(current).resolves.toEqual(metadata);
    expect(connection.metadataCalls[0]?.aborted).toBe(true);
  });

  it("does not cancel an active bounded latency probe when diagnostics refresh", async () => {
    const connection = new RecordingConnection();
    let resolveLatency: ((measurement: KafkaLatencyProbeMeasurement) => void) | undefined;
    connection.latencyOperation = (signal): Promise<KafkaLatencyProbeMeasurement> =>
      new Promise((resolve, reject) => {
        resolveLatency = resolve;
        signal.addEventListener(
          "abort",
          () => {
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      });
    const session = await connectedSession(connection);

    const pendingLatency = session.runLatencyProbe(
      latencyRequest,
      "run-current",
      new AbortController().signal,
    );
    await expect(session.describeClusterMetadata()).resolves.toEqual(metadata);
    expect(connection.latencyCalls[0]?.signal.aborted).toBe(false);

    resolveLatency?.(latencyMeasurement);
    await expect(pendingLatency).resolves.toEqual(latencyMeasurement);
  });

  it("cancels and awaits an active bounded latency probe during disconnect", async () => {
    const connection = new RecordingConnection();
    connection.latencyOperation = (signal): Promise<KafkaLatencyProbeMeasurement> =>
      new Promise((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      });
    const session = await connectedSession(connection);

    const pendingLatency = session.runLatencyProbe(
      latencyRequest,
      "run-disconnected",
      new AbortController().signal,
    );
    const disconnect = session.disconnect();

    await expect(pendingLatency).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(disconnect).resolves.toBeUndefined();
    expect(connection.latencyCalls[0]?.signal.aborted).toBe(true);
    expect(session.snapshot()).toEqual({ connectionName: null, state: "disconnected" });
  });

  it("reports owned latency cleanup failure during disconnect", async () => {
    const connection = new RecordingConnection();
    const cleanupFailure = new Error("Latency consumer close failed.");
    connection.latencyOperation = (signal): Promise<KafkaLatencyProbeMeasurement> =>
      new Promise((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            reject(
              Object.assign(new Error("The latency probe was cancelled."), {
                cleanupCause: cleanupFailure,
                code: "CANCELLED",
              }),
            );
          },
          { once: true },
        );
      });
    const session = await connectedSession(connection);
    const pendingLatency = session.runLatencyProbe(
      latencyRequest,
      "run-cleanup-failed",
      new AbortController().signal,
    );

    const disconnect = session.disconnect();

    await expect(pendingLatency).rejects.toMatchObject({
      cleanupFailure,
      name: "ConnectionAttemptSupersededError",
    });
    await expect(disconnect).rejects.toThrow("did not close cleanly");
    expect(session.snapshot()).toMatchObject({
      connectionName: "Local aio",
      state: "failed",
    });
  });

  it("does not open a replacement connection until old probe cleanup settles", async () => {
    const oldConnection = new RecordingConnection();
    const replacementConnection = new RecordingConnection();
    let rejectLatency: ((error: unknown) => void) | undefined;
    oldConnection.latencyOperation = (): Promise<KafkaLatencyProbeMeasurement> =>
      new Promise((_resolve, reject) => {
        rejectLatency = reject;
      });
    const port = new SequencedConnectionPort([oldConnection, replacementConnection]);
    const session = new KafkaApplicationSession(port);
    await session.connect(connectionInput);
    const pendingLatency = session.runLatencyProbe(
      latencyRequest,
      "run-replaced",
      new AbortController().signal,
    );

    const replacement = session.connect({
      ...connectionInput,
      brokers: ["replacement:9093"],
      name: "Replacement",
    });
    await Promise.resolve();
    expect(oldConnection.latencyCalls[0]?.signal.aborted).toBe(true);
    expect(port.openCalls).toHaveLength(1);

    rejectLatency?.(new DOMException("Aborted", "AbortError"));
    await expect(pendingLatency).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(replacement).resolves.toBeUndefined();
    expect(port.openCalls).toHaveLength(2);
    expect(session.snapshot()).toEqual({
      connectionName: "Replacement",
      state: "connected",
    });
  });

  it("cancels metadata on disconnect and rejects diagnostics while disconnected", async () => {
    const connection = new RecordingConnection();
    connection.metadataOperation = (signal): Promise<KafkaClusterMetadata> =>
      new Promise((_resolve, reject) => {
        signal?.addEventListener(
          "abort",
          () => {
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      });
    const session = await connectedSession(connection);

    const pending = session.describeClusterMetadata();
    const disconnect = session.disconnect();

    await expect(pending).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await disconnect;
    await expect(session.describeClusterMetadata()).rejects.toThrow(
      "Connect to a Kafka cluster before reading cluster metadata.",
    );
    await expect(session.describeBrokerConfiguration(1)).rejects.toThrow(
      "Connect to a Kafka cluster before reading broker configuration.",
    );
  });
});
