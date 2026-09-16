import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaFetchRequest,
  type KafkaLatencyProbeRequest,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaRuleStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  type KafkaActiveConnection,
  type KafkaClusterMetadata,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaLatencyProbeMeasurement,
  type KafkaMessageStream,
} from "../../src/features/kafka/application";
import { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine";

const connection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

class LatencyConnection implements KafkaActiveConnection {
  closeCalls = 0;

  alterTopicConfiguration(): Promise<void> {
    return Promise.reject(new Error("Topic configuration is outside this fixture."));
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  describeBrokerConfiguration(): Promise<readonly never[]> {
    return Promise.resolve([]);
  }

  describeClusterMetadata(): Promise<KafkaClusterMetadata> {
    return Promise.resolve({
      brokers: [{ host: "kafka", nodeId: 1, port: 9093, rack: null }],
      clusterId: "fixture-cluster",
      controllerId: 1,
    });
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.resolve([]);
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve(["orders.events"]);
  }

  openMessageStream(
    _request: KafkaFetchRequest,
    _signal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    return Promise.reject(new Error("Message consumption is outside this fixture."));
  }

  runLatencyProbe(
    request: KafkaLatencyProbeRequest,
    runId: string,
    _signal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement> {
    const samples = Array.from({ length: request.messageCount }, (_value, index) => index + 1);
    return Promise.resolve({
      endToEndDurationsMs: samples.map((sample) => sample + 4),
      fetchSamples: [{ broker: "kafka:9093", durationMs: 2, nodeId: 1 }],
      issues: [],
      network: {
        endpoint: "localhost:19093",
        tcpConnectMs: 1,
        tlsHandshakeMs: 2,
      },
      observedSampleIds: samples.map((sample) => `${runId}-sample-${String(sample)}`),
      producerDurationsMs: samples.map((sample) => sample + 2),
    });
  }
}

class LatencyConnectionPort implements KafkaConnectionPort {
  constructor(private readonly connections: LatencyConnection[]) {}

  openConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    const opened = this.connections.shift();
    return opened === undefined
      ? Promise.reject(new Error("The connection fixture was exhausted."))
      : Promise.resolve(opened);
  }

  testConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 1 });
  }
}

function createFacade(port: KafkaConnectionPort): KafkaBackendFacade {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  const session = new KafkaApplicationSession(port);
  return new KafkaBackendFacade(
    session,
    new KafkaProfileService(
      new InMemoryKafkaProfileStore({
        durability: "session",
        protection: "memory",
        state: "ready",
      }),
      {
        decode: () => Promise.reject(new Error("Profile decode is outside this fixture.")),
      },
    ),
    new KafkaConnectionTemplateService(
      new InMemoryKafkaConnectionTemplateStore({
        durability: "session",
        state: "ready",
      }),
    ),
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    ),
    {
      createCorrelationId: () => "latency-lifecycle-correlation",
      now: () => new Date("2026-07-25T16:00:00.000Z"),
    },
  );
}

function connectCommand(
  id: string,
  input: SecureConnectionInput = connection,
): Extract<HostCommand, { readonly command: "connection.connect" }> {
  return {
    command: "connection.connect",
    id,
    payload: input,
    version: HOST_PROTOCOL_VERSION,
  };
}

function latencyCommand(name: "latency.export" | "latency.start", id: string): HostCommand {
  return name === "latency.start"
    ? {
        command: name,
        id,
        payload: {
          acknowledgements: -1,
          messageCount: 1,
          timeoutMs: 10_000,
          topic: "orders.events",
        },
        version: HOST_PROTOCOL_VERSION,
      }
    : {
        command: name,
        id,
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      };
}

describe("Kafka latency facade connection lifecycle", () => {
  it("marks completed evidence stale and non-exportable across replacement", async () => {
    const firstConnection = new LatencyConnection();
    const facade = createFacade(
      new LatencyConnectionPort([firstConnection, new LatencyConnection()]),
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(connectCommand("connect-first"));
    await expect(
      facade.execute(latencyCommand("latency.start", "latency-first")),
    ).resolves.toMatchObject({ ok: true });
    expect(
      events.some((event) => event.event === "latency.changed" && event.payload.state === "ready"),
    ).toBe(true);
    expect(
      events.find(
        (event) => event.event === "latency.history.changed" && event.payload.entries.length > 0,
      ),
    ).toMatchObject({
      payload: {
        connectionName: "Local aio",
        entries: [{ state: "ready", topic: "orders.events" }],
      },
    });
    events.length = 0;

    await expect(
      facade.execute(
        connectCommand("connect-replacement", {
          ...connection,
          name: "Replacement",
        }),
      ),
    ).resolves.toMatchObject({ ok: true });

    expect(
      events.some((event) => event.event === "latency.changed" && event.payload.state === "stale"),
    ).toBe(true);
    expect(
      events.some(
        (event) =>
          event.event === "latency.changed" &&
          event.payload.state === "idle" &&
          event.payload.evidence === null,
      ),
    ).toBe(true);
    expect(
      events.some(
        (event) =>
          event.event === "latency.history.changed" &&
          event.payload.connectionName === null &&
          event.payload.entries.length === 0,
      ),
    ).toBe(true);
    expect(firstConnection.closeCalls).toBe(1);
    await expect(
      facade.execute(latencyCommand("latency.export", "latency-stale-export")),
    ).resolves.toMatchObject({
      error: { code: "VALIDATION" },
      ok: false,
    });
  });
});
