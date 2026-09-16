import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaConfigurationEntry,
  type KafkaFetchRequest,
  type KafkaMessage,
  type ProfileStoreCapability,
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
  type KafkaMessageStream,
  type KafkaProfileRecord,
  type KafkaProfileTrustDecoder,
} from "../../src/features/kafka/application";
import { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { KafkaEngineFailure, StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine";

const connection: SecureConnectionInput = {
  brokers: ["127.0.0.1:19093"],
  name: "Local validation",
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

const profileCapability: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

const storedProfile: KafkaProfileRecord = {
  brokers: connection.brokers,
  createdAt: "2026-07-25T12:00:00.000Z",
  id: "profile-local",
  name: connection.name,
  trust: {
    kind: "pem",
    label: "ca.pem",
    material: connection.tls.caPem,
  },
  updatedAt: "2026-07-25T12:00:00.000Z",
};

const metadata: KafkaClusterMetadata = {
  brokers: [{ host: "kafka-1", nodeId: 1, port: 9093, rack: null }],
  clusterId: "fixture-cluster",
  controllerId: 1,
};

const configuration: readonly KafkaConfigurationEntry[] = [
  {
    documentation: null,
    isDefault: true,
    isSensitive: false,
    name: "num.partitions",
    readOnly: false,
    source: "default",
    synonyms: [],
    type: "int",
    value: "1",
  },
];

class EmptyStream implements KafkaMessageStream {
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

class RecordingActiveConnection implements KafkaActiveConnection {
  configurationOperations: Array<() => Promise<readonly KafkaConfigurationEntry[]>> = [
    (): Promise<readonly KafkaConfigurationEntry[]> => Promise.resolve(configuration),
  ];
  metadataOperations: Array<() => Promise<KafkaClusterMetadata>> = [
    (): Promise<KafkaClusterMetadata> => Promise.resolve(metadata),
  ];

  alterTopicConfiguration(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  describeBrokerConfiguration(
    _brokerId: number,
    _signal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]> {
    const operation = this.configurationOperations.shift();
    return operation === undefined ? Promise.resolve(configuration) : operation();
  }

  describeClusterMetadata(_signal?: AbortSignal): Promise<KafkaClusterMetadata> {
    const operation = this.metadataOperations.shift();
    return operation === undefined ? Promise.resolve(metadata) : operation();
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.resolve([]);
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve([]);
  }

  openMessageStream(
    _request: KafkaFetchRequest,
    _signal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    return Promise.resolve(new EmptyStream());
  }
}

class RecordingConnectionPort implements KafkaConnectionPort {
  readonly active = new RecordingActiveConnection();

  openConnection(
    _input: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    return Promise.resolve(this.active);
  }

  testConnection(
    _input: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 0 });
  }
}

class TrustDecoder implements KafkaProfileTrustDecoder {
  decode(): Promise<{ readonly caPem: string; readonly kind: "pem" }> {
    return Promise.resolve({ caPem: connection.tls.caPem, kind: "pem" });
  }
}

function command(
  name:
    | "clusterDetails.export"
    | "clusterDetails.load"
    | "connection.connect"
    | "connection.disconnect"
    | "profiles.connect",
  payload: HostCommand["payload"] = {},
): HostCommand {
  return {
    command: name,
    id: `${name}-request`,
    payload,
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand;
}

function setup(records: readonly KafkaProfileRecord[] = []): {
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly port: RecordingConnectionPort;
} {
  const port = new RecordingConnectionPort();
  const session = new KafkaApplicationSession(port);
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore(profileCapability, records),
    new TrustDecoder(),
  );
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  let correlation = 0;
  const facade = new KafkaBackendFacade(
    session,
    profiles,
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
      createCorrelationId: (): string => `cluster-correlation-${String(++correlation)}`,
      now: (): Date => new Date("2026-07-25T13:00:00.000Z"),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { events, facade, port };
}

function clusterEvents(
  events: readonly HostEvent[],
): Array<Extract<HostEvent, { readonly event: "clusterDetails.changed" }>> {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "clusterDetails.changed" }> =>
      event.event === "clusterDetails.changed",
  );
}

function activities(
  events: readonly HostEvent[],
): Array<Extract<HostEvent, { readonly event: "activity.recorded" }>> {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "activity.recorded" }> =>
      event.event === "activity.recorded",
  );
}

describe("Kafka cluster-diagnostics facade", () => {
  it("publishes loading and ready with direct-connection context and redacted Activity", async () => {
    const { events, facade } = setup();
    await facade.execute(command("connection.connect", connection));

    await expect(facade.execute(command("clusterDetails.load"))).resolves.toMatchObject({
      ok: true,
      result: { correlationId: "cluster-correlation-2" },
    });

    expect(clusterEvents(events).slice(-2)).toMatchObject([
      {
        payload: {
          endpoint: "127.0.0.1:19093",
          profile: { brokers: ["127.0.0.1:19093"], id: null, name: "Local validation" },
          state: "loading",
        },
      },
      {
        payload: {
          cluster: {
            brokers: [{ nodeId: 1 }],
            configuration: [{ name: "num.partitions" }],
          },
          profile: { id: null },
          state: "ready",
        },
      },
    ]);
    expect(activities(events).at(-1)?.payload).toMatchObject({
      correlationId: "cluster-correlation-2",
      object: "Local validation · 127.0.0.1:19093",
      operation: "Refresh cluster details",
      outcome: "succeeded",
    });
    expect(JSON.stringify(activities(events).at(-1))).not.toContain('"value":"1"');
  });

  it("enriches the snapshot with the active stored profile identifier", async () => {
    const { events, facade } = setup([storedProfile]);
    await facade.execute(command("profiles.connect", { profileId: storedProfile.id }));

    await facade.execute(command("clusterDetails.load"));

    expect(clusterEvents(events).at(-1)?.payload).toMatchObject({
      profile: {
        brokers: ["127.0.0.1:19093"],
        id: "profile-local",
        name: "Local validation",
      },
      state: "ready",
    });
  });

  it("publishes partial configuration and returns the exact host-produced export", async () => {
    const { events, facade, port } = setup();
    port.active.configurationOperations = [
      (): Promise<readonly KafkaConfigurationEntry[]> =>
        Promise.reject(
          new KafkaEngineFailure({
            code: "AUTHORIZATION_DENIED",
            recovery: "Request DESCRIBE_CONFIGS.",
            retryable: false,
            stage: "authorization",
            summary: "Kafka denied broker configuration.",
          }),
        ),
    ];
    await facade.execute(command("connection.connect", connection));
    await facade.execute(command("clusterDetails.load"));

    expect(clusterEvents(events).at(-1)?.payload).toMatchObject({
      cluster: {
        configuration: [],
        configurationIssue: { code: "authorization-denied" },
      },
      state: "partial",
    });

    const response = await facade.execute(command("clusterDetails.export"));
    expect(response).toMatchObject({
      command: "clusterDetails.export",
      ok: true,
      result: {
        correlationId: "cluster-correlation-3",
        document: {
          fileName: "streamskope-cluster-fixture-cluster.json",
          mediaType: "application/json",
        },
      },
    });
    if (!response.ok || !("document" in response.result)) {
      throw new Error("Expected the cluster export document.");
    }
    const exported = JSON.parse(response.result.document.content) as {
      readonly cluster: { readonly configurationIssue?: { readonly code: string } };
    };
    expect(exported.cluster.configurationIssue?.code).toBe("authorization-denied");
    expect(response.result.document.byteSize).toBe(
      new TextEncoder().encode(response.result.document.content).byteLength,
    );
    expect(activities(events).at(-1)?.payload).toMatchObject({
      operation: "Export cluster details",
      outcome: "succeeded",
    });
  });

  it("retains prior data as stale after refresh failure and rejects its export", async () => {
    const { events, facade, port } = setup();
    await facade.execute(command("connection.connect", connection));
    await facade.execute(command("clusterDetails.load"));
    port.active.metadataOperations = [
      (): Promise<KafkaClusterMetadata> =>
        Promise.reject(
          new KafkaEngineFailure({
            code: "TIMEOUT",
            recovery: "Retry.",
            retryable: true,
            stage: "broker",
            summary: "Kafka broker metadata access timed out.",
          }),
        ),
    ];

    await expect(facade.execute(command("clusterDetails.load"))).resolves.toMatchObject({
      error: { code: "TIMEOUT" },
      ok: false,
    });
    expect(clusterEvents(events).at(-1)?.payload).toMatchObject({
      cluster: { clusterId: "fixture-cluster" },
      error: { code: "TIMEOUT" },
      state: "stale",
    });
    await expect(facade.execute(command("clusterDetails.export"))).resolves.toMatchObject({
      error: {
        code: "VALIDATION",
        summary: "Refresh cluster details for the current connection before exporting.",
      },
      ok: false,
    });
  });

  it("clears diagnostics on disconnect and rejects disconnected load without a Kafka call", async () => {
    const { events, facade, port } = setup();
    await facade.execute(command("connection.connect", connection));
    await facade.execute(command("clusterDetails.load"));
    const metadataCallsBefore = port.active.metadataOperations.length;

    await facade.execute(command("connection.disconnect"));

    await expect(facade.execute(command("clusterDetails.load"))).resolves.toMatchObject({
      error: { code: "VALIDATION" },
      ok: false,
    });
    expect(clusterEvents(events).at(-1)?.payload).toEqual({
      cluster: null,
      endpoint: null,
      fetchedAt: null,
      profile: null,
      state: "unavailable",
    });
    expect(port.active.metadataOperations).toHaveLength(metadataCallsBefore);
  });
});
