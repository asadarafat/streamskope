import { describe, expect, it } from "vitest";

import type {
  KafkaClusterDetailsDocument,
  KafkaClusterProfileContext,
  KafkaConfigurationEntry,
} from "../../src/features/kafka/contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaClusterDiagnosticsService,
  KafkaClusterDiagnosticsValidationError,
  type KafkaClusterDiagnosticsSessionPort,
  type KafkaClusterMetadata,
} from "../../src/features/kafka/application";

const profile: KafkaClusterProfileContext = {
  brokers: ["127.0.0.1:19093"],
  id: "profile-local",
  name: "Local validation",
};

const metadata: KafkaClusterMetadata = {
  brokers: [
    { host: "kafka-7", nodeId: 7, port: 9093, rack: "rack-b" },
    { host: "kafka-2", nodeId: 2, port: 9094, rack: null },
  ],
  clusterId: "fixture-cluster",
  controllerId: 7,
};

const configurationEntries: readonly KafkaConfigurationEntry[] = [
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
  {
    documentation: null,
    isDefault: false,
    isSensitive: true,
    name: "ssl.keystore.password",
    readOnly: true,
    source: "static-broker",
    synonyms: [
      {
        name: "ssl.keystore.password",
        source: "static-broker",
        value: "unique-broker-secret",
      },
    ],
    type: "password",
    value: "unique-broker-secret",
  },
];

interface Deferred<Value> {
  readonly promise: Promise<Value>;
  reject(reason: unknown): void;
  resolve(value: Value): void;
}

function deferred<Value>(): Deferred<Value> {
  let resolvePromise: ((value: Value) => void) | undefined;
  let rejectPromise: ((reason: unknown) => void) | undefined;
  const promise = new Promise<Value>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    reject: (reason): void => {
      rejectPromise?.(reason);
    },
    resolve: (value): void => {
      resolvePromise?.(value);
    },
  };
}

function structuredFailure(
  code: "AUTHORIZATION_DENIED" | "TIMEOUT",
  summary: string,
): Error & {
  readonly code: "AUTHORIZATION_DENIED" | "TIMEOUT";
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: "authorization" | "kafka";
} {
  return Object.assign(new Error(summary), {
    code,
    recovery: "Retry after correcting broker access.",
    retryable: code === "TIMEOUT",
    stage: code === "AUTHORIZATION_DENIED" ? ("authorization" as const) : ("kafka" as const),
  });
}

class RecordingSession implements KafkaClusterDiagnosticsSessionPort {
  readonly brokerCalls: number[] = [];
  context: ReturnType<KafkaClusterDiagnosticsSessionPort["activeConnectionContext"]> = {
    connectionBrokers: profile.brokers,
    connectionName: profile.name,
    connectionTarget: profile.brokers.join(", "),
  };
  metadataOperations: Array<() => Promise<KafkaClusterMetadata>> = [
    (): Promise<KafkaClusterMetadata> => Promise.resolve(metadata),
  ];
  configurationOperations: Array<() => Promise<readonly KafkaConfigurationEntry[]>> = [
    (): Promise<readonly KafkaConfigurationEntry[]> => Promise.resolve(configurationEntries),
  ];

  activeConnectionContext(): ReturnType<
    KafkaClusterDiagnosticsSessionPort["activeConnectionContext"]
  > {
    return this.context;
  }

  describeBrokerConfiguration(
    brokerId: number,
    _signal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]> {
    this.brokerCalls.push(brokerId);
    const operation = this.configurationOperations.shift();
    return operation === undefined ? Promise.resolve([]) : operation();
  }

  describeClusterMetadata(_signal?: AbortSignal): Promise<KafkaClusterMetadata> {
    const operation = this.metadataOperations.shift();
    return operation === undefined ? Promise.resolve(metadata) : operation();
  }
}

function service(session: RecordingSession): KafkaClusterDiagnosticsService {
  return new KafkaClusterDiagnosticsService(session, {
    now: () => new Date("2026-07-25T13:00:00.000Z"),
  });
}

describe("Kafka cluster-diagnostics application service", () => {
  it("loads the controller configuration, sorts data and removes sensitive values", async () => {
    const session = new RecordingSession();
    const diagnostics = service(session);

    await expect(diagnostics.load(profile)).resolves.toEqual({
      document: {
        cluster: {
          brokers: [
            { host: "kafka-2", nodeId: 2, port: 9094, rack: null },
            { host: "kafka-7", nodeId: 7, port: 9093, rack: "rack-b" },
          ],
          clusterId: "fixture-cluster",
          configuration: [
            configurationEntries[0],
            {
              ...configurationEntries[1],
              synonyms: [
                {
                  name: "ssl.keystore.password",
                  source: "static-broker",
                  value: null,
                },
              ],
              value: null,
            },
          ],
          configurationSourceBrokerId: 7,
          controllerId: 7,
        },
        endpoint: "127.0.0.1:19093",
        fetchedAt: "2026-07-25T13:00:00.000Z",
        profile,
      },
      state: "ready",
    });
    expect(session.brokerCalls).toEqual([7]);
    expect(JSON.stringify(diagnostics.currentDocument())).not.toContain("unique-broker-secret");
  });

  it("uses the lowest broker when the controller is unavailable", async () => {
    const session = new RecordingSession();
    session.metadataOperations = [
      (): Promise<KafkaClusterMetadata> =>
        Promise.resolve({
          ...metadata,
          controllerId: null,
        }),
    ];

    const result = await service(session).load(profile);

    expect(result.document.cluster.configurationSourceBrokerId).toBe(2);
    expect(session.brokerCalls).toEqual([2]);
  });

  it("returns honest partial snapshots for no brokers, authorization and timeout", async () => {
    const noBrokerSession = new RecordingSession();
    noBrokerSession.metadataOperations = [
      (): Promise<KafkaClusterMetadata> =>
        Promise.resolve({
          brokers: [],
          clusterId: "empty-cluster",
          controllerId: null,
        }),
    ];
    await expect(service(noBrokerSession).load(profile)).resolves.toMatchObject({
      document: {
        cluster: {
          brokers: [],
          configuration: [],
          configurationIssue: { code: "no-brokers" },
          configurationSourceBrokerId: null,
        },
      },
      state: "partial",
    });
    expect(noBrokerSession.brokerCalls).toEqual([]);

    for (const [code, expected] of [
      ["AUTHORIZATION_DENIED", "authorization-denied"],
      ["TIMEOUT", "unavailable"],
    ] as const) {
      const session = new RecordingSession();
      session.configurationOperations = [
        (): Promise<readonly KafkaConfigurationEntry[]> =>
          Promise.reject(structuredFailure(code, `Broker config ${code}`)),
      ];
      await expect(service(session).load(profile)).resolves.toMatchObject({
        document: {
          cluster: {
            configuration: [],
            configurationIssue: { code: expected },
            configurationSourceBrokerId: 7,
          },
        },
        state: "partial",
      });
    }
  });

  it("rethrows cancellation and suppresses a late obsolete load", async () => {
    const session = new RecordingSession();
    session.configurationOperations = [
      (): Promise<readonly KafkaConfigurationEntry[]> =>
        Promise.reject(new ConnectionAttemptSupersededError()),
    ];
    await expect(service(session).load(profile)).rejects.toBeInstanceOf(
      ConnectionAttemptSupersededError,
    );

    const first = deferred<KafkaClusterMetadata>();
    session.metadataOperations = [
      (): Promise<KafkaClusterMetadata> => first.promise,
      (): Promise<KafkaClusterMetadata> => Promise.resolve(metadata),
    ];
    session.configurationOperations = [
      (): Promise<readonly KafkaConfigurationEntry[]> => Promise.resolve(configurationEntries),
    ];
    const diagnostics = service(session);
    const obsolete = diagnostics.load(profile);
    const current = diagnostics.load(profile);
    await expect(current).resolves.toMatchObject({ state: "ready" });
    first.resolve(metadata);
    await expect(obsolete).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
  });

  it("retains prior same-session data only as stale while refresh is unresolved or fails", async () => {
    const session = new RecordingSession();
    const diagnostics = service(session);
    const first = await diagnostics.load(profile);
    const stalled = deferred<KafkaClusterMetadata>();
    session.metadataOperations = [(): Promise<KafkaClusterMetadata> => stalled.promise];

    const refresh = diagnostics.load(profile);

    expect(diagnostics.currentDocument()).toEqual(null);
    expect(diagnostics.staleDocument()).toEqual(first.document);
    expect(() => diagnostics.exportDocument()).toThrow(KafkaClusterDiagnosticsValidationError);
    stalled.reject(structuredFailure("TIMEOUT", "Metadata timed out"));
    await expect(refresh).rejects.toMatchObject({ code: "TIMEOUT" });
    expect(diagnostics.staleDocument()).toEqual(first.document);

    diagnostics.clear();
    expect(diagnostics.staleDocument()).toBeNull();
  });

  it("exports only the exact fresh current document as bounded canonical JSON", async () => {
    const session = new RecordingSession();
    const diagnostics = service(session);
    const result = await diagnostics.load(profile);

    const exported = diagnostics.exportDocument();

    expect(exported.content).toBe(`${JSON.stringify(result.document, null, 2)}\n`);
    expect(exported.byteSize).toBe(new TextEncoder().encode(exported.content).byteLength);
    expect(exported.fileName).toBe("streamskope-cluster-fixture-cluster.json");
    expect(exported.mediaType).toBe("application/json");
    expect(JSON.parse(exported.content) as KafkaClusterDetailsDocument).toEqual(result.document);
    expect(exported.content).not.toContain("unique-broker-secret");

    session.context = {
      connectionBrokers: ["replacement:9093"],
      connectionName: "Replacement",
      connectionTarget: "replacement:9093",
    };
    expect(() => diagnostics.exportDocument()).toThrow(KafkaClusterDiagnosticsValidationError);
  });

  it("rejects mismatched profile identity and malformed broker metadata", async () => {
    const mismatched = new RecordingSession();
    await expect(
      service(mismatched).load({
        ...profile,
        name: "Another connection",
      }),
    ).rejects.toBeInstanceOf(KafkaClusterDiagnosticsValidationError);

    const duplicate = new RecordingSession();
    duplicate.metadataOperations = [
      (): Promise<KafkaClusterMetadata> =>
        Promise.resolve({
          ...metadata,
          brokers: [metadata.brokers[0]!, metadata.brokers[0]!],
        }),
    ];
    await expect(service(duplicate).load(profile)).rejects.toBeInstanceOf(
      KafkaClusterDiagnosticsValidationError,
    );
  });
});
