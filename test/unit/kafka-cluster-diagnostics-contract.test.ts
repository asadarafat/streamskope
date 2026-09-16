import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_CLUSTER_CONFIGURATION_ISSUE_CODES,
  KAFKA_CLUSTER_DIAGNOSTIC_LIMITS,
  KAFKA_CLUSTER_DIAGNOSTIC_STATES,
  HostContractValidationError,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../src/features/kafka/contracts";

const profile = {
  brokers: ["127.0.0.1:19093"],
  id: "profile-local",
  name: "Local validation",
} as const;

const configurationEntry = {
  documentation: "The default number of log partitions.",
  isDefault: true,
  isSensitive: false,
  name: "num.partitions",
  readOnly: true,
  source: "default",
  synonyms: [
    {
      name: "num.partitions",
      source: "default",
      value: "1",
    },
  ],
  type: "int",
  value: "1",
} as const;

const cluster = {
  brokers: [
    {
      host: "kafka-1",
      nodeId: 1,
      port: 9093,
      rack: null,
    },
  ],
  clusterId: "fixture-cluster",
  configuration: [configurationEntry],
  configurationSourceBrokerId: 1,
  controllerId: 1,
} as const;

const document = {
  cluster,
  endpoint: "127.0.0.1:19093",
  fetchedAt: "2026-07-25T13:00:00.000Z",
  profile,
} as const;

const readySnapshot = {
  ...document,
  state: "ready",
} as const;

describe("Kafka cluster-diagnostics contract", () => {
  it("declares protocol v14, bounded cluster vocabulary and no mutation command", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining(["clusterDetails.load", "clusterDetails.export"]),
    );
    expect(HOST_COMMANDS).not.toEqual(
      expect.arrayContaining(["clusterDetails.alter", "clusterDetails.delete"]),
    );
    expect(HOST_EVENTS).toEqual(expect.arrayContaining(["clusterDetails.changed"]));
    expect(KAFKA_CLUSTER_DIAGNOSTIC_STATES).toEqual([
      "unavailable",
      "loading",
      "ready",
      "partial",
      "failed",
      "stale",
    ]);
    expect(KAFKA_CLUSTER_CONFIGURATION_ISSUE_CODES).toEqual([
      "authorization-denied",
      "no-brokers",
      "unavailable",
    ]);
    expect(KAFKA_CLUSTER_DIAGNOSTIC_LIMITS).toEqual({
      brokerHostCharacters: 512,
      brokers: 4_096,
      clusterIdCharacters: 512,
      endpointCharacters: 16_384,
      exportBytes: 8 * 1_048_576,
      fileNameCharacters: 255,
      profileBrokers: 32,
      profileIdCharacters: 128,
      profileNameCharacters: 256,
      snapshotBytes: 8 * 1_048_576,
    });
  });

  it("parses exact empty load and export commands", () => {
    for (const command of ["clusterDetails.load", "clusterDetails.export"] as const) {
      expect(
        parseHostCommand({
          command,
          id: command,
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toEqual({
        command,
        id: command,
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      expect(() =>
        parseHostCommand({
          command,
          id: `${command}-invalid`,
          payload: { profileId: "caller-controlled" },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("parses ready, partial and stale states with their exact invariants", () => {
    expect(
      parseHostEvent({
        event: "clusterDetails.changed",
        payload: readySnapshot,
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "clusterDetails.changed",
      payload: {
        cluster: {
          brokers: [{ nodeId: 1 }],
          configuration: [{ name: "num.partitions", value: "1" }],
        },
        state: "ready",
      },
    });

    const issue = {
      code: "authorization-denied",
      recovery: "Grant DESCRIBE_CONFIGS or continue with cluster metadata only.",
      summary: "Broker configuration is not permitted for this connection.",
    } as const;
    const partial = {
      ...readySnapshot,
      cluster: {
        ...cluster,
        configuration: [],
        configurationIssue: issue,
      },
      state: "partial",
    } as const;
    expect(
      parseHostEvent({
        event: "clusterDetails.changed",
        payload: partial,
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        cluster: { configurationIssue: { code: "authorization-denied" } },
        state: "partial",
      },
    });

    const refreshError = {
      activeStateChanged: false,
      code: "TIMEOUT",
      correlationId: "cluster-correlation",
      recovery: "Retry cluster details.",
      retryable: true,
      stage: "broker",
      summary: "Kafka broker metadata access timed out.",
      target: "127.0.0.1:19093",
    } as const;
    expect(
      parseHostEvent({
        event: "clusterDetails.changed",
        payload: {
          ...readySnapshot,
          error: refreshError,
          state: "stale",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: { error: { code: "TIMEOUT" }, state: "stale" },
    });

    for (const invalid of [
      { ...readySnapshot, state: "partial" },
      {
        ...readySnapshot,
        cluster: { ...cluster, configurationIssue: issue },
        state: "ready",
      },
      { ...readySnapshot, state: "stale" },
      {
        cluster: null,
        endpoint: readySnapshot.endpoint,
        fetchedAt: null,
        profile,
        state: "ready",
      },
    ]) {
      expect(() =>
        parseHostEvent({
          event: "clusterDetails.changed",
          payload: invalid,
          sequence: 4,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("rejects duplicate brokers, invalid ports and leaked sensitive configuration", () => {
    const sensitiveEntry = {
      ...configurationEntry,
      isSensitive: true,
      name: "ssl.keystore.password",
      synonyms: [
        {
          name: "ssl.keystore.password",
          source: "static-broker",
          value: null,
        },
      ],
      type: "password",
      value: null,
    } as const;
    expect(
      parseHostEvent({
        event: "clusterDetails.changed",
        payload: {
          ...readySnapshot,
          cluster: { ...cluster, configuration: [sensitiveEntry] },
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        cluster: {
          configuration: [{ isSensitive: true, value: null }],
        },
      },
    });

    const invalidClusters = [
      { ...cluster, brokers: [cluster.brokers[0], cluster.brokers[0]] },
      { ...cluster, brokers: [{ ...cluster.brokers[0], port: 65_536 }] },
      {
        ...cluster,
        configuration: [{ ...sensitiveEntry, value: "unique-broker-secret" }],
      },
      {
        ...cluster,
        configuration: [
          {
            ...sensitiveEntry,
            synonyms: [
              {
                name: "ssl.keystore.password",
                source: "static-broker",
                value: "unique-broker-secret",
              },
            ],
          },
        ],
      },
    ];
    for (const invalidCluster of invalidClusters) {
      expect(() =>
        parseHostEvent({
          event: "clusterDetails.changed",
          payload: { ...readySnapshot, cluster: invalidCluster },
          sequence: 6,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });

  it("parses the deterministic JSON export result and correlates it to the command", () => {
    const content = `${JSON.stringify(document, null, 2)}\n`;
    const command = parseHostCommand({
      command: "clusterDetails.export",
      id: "cluster-export-request",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    const response = {
      command: "clusterDetails.export",
      id: "cluster-export-request",
      ok: true,
      result: {
        correlationId: "cluster-export-correlation",
        document: {
          byteSize: new TextEncoder().encode(content).byteLength,
          content,
          fileName: "streamskope-cluster-fixture-cluster.json",
          mediaType: "application/json",
        },
      },
      version: HOST_PROTOCOL_VERSION,
    } as const;

    expect(parseHostCommandResponse(response)).toEqual(response);
    expect(parseCorrelatedHostResponse(response, command)).toEqual(response);

    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: {
          ...response.result,
          document: {
            ...response.result.document,
            byteSize: response.result.document.byteSize + 1,
          },
        },
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: {
          ...response.result,
          document: {
            ...response.result.document,
            content: JSON.stringify(document),
          },
        },
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommandResponse({
        ...response,
        command: "topics.list",
      }),
    ).toThrow(HostContractValidationError);
  });
});
