import type {
  KafkaActiveConnection,
  SchemaRegistryPort,
} from "../../src/features/kafka/application";
import type { SchemaVersionDetail, KafkaMessage } from "../../src/features/kafka/contracts";
import type { ConnectPort } from "../../src/features/kafka/application/connect-service";
import { RelationshipService } from "../../src/features/kafka/application/relationship-service";

import { OBSERVED_AT } from "./observation-fixture";

export function relationshipFixture(): {
  connection: KafkaActiveConnection;
  connect: ConnectPort;
  registry: SchemaRegistryPort;
  service: RelationshipService;
  disconnect(): void;
} {
  const noWrite = (): never => {
    throw new Error("Discovery must not write.");
  };
  const schemas: SchemaVersionDetail[] = [
    {
      subject: "base",
      version: 1,
      id: 10,
      schemaType: "AVRO",
      schema: "private schema body",
      references: [],
    },
    {
      subject: "events-value",
      version: 2,
      id: 11,
      schemaType: "AVRO",
      schema: "private dependent body",
      references: [{ name: "Base", subject: "base", version: 1 }],
    },
  ];
  const serviceContext = {
    baseUrl: "https://fixture.invalid/private",
    authorization: (): Promise<string> => Promise.resolve("Bearer private credential"),
  };
  const message: KafkaMessage = {
    id: "message",
    topic: "events",
    partition: 0,
    offset: "5",
    timestamp: new Date(OBSERVED_AT - 1000).toISOString(),
    key: null,
    payload: "private payload",
    preview: "private payload",
    originalByteSize: 8,
    headers: {},
    truncated: false,
    original: {
      state: "complete",
      encoding: "base64",
      key: null,
      value: Buffer.from([0, 0, 0, 0, 11, 1, 2, 3]).toString("base64"),
      headers: [],
    },
  };
  const connection: KafkaActiveConnection = {
    describeClusterMetadata: () =>
      Promise.resolve({ clusterId: "fixture-cluster", brokers: [], controllerId: 1 }),
    listTopics: () => Promise.resolve(["events", "unrelated"]),
    listConsumerGroups: () =>
      Promise.resolve({
        groups: [
          { id: "workers", state: "stable", groupType: "consumer", protocolType: "consumer" },
        ],
        omittedGroups: 0,
      }),
    describeConsumerGroup: () =>
      Promise.resolve({
        id: "workers",
        state: "stable",
        protocol: "range",
        protocolType: "consumer",
        omittedAssignments: 0,
        omittedMembers: 0,
        omittedOffsets: 0,
        members: [
          {
            id: "private member",
            clientId: "private client",
            clientHost: "private host",
            groupInstanceId: null,
            assignments: [{ topic: "events", partitions: [0] }],
          },
        ],
        offsets: [
          { topic: "events", partition: 0, committedOffset: "5", endOffset: "10", lag: "5" },
        ],
      }),
    clusterServiceContext: () => serviceContext,
    openMessageStream: () =>
      Promise.resolve({
        close: () => Promise.resolve(),
        coverage: () => ({
          reason: "range-complete",
          scannedRecords: 1,
          scannedBytes: 8,
          matchedRecords: 1,
          unavailableRecords: 0,
          partitions: [],
        }),
        async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
          yield await Promise.resolve(message);
        },
      }),
    close: () => Promise.resolve(),
    alterTopicConfiguration: noWrite,
    describeBrokerConfiguration: noWrite,
    describeTopicConfiguration: noWrite,
  };
  const registry: SchemaRegistryPort = {
    listSubjects: () =>
      Promise.resolve({ subjects: schemas.map((s) => s.subject), omittedSubjects: 0 }),
    loadSubject: (_context, identity) => {
      const schema = schemas.find(
        (s) =>
          s.subject === identity.subject &&
          (identity.version === "latest" || s.version === identity.version),
      );
      return schema
        ? Promise.resolve({ schema, compatibilityLevel: "BACKWARD", versions: [schema.version] })
        : Promise.reject(new Error("private registry error"));
    },
    loadLatestSubject: noWrite,
    register: noWrite,
    delete: noWrite,
    checkCompatibility: noWrite,
  };
  const connect: ConnectPort = {
    clusterId: () => Promise.resolve("fixture-cluster"),
    list: () => Promise.resolve({ names: ["source"], plugins: [] }),
    relationships: () =>
      Promise.resolve({
        type: "source",
        configuredTopics: ["events"],
        reportedTopics: ["events"],
        regexSubscription: false,
      }),
    apply: noWrite,
    validate: noWrite,
    load: noWrite,
  };
  let connected = true;
  const service = new RelationshipService(
    () => (connected ? { connection, generation: 1, connectionName: "fixture" } : null),
    connect,
    registry,
    () => OBSERVED_AT,
  );
  return {
    connection,
    registry,
    connect,
    service,
    disconnect: (): void => {
      connected = false;
    },
  };
}
