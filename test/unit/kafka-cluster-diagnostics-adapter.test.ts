import type { ClusterMetadata, ConfigDescription } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import { PlatformaticAdminPort } from "../../src/kafka/engine/platformatic-admin";

class RecordingPlatformaticAdmin {
  readonly describeCalls: unknown[] = [];
  readonly metadataCalls: unknown[] = [];
  describeResult: Promise<ConfigDescription[]> = Promise.resolve([]);
  metadataResult: Promise<ClusterMetadata> = Promise.resolve({
    brokers: new Map(),
    controllerId: -1,
    id: "",
    lastUpdate: 0,
    topics: new Map(),
  });

  close(): Promise<void> {
    return Promise.resolve();
  }

  describeConfigs(options: unknown): Promise<ConfigDescription[]> {
    this.describeCalls.push(options);
    return this.describeResult;
  }

  describeGroups(): Promise<Map<never, never>> {
    return Promise.resolve(new Map<never, never>());
  }

  incrementalAlterConfigs(): Promise<void> {
    return Promise.resolve();
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve([]);
  }

  listConsumerGroupOffsets(): Promise<never[]> {
    return Promise.resolve([]);
  }

  listGroups(): Promise<Map<never, never>> {
    return Promise.resolve(new Map<never, never>());
  }

  listOffsets(): Promise<never[]> {
    return Promise.resolve([]);
  }

  metadata(options: unknown): Promise<ClusterMetadata> {
    this.metadataCalls.push(options);
    return this.metadataResult;
  }
}

describe("Platformatic Kafka cluster-diagnostics adapter", () => {
  it("forces fresh metadata and normalizes sorted brokers, cluster and controller", async () => {
    const admin = new RecordingPlatformaticAdmin();
    admin.metadataResult = Promise.resolve({
      brokers: new Map([
        [7, { host: "kafka-7", port: 9093, rack: "rack-b" }],
        [2, { host: "kafka-2", port: 9094, rack: null }],
      ]),
      controllerId: 7,
      id: "fixture-cluster",
      lastUpdate: 1_722_000_000_000,
      topics: new Map(),
    });
    const port = new PlatformaticAdminPort(admin);

    await expect(port.describeClusterMetadata()).resolves.toEqual({
      brokers: [
        { host: "kafka-2", nodeId: 2, port: 9094, rack: null },
        { host: "kafka-7", nodeId: 7, port: 9093, rack: "rack-b" },
      ],
      clusterId: "fixture-cluster",
      controllerId: 7,
    });
    expect(admin.metadataCalls).toEqual([{ forceUpdate: true }]);
  });

  it("normalizes absent cluster/controller metadata without inventing values", async () => {
    const admin = new RecordingPlatformaticAdmin();
    const port = new PlatformaticAdminPort(admin);

    await expect(port.describeClusterMetadata()).resolves.toEqual({
      brokers: [],
      clusterId: null,
      controllerId: null,
    });
  });

  it("describes one broker with shared metadata mapping and removes sensitive values", async () => {
    const admin = new RecordingPlatformaticAdmin();
    admin.describeResult = Promise.resolve([
      {
        configs: [
          {
            configSource: 2,
            configType: 3,
            documentation: "Default partition count.",
            isSensitive: false,
            name: "num.partitions",
            readOnly: false,
            synonyms: [{ name: "num.partitions", source: 5, value: "1" }],
            value: "3",
          },
          {
            configSource: 4,
            configType: 9,
            documentation: null,
            isSensitive: true,
            name: "ssl.keystore.password",
            readOnly: true,
            synonyms: [
              {
                name: "ssl.keystore.password",
                source: 4,
                value: "unique-broker-secret",
              },
            ],
            value: "unique-broker-secret",
          },
        ],
        resourceName: "7",
        resourceType: 4,
      },
    ]);
    const port = new PlatformaticAdminPort(admin);

    const entries = await port.describeBrokerConfiguration(7);

    expect(entries).toEqual([
      {
        documentation: "Default partition count.",
        isDefault: false,
        isSensitive: false,
        name: "num.partitions",
        readOnly: false,
        source: "dynamic-broker",
        synonyms: [{ name: "num.partitions", source: "default", value: "1" }],
        type: "int",
        value: "3",
      },
      {
        documentation: null,
        isDefault: false,
        isSensitive: true,
        name: "ssl.keystore.password",
        readOnly: true,
        source: "static-broker",
        synonyms: [{ name: "ssl.keystore.password", source: "static-broker", value: null }],
        type: "password",
        value: null,
      },
    ]);
    expect(admin.describeCalls).toEqual([
      {
        includeDocumentation: true,
        includeSynonyms: true,
        resources: [
          {
            resourceName: "7",
            resourceType: 4,
          },
        ],
      },
    ]);
    expect(JSON.stringify(entries)).not.toContain("unique-broker-secret");
  });
});
