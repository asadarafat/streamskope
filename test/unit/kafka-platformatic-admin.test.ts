import type { Acl, ClusterMetadata, ConfigDescription } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import {
  KAFKA_TOPIC_CONFIGURATION_REDACTION,
  type KafkaTopicConfigurationChange,
} from "../../src/kafka/contracts";
import { PlatformaticAdminPort } from "../../src/kafka/engine/platformatic-admin";

class RecordingPlatformaticAdmin {
  readonly aclCreateCalls: unknown[] = [];
  readonly aclDeleteCalls: unknown[] = [];
  readonly aclDescribeCalls: unknown[] = [];
  readonly alterCalls: unknown[] = [];
  readonly describeCalls: unknown[] = [];

  alterResult: Promise<void> = Promise.resolve();
  deleteResult: readonly Acl[] = [];
  describeResult: Promise<ConfigDescription[]> = Promise.resolve([]);

  close(): Promise<void> {
    return Promise.resolve();
  }

  createAcls(options: unknown): Promise<void> {
    this.aclCreateCalls.push(options);
    return Promise.resolve();
  }

  deleteAcls(options: unknown): Promise<readonly Acl[]> {
    this.aclDeleteCalls.push(options);
    return Promise.resolve(this.deleteResult);
  }

  describeAcls(options: unknown): Promise<never[]> {
    this.aclDescribeCalls.push(options);
    return Promise.resolve([
      {
        acls: [{ host: "*", operation: 3, permissionType: 3, principal: "User:orders-api" }],
        resourceName: "orders.events",
        resourcePatternType: 3,
        resourceType: 2,
      },
    ] as never[]);
  }

  describeConfigs(options: unknown): Promise<ConfigDescription[]> {
    this.describeCalls.push(options);
    return this.describeResult;
  }

  describeGroups(): Promise<Map<never, never>> {
    return Promise.resolve(new Map<never, never>());
  }

  incrementalAlterConfigs(options: unknown): Promise<void> {
    this.alterCalls.push(options);
    return this.alterResult;
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

  metadata(): Promise<ClusterMetadata> {
    return Promise.resolve({
      brokers: new Map(),
      controllerId: -1,
      id: "",
      lastUpdate: 0,
      topics: new Map(),
    });
  }
}

const changes: readonly KafkaTopicConfigurationChange[] = [
  {
    isSensitive: false,
    name: "retention.ms",
    value: "604800000",
  },
];

describe("Platformatic Kafka topic-configuration admin adapter", () => {
  it("describes one topic with synonyms and documentation and removes sensitive values", async () => {
    const admin = new RecordingPlatformaticAdmin();
    admin.describeResult = Promise.resolve([
      {
        configs: [
          {
            configSource: 1,
            configType: 5,
            documentation: "Retention in milliseconds.",
            isSensitive: false,
            name: "retention.ms",
            readOnly: false,
            synonyms: [{ name: "retention.ms", source: 5, value: "604800000" }],
            value: "86400000",
          },
          {
            configSource: 1,
            configType: 9,
            documentation: null,
            isSensitive: true,
            name: "ssl.keystore.password",
            readOnly: false,
            synonyms: [{ name: "ssl.keystore.password", source: 1, value: "broker-secret" }],
            value: "broker-secret",
          },
        ],
        resourceName: "orders.events",
        resourceType: 2,
      },
    ]);
    const port = new PlatformaticAdminPort(admin);

    await expect(port.describeTopicConfiguration("orders.events")).resolves.toEqual([
      {
        documentation: "Retention in milliseconds.",
        isDefault: false,
        isSensitive: false,
        name: "retention.ms",
        readOnly: false,
        source: "topic",
        synonyms: [{ name: "retention.ms", source: "default", value: "604800000" }],
        type: "long",
        value: "86400000",
      },
      {
        documentation: null,
        isDefault: false,
        isSensitive: true,
        name: "ssl.keystore.password",
        readOnly: false,
        source: "topic",
        synonyms: [{ name: "ssl.keystore.password", source: "topic", value: null }],
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
            resourceName: "orders.events",
            resourceType: 2,
          },
        ],
      },
    ]);
    expect(JSON.stringify(await port.describeTopicConfiguration("orders.events"))).not.toContain(
      "broker-secret",
    );
    expect(KAFKA_TOPIC_CONFIGURATION_REDACTION).toBe("<redacted>");
  });

  it.each([
    { label: "dry-run", validateOnly: true },
    { label: "apply", validateOnly: false },
  ])("sets only submitted names for $label", async ({ validateOnly }) => {
    const admin = new RecordingPlatformaticAdmin();
    const port = new PlatformaticAdminPort(admin);

    await port.alterTopicConfiguration("orders.events", changes, validateOnly);

    expect(admin.alterCalls).toEqual([
      {
        resources: [
          {
            configs: [
              {
                configOperation: 0,
                name: "retention.ms",
                value: "604800000",
              },
            ],
            resourceName: "orders.events",
            resourceType: 2,
          },
        ],
        validateOnly,
      },
    ]);
  });
});

describe("Platformatic Kafka ACL admin adapter", () => {
  const acl = {
    host: "*",
    operation: "READ",
    patternType: "LITERAL",
    permission: "ALLOW",
    principal: "User:orders-api",
    resourceName: "orders.events",
    resourceType: "TOPIC",
  } as const;

  it("maps native ACL enumeration values without broadening the selected binding", async () => {
    const admin = new RecordingPlatformaticAdmin();
    admin.deleteResult = [
      {
        host: "*",
        operation: 3,
        permissionType: 3,
        principal: "User:orders-api",
        resourceName: "orders.events",
        resourcePatternType: 3,
        resourceType: 2,
      },
    ];
    const port = new PlatformaticAdminPort(admin);

    await expect(port.listAcls()).resolves.toEqual([acl]);
    await port.createAcl(acl);
    await port.deleteAcl(acl);

    expect(admin.aclDescribeCalls).toEqual([
      {
        filter: {
          host: null,
          operation: 1,
          permissionType: 1,
          principal: null,
          resourceName: null,
          resourcePatternType: 1,
          resourceType: 1,
        },
      },
    ]);
    expect(admin.aclCreateCalls).toEqual([
      {
        creations: [
          {
            host: "*",
            operation: 3,
            permissionType: 3,
            principal: "User:orders-api",
            resourceName: "orders.events",
            resourcePatternType: 3,
            resourceType: 2,
          },
        ],
      },
    ]);
    expect(admin.aclDeleteCalls).toEqual([
      {
        filters: [
          {
            host: "*",
            operation: 3,
            permissionType: 3,
            principal: "User:orders-api",
            resourceName: "orders.events",
            resourcePatternType: 3,
            resourceType: 2,
          },
        ],
      },
    ]);
  });

  it("rejects zero or non-identical ACL deletion results", async () => {
    const admin = new RecordingPlatformaticAdmin();
    const port = new PlatformaticAdminPort(admin);

    await expect(port.deleteAcl(acl)).rejects.toThrow("inconsistent ACL deletion result");

    admin.deleteResult = [
      {
        host: "*",
        operation: 4,
        permissionType: 3,
        principal: "User:orders-api",
        resourceName: "orders.events",
        resourcePatternType: 3,
        resourceType: 2,
      },
    ];
    await expect(port.deleteAcl(acl)).rejects.toThrow("inconsistent ACL deletion result");
  });
});
