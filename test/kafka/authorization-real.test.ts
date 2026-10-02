import {
  AclOperations,
  AclPermissionTypes,
  ResourcePatternTypes,
  ResourceTypes,
} from "@platformatic/kafka";
import { expect, it } from "vitest";

import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { PlatformaticAdminPort } from "../../src/features/kafka/engine/platformatic-admin";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";

it("reconciles exact ACL bindings and reports real produce, group and ACL permission denials", async () => {
  const fixture = await startAuthorizationFixture();
  const topic = "streamskope-owned-permission-test";
  const acl = {
    host: "*",
    principal: "User:ANONYMOUS",
    resourcePatternType: ResourcePatternTypes.LITERAL,
    permissionType: AclPermissionTypes.ALLOW,
  };
  let connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>> | undefined;
  try {
    await fixture.admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    const privileged = new PlatformaticAdminPort(fixture.admin);
    const exact = {
      host: "*",
      principal: "User:fixture-reader",
      resourceType: "TOPIC",
      resourceName: topic,
      operation: "READ",
      permission: "ALLOW",
      patternType: "LITERAL",
    } as const;
    const adjacent = { ...exact, operation: "DESCRIBE" } as const;
    await privileged.createAcl(exact);
    await privileged.createAcl(adjacent);
    expect(await privileged.listAcls()).toEqual(expect.arrayContaining([exact, adjacent]));
    await privileged.deleteAcl(exact);
    expect(await privileged.listAcls()).not.toContainEqual(exact);
    expect(await privileged.listAcls()).toContainEqual(adjacent);
    await fixture.admin.createAcls({
      creations: [
        {
          ...acl,
          resourceType: ResourceTypes.CLUSTER,
          resourceName: "kafka-cluster",
          operation: AclOperations.CLUSTER_ACTION,
        },
        {
          ...acl,
          resourceType: ResourceTypes.CLUSTER,
          resourceName: "kafka-cluster",
          operation: AclOperations.DESCRIBE,
        },
        {
          ...acl,
          resourceType: ResourceTypes.CLUSTER,
          resourceName: "kafka-cluster",
          operation: AclOperations.ALTER,
          permissionType: AclPermissionTypes.DENY,
        },
        {
          ...acl,
          resourceType: ResourceTypes.TOPIC,
          resourceName: topic,
          operation: AclOperations.DESCRIBE,
        },
        {
          ...acl,
          resourceType: ResourceTypes.TOPIC,
          resourceName: topic,
          operation: AclOperations.WRITE,
          permissionType: AclPermissionTypes.DENY,
        },
        {
          ...acl,
          resourceType: ResourceTypes.TOPIC,
          resourceName: "denied-creation",
          operation: AclOperations.CREATE,
          permissionType: AclPermissionTypes.DENY,
        },
        {
          ...acl,
          resourceType: ResourceTypes.GROUP,
          resourceName: "denied-group",
          operation: AclOperations.DESCRIBE,
          permissionType: AclPermissionTypes.DENY,
        },
      ],
    });
    connection = await new StreamSkopeKafkaEngine({ operationTimeoutMs: 5_000 }).openConnection(
      fixture.connection,
      new AbortController().signal,
    );
    const before = await connection.listAcls!();
    expect(before).toContainEqual({
      host: "*",
      principal: "User:ANONYMOUS",
      resourceType: "TOPIC",
      resourceName: topic,
      operation: "WRITE",
      permission: "DENY",
      patternType: "LITERAL",
    });
    await expect(
      connection.createAcl!({
        host: "*",
        principal: "User:unprivileged",
        resourceType: "TOPIC",
        resourceName: topic,
        operation: "READ",
        permission: "ALLOW",
        patternType: "LITERAL",
      }),
    ).rejects.toMatchObject({ code: "AUTHORIZATION_DENIED" });
    await expect(
      connection.deleteAcl!(before.find((entry) => entry.resourceName === topic)!),
    ).rejects.toMatchObject({ code: "AUTHORIZATION_DENIED" });
    expect(await connection.listAcls!()).toEqual(before);
    await expect(connection.describeConsumerGroup!("denied-group")).rejects.toMatchObject({
      code: "AUTHORIZATION_DENIED",
    });
    const write = {
      kind: "record",
      topic,
      partition: 0,
      record: { state: "complete", encoding: "base64", key: null, value: "dGVzdA==", headers: [] },
    } as const;
    await connection.reviewWrite!(write);
    expect(await connection.applyWrite!(write)).toMatchObject({ state: "rejected", receipt: null });
    const create = {
      kind: "topic",
      topic: "denied-creation",
      partitions: 1,
      replicationFactor: 1,
      configs: [],
    } as const;
    await connection.reviewWrite!(create);
    expect(await connection.applyWrite!(create)).toMatchObject({
      state: "rejected",
      receipt: null,
    });
    expect(await fixture.admin.listTopics()).not.toContain(create.topic);
    const offsets = await fixture.admin.listOffsets({
      topics: [{ name: topic, partitions: [{ partitionIndex: 0, timestamp: -1n }] }],
    });
    expect(offsets[0]?.partitions[0]?.offset).toBe(0n);
  } finally {
    await connection?.close();
    await fixture.dispose();
  }
}, 180_000);
