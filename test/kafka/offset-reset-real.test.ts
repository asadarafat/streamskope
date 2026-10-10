import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import {
  Admin,
  AclOperations,
  AclPermissionTypes,
  ResourceTypes,
  ResourcePatternTypes,
} from "@platformatic/kafka";
import { expect, it } from "vitest";

import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { OffsetResetService } from "../../src/features/kafka/application/offset-reset-service";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  fixtureClientOptions,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
} from "../support/kafka-fixture";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";

it("retains a real topic authorization rejection and leaves subsequent partitions unsent", async () => {
  const fixture = await startAuthorizationFixture();
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const topic = "offset-reset-permissions",
    groupId = "offset-reset-group";
  try {
    await fixture.admin.createTopics({ topics: [{ topic, partitions: 2, replicas: 1 }] });
    // Metadata readiness precedes coordinator readiness on a fresh broker.
    await expect
      .poll(
        async () => {
          try {
            await fixture.admin.alterConsumerGroupOffsets({
              groupId,
              topics: [
                {
                  name: topic,
                  partitionOffsets: [
                    { partition: 0, offset: 0n },
                    { partition: 1, offset: 0n },
                  ],
                },
              ],
            });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15_000, interval: 250 },
      )
      .toBe(true);
    await session.connect(fixture.connection);
    const service = new OffsetResetService(() => session.administrationScopes.offsetReset());
    const review = await service.review({
      groupId,
      targets: [
        { topic, partition: 0, offset: "0" },
        { topic, partition: 1, offset: "0" },
      ],
    });
    await fixture.admin.createAcls({
      creations: [
        {
          resourceType: ResourceTypes.TOPIC,
          resourcePatternType: ResourcePatternTypes.LITERAL,
          resourceName: topic,
          principal: "User:ANONYMOUS",
          host: "*",
          permissionType: AclPermissionTypes.ALLOW,
          operation: AclOperations.DESCRIBE,
        },
        {
          resourceType: ResourceTypes.TOPIC,
          resourcePatternType: ResourcePatternTypes.LITERAL,
          resourceName: topic,
          principal: "User:ANONYMOUS",
          host: "*",
          permissionType: AclPermissionTypes.DENY,
          operation: AclOperations.READ,
        },
      ],
    });
    const outcome = await service.apply(review.planId, groupId);
    expect(outcome.partitions.map((p) => p.state)).toEqual(["rejected", "unsent"]);
  } finally {
    await session.shutdown();
    await fixture.dispose();
  }
}, 90_000);

it("previews real positions without committing, resets and reads back a stopped disposable group, rejects stale positions", async () => {
  const seeded = await provisionSeededFixtureTopic();
  const fixture = await loadFixtureConnection();
  const admin = new Admin(
    await fixtureClientOptions(fixture, seeded.config, "streamskope-reset-setup"),
  );
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const groupId = `streamskope-reset-${randomUUID()}`,
    topic = seeded.config.topic;
  const commit = (offset: bigint): Promise<void> =>
    admin.alterConsumerGroupOffsets({
      groupId,
      topics: [{ name: topic, partitionOffsets: [{ partition: 0, offset }] }],
    });
  try {
    await commit(1n);
    await session.connect({
      brokers: [fixture.kafkaEndpoint],
      name: "Recovery qualification",
      tls: { enabled: true, caPem: await readFile(fixture.caPath, "utf8") },
      oauth: {
        clientId: seeded.config.oauthClientId,
        clientSecret: seeded.config.oauthClientSecret,
        scope: seeded.config.oauthScope,
        tokenEndpoint: fixture.oauthEndpoint,
      },
    });
    const service = new OffsetResetService(() => session.administrationScopes.offsetReset());
    const input = { groupId, targets: [{ topic, partition: 0, offset: "0" }] };
    const review = await service.review(input);
    expect(review.baseline).toMatchObject({
      inactive: true,
      partitions: [{ before: "1", offset: "0", low: "0", high: "1", replayUpperBound: "1" }],
    });
    expect(review.examples).toEqual([
      expect.objectContaining({ topic, partition: 0, offset: "0" }),
    ]);
    expect((await session.describeConsumerGroup(groupId)).offsets[0]?.committedOffset).toBe("1");
    const outcome = await service.apply(review.planId, groupId);
    expect(outcome.partitions).toEqual([
      {
        topic,
        partition: 0,
        offset: "0",
        state: "acknowledged",
        observed: "0",
        verified: true,
        cleanup: "confirmed",
      },
    ]);
    const stale = await service.review(input);
    await commit(1n);
    expect((await service.apply(stale.planId, groupId)).partitions[0]?.state).toBe("unsent");
    expect((await session.describeConsumerGroup(groupId)).offsets[0]?.committedOffset).toBe("1");
  } finally {
    await session.shutdown();
    await admin.deleteGroups({ groups: [groupId] }).catch(() => undefined);
    await admin.close();
    await seeded.dispose();
  }
}, 60_000);
