import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";
import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  fixtureClientOptions,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
} from "../support/kafka-fixture";

describe("real Kafka consumer-group exploration", () => {
  it("lists a controlled group and reports its committed offset and lag", async () => {
    const seeded = await provisionSeededFixtureTopic();
    const fixture = await loadFixtureConnection();
    const groupId = `streamskope-group-${randomUUID()}`;
    const admin = new Admin(
      await fixtureClientOptions(fixture, seeded.config, `streamskope-group-setup-${randomUUID()}`),
    );
    const connection: SecureConnectionInput = {
      brokers: [fixture.kafkaEndpoint],
      name: "Consumer-group acceptance",
      oauth: {
        clientId: seeded.config.oauthClientId,
        clientSecret: seeded.config.oauthClientSecret,
        scope: seeded.config.oauthScope,
        tokenEndpoint: fixture.oauthEndpoint,
      },
      tls: {
        caPem: await readFile(fixture.caPath, "utf8"),
        enabled: true,
      },
    };
    const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());

    try {
      await admin.alterConsumerGroupOffsets({
        groupId,
        topics: [
          {
            name: seeded.config.topic,
            partitionOffsets: [{ offset: 0n, partition: 0 }],
          },
        ],
      });
      await session.connect(connection);

      const inventory = await session.listConsumerGroups();
      expect(inventory.groups).toContainEqual(
        expect.objectContaining({ id: groupId, state: "empty" }),
      );

      const detail = await session.describeConsumerGroup(groupId);
      expect(detail).toMatchObject({
        id: groupId,
        members: [],
        omittedAssignments: 0,
        omittedMembers: 0,
        omittedOffsets: 0,
        state: "empty",
      });
      expect(detail.offsets).toContainEqual({
        committedOffset: "0",
        endOffset: "1",
        lag: "1",
        partition: 0,
        topic: seeded.config.topic,
      });
    } finally {
      await session.shutdown();
      await admin.deleteGroups({ groups: [groupId] }).catch(() => undefined);
      await admin.close();
      await seeded.dispose();
    }
  }, 30_000);
});
