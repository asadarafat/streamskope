import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";

import { Admin, type BaseOptions } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaApplicationSession,
  KafkaTopicConfigurationService,
} from "../../src/features/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  fetchFixtureToken,
  loadFixtureConfig,
  loadFixtureConnection,
  type FixtureConfig,
  type FixtureConnection,
} from "../support/kafka-fixture";

async function clientOptions(
  fixture: FixtureConnection,
  config: FixtureConfig,
): Promise<BaseOptions> {
  return {
    bootstrapBrokers: [fixture.kafkaEndpoint],
    clientId: `streamskope-topic-configuration-${randomUUID()}`,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    retries: 0,
    sasl: {
      mechanism: "OAUTHBEARER",
      token: await fetchFixtureToken(fixture, config),
    },
    tls: {
      ca: [await readFile(fixture.caPath, "utf8")],
      rejectUnauthorized: true,
    },
  };
}

async function connectionInput(
  fixture: FixtureConnection,
  config: FixtureConfig,
): Promise<SecureConnectionInput> {
  return {
    brokers: [fixture.kafkaEndpoint.replace(/^127\.0\.0\.1:/u, "localhost:")],
    name: "Topic configuration acceptance",
    oauth: {
      clientId: config.oauthClientId,
      clientSecret: config.oauthClientSecret,
      scope: config.oauthScope,
      tokenEndpoint: fixture.oauthEndpoint.replace("127.0.0.1", "localhost"),
    },
    tls: {
      caPem: await readFile(fixture.caPath, "utf8"),
      enabled: true,
    },
  };
}

function valueOf(
  entries: Awaited<ReturnType<KafkaTopicConfigurationService["load"]>>["entries"],
  name: string,
): string | null {
  const entry = entries.find((candidate) => candidate.name === name);
  if (entry === undefined) {
    throw new Error(`Kafka did not describe ${name}.`);
  }
  return entry.value;
}

describe("real Kafka topic configuration", () => {
  it("dry-runs and applies only one named value while preserving unrelated configuration", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const admin = new Admin(await clientOptions(fixture, config));
    const topic = `streamskope-topic-configuration-${randomUUID()}`;
    const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
    const service = new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    );
    let topicCreated = false;

    try {
      await admin.createTopics({ partitions: 1, replicas: 1, topics: [topic] });
      topicCreated = true;
      await session.connect(await connectionInput(fixture, config));

      const baseline = await service.load(topic);
      const retentionBefore = valueOf(baseline.entries, "retention.ms");
      const segmentBefore = valueOf(baseline.entries, "segment.ms");
      const nextRetention = retentionBefore === "600001" ? "600002" : "600001";
      const change = {
        isSensitive: false,
        name: "retention.ms",
        value: nextRetention,
      } as const;

      const validated = await service.validate({ changes: [change], topic });
      expect(valueOf(validated.configuration.entries, "retention.ms")).toBe(retentionBefore);
      const afterValidation = await service.load(topic);
      expect(valueOf(afterValidation.entries, "retention.ms")).toBe(retentionBefore);
      expect(valueOf(afterValidation.entries, "segment.ms")).toBe(segmentBefore);

      const applied = await service.apply({ changes: [change], topic });
      expect(valueOf(applied.configuration.entries, "retention.ms")).toBe(nextRetention);
      expect(valueOf(applied.configuration.entries, "segment.ms")).toBe(segmentBefore);

      const history = await service.history(topic);
      expect(history.snapshot.store).toEqual({
        durability: "session",
        state: "ready",
      });
      expect(history.snapshot.entries).toHaveLength(2);
      expect(history.snapshot.entries.map((entry) => entry.action)).toEqual(["apply", "validate"]);
      expect(history.snapshot.entries[0]?.changes).toEqual([
        {
          from: retentionBefore,
          isSensitive: false,
          name: "retention.ms",
          to: nextRetention,
          wasDefault: baseline.entries.find((entry) => entry.name === "retention.ms")?.isDefault,
        },
      ]);
    } finally {
      await session.shutdown();
      if (topicCreated) {
        await admin.deleteTopics({ topics: [topic] });
      }
      await admin.close();
    }
  }, 40_000);
});
