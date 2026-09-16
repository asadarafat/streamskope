import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import type {
  KafkaClusterProfileContext,
  SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import {
  KafkaApplicationSession,
  KafkaClusterDiagnosticsService,
} from "../../src/features/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  loadFixtureConfig,
  loadFixtureConnection,
  type FixtureConfig,
  type FixtureConnection,
} from "../support/kafka-fixture";

async function connectionInput(
  fixture: FixtureConnection,
  config: FixtureConfig,
): Promise<SecureConnectionInput> {
  return {
    brokers: [fixture.kafkaEndpoint.replace(/^127\.0\.0\.1:/u, "localhost:")],
    name: "Cluster diagnostics acceptance",
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

describe("real Kafka cluster diagnostics", () => {
  it("reads and exports the current aio-kafka cluster without changing it", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const input = await connectionInput(fixture, config);
    const profile: KafkaClusterProfileContext = {
      brokers: input.brokers,
      id: null,
      name: input.name,
    };
    const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
    const diagnostics = new KafkaClusterDiagnosticsService(session);

    try {
      await session.connect(input);
      const topicsBefore = await session.listTopics();

      const result = await diagnostics.load(profile);

      expect(result.document.cluster.clusterId).not.toBeNull();
      expect(result.document.cluster.brokers.length).toBeGreaterThan(0);
      expect(result.document.cluster.brokers.map((broker) => broker.nodeId)).toEqual(
        [...result.document.cluster.brokers]
          .map((broker) => broker.nodeId)
          .sort((left, right) => left - right),
      );
      expect(result.document.cluster.configurationSourceBrokerId).not.toBeNull();
      if (result.state === "partial") {
        const issue = result.document.cluster.configurationIssue;
        if (issue === undefined) {
          throw new Error("Partial cluster diagnostics require an explicit configuration issue.");
        }
        expect(["authorization-denied", "unavailable"]).toContain(issue.code);
        expect(issue.recovery.length).toBeGreaterThan(0);
        expect(issue.summary.length).toBeGreaterThan(0);
      } else {
        expect(result.document.cluster.configuration.length).toBeGreaterThan(0);
      }
      for (const entry of result.document.cluster.configuration) {
        if (entry.isSensitive) {
          expect(entry.value).toBeNull();
          expect(entry.synonyms.every((synonym) => synonym.value === null)).toBe(true);
        }
      }

      const exported = diagnostics.exportDocument();
      expect(exported.content).toBe(`${JSON.stringify(result.document, null, 2)}\n`);
      expect(exported.byteSize).toBe(new TextEncoder().encode(exported.content).byteLength);
      expect(JSON.parse(exported.content)).toEqual(result.document);
      expect(exported.content).not.toContain(config.oauthClientSecret);
      expect(await session.listTopics()).toEqual(topicsBefore);
    } finally {
      await session.shutdown();
    }
  }, 30_000);
});
