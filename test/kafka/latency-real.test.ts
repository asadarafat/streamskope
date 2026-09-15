import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import type { SecureConnectionInput } from "../../src/kafka/contracts";
import { KafkaApplicationSession, KafkaLatencyProbeService } from "../../src/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/kafka/engine";
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
    name: "Latency acceptance",
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

describe("real Kafka latency probing", () => {
  it("produces and observes an exact bounded probe through the secure aio-kafka fixture", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
    const latency = new KafkaLatencyProbeService(session);

    try {
      await session.connect(await connectionInput(fixture, config));
      const result = await latency.start({
        acknowledgements: -1,
        messageCount: 3,
        timeoutMs: 15_000,
        topic: config.topic,
      });

      expect(result.state).toBe("ready");
      expect(result.evidence).toMatchObject({
        acknowledgements: -1,
        observedMessages: 3,
        producer: {
          semantics: "acknowledged",
          summary: { samples: 3 },
        },
        requestedMessages: 3,
        topic: config.topic,
      });
      expect(result.evidence.endToEnd?.samples).toBe(3);
      expect(result.evidence.fetch.summary?.samples).toBeGreaterThan(0);
      expect(result.evidence.network.tcpConnectMs).toBeGreaterThanOrEqual(0);
      expect(result.evidence.network.tlsHandshakeMs).toBeGreaterThanOrEqual(0);
      expect(new Set(result.evidence.sampleIds).size).toBe(3);

      const exported = latency.exportDocument();
      expect(JSON.parse(exported.content)).toEqual(result.evidence);
      expect(exported.content).not.toContain(config.oauthClientSecret);
    } finally {
      await session.shutdown();
    }
  }, 30_000);
});
