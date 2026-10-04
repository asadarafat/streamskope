import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostEvent,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { loadFixtureConnection, provisionSeededFixtureTopic } from "../support/kafka-fixture";

describe("real Kafka stream monitoring", () => {
  it("reports strict aggregate publication evidence for a bounded aio-kafka fetch", async () => {
    const seeded = await provisionSeededFixtureTopic();
    const fixture = await loadFixtureConnection();
    const connection: SecureConnectionInput = {
      brokers: [fixture.kafkaEndpoint],
      name: "Stream monitor acceptance",
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
    const backend = createKafkaBackend();
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(parseHostEvent(event));
    });

    try {
      await expect(
        backend.execute({
          command: "connection.connect",
          id: "monitor-connect",
          payload: connection,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      await expect(
        backend.execute({
          command: "messages.start",
          id: "monitor-fetch",
          payload: {
            maxMessages: 1,
            mode: "earliest",
            topic: seeded.config.topic,
          },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });

      await vi.waitFor(
        () => {
          expect(
            events.filter((event) => event.event === "streamMetrics.changed").at(-1)?.payload.state,
          ).toBe("complete");
        },
        { timeout: 10_000 },
      );

      const monitorEvents = events.filter((event) => event.event === "streamMetrics.changed");
      const published = monitorEvents.find(
        (event) => event.payload.delivery?.publishedMessages === 1,
      );
      expect(published).toMatchObject({
        payload: {
          connectionName: "Stream monitor acceptance",
          delivery: {
            batchCount: 1,
            publishedMessages: 1,
            lastBatchMessages: 1,
            receivedMessages: 1,
          },
          queue: {
            capacityMessages: 1_000,
            currentMessages: 0,
            droppedMessages: 0,
            droppedSincePrevious: 0,
            peakMessages: 1,
          },
          request: {
            maxMessages: 1,
            mode: "earliest",
            topic: seeded.config.topic,
          },
          status: "nominal",
        },
      });
      expect(published?.payload.delivery?.messagesPerSecond).toBeGreaterThan(0);
      expect(published?.payload.delivery?.publicationDurationMs).toBeGreaterThanOrEqual(0);
      expect(published?.payload.delivery?.queueWaitMs).toBeGreaterThanOrEqual(0);
      expect(monitorEvents.length).toBeGreaterThanOrEqual(3);
      expect(monitorEvents.every((event) => event.payload.operationId === "monitor-fetch")).toBe(
        true,
      );

      const visibleEvidence = JSON.stringify(monitorEvents);
      expect(visibleEvidence).not.toContain(seeded.config.oauthClientSecret);
      expect(visibleEvidence).not.toContain(seeded.config.seedPayload);
      expect(visibleEvidence).not.toContain("streamskope-seed");
    } finally {
      await backend.shutdown();
      await seeded.dispose();
    }
  }, 30_000);
});
