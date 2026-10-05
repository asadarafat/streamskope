import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import {
  Producer,
  AclOperations,
  AclPermissionTypes,
  ResourceTypes,
  ResourcePatternTypes,
} from "@platformatic/kafka";
import { expect, it } from "vitest";

import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { ObservationService } from "../../src/features/kafka/application/observation-service";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { observationLag } from "../../src/features/kafka/contracts/observations";
import { analyzeObservations } from "../../src/features/kafka/contracts/observation-analysis";
import { AtomicObservationFileStore } from "../../src/platform/node/kafka-observation-file-store";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";

it("observes real known offsets/lag and replication, retains history across restart, and treats denied group reads as unknown", async () => {
  const fixture = await startAuthorizationFixture();
  const dir = await mkdtemp(join(tmpdir(), "streamskope-observed-real-"));
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "streamskope-observation-seed",
    retries: 0,
    autocreateTopics: false,
  });
  const topic = "observed-events",
    unrelated = "unrelated-observed-events",
    groupId = "observed-group";
  try {
    await fixture.admin.createTopics({
      topics: [
        { topic, partitions: 2, replicas: 1 },
        { topic: unrelated, partitions: 2, replicas: 1 },
      ],
    });
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
                {
                  name: unrelated,
                  partitionOffsets: [
                    { partition: 0, offset: 500n },
                    { partition: 1, offset: 500n },
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
    await producer.send({
      messages: Array.from({ length: 20 }, (_, i) => ({
        topic,
        partition: i % 2,
        key: Buffer.from("fixture-key"),
        value: Buffer.from("fixture-value"),
      })),
    });
    // A group can contain unrelated inaccessible topics. Their positions cannot invalidate
    // selected-topic lag or cause observations to query those topics' end positions.
    await fixture.admin.createAcls({
      creations: [
        {
          resourceType: ResourceTypes.TOPIC,
          resourcePatternType: ResourcePatternTypes.LITERAL,
          resourceName: unrelated,
          principal: "User:ANONYMOUS",
          host: "*",
          operation: AclOperations.DESCRIBE,
          permissionType: AclPermissionTypes.DENY,
        },
      ],
    });
    await session.connect(fixture.connection);
    const store = new AtomicObservationFileStore(join(dir, "history.json"));
    const service = new ObservationService(() => session.observationScope(), store);
    const input = { topic, groupId, sampleRecords: true, thresholds: { lag: 15, requestMs: null } };
    const first = (await service.capture(input)).series.samples.at(-1)!;
    expect(observationLag(first)).toBe(20);
    expect(first.groupCoverage).toBe("complete");
    expect(first.issues?.filter((issue) => issue.measurement === "group-offsets")).toEqual([]);
    expect(first.records).toMatchObject({
      count: 20,
      state: "complete",
      knownKeys: 20,
      distinctKeys: 1,
    });
    expect(first.records?.topKeys[0]?.count).toBe(20);
    expect(JSON.stringify(first)).not.toMatch(/fixture-key|fixture-value/);
    expect(first.partitions.map((p) => p.endOffset)).toEqual(["10", "10"]);
    expect(
      first.partitions.every(
        (p) => p.replicas === 1 && p.inSyncReplicas === 1 && p.leader !== null,
      ),
    ).toBe(true);
    expect(first.alerts).toEqual([{ metric: "lag", observed: 20, threshold: 15 }]);
    await fixture.admin.alterConsumerGroupOffsets({
      groupId,
      topics: [
        {
          name: topic,
          partitionOffsets: [
            { partition: 0, offset: 7n },
            { partition: 1, offset: 3n },
          ],
        },
      ],
    });
    await delay(10_010);
    const second = (await service.capture(input)).series.samples.at(-1)!;
    expect(observationLag(second)).toBe(10);
    expect(second.alerts).toEqual([]);
    expect(second.observedAt - first.observedAt).toBeGreaterThanOrEqual(10_000);
    const restart = new ObservationService(
      () => session.observationScope(),
      new AtomicObservationFileStore(join(dir, "history.json")),
    );
    expect((await restart.history()).series[0]?.samples).toHaveLength(2);
    await fixture.admin.createAcls({
      creations: [
        {
          resourceType: ResourceTypes.GROUP,
          resourcePatternType: ResourcePatternTypes.LITERAL,
          resourceName: groupId,
          principal: "User:ANONYMOUS",
          host: "*",
          operation: AclOperations.DESCRIBE,
          permissionType: AclPermissionTypes.DENY,
        },
      ],
    });
    const denied = (await restart.capture(input)).series.samples.at(-1)!;
    expect(denied.groupCoverage).toBe("unavailable");
    expect(denied.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          measurement: "group-offsets",
          code: "AUTHORIZATION_DENIED",
        }),
      ]),
    );
    expect(observationLag(denied)).toBeNull();
    expect(denied.alerts).toEqual([]);

    // Real Kafka fetches with controlled CreateTime timestamps make the window boundary
    // deterministic without pretending an injected clock qualifies wall-clock polling.
    const busyTopic = "busy-observed-events";
    let clock = Date.now();
    const adaptive = new ObservationService(
      () => session.observationScope(),
      undefined,
      () => clock,
    );
    const busyInput = {
      topic: busyTopic,
      groupId: null,
      sampleRecords: true,
      thresholds: { lag: null, requestMs: null },
    };
    await fixture.admin.createTopics({
      topics: [{ topic: busyTopic, partitions: 2, replicas: 1 }],
    });
    await expect
      .poll(
        async () => {
          try {
            return (
              await fixture.admin.metadata({ forceUpdate: true, topics: [busyTopic] })
            ).topics.get(busyTopic)?.partitions.length;
          } catch {
            return 0;
          }
        },
        { timeout: 15_000, interval: 250 },
      )
      .toBe(2);
    await producer.send({
      messages: Array.from({ length: 240 }, (_, index) => ({
        topic: busyTopic,
        partition: index % 2,
        key: Buffer.from("busy-key"),
        value: Buffer.from("busy-value"),
        timestamp: BigInt(clock - 1_000),
      })),
    });
    const capped = await adaptive.capture(busyInput);
    expect(capped.series.samples.at(-1)?.records).toMatchObject({
      count: 200,
      state: "partial",
      reason: "limit",
      analysisEligible: false,
    });
    expect(analyzeObservations(capped.series, clock).hotKey).toBeNull();
    await producer.send({
      messages: Array.from({ length: 80 }, (_, index) => ({
        topic: busyTopic,
        partition: index % 2,
        key: Buffer.from("busy-key"),
        value: Buffer.from("busy-value"),
        timestamp: BigInt(clock + 9_999),
      })),
    });
    clock += 10_000;
    const bounded = await adaptive.capture(busyInput);
    const protectedWindow = bounded.series.samples.at(-1)?.records;
    expect(protectedWindow).toMatchObject({
      count: 80,
      state: "complete",
      reason: "range-complete",
      analysisEligible: true,
      partitionCoverage: { expected: 2, completed: 2 },
    });
    expect(protectedWindow!.endTimeMs - protectedWindow!.startTimeMs).toBe(10_000);
    expect(analyzeObservations(bounded.series, clock).hotKey).toMatchObject({
      known: 80,
      suspected: true,
    });
    expect(JSON.stringify(bounded)).not.toMatch(/busy-key|busy-value/);
    expect((await restart.clear()).series).toEqual([]);
  } finally {
    await producer.close();
    await session.disconnect();
    await fixture.dispose();
    await rm(dir, { recursive: true, force: true });
  }
}, 120_000);
