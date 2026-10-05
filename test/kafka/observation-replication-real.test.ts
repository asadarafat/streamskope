import { Producer } from "@platformatic/kafka";
import { expect, it } from "vitest";

import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { ObservationService } from "../../src/features/kafka/application/observation-service";
import { analyzeObservations } from "../../src/features/kafka/contracts/observation-analysis";
import type { ObservationCapture } from "../../src/features/kafka/contracts/observations";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { startReplicationFixture } from "../support/kafka-replication-fixture";

it("reports actual ISR loss and an offline leader, then clears replication findings after broker recovery", async () => {
  const fixture = await startReplicationFixture();
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "streamskope-replication-seed",
    retries: 0,
    autocreateTopics: false,
  });
  let clock = Date.now();
  const service = new ObservationService(
    () => session.observationScope(),
    undefined,
    () => clock,
  );
  const capture = async (topic: string): Promise<ObservationCapture> => {
    clock += 10_000;
    return service.capture({
      topic,
      groupId: null,
      thresholds: { lag: null, requestMs: null },
    });
  };
  const replicated = "observed-replicated",
    single = "observed-offline";
  let stage = "create topics";
  try {
    await fixture.admin.createTopics({
      topics: [replicated],
      replicas: -1,
      partitions: -1,
      assignments: [{ partition: 0, brokers: [1, 2] }],
    });
    await fixture.admin.createTopics({
      topics: [single],
      replicas: -1,
      partitions: -1,
      assignments: [{ partition: 0, brokers: [1] }],
    });
    await expect
      .poll(
        async () => {
          try {
            const metadata = await fixture.admin.metadata({
              forceUpdate: true,
              topics: [replicated, single],
            });
            return [...metadata.topics.values()].filter((t) => t.partitions[0]?.leader === 1)
              .length;
          } catch {
            return 0;
          }
        },
        { timeout: 30_000, interval: 500 },
      )
      .toBe(2);
    stage = "produce known records";
    await producer.send({
      messages: [replicated, single].map((topic) => ({ topic, value: Buffer.from("evidence") })),
    });
    stage = "connect and capture healthy replicas";
    await session.connect(fixture.connection);
    await expect
      .poll(async () => (await capture(replicated)).series.samples.at(-1)?.partitions[0], {
        timeout: 30_000,
        interval: 1_000,
      })
      .toMatchObject({ replicas: 2, inSyncReplicas: 2, endOffset: "1" });
    const before = (await capture(single)).series;
    expect(before.samples.at(-1)?.partitions[0]).toMatchObject({ leader: 1, endOffset: "1" });
    expect(analyzeObservations(before, clock).hints).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Replication evidence needs attention" }),
      ]),
    );

    stage = "stop broker and observe ISR loss";
    await fixture.stopBroker(1);
    let underReplicated = (await capture(replicated)).series;
    await expect
      .poll(
        async () => {
          underReplicated = (await capture(replicated)).series;
          return underReplicated.samples.at(-1)?.partitions[0];
        },
        { timeout: 45_000, interval: 1_000 },
      )
      .toMatchObject({ leader: 2, replicas: 2, inSyncReplicas: 1 });
    expect(analyzeObservations(underReplicated, clock).hints).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Replication evidence needs attention" }),
      ]),
    );
    stage = "observe offline leader";
    let offline = (await capture(single)).series;
    await expect
      .poll(
        async () => {
          offline = (await capture(single)).series;
          return offline.samples.at(-1)?.partitions[0];
        },
        { timeout: 30_000, interval: 1_000 },
      )
      .toMatchObject({ leader: null, endOffset: null });
    expect(offline.samples.at(-1)?.state).toBe("partial");
    expect(analyzeObservations(offline, clock).hints).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Replication evidence needs attention" }),
      ]),
    );

    stage = "restart broker and observe recovery";
    await fixture.startBroker(1);
    let recovered = (await capture(replicated)).series;
    await expect
      .poll(
        async () => {
          recovered = (await capture(replicated)).series;
          return recovered.samples.at(-1)?.partitions[0];
        },
        { timeout: 45_000, interval: 1_000 },
      )
      .toMatchObject({ replicas: 2, inSyncReplicas: 2, endOffset: "1" });
    expect(analyzeObservations(recovered, clock).hints).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "Replication evidence needs attention" }),
      ]),
    );
    await expect
      .poll(async () => (await capture(single)).series.samples.at(-1)?.partitions[0], {
        timeout: 30_000,
        interval: 1_000,
      })
      .toMatchObject({ leader: 1, replicas: 1, inSyncReplicas: 1, endOffset: "1" });
  } catch (error) {
    throw new Error(`Real replication qualification failed while attempting to ${stage}.`, {
      cause: error,
    });
  } finally {
    await Promise.allSettled([producer.close(), session.disconnect()]);
    await fixture.dispose();
  }
}, 180_000);
