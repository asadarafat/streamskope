import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin } from "@platformatic/kafka";
import { expect, it, vi } from "vitest";

import { startNativeKafkaFixture } from "../support/native-kafka-fixture";
import {
  MemoryRepairJobStore,
  RepairJournal,
} from "../../src/features/kafka/application/repair-journal";
import {
  KafkaApplicationSession,
  KafkaProfileService,
  InMemoryKafkaProfileStore,
  type KafkaActiveConnection,
} from "../../src/features/kafka/application";
import { RecordReplayService } from "../../src/features/kafka/application/record-replay-service";
import { SavedReplayDestinations } from "../../src/features/kafka/application/replay-destination";
import { RepairReconciliationReader } from "../../src/features/kafka/application/repair-reconciliation-reader";
import { RepairRecoveryService } from "../../src/features/kafka/application/repair-recovery-service";
import { KAFKA_RECORD_PROTECTION_DEFAULTS } from "../../src/features/kafka/contracts";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  UNCHANGED_REPLAY_TRANSFORM,
  replayConfirmation,
  type ReplayRecord,
} from "../../src/features/kafka/contracts/record-replay";
import {
  fixtureClientOptions,
  type FixtureConnection,
  provisionSeededFixtureTopic,
} from "../support/kafka-fixture";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";

async function read(connection: KafkaActiveConnection, topic: string): Promise<ReplayRecord[]> {
  const stream = await connection.openMessageStream(
    { topic, mode: "earliest", maxMessages: 20 },
    AbortSignal.timeout(15_000),
  );
  const records: ReplayRecord[] = [];
  try {
    for await (const message of stream) {
      if (message.original?.state !== "complete") throw new Error("Missing complete bytes");
      records.push({
        topic,
        partition: message.partition,
        offset: message.offset,
        timestampMs: String(Date.parse(message.timestamp)),
        original: message.original,
      });
    }
  } finally {
    await stream.close();
  }
  return records;
}
it("replays frozen original bytes to the same topic, another topic and a separately authenticated broker, retaining tombstones, headers and timestamps", async () => {
  const native = await startNativeKafkaFixture();
  try {
    await qualifyReplay({
      kafkaEndpoint: native.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
      oauthEndpoint: native.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
      caPath: native.environment.STREAMSKOPE_TEST_CA_PATH!,
    });
  } finally {
    await native.dispose();
  }
}, 120_000);

async function qualifyReplay(fixture: FixtureConnection): Promise<void> {
  const seeded = await provisionSeededFixtureTopic(fixture);
  const admin = new Admin(await fixtureClientOptions(fixture, seeded.config, "replay-real-setup"));
  const engine = new StreamSkopeKafkaEngine(),
    session = new KafkaApplicationSession(engine);
  const otherTopic = `replay-${randomUUID()}`;
  const target = await startAuthorizationFixture();
  let targetConnection: KafkaActiveConnection | undefined;
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore({ durability: "session", protection: "memory", state: "ready" }),
    {
      decode: (): Promise<never> =>
        Promise.reject(new Error("No trust decode for the plaintext test profile")),
    },
  );
  const repairStore = new MemoryRepairJobStore();
  const journal = new RepairJournal(repairStore);
  const service = new RecordReplayService(
    () => session.reviewedWriteScope(),
    new SavedReplayDestinations(profiles, engine),
    undefined,
    journal,
  );
  let recovery: RepairRecoveryService | undefined;
  try {
    await admin.createTopics({ topics: [otherTopic], partitions: 1, replicas: 1 });
    await target.admin.createTopics({ topics: [otherTopic], partitions: 1, replicas: 1 });
    await expect
      .poll(
        async () => {
          try {
            await target.admin.findCoordinator({ keyType: 0, keys: ["replay-readback-readiness"] });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15_000, interval: 250 },
      )
      .toBe(true);
    await expect
      .poll(
        async () => {
          try {
            await admin.findCoordinator({ keyType: 0, keys: ["replay-source-readiness"] });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15_000, interval: 250 },
      )
      .toBe(true);
    const snapshot = await profiles.create({
      name: "Other real broker",
      transport: "plaintext",
      brokers: target.connection.brokers,
    });
    const profile = snapshot.profiles[0]!;
    await session.connect({
      brokers: [fixture.kafkaEndpoint],
      name: "Original source",
      tls: { enabled: true, caPem: await readFile(fixture.caPath, "utf8") },
      oauth: {
        clientId: seeded.config.oauthClientId,
        clientSecret: seeded.config.oauthClientSecret,
        scope: seeded.config.oauthScope,
        tokenEndpoint: fixture.oauthEndpoint,
      },
    });
    const connection = session.writeContext()!.connection;
    const timestamp = String(Date.now() - 1000);
    const headers = [
      { key: Buffer.from("repeated").toString("base64"), value: null },
      { key: Buffer.from("repeated").toString("base64"), value: "" },
    ];
    for (const value of [Buffer.from("before before").toString("base64"), null]) {
      expect(
        await connection.applyWrite!({
          kind: "record",
          topic: seeded.config.topic,
          partition: 0,
          timestamp,
          record: { state: "complete", encoding: "base64", key: "", value, headers },
        }),
      ).toMatchObject({ state: "acknowledged", verification: "verified" });
    }
    const records = (await read(connection, seeded.config.topic)).filter(
      (r) => r.original.key === "",
    );
    expect(records).toHaveLength(2);
    expect(records.map((r) => r.timestampMs)).toEqual([timestamp, timestamp]);
    const input = {
      targetProfile: null,
      topic: seeded.config.topic,
      partition: 0,
      ratePerSecond: 10,
      records,
      transform: UNCHANGED_REPLAY_TRANSFORM,
    };
    const same = await service.review(input);
    expect((await read(connection, seeded.config.topic)).length).toBe(3); // Preview did not produce.
    expect(await service.apply(same.planId, replayConfirmation(same))).toMatchObject({
      total: 2,
      unsent: 0,
      stopReason: "complete",
    });
    expect(await new RepairJournal(repairStore).list()).toMatchObject([
      {
        id: same.planId,
        status: "complete",
        unsent: 0,
        outcomes: [{ state: "acknowledged" }, { state: "acknowledged" }],
      },
    ]);
    const copied = await read(connection, seeded.config.topic);
    expect(copied.slice(-2).map((r) => r.original)).toEqual(records.map((r) => r.original));
    const other = await service.review({ ...input, topic: otherTopic });
    expect(await service.apply(other.planId, replayConfirmation(other))).toMatchObject({
      stopReason: "complete",
    });
    expect((await read(connection, otherTopic)).map((r) => r.original)).toEqual(
      records.map((r) => r.original),
    );
    const cross = await service.review({
      ...input,
      topic: otherTopic,
      targetProfile: { id: profile.id, revision: profile.revision ?? 1 },
      transform: {
        ...UNCHANGED_REPLAY_TRANSFORM,
        valueText: { search: "before", replacement: "after" },
      },
    });
    expect(cross.destination.clusterId).not.toBe(same.destination.clusterId);
    const result = await service.apply(cross.planId, replayConfirmation(cross));
    expect(result).toMatchObject({ stopReason: "complete", cleanup: "complete", unsent: 0 });
    expect(result.outcomes.every((r) => r.verification === "verified")).toBe(true);
    expect(session.writeContext()?.connection).toBe(connection);
    expect(session.snapshot().connectionName).toBe("Original source");
    targetConnection = await engine.openConnection(target.connection, AbortSignal.timeout(15_000));
    const delivered = await read(targetConnection, otherTopic);
    expect(delivered.map((r) => r.original)).toEqual([
      { ...records[0]!.original, value: Buffer.from("after after").toString("base64") },
      records[1]!.original,
    ]);
    expect(delivered.map((r) => r.timestampMs)).toEqual([timestamp, timestamp]);
    expect(await service.apply(cross.planId, replayConfirmation(cross))).toEqual(result);
    expect(await read(targetConnection, otherTopic)).toHaveLength(2);
    // Independently inject receipt-storage loss after an actual acknowledged broker send.
    // A fresh owner must skip that uncertain position and publish only the unsent tombstone.
    const interrupted = await service.review({
      ...input,
      topic: otherTopic,
      targetProfile: { id: profile.id, revision: profile.revision ?? 1 },
    });
    const commit = repairStore.commit.bind(repairStore);
    const failure = vi.spyOn(repairStore, "commit").mockImplementation(async (d) => {
      if (d.jobs.find((j) => j.id === interrupted.planId)?.outcomes.length)
        throw new Error("injected receipt loss");
      await commit(d);
    });
    const uncertain = await service.apply(interrupted.planId, replayConfirmation(interrupted));
    failure.mockRestore();
    expect(uncertain).toMatchObject({
      stopReason: "journal-unavailable",
      unsent: 1,
      outcomes: [{ state: "acknowledged" }],
    });
    expect(await read(targetConnection, otherTopic)).toHaveLength(3);
    const reopenedJournal = new RepairJournal(repairStore);
    const reopenedReplay = new RecordReplayService(
      () => session.reviewedWriteScope(),
      new SavedReplayDestinations(profiles, engine),
      undefined,
      reopenedJournal,
    );
    recovery = new RepairRecoveryService(
      reopenedJournal,
      reopenedReplay,
      new RepairReconciliationReader(
        () => {
          const scope = session.reviewedWriteScope(),
            readScope = session.recordReadScope();
          return scope && readScope
            ? { scope, readScope, close: (): Promise<void> => Promise.resolve() }
            : null;
        },
        () => ({
          codecs: { key: "auto", value: "auto" },
          protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
        }),
        new SavedReplayDestinations(profiles, engine),
      ),
    );
    const observation = await recovery.reconcile({
      jobId: interrupted.planId,
      recordIndex: 0,
      offset: uncertain.outcomes[0]!.receipt!.offset,
      targetProfile: { id: profile.id, revision: profile.revision ?? 1 },
    });
    expect(observation).toMatchObject({ state: "equivalent", cleanup: "complete" });
    expect((await reopenedJournal.snapshot(interrupted.planId)).pendingIndex).toBe(0);
    const continuation = await recovery.review({
      jobId: interrupted.planId,
      targetProfile: { id: profile.id, revision: profile.revision ?? 1 },
    });
    expect(continuation.skipped).toEqual({ acknowledged: 0, rejected: 0, uncertain: 1 });
    expect(continuation.review.batch.records).toEqual([records[1]!.original]);
    expect(
      await reopenedReplay.apply(
        continuation.review.planId,
        replayConfirmation(continuation.review),
      ),
    ).toMatchObject({ stopReason: "complete", total: 1, unsent: 0 });
    const recovered = await read(targetConnection, otherTopic);
    expect(recovered).toHaveLength(4);
    expect(recovered.slice(2).map((r) => r.original)).toEqual(records.map((r) => r.original));
    const root = (await reopenedJournal.list()).find((j) => j.id === interrupted.planId)!;
    expect(root).toMatchObject({
      uncertainIndex: 0,
      continuationId: continuation.review.planId,
      canArchive: false,
    });
  } finally {
    await recovery?.invalidate();
    await service.invalidate();
    await targetConnection?.close();
    await session.shutdown();
    await admin.deleteTopics({ topics: [otherTopic] }).catch(() => undefined);
    await admin.close();
    await seeded.dispose();
    await target.dispose();
  }
}
