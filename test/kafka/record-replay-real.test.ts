import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin } from "@platformatic/kafka";
import avro from "avsc";
import protobuf from "protobufjs";
import { expect, it, vi } from "vitest";

import {
  startNativeKafkaFixture,
  disposeNativeFixtureResources,
} from "../support/native-kafka-fixture";
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
import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type SchemaRegistryType,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend, type NodeKafkaBackend } from "../../src/platform/node/kafka-backend";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { startSchemaRegistryServerFixture } from "../support/schema-registry-server-fixture";
import type { KafkaCompleteRecord } from "../../src/features/kafka/contracts/record-bytes";
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

it("reviews missing destination writers, translates colliding Registry IDs and continues frozen bytes after real receipt loss with the source Registry offline", async () => {
  const fixture = await startNativeKafkaFixture();
  const store = new InMemoryKafkaProfileStore({
    durability: "session",
    protection: "memory",
    state: "ready",
  });
  const repairStore = new MemoryRepairJobStore();
  let backend: NodeKafkaBackend = createKafkaBackend({ profileStore: store, repairStore });
  const adapter = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp());
  const engine = new StreamSkopeKafkaEngine();
  let sourceRegistry: Awaited<ReturnType<typeof startSchemaRegistryServerFixture>> | undefined;
  let destinationRegistry: Awaited<ReturnType<typeof startSchemaRegistryServerFixture>> | undefined;
  let connection: KafkaActiveConnection | undefined;
  const admin = new Admin({
    bootstrapBrokers: [fixture.internalBroker],
    clientId: "structured-replay-fixture",
    retries: 0,
  });
  const sourceTopic = `structured-source-${randomUUID()}`,
    destinationTopic = `structured-destination-${randomUUID()}`;
  const signal = AbortSignal.timeout(180_000);
  const execute = async (
    command: string,
    payload: unknown,
  ): Promise<ReturnType<typeof parseHostCommandResponse>> =>
    parseHostCommandResponse(
      await backend.execute(
        parseHostCommand({ command, payload, version: HOST_PROTOCOL_VERSION, id: randomUUID() }),
      ),
    );
  const context = (baseUrl: string): { baseUrl: string; authorization(): Promise<undefined> } => ({
    baseUrl,
    authorization: () => Promise.resolve(undefined),
  });
  const connect = async (url?: string): Promise<void> => {
    expect(
      await execute("connection.connect", {
        name: "Structured source",
        tls: { enabled: false },
        brokers: [fixture.internalBroker],
        ...(url ? { services: { schemaRegistry: { baseUrl: url, authentication: "none" } } } : {}),
      }),
    ).toMatchObject({ ok: true });
  };
  const reviewedRegister = async (
    subject: string,
    schemaType: SchemaRegistryType,
    schema: string,
    references: { name: string; subject: string; version: number }[] = [],
  ): Promise<number> => {
    const reviewed = await execute("schemas.change.review", {
      draft: { subject, version: "latest", schemaType, schema, references, normalize: false },
      expectedWriter: null,
    });
    expect(reviewed, JSON.stringify(reviewed)).toMatchObject({
      ok: true,
      result: { review: { before: null, compatible: true } },
    });
    if (!reviewed.ok || reviewed.command !== "schemas.change.review")
      throw new Error("Missing reviewed registration.");
    expect(
      await adapter.loadReviewSchema(
        context(destinationRegistry!.url),
        { subject, version: "latest" },
        signal,
      ),
    ).toBeNull();
    const result = await execute("schemas.change.apply", {
      planId: reviewed.result.review.planId,
      confirmation: subject,
    });
    expect(result, JSON.stringify(result)).toMatchObject({
      ok: true,
      result: {
        outcome: { state: "acknowledged", verification: "verified", observed: { version: 1 } },
      },
    });
    if (
      !result.ok ||
      result.command !== "schemas.change.apply" ||
      result.result.outcome.id === null
    )
      throw new Error("Missing registration readback.");
    return result.result.outcome.id;
  };
  const frame = (id: number, body: Uint8Array): string => {
    const header = Buffer.alloc(5);
    header.writeUInt32BE(id, 1);
    return Buffer.concat([header, body]).toString("base64");
  };
  try {
    sourceRegistry = await startSchemaRegistryServerFixture(fixture.internalBroker);
    destinationRegistry = await startSchemaRegistryServerFixture(fixture.internalBroker);
    await admin.createTopics({
      topics: [sourceTopic, destinationTopic],
      partitions: 1,
      replicas: 1,
    });
    await expect
      .poll(
        async () => {
          try {
            await admin.findCoordinator({ keyType: 0, keys: ["structured-replay-reader"] });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15_000, interval: 250 },
      )
      .toBe(true);
    const sourceAvro =
      '{"type":"record","name":"Event","fields":[{"name":"name","type":"string"},{"name":"id","type":"long"},{"name":"detail","type":{"type":"record","name":"Detail","fields":[{"name":"name","type":"string"}]}}]}';
    const sourceProto =
      'syntax="proto3"; message Event { string name=1; int64 id=2; message Detail { string name=1; } Detail detail=3; }';
    const registerSource = (
      suffix: string,
      schemaType: SchemaRegistryType,
      schema: string,
    ): Promise<{ id: number }> =>
      adapter.register(
        context(sourceRegistry!.url),
        {
          subject: `${sourceTopic}-${suffix}`,
          version: "latest",
          schemaType,
          schema,
          references: [],
          normalize: false,
        },
        signal,
      );
    const avroId = (await registerSource("avro", "AVRO", sourceAvro)).id;
    const protoId = (await registerSource("proto", "PROTOBUF", sourceProto)).id;
    expect(avroId).toBe(1);
    await connect(destinationRegistry.url);
    const unrelated = await reviewedRegister(`${destinationTopic}-collision`, "AVRO", '"string"');
    expect(unrelated).toBe(avroId);
    expect(
      (await adapter.byId(context(destinationRegistry.url), unrelated, signal)).schema,
    ).not.toBe(sourceAvro);
    const avroDetailSubject = `${destinationTopic}-avro-detail`,
      avroSubject = `${destinationTopic}-avro`;
    await reviewedRegister(
      avroDetailSubject,
      "AVRO",
      '{"type":"record","name":"Detail","fields":[{"name":"name","type":"string"}]}',
    );
    const targetAvro =
      '{"type":"record","name":"Event","fields":[{"name":"name","type":"string"},{"name":"id","type":"long"},{"name":"detail","type":"Detail"}]}';
    const targetAvroId = await reviewedRegister(avroSubject, "AVRO", targetAvro, [
      { name: "Detail", subject: avroDetailSubject, version: 1 },
    ]);
    const protoDetailSubject = `${destinationTopic}-proto-detail`,
      protoSubject = `${destinationTopic}-proto`;
    const targetDetail = 'syntax="proto3"; message Detail { string name=1; }';
    const targetProto =
      'syntax="proto3"; import "detail.proto"; message Other { bool ignored=1; } message Event { string name=1; int64 id=2; Detail detail=3; }';
    await reviewedRegister(protoDetailSubject, "PROTOBUF", targetDetail);
    const targetProtoId = await reviewedRegister(protoSubject, "PROTOBUF", targetProto, [
      { name: "detail.proto", subject: protoDetailSubject, version: 1 },
    ]);
    const jsonSubject = `${destinationTopic}-json`;
    await reviewedRegister(
      jsonSubject,
      "JSON",
      '{"type":"object","required":["name","id","detail"],"additionalProperties":false,"properties":{"name":{"type":"string"},"id":{"type":"string"},"detail":{"type":"object","required":["name"],"additionalProperties":false,"properties":{"name":{"type":"string"}}}}}',
    );
    const profileResult = await execute("profiles.create", {
      profile: {
        name: "Destination Registry",
        brokers: [fixture.internalBroker],
        transport: "plaintext",
        services: { schemaRegistry: { baseUrl: destinationRegistry.url, authentication: "none" } },
      },
    });
    expect(profileResult).toMatchObject({ ok: true });
    if (!profileResult.ok || profileResult.command !== "profiles.create")
      throw new Error("Missing destination profile.");
    const profile = store.records()[0]!;
    const targetProfile = { id: profile.id, revision: profile.revision ?? 1 };
    await connect(sourceRegistry.url);
    connection = await engine.openConnection(
      {
        name: "Independent fixture reader",
        tls: { enabled: false },
        brokers: [fixture.internalBroker],
      },
      signal,
    );
    const payload = { name: "before", id: 17, detail: { name: "child" } };
    const independentSourceProto = protobuf
      .parse(sourceProto, { keepCase: true })
      .root.lookupType("Event");
    const values = [
      frame(avroId, avro.Type.forSchema(JSON.parse(sourceAvro) as avro.Schema).toBuffer(payload)),
      frame(
        protoId,
        Buffer.concat([
          Buffer.from([0]),
          independentSourceProto.encode(independentSourceProto.fromObject(payload)).finish(),
        ]),
      ),
      Buffer.from(JSON.stringify({ ...payload, id: "17" })).toString("base64"),
      null,
    ];
    const timestamp = String(Date.now() - 1000),
      headers = [
        { key: "aA==", value: null },
        { key: "aA==", value: "" },
      ];
    for (const value of values)
      expect(
        await connection.applyWrite!({
          kind: "record",
          topic: sourceTopic,
          partition: 0,
          timestamp,
          record: { state: "complete", encoding: "base64", key: null, value, headers },
        }),
      ).toMatchObject({ state: "acknowledged" });
    const originals = await read(connection, sourceTopic);
    expect(originals).toHaveLength(4);
    const mappings = [
      {
        format: "avro",
        sourceId: avroId,
        target: { subject: avroSubject, version: 1, messageType: "" },
      },
      {
        format: "protobuf",
        sourceId: protoId,
        target: { subject: protoSubject, version: 1, messageType: "Event" },
      },
      {
        format: "json",
        sourceId: null,
        target: { subject: jsonSubject, version: 1, messageType: "" },
      },
    ];
    const input = {
      targetProfile,
      topic: destinationTopic,
      partition: 0,
      ratePerSecond: 10,
      records: originals,
      transform: {
        ...UNCHANGED_REPLAY_TRANSFORM,
        structured: {
          key: null,
          value: {
            codec: "auto",
            patches: [{ op: "set", path: "/name", json: '"after"' }],
            mappings,
          },
        },
      },
    };
    const subjectsBefore = await adapter.listSubjects(context(destinationRegistry.url), signal);
    const reviewed = await execute("records.replay.review", input);
    expect(reviewed, JSON.stringify(reviewed)).toMatchObject({ ok: true });
    if (!reviewed.ok || reviewed.command !== "records.replay.review")
      throw new Error("Missing structured replay.");
    const review = reviewed.result.review;
    expect(await adapter.listSubjects(context(destinationRegistry.url), signal)).toEqual(
      subjectsBefore,
    );
    expect(
      await admin.listOffsets({
        topics: [{ name: destinationTopic, partitions: [{ partitionIndex: 0, timestamp: -1n }] }],
      }),
    ).toMatchObject([{ partitions: [{ offset: 0n }] }]);
    expect(
      await execute("records.replay.apply", {
        planId: review.planId,
        confirmation: replayConfirmation(review),
      }),
    ).toMatchObject({
      ok: true,
      result: { outcome: { total: 4, unsent: 0, stopReason: "complete", journal: "confirmed" } },
    });
    const delivered = await read(connection, destinationTopic);
    expect(delivered.map((r) => r.original)).toEqual(review.batch.records);
    expect(delivered.map((r) => r.timestampMs)).toEqual(Array(4).fill(timestamp));
    const independentAvro = avro.Type.forSchema(JSON.parse(targetAvro) as avro.Schema, {
      registry: {
        Detail: avro.Type.forSchema({
          type: "record",
          name: "Detail",
          fields: [{ name: "name", type: "string" }],
        }),
      },
    });
    const avroWire = Buffer.from(delivered[0]!.original.value!, "base64");
    expect(avroWire.readUInt32BE(1)).toBe(targetAvroId);
    expect(independentAvro.fromBuffer(avroWire.subarray(5))).toEqual({ ...payload, name: "after" });
    const independentProto = protobuf.parse(targetProto, { keepCase: true }).root;
    protobuf.parse(targetDetail, independentProto, { keepCase: true });
    const protoWire = Buffer.from(delivered[1]!.original.value!, "base64"),
      event = independentProto.lookupType("Event");
    expect(protoWire.readUInt32BE(1)).toBe(targetProtoId);
    expect(protoWire.subarray(5, 7).toString("hex")).toBe("0202");
    expect(event.toObject(event.decode(protoWire.subarray(7)), { longs: String })).toEqual({
      ...payload,
      id: "17",
      name: "after",
    });
    expect(JSON.parse(Buffer.from(delivered[2]!.original.value!, "base64").toString())).toEqual({
      ...payload,
      id: "17",
      name: "after",
    });
    expect(delivered[3]!.original).toEqual({
      state: "complete",
      encoding: "base64",
      key: null,
      value: null,
      headers,
    });
    expect((await read(connection, sourceTopic)).map((r) => r.original)).toEqual(
      originals.map((r) => r.original),
    );
    const interrupted = await execute("records.replay.review", input);
    if (!interrupted.ok || interrupted.command !== "records.replay.review")
      throw new Error("Missing interrupted review.");
    const originalCommit = repairStore.commit.bind(repairStore),
      crash = vi.spyOn(repairStore, "commit").mockImplementation(async (document) => {
        if (document.jobs.find((j) => j.id === interrupted.result.review.planId)?.outcomes.length)
          throw new Error("injected receipt failure after actual send");
        await originalCommit(document);
      });
    try {
      expect(
        await execute("records.replay.apply", {
          planId: interrupted.result.review.planId,
          confirmation: replayConfirmation(interrupted.result.review),
        }),
      ).toMatchObject({
        ok: true,
        result: {
          outcome: {
            outcomes: [{ state: "acknowledged" }],
            unsent: 3,
            stopReason: "journal-unavailable",
          },
        },
      });
    } finally {
      crash.mockRestore();
    }
    await backend.shutdown();
    await sourceRegistry.dispose();
    sourceRegistry = undefined;
    backend = createKafkaBackend({ profileStore: store, repairStore });
    await connect(); // No source Registry: continuation must use its original frozen output.
    const continuation = await execute("records.repair.review", {
      jobId: interrupted.result.review.planId,
      targetProfile,
    });
    expect(continuation, JSON.stringify(continuation)).toMatchObject({
      ok: true,
      result: { continuation: { skipped: { uncertain: 1, acknowledged: 0, rejected: 0 } } },
    });
    if (!continuation.ok || continuation.command !== "records.repair.review")
      throw new Error("Missing frozen continuation.");
    const continued = continuation.result.continuation.review;
    expect(continued.batch.records).toEqual(interrupted.result.review.batch.records.slice(1));
    expect(continued.encoding).toEqual(interrupted.result.review.encoding!.slice(1));
    expect(
      await execute("records.replay.apply", {
        planId: continued.planId,
        confirmation: replayConfirmation(continued),
      }),
    ).toMatchObject({
      ok: true,
      result: { outcome: { total: 3, unsent: 0, stopReason: "complete" } },
    });
    expect((await read(connection, destinationTopic)).map((r) => r.original)).toEqual([
      ...review.batch.records,
      ...interrupted.result.review.batch.records,
    ]);
    const parent = (await new RepairJournal(repairStore).list()).find(
      (j) => j.id === interrupted.result.review.planId,
    )!;
    expect(parent).toMatchObject({
      uncertainIndex: 0,
      canArchive: false,
      continuationId: continued.planId,
    });
    // Pin the already encoded child, then remove its writer. Fresh admission must stop before send.
    const frozen = await new RepairJournal(repairStore).snapshot(continued.planId);
    await connect(destinationRegistry.url);
    const staleInput = {
      ...input,
      records: [
        { ...originals[2]!, original: frozen.review.batch.records[1] as KafkaCompleteRecord },
      ],
      transform: {
        ...UNCHANGED_REPLAY_TRANSFORM,
        structured: {
          key: null,
          value: {
            codec: "json",
            patches: [],
            mappings: [
              {
                format: "json",
                sourceId: null,
                target: { subject: jsonSubject, version: 1, messageType: "" },
              },
            ],
          },
        },
      },
    };
    const stale = await execute("records.replay.review", staleInput);
    if (!stale.ok || stale.command !== "records.replay.review")
      throw new Error("Missing stale review.");
    await adapter.delete(
      context(destinationRegistry.url),
      {
        target: { kind: "subject", subject: jsonSubject },
        mode: "soft",
        confirmation: jsonSubject,
      },
      signal,
    );
    expect(
      await execute("records.replay.apply", {
        planId: stale.result.review.planId,
        confirmation: replayConfirmation(stale.result.review),
      }),
    ).toMatchObject({
      ok: true,
      result: { outcome: { stopReason: "destination-changed", unsent: 1, outcomes: [] } },
    });
    expect(await read(connection, destinationTopic)).toHaveLength(8);
  } finally {
    await disposeNativeFixtureResources([
      (): Promise<void> => backend.shutdown(),
      async (): Promise<void> => {
        await connection?.close();
      },
      (): Promise<void> => admin.close(),
      async (): Promise<void> => {
        await destinationRegistry?.dispose();
      },
      async (): Promise<void> => {
        await sourceRegistry?.dispose();
      },
      (): Promise<void> => fixture.dispose(),
    ]);
  }
}, 240_000);
