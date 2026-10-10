import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin } from "@platformatic/kafka";
import avro from "avsc";
import protobuf from "protobufjs";
import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type KafkaExploredMessage,
  type SchemaRegistryType,
} from "../../src/features/kafka/contracts";
import type { KafkaCompleteRecord } from "../../src/features/kafka/contracts/record-bytes";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import {
  startNativeKafkaFixture,
  disposeNativeFixtureResources,
} from "../support/native-kafka-fixture";
import { startSchemaRegistryServerFixture } from "../support/schema-registry-server-fixture";
import { loadFixtureConfig, fixtureClientOptions } from "../support/kafka-fixture";

it("authors registered referenced schemas, publishes reviewed bytes and independently decodes real broker receipts", async () => {
  const fixture = await startNativeKafkaFixture();
  let registry: Awaited<ReturnType<typeof startSchemaRegistryServerFixture>> | undefined;
  let admin: Admin | undefined;
  const backend = createKafkaBackend(),
    adapter = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp());
  const received: KafkaExploredMessage[] = [];
  backend.subscribe((event) => {
    if (event.event === "messages.batch") received.push(...event.payload.messages);
  });
  const topic = `authoring-${randomUUID()}`,
    subjects: string[] = [];
  const signal = AbortSignal.timeout(90_000);
  let topicCreated = false;
  const failures: unknown[] = [];
  const execute = async (
    command: string,
    payload: unknown,
  ): Promise<ReturnType<typeof parseHostCommandResponse>> =>
    parseHostCommandResponse(
      await backend.execute(
        parseHostCommand({ command, payload, version: HOST_PROTOCOL_VERSION, id: randomUUID() }),
      ),
    );
  try {
    registry = await startSchemaRegistryServerFixture(fixture.internalBroker);
    const config = await loadFixtureConfig();
    const connection = {
      kafkaEndpoint: fixture.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
      oauthEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
      caPath: fixture.environment.STREAMSKOPE_TEST_CA_PATH!,
    };
    admin = new Admin(await fixtureClientOptions(connection, config, topic));
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    topicCreated = true;
    const context = {
      baseUrl: registry.url,
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
    };
    const register = async (
      suffix: string,
      schemaType: SchemaRegistryType,
      schema: string,
      references: { name: string; subject: string; version: number }[] = [],
    ): Promise<number> => {
      const subject = `${topic}-${suffix}`;
      const result = await adapter.register(
        context,
        { subject, version: "latest", normalize: false, schemaType, schema, references },
        signal,
      );
      subjects.push(subject);
      return result.id;
    };
    const childAvro =
      '{"type":"record","name":"Detail","fields":[{"name":"name","type":"string"}]}';
    const rootAvro =
      '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"detail","type":"Detail"}]}';
    await register("avro-detail", "AVRO", childAvro);
    const avroId = await register("avro", "AVRO", rootAvro, [
      { name: "Detail", subject: `${topic}-avro-detail`, version: 1 },
    ]);
    const childProto = 'syntax="proto3"; package author; message Detail { string name=1; }';
    const rootProto =
      'syntax="proto3"; package author; import "detail.proto"; message Other { bool ignored=1; } message Event { int64 id=1; Detail detail=2; }';
    await register("proto-detail", "PROTOBUF", childProto);
    const protoId = await register("proto", "PROTOBUF", rootProto, [
      { name: "detail.proto", subject: `${topic}-proto-detail`, version: 1 },
    ]);
    const jsonId = await register(
      "json",
      "JSON",
      '{"type":"object","required":["id"],"additionalProperties":false,"properties":{"id":{"type":"string"}}}',
    );
    expect(
      await execute("connection.connect", {
        name: "Authoring fixture",
        brokers: [connection.kafkaEndpoint],
        oauth: {
          clientId: config.oauthClientId,
          clientSecret: config.oauthClientSecret,
          scope: config.oauthScope,
          tokenEndpoint: connection.oauthEndpoint,
        },
        tls: { enabled: true, caPem: await readFile(connection.caPath, "utf8") },
        services: { schemaRegistry: { baseUrl: registry.url, authentication: "none" } },
      }),
    ).toMatchObject({ ok: true });
    const records: KafkaCompleteRecord[] = [];
    const payload = '{"id":"9223372036854775807","detail":{"name":"edited"}}';
    for (const [suffix, schemaId, messageType, json] of [
      ["avro", avroId, "", payload],
      ["proto", protoId, "author.Event", payload],
      ["json", jsonId, "", '{"id":"edited"}'],
    ] as const) {
      const result = await execute("schemas.author", {
        subject: `${topic}-${suffix}`,
        version: 1,
        schemaId,
        messageType,
        payload: json,
      });
      expect(result).toMatchObject({
        ok: true,
        result: { authoring: { state: "valid", writer: { id: schemaId, version: 1 } } },
      });
      if (
        !result.ok ||
        result.command !== "schemas.author" ||
        result.result.authoring.state !== "valid"
      )
        throw new Error("Authoring did not return validated bytes.");
      records.push(result.result.authoring.record);
    }
    const offsets = (): Promise<unknown> =>
      admin!.listOffsets({
        topics: [{ name: topic, partitions: [{ partitionIndex: 0, timestamp: -1n }] }],
      });
    expect(await offsets()).toMatchObject([{ partitions: [{ offset: 0n }] }]);
    const bad = await execute("schemas.author", {
      subject: `${topic}-avro`,
      version: 1,
      schemaId: avroId,
      messageType: "",
      payload: '{"id":9007199254740993,"detail":{"name":"private-authoring-sentinel"}}',
    });
    expect(bad).toMatchObject({
      ok: true,
      result: { authoring: { state: "invalid", issues: [{ code: "precision" }] } },
    });
    expect(JSON.stringify(bad)).not.toContain("private-authoring-sentinel");
    const samples = await execute("schemas.samples", {
      subject: `${topic}-avro`,
      version: 1,
      messageType: "",
      count: 2,
      seed: 7,
    });
    expect(samples).toMatchObject({ ok: true });
    expect(await offsets()).toMatchObject([{ partitions: [{ offset: 0n }] }]);
    const review = await execute("records.batch.review", {
      topic,
      partition: 0,
      ratePerSecond: 10,
      records,
    });
    if (!review.ok || review.command !== "records.batch.review")
      throw new Error("Destination review unavailable.");
    expect(
      await execute("records.batch.apply", { planId: review.result.review.planId }),
    ).toMatchObject({
      ok: true,
      result: {
        outcome: {
          total: 3,
          unsent: 0,
          stopReason: "complete",
          outcomes: [
            { state: "acknowledged" },
            { state: "acknowledged" },
            { state: "acknowledged" },
          ],
        },
      },
    });
    expect(
      await execute("messages.start", { topic, mode: "earliest", maxMessages: 3 }),
    ).toMatchObject({ ok: true });
    await expect.poll(() => received.length, { timeout: 30_000 }).toBe(3);
    const originals = received.map((message) => {
      if (message.original?.state !== "complete" || message.original.value === null)
        throw new Error("Broker original bytes missing.");
      return Buffer.from(message.original.value, "base64");
    });
    expect(originals.map((wire) => wire.toString("base64"))).toEqual(
      records.map((record) => record.value),
    );
    // Independent libraries compile the original registered definitions, not the production bundle/compiler.
    const long = avro.types.LongType.__with({
      fromBuffer: (b: Buffer) => b.readBigInt64LE().toString(),
      toBuffer: () => Buffer.alloc(8),
      fromJSON: String,
      toJSON: String,
      isValid: (v: unknown) => typeof v === "string",
      compare: () => 0,
    });
    const avroType = avro.Type.forSchema(JSON.parse(rootAvro) as avro.Schema, {
      registry: { Detail: avro.Type.forSchema(JSON.parse(childAvro) as avro.Schema) },
      typeHook: (s) => (s === "long" ? long : undefined),
    });
    expect(avroType.fromBuffer(originals[0]!.subarray(5))).toMatchObject(
      JSON.parse(payload) as object,
    );
    const protoRoot = new protobuf.Root();
    protobuf.parse(childProto, protoRoot, { keepCase: true });
    protobuf.parse(rootProto, protoRoot, { keepCase: true });
    const protoType = protoRoot.lookupType("author.Event");
    expect(originals[1]!.subarray(5, 7).toString("hex")).toBe("0202");
    expect(
      protoType.toObject(protoType.decode(originals[1]!.subarray(7)), { longs: String }),
    ).toEqual(JSON.parse(payload) as object);
    expect(JSON.parse(originals[2]!.toString()) as unknown).toEqual({ id: "edited" });
    await adapter.delete(
      context,
      {
        target: { kind: "subject", subject: `${topic}-avro` },
        mode: "permanent",
        confirmation: `${topic}-avro`,
      },
      signal,
    );
    const replacement = await register("avro", "AVRO", '"string"');
    expect(replacement).not.toBe(avroId);
    expect(
      await execute("schemas.author", {
        subject: `${topic}-avro`,
        version: 1,
        schemaId: avroId,
        messageType: "",
        payload: '"new"',
      }),
    ).toMatchObject({
      ok: true,
      result: { authoring: { state: "invalid", issues: [{ code: "schema" }] } },
    });
  } catch (error) {
    failures.push(error);
  } finally {
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => backend.shutdown(),
        async (): Promise<void> => {
          if (admin && topicCreated) await admin.deleteTopics({ topics: [topic] });
        },
        async (): Promise<void> => {
          if (registry)
            for (const subject of [...new Set(subjects)].reverse())
              await adapter.delete(
                {
                  baseUrl: registry.url,
                  authorization: (): Promise<undefined> => Promise.resolve(undefined),
                },
                { target: { kind: "subject", subject }, mode: "permanent", confirmation: subject },
                new AbortController().signal,
              );
        },
        (): Promise<void> => admin?.close() ?? Promise.resolve(),
        (): Promise<void> => registry?.dispose() ?? Promise.resolve(),
        (): Promise<void> => fixture.dispose(),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Schema authoring or owned cleanup failed", {
      cause: failures[0],
    });
}, 240_000);
