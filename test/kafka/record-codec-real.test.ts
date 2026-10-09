import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";

import { Admin, Producer } from "@platformatic/kafka";
import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
  type KafkaMessage,
  type RecordFormat,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import {
  fetchFixtureToken,
  loadFixtureConfig,
  loadFixtureConnection,
} from "../support/kafka-fixture";

it("decodes real Kafka original bytes using authenticated Registry IDs and Protobuf references", async () => {
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  if (!fixture.schemaRegistryEndpoint) throw new Error("A real Registry is required");
  const ca = await readFile(fixture.caPath, "utf8");
  const options = {
    bootstrapBrokers: [fixture.kafkaEndpoint],
    clientId: `codec-${randomUUID()}`,
    retries: 0,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    sasl: { mechanism: "OAUTHBEARER" as const, token: await fetchFixtureToken(fixture, config) },
    tls: { ca: [ca], rejectUnauthorized: true },
  };
  const admin = new Admin(options);
  const producer = new Producer<Buffer, Buffer | null, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
  });
  const backend = createKafkaBackend();
  const registry = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp());
  const context = {
    baseUrl: fixture.schemaRegistryEndpoint,
    authorization: async (): Promise<string> =>
      `Bearer ${await fetchFixtureToken(fixture, config)}`,
  };
  const signal = new AbortController().signal;
  const topic = `streamskope-codec-${randomUUID()}`;
  const subjects: string[] = [];
  const messages: KafkaMessage[] = [];
  backend.subscribe((event) => {
    if (event.event === "messages.batch") messages.push(...event.payload.messages);
  });
  let created = false;
  let secondCreated = false;
  const secondTopic = `${topic}-trace`;
  try {
    const register = async (
      suffix: string,
      schemaType: "AVRO" | "PROTOBUF",
      schema: string,
      references: { name: string; subject: string; version: number }[] = [],
    ): Promise<number> => {
      const subject = `${topic}-${suffix}`;
      const result = await registry.register(
        context,
        { subject, version: "latest", normalize: false, schemaType, schema, references },
        signal,
      );
      subjects.push(subject);
      return result.id;
    };
    const avroId = await register(
      "avro",
      "AVRO",
      '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}',
    );
    await register(
      "detail",
      "PROTOBUF",
      'syntax="proto3"; package fixture; message Detail { string name=1; }',
    );
    const protoId = await register(
      "proto",
      "PROTOBUF",
      'syntax="proto3"; package fixture; import "detail.proto"; message Event { int64 id=1; Detail detail=2; }',
      [{ name: "detail.proto", subject: `${topic}-detail`, version: 1 }],
    );
    const frame = (id: number, hex: string): Buffer => {
      const header = Buffer.alloc(5);
      header.writeUInt32BE(id, 1);
      return Buffer.concat([header, Buffer.from(hex, "hex")]);
    };
    const values = [
      frame(avroId, "feffffffffffffffff01046f6b"),
      frame(protoId, "0008ffffffffffffffff7f12040a026f6b"),
      frame(2147483647, "00"),
      null,
    ];
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    created = true;
    await producer.send({
      messages: values.map((value) => ({
        topic,
        partition: 0,
        key: Buffer.from("fixture"),
        value,
      })),
    });
    expect(
      await backend.execute({
        command: "connection.connect",
        id: "codec-connect",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          name: "Structured fixture",
          brokers: [fixture.kafkaEndpoint],
          oauth: {
            clientId: config.oauthClientId,
            clientSecret: config.oauthClientSecret,
            scope: config.oauthScope,
            tokenEndpoint: fixture.oauthEndpoint,
          },
          tls: { caPem: ca, enabled: true },
          services: {
            schemaRegistry: {
              baseUrl: fixture.schemaRegistryEndpoint,
              authentication: "oauth",
            },
          },
        },
      }),
    ).toMatchObject({ ok: true });
    expect(
      await backend.execute({
        command: "messages.start",
        id: "codec-read",
        version: HOST_PROTOCOL_VERSION,
        payload: { mode: "earliest", topic, maxMessages: values.length },
      }),
    ).toMatchObject({ ok: true });
    await vi.waitFor(() => expect(messages).toHaveLength(values.length), { timeout: 10_000 });
    // Production reads prepare each record before emitting it, using the record's
    // writer ID rather than a topic-wide encoding or a separate inspector decode.
    expect(messages[0]?.structured?.value).toMatchObject({
      state: "decoded",
      codec: "avro",
      writerSchema: {
        id: avroId,
        format: "avro",
        messageType: null,
        registry: new URL(fixture.schemaRegistryEndpoint).toString(),
      },
    });
    expect(messages[1]?.structured?.value).toMatchObject({
      state: "decoded",
      codec: "protobuf",
      writerSchema: {
        id: protoId,
        format: "protobuf",
        messageType: ".fixture.Event",
        registry: new URL(fixture.schemaRegistryEndpoint).toString(),
      },
    });
    expect(JSON.parse(messages[0]!.payload!)).toEqual({ id: "9223372036854775807", name: "ok" });
    expect(JSON.parse(messages[1]!.payload!)).toEqual({
      id: "9223372036854775807",
      detail: { name: "ok" },
    });
    expect(messages[2]?.structured?.value).toMatchObject({
      state: "error",
      code: "schema-unavailable",
    });
    expect(messages[3]?.structured?.value).toMatchObject({ state: "null" });
    const decoded = [];
    for (const [index, message] of messages.entries()) {
      expect(message.original?.state).toBe("complete");
      if (message.original?.state !== "complete") throw new Error("Original bytes missing");
      expect(message.original.value).toBe(values[index]?.toString("base64") ?? null);
      const format: RecordFormat = index === 1 ? "protobuf" : "avro";
      const result = parseHostCommandResponse(
        await backend.execute({
          command: "records.decode",
          id: `decode-${index}`,
          version: HOST_PROTOCOL_VERSION,
          payload: { format, bytes: message.original.value },
        }),
      );
      if (!result.ok || result.command !== "records.decode")
        throw new Error("Host rejected decode");
      decoded.push(result.result.decoded);
    }
    expect(decoded[0], JSON.stringify(decoded[0])).toMatchObject({
      state: "decoded",
      schemaId: avroId,
    });
    expect(decoded[1]).toMatchObject({
      state: "decoded",
      schemaId: protoId,
      messageType: ".fixture.Event",
    });
    if (decoded[0]?.state === "decoded")
      expect(JSON.parse(decoded[0].json)).toEqual({ id: "9223372036854775807", name: "ok" });
    if (decoded[1]?.state === "decoded")
      expect(JSON.parse(decoded[1].json)).toEqual({
        id: "9223372036854775807",
        detail: { name: "ok" },
      });
    expect(decoded[2]).toMatchObject({ state: "error", code: "schema-unavailable" });
    expect(decoded[3]).toEqual({ state: "null", format: "avro" });
    const inspection = parseHostCommandResponse(
      await backend.execute({
        command: "schemas.inspect",
        id: "inspect-proto",
        version: HOST_PROTOCOL_VERSION,
        payload: { subject: `${topic}-proto`, version: 1 },
      }),
    );
    expect(inspection).toMatchObject({
      ok: true,
      command: "schemas.inspect",
      result: {
        inspection: {
          root: { subject: `${topic}-proto`, version: 1, id: protoId },
          limited: false,
          edges: [
            {
              to: { subject: `${topic}-detail`, version: 1 },
              state: "resolved",
              name: "detail.proto",
            },
          ],
        },
      },
    });
    const endOffset = async (): Promise<bigint | undefined> =>
      (
        await admin.listOffsets({
          topics: [{ name: topic, partitions: [{ partitionIndex: 0, timestamp: -1n }] }],
        })
      )[0]?.partitions[0]?.offset;
    const before = await endOffset();
    const generated = [];
    for (const suffix of ["avro", "proto"]) {
      const response = parseHostCommandResponse(
        await backend.execute({
          command: "schemas.samples",
          id: `sample-${suffix}`,
          version: HOST_PROTOCOL_VERSION,
          payload: {
            subject: `${topic}-${suffix}`,
            version: 1,
            seed: 42,
            count: 2,
            messageType: "",
          },
        }),
      );
      if (!response.ok || response.command !== "schemas.samples")
        throw new Error(JSON.stringify(response));
      generated.push(...response.result.samples.samples);
    }
    expect(await endOffset()).toBe(before); // Preview has no broker side effect.
    const review = parseHostCommandResponse(
      await backend.execute({
        command: "records.batch.review",
        id: "sample-review",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          topic,
          partition: 0,
          ratePerSecond: 10,
          records: generated.map((sample) => sample.record),
        },
      }),
    );
    if (!review.ok || review.command !== "records.batch.review")
      throw new Error(JSON.stringify(review));
    expect(await endOffset()).toBe(before); // Destination validation also does not produce.
    const apply = {
      command: "records.batch.apply" as const,
      id: "sample-apply",
      version: HOST_PROTOCOL_VERSION,
      payload: { planId: review.result.review.planId },
    };
    expect(await backend.execute(apply)).toMatchObject({
      ok: true,
      result: { outcome: { total: 4, unsent: 0, stopReason: "complete" } },
    });
    expect(await endOffset()).toBe(before! + 4n);
    await backend.execute({ ...apply, id: "duplicate-confirmation" });
    expect(await endOffset()).toBe(before! + 4n);
    messages.length = 0;
    await backend.execute({
      command: "messages.start",
      id: "sample-readback",
      version: HOST_PROTOCOL_VERSION,
      payload: { mode: "earliest", topic, maxMessages: 8 },
    });
    await vi.waitFor(() => expect(messages).toHaveLength(8), { timeout: 10_000 });
    for (const [index, sample] of generated.entries()) {
      expect(messages[index + 4]?.original).toEqual(sample.record);
    }
    await admin.createTopics({ topics: [secondTopic], partitions: 1, replicas: 1 });
    secondCreated = true;
    await producer.send({
      messages: [
        {
          topic: secondTopic,
          partition: 0,
          key: Buffer.from("fixture"),
          value: Buffer.from('{"cid":"fixture"}'),
          headers: new Map([[Buffer.from("cid"), Buffer.from("fixture")]]),
        },
        {
          topic: secondTopic,
          partition: 0,
          key: Buffer.from("fixture"),
          value: Buffer.from('{"cid":"fixture"}'),
          headers: new Map([[Buffer.from("cid"), Buffer.from("fixture")]]),
        },
        {
          topic: secondTopic,
          partition: 0,
          key: Buffer.from("different"),
          value: Buffer.from('{"cid":"different"}'),
        },
      ],
    });
    const traceInput = {
      traceId: "real-trace",
      topics: [topic, secondTopic],
      startTimeMs: Date.now() - 120_000,
      endTimeMs: Date.now() + 1_000,
      value: "fixture",
      selector: { source: "key" as const, path: "", format: "json" as const },
    };
    const traced = parseHostCommandResponse(
      await backend.execute({
        command: "records.trace",
        id: "real-trace",
        version: HOST_PROTOCOL_VERSION,
        payload: traceInput,
      }),
    );
    expect(traced).toMatchObject({
      ok: true,
      command: "records.trace",
      result: {
        trace: {
          topics: [
            { topic, state: "searched", matches: 4 },
            { topic: secondTopic, state: "searched", matches: 2 },
          ],
        },
      },
    });
    if (!traced.ok || traced.command !== "records.trace") throw new Error("Trace failed");
    expect(traced.result.trace.matches.map((item) => [item.topic, item.offset])).toEqual([
      [topic, "0"],
      [topic, "1"],
      [topic, "2"],
      [topic, "3"],
      [secondTopic, "0"],
      [secondTopic, "1"],
    ]);
    const payloadTrace = await backend.execute({
      command: "records.trace",
      id: "payload-trace",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        ...traceInput,
        traceId: "payload-trace",
        topics: [secondTopic],
        selector: { source: "payload", path: "/cid", format: "json" },
      },
    });
    expect(payloadTrace).toMatchObject({
      ok: true,
      result: { trace: { topics: [{ state: "searched", matches: 2 }] } },
    });
    expect(
      await backend.execute({
        command: "records.trace",
        id: "header-trace",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          ...traceInput,
          traceId: "header-trace",
          topics: [secondTopic],
          selector: { source: "header", path: "cid", format: "json" },
        },
      }),
    ).toMatchObject({
      ok: true,
      result: { trace: { topics: [{ state: "searched", matches: 2 }] } },
    });
    expect(
      await backend.execute({
        command: "records.trace",
        id: "protobuf-trace",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          ...traceInput,
          traceId: "protobuf-trace",
          topics: [topic],
          value: "ok",
          selector: { source: "payload", path: "/detail/name", format: "protobuf" },
        },
      }),
    ).toMatchObject({
      ok: true,
      result: {
        trace: { matches: [{ topic, offset: "1" }], topics: [{ state: "partial", matches: 1 }] },
      },
    });
    const mixedTrace = await backend.execute({
      command: "records.trace",
      id: "mixed-schema-trace",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        ...traceInput,
        traceId: "mixed-schema-trace",
        topics: [topic],
        value: "9223372036854775807",
        selector: { source: "payload", path: "/id", format: "auto" },
      },
    });
    expect(mixedTrace).toMatchObject({
      ok: true,
      result: { trace: { topics: [{ state: "partial" }] } },
    });
    if (!mixedTrace.ok || mixedTrace.command !== "records.trace")
      throw new Error("Mixed trace failed");
    expect(mixedTrace.result.trace.matches.map((match) => match.offset)).toEqual(["0", "1"]);
    expect(await endOffset()).toBe(before! + 4n); // Trace never writes or changes the earlier sample count.
  } finally {
    await backend.shutdown();
    await producer.close();
    try {
      if (created) await admin.deleteTopics({ topics: [topic] });
      if (secondCreated) await admin.deleteTopics({ topics: [secondTopic] });
    } finally {
      await admin.close();
    }
    for (const subject of subjects.reverse())
      await registry.delete(
        context,
        { target: { kind: "subject", subject }, mode: "permanent", confirmation: subject },
        signal,
      );
  }
}, 60_000);
