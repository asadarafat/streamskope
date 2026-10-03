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
    expect(decoded[0]).toMatchObject({ state: "decoded", schemaId: avroId });
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
  } finally {
    await backend.shutdown();
    await producer.close();
    try {
      if (created) await admin.deleteTopics({ topics: [topic] });
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
