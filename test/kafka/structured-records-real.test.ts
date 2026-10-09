import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin, Producer } from "@platformatic/kafka";
import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseHostEvent,
  type HostCommand,
  type KafkaExploredMessage,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { runReadOnlyCli } from "../../src/platform/node/read-only-cli";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import {
  createKafkaMessageExportDocument,
  initialKafkaMessageFilters,
} from "../../src/features/kafka/ui/message-operations";
import { startNativeKafkaFixture } from "../support/native-kafka-fixture";
import { fetchFixtureToken, loadFixtureConfig } from "../support/kafka-fixture";
import { createSchemaRegistryProtocolFixture } from "../support/schema-registry-protocol-fixture";

it("prepares mixed real Kafka records once for protected search, trace and export using their own writer IDs", async () => {
  const fixture = await startNativeKafkaFixture();
  try {
    await qualifyRecords(fixture);
  } finally {
    await fixture.dispose();
  }
}, 180_000);

async function qualifyRecords(
  fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>>,
): Promise<void> {
  const config = await loadFixtureConfig();
  const connection = {
    kafkaEndpoint: fixture.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
    oauthEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
    caPath: fixture.environment.STREAMSKOPE_TEST_CA_PATH!,
  };
  const options = {
    bootstrapBrokers: [connection.kafkaEndpoint],
    clientId: `structured-${randomUUID()}`,
    retries: 0,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    sasl: { mechanism: "OAUTHBEARER" as const, token: await fetchFixtureToken(connection, config) },
    tls: { ca: [await readFile(connection.caPath, "utf8")], rejectUnauthorized: true },
  };
  const admin = new Admin(options);
  const producer = new Producer<Buffer, Buffer | null, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
  });
  const backend = createKafkaBackend();
  const messages: KafkaExploredMessage[] = [];
  backend.subscribe((wire) => {
    const event = parseHostEvent(JSON.parse(JSON.stringify(wire)));
    if (event.event === "messages.batch") messages.push(...event.payload.messages);
  });
  const registry = createSchemaRegistryProtocolFixture([
    {
      id: 7,
      subject: "event-avro",
      version: 1,
      schemaType: "AVRO",
      references: [],
      schema:
        '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}',
    },
    {
      id: 8,
      subject: "event-protobuf",
      version: 1,
      schemaType: "PROTOBUF",
      references: [],
      schema: 'syntax="proto3"; message Event { int64 id=1; string name=2; }',
    },
  ]);
  const topic = `structured-${randomUUID()}`;
  const integer = "9223372036854775807";
  const values = [
    Buffer.from(`{"id":${integer},"name":"ok"}`),
    Buffer.from("0000000007feffffffffffffffff01046f6b", "hex"),
    Buffer.from("00000000080008ffffffffffffffff7f12026f6b", "hex"),
    Buffer.from('{"id":'),
    Buffer.from("000000006301", "hex"),
    null,
    Buffer.alloc(0),
  ];
  const execute = (
    command: HostCommand["command"],
    payload: unknown,
  ): ReturnType<typeof backend.execute> =>
    backend.execute({
      command,
      payload,
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
    } as HostCommand);
  let topicCreated = false;
  const failures: unknown[] = [];
  try {
    const registryUrl = await registry.listen();
    const input: SecureConnectionInput = {
      name: "Structured real fixture",
      brokers: [connection.kafkaEndpoint],
      oauth: {
        clientId: config.oauthClientId,
        clientSecret: config.oauthClientSecret,
        scope: config.oauthScope,
        tokenEndpoint: connection.oauthEndpoint,
      },
      tls: { enabled: true, caPem: await readFile(connection.caPath, "utf8") },
      services: { schemaRegistry: { baseUrl: registryUrl, authentication: "none" } },
    };
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    topicCreated = true;
    await producer.send({
      messages: values.map((value) => ({
        topic,
        partition: 0,
        key: Buffer.from("key"),
        value,
        headers: new Map([
          [Buffer.from("cid"), Buffer.from("wrong")],
          [Buffer.from("cid"), Buffer.from(integer)],
        ]),
      })),
    });
    expect(await execute("connection.connect", input)).toMatchObject({ ok: true });
    expect(
      await execute("messages.start", { mode: "earliest", topic, maxMessages: values.length }),
    ).toMatchObject({ ok: true });
    await vi.waitFor(() => expect(messages).toHaveLength(values.length), { timeout: 20_000 });
    const captured = [...messages];
    for (const [index, message] of captured.entries()) {
      expect(message.original?.state).toBe("complete");
      if (message.original?.state !== "complete") throw new Error("Missing wire bytes");
      expect(message.original.value).toBe(values[index]?.toString("base64") ?? null);
      expect(message.original.headers).toEqual([
        {
          key: Buffer.from("cid").toString("base64"),
          value: Buffer.from("wrong").toString("base64"),
        },
        {
          key: Buffer.from("cid").toString("base64"),
          value: Buffer.from(integer).toString("base64"),
        },
      ]);
      expect(message.structured?.headers.map((header) => header.value)).toEqual(["wrong", integer]);
    }
    expect(captured.slice(0, 3).map((message) => JSON.parse(message.payload!) as unknown)).toEqual(
      Array.from({ length: 3 }, () => ({ id: integer, name: "ok" })),
    );
    expect(captured.map((message) => message.structured?.value.state)).toEqual([
      "decoded",
      "decoded",
      "decoded",
      "error",
      "error",
      "null",
      "decoded",
    ]);
    expect(captured[1]?.structured?.value.writerSchema).toEqual({
      id: 7,
      format: "avro",
      messageType: null,
      registry: new URL(registryUrl).toString(),
    });
    expect(captured[2]?.structured?.value.writerSchema).toEqual({
      id: 8,
      format: "protobuf",
      messageType: ".Event",
      registry: new URL(registryUrl).toString(),
    });
    expect(new Set(registry.lookups)).toEqual(new Set([7, 8, 99]));
    const cliOutput: unknown[] = [];
    await runReadOnlyCli(
      "query",
      {
        connection: input,
        protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
      },
      { topic, mode: "earliest", maxMessages: values.length },
      {
        write: (value): Promise<void> => {
          cliOutput.push(value);
          return Promise.resolve();
        },
      },
      AbortSignal.timeout(30_000),
    );
    expect(cliOutput).toHaveLength(values.length + 1);
    expect(cliOutput.slice(0, values.length)).toMatchObject(
      captured.map((message) => ({
        kind: "record",
        record: {
          structured: message.structured,
          original: message.original,
          payload: message.payload,
        },
      })),
    );
    cliOutput.length = 0;
    await runReadOnlyCli(
      "query",
      {
        connection: input,
        codecs: { key: "utf8", value: "auto" },
        protection: {
          ...KAFKA_RECORD_PROTECTION_DEFAULTS,
          maskHeaders: ["cid"],
          valuePaths: ["/id"],
        },
      },
      {
        topic,
        mode: "earliest",
        maxMessages: values.length,
        search: {
          key: "",
          value: "",
          offset: "",
          timestamp: "",
          partition: null,
          expression: '$.name == "ok"',
        },
      },
      {
        write: (value): Promise<void> => {
          cliOutput.push(value);
          return Promise.resolve();
        },
      },
      AbortSignal.timeout(30_000),
    );
    expect(cliOutput).toHaveLength(4);
    expect(cliOutput.slice(0, 3)).toMatchObject(
      Array.from({ length: 3 }, () => ({
        kind: "record",
        record: {
          payload: '{"id":"[MASKED]","name":"ok"}',
          original: { state: "unavailable", reason: "masked" },
          structured: { protection: "masked" },
        },
      })),
    );
    expect(JSON.stringify(cliOutput)).not.toContain(integer);
    const traced = await execute("records.trace", {
      traceId: "mixed",
      topics: [topic],
      startTimeMs: Date.now() - 120_000,
      endTimeMs: Date.now() + 1_000,
      value: integer,
      selector: { source: "payload", path: "/id", format: "auto" },
    });
    expect(traced).toMatchObject({
      ok: true,
      result: {
        trace: {
          topics: [{ state: "partial", matches: 3 }],
          matches: [{ offset: "0" }, { offset: "1" }, { offset: "2" }],
        },
      },
    });
    await execute("messages.stop", {});
    messages.length = 0;
    expect(
      await execute("messages.start", {
        mode: "earliest",
        topic,
        maxMessages: values.length,
        search: {
          key: "",
          value: "",
          offset: "",
          timestamp: "",
          partition: null,
          expression: `$.id == "${integer}"`,
        },
      }),
    ).toMatchObject({ ok: true });
    await vi.waitFor(() => expect(messages).toHaveLength(3), { timeout: 20_000 });
    expect(messages.map((message) => message.offset)).toEqual(["0", "1", "2"]);
    const exported = createKafkaMessageExportDocument({
      topic,
      filters: initialKafkaMessageFilters,
      messages: captured,
      retainedMessageCount: captured.length,
      stale: false,
    });
    expect(JSON.parse(exported.content)).toMatchObject({
      schemaVersion: 3,
      messages: captured.map((message) => ({
        structured: message.structured,
        original: message.original,
      })),
    });

    await execute("messages.stop", {});
    expect(await execute("connection.disconnect", {})).toMatchObject({ ok: true });
    expect(
      await execute("preferences.update", {
        patch: {
          protection: {
            ...KAFKA_RECORD_PROTECTION_DEFAULTS,
            maskHeaders: ["cid"],
            valuePaths: ["/id"],
          },
        },
      }),
    ).toMatchObject({ ok: true });
    messages.length = 0;
    expect(await execute("connection.connect", input)).toMatchObject({ ok: true });
    expect(
      await execute("messages.start", { mode: "earliest", topic, maxMessages: values.length }),
    ).toMatchObject({ ok: true });
    await vi.waitFor(() => expect(messages).toHaveLength(values.length), { timeout: 20_000 });
    expect(messages.slice(0, 3).map((message) => JSON.parse(message.payload!) as unknown)).toEqual(
      Array.from({ length: 3 }, () => ({ id: "[MASKED]", name: "ok" })),
    );
    expect(
      messages.every(
        (message) =>
          message.original?.state === "unavailable" && message.original.reason === "masked",
      ),
    ).toBe(true);
    expect(JSON.stringify(messages)).not.toContain(integer);
    const maskedTrace = await execute("records.trace", {
      traceId: "masked",
      topics: [topic],
      startTimeMs: Date.now() - 120_000,
      endTimeMs: Date.now() + 1_000,
      value: integer,
      selector: { source: "payload", path: "/id", format: "auto" },
    });
    expect(maskedTrace).toMatchObject({ ok: true, result: { trace: { matches: [] } } });
    expect(JSON.stringify(captured.map((message) => message.original))).toContain(
      Buffer.from(integer).toString("base64"),
    );
  } catch (error) {
    failures.push(error);
  } finally {
    const cleanup: readonly (() => Promise<unknown>)[] = [
      (): Promise<void> => backend.shutdown(),
      (): Promise<void> => producer.close(),
      async (): Promise<void> => {
        if (topicCreated) await admin.deleteTopics({ topics: [topic] });
      },
      (): Promise<void> => admin.close(),
      (): Promise<void> => registry.close(),
    ];
    for (const action of cleanup) {
      try {
        await action();
      } catch (error) {
        failures.push(error);
      }
    }
  }
  if (failures.length > 0)
    throw new AggregateError(failures, "Structured record qualification or owned cleanup failed", {
      cause: failures[0],
    });
}
