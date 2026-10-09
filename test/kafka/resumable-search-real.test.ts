import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin, Producer } from "@platformatic/kafka";
import { afterAll, beforeAll, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseHostEvent,
  type HostCommand,
  type HostEvent,
  type KafkaExploredMessage,
  type KafkaFetchRequest,
  type KafkaReadCoverage,
  type KafkaSearchProgress,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { fetchFixtureToken, loadFixtureConfig } from "../support/kafka-fixture";
import {
  disposeNativeFixtureResources,
  startNativeKafkaFixture,
} from "../support/native-kafka-fixture";
import { createSchemaRegistryProtocolFixture } from "../support/schema-registry-protocol-fixture";

type State = Extract<HostEvent, { event: "consumption.state" }>["payload"];
type Finished = State & { coverage: KafkaReadCoverage; searchProgress: KafkaSearchProgress };
type RecordInput = {
  partition: number;
  key: Buffer;
  value: Buffer | null;
  headers?: Map<Buffer, Buffer>;
};
let fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>>;

beforeAll(async () => {
  fixture = await startNativeKafkaFixture();
}, 180_000);
afterAll(async () => {
  await fixture?.dispose();
}, 30_000);

function search(expression: string): NonNullable<KafkaFetchRequest["search"]> {
  return { key: "", value: "", offset: "", timestamp: "", partition: null, expression };
}
function locator(message: KafkaExploredMessage): string {
  return `${String(message.partition)}:${message.offset}`;
}
function avroRecord(id: number): Buffer {
  // Independent Avro long/string encoding for the fixture's { id, name } writer.
  let encoded = id * 2;
  const bytes: number[] = [];
  do {
    const byte = encoded & 0x7f;
    encoded = Math.floor(encoded / 128);
    bytes.push(byte | (encoded === 0 ? 0 : 0x80));
  } while (encoded > 0);
  return Buffer.concat([
    Buffer.from("0000000007", "hex"),
    Buffer.from(bytes),
    Buffer.from("046f6b", "hex"),
  ]);
}
function token(state: Finished): string {
  const continuation = state.searchProgress.continuation;
  expect(continuation, "A partial, delivered read must offer a continuation").not.toBeNull();
  if (continuation === null) throw new Error("Missing continuation");
  expect(Date.parse(continuation.expiresAt)).toBeGreaterThan(Date.now());
  expect(Date.parse(continuation.expiresAt)).toBeLessThanOrEqual(Date.now() + 30 * 60_000);
  return continuation.id;
}

async function withTopic(
  partitions: number,
  work: (context: {
    topic: string;
    admin: Admin;
    messages: KafkaExploredMessage[];
    states: State[];
    connect: () => Promise<void>;
    execute: (
      command: HostCommand["command"],
      payload: unknown,
    ) => ReturnType<ReturnType<typeof createKafkaBackend>["execute"]>;
    seed: (records: readonly RecordInput[]) => Promise<void>;
    pass: (command: "messages.start" | "messages.continue", payload: unknown) => Promise<Finished>;
    onBatch: (callback: (() => void) | undefined) => void;
  }) => Promise<void>,
): Promise<void> {
  const config = await loadFixtureConfig();
  const connection = {
    kafkaEndpoint: fixture.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
    oauthEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
    caPath: fixture.environment.STREAMSKOPE_TEST_CA_PATH!,
  };
  const caPem = await readFile(connection.caPath, "utf8");
  const options = {
    bootstrapBrokers: [connection.kafkaEndpoint],
    clientId: `continuation-${randomUUID()}`,
    retries: 0,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    sasl: { mechanism: "OAUTHBEARER" as const, token: await fetchFixtureToken(connection, config) },
    tls: { ca: [caPem], rejectUnauthorized: true },
  };
  const admin = new Admin(options);
  const producer = new Producer<Buffer, Buffer | null, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
  });
  const backend = createKafkaBackend();
  const messages: KafkaExploredMessage[] = [];
  const states: State[] = [];
  let onBatch: (() => void) | undefined;
  backend.subscribe((wire) => {
    const event = parseHostEvent(JSON.parse(JSON.stringify(wire)));
    if (event.event === "messages.batch") {
      messages.push(...event.payload.messages);
      onBatch?.();
    } else if (event.event === "consumption.state") states.push(event.payload);
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
  const topic = `continuation-${randomUUID()}`;
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
  let created = false;
  try {
    const registryUrl = await registry.listen();
    const input: SecureConnectionInput = {
      name: "Resumable read fixture",
      brokers: [connection.kafkaEndpoint],
      oauth: {
        clientId: config.oauthClientId,
        clientSecret: config.oauthClientSecret,
        scope: config.oauthScope,
        tokenEndpoint: connection.oauthEndpoint,
      },
      tls: { enabled: true, caPem },
      services: { schemaRegistry: { baseUrl: registryUrl, authentication: "none" } },
    };
    await admin.createTopics({ topics: [topic], partitions, replicas: 1 });
    created = true;
    await work({
      topic,
      admin,
      messages,
      states,
      execute,
      connect: async (): Promise<void> => {
        expect(await execute("connection.connect", input)).toMatchObject({ ok: true });
      },
      seed: async (records): Promise<void> => {
        for (let index = 0; index < records.length; index += 500) {
          await producer.send({
            messages: records.slice(index, index + 500).map((record) => ({ topic, ...record })),
          });
        }
      },
      onBatch: (callback): void => {
        onBatch = callback;
      },
      pass: async (command, payload): Promise<Finished> => {
        const start = states.length;
        const response = await execute(command, payload);
        expect(response, JSON.stringify(response)).toMatchObject({ ok: true });
        let terminal: State | undefined;
        await vi.waitFor(
          () => {
            terminal = states
              .slice(start)
              .find(
                (state) =>
                  state.request?.topic === topic &&
                  ["complete", "empty", "stopped", "failed"].includes(state.state) &&
                  state.coverage !== undefined &&
                  state.coverage.reason !== "reading",
              );
            expect(terminal, "Expected a terminal coverage receipt").toBeDefined();
          },
          { timeout: 45_000 },
        );
        expect(terminal, JSON.stringify(terminal)).not.toMatchObject({ state: "failed" });
        expect(terminal?.searchProgress).toBeDefined();
        expect(terminal?.droppedMessages).toBe(0);
        return terminal as Finished;
      },
    });
  } finally {
    await disposeNativeFixtureResources([
      (): Promise<void> => backend.shutdown(),
      (): Promise<void> => producer.close(),
      async (): Promise<void> => {
        if (created) await admin.deleteTopics({ topics: [topic] });
      },
      (): Promise<void> => admin.close(),
      (): Promise<void> => registry.close(),
    ]);
  }
}

it("continues beyond 10,000 records with exact partition coverage and excludes later arrivals", async () => {
  await withTopic(3, async ({ topic, seed, connect, pass, messages, execute }) => {
    const records: RecordInput[] = [];
    const expected: string[] = [];
    for (let partition = 0; partition < 3; partition += 1) {
      for (let index = 0; index < 4_001; index += 1) {
        const selected = index % 997 === 0;
        records.push({
          partition,
          key: Buffer.from(`key-${String(index)}`),
          value: Buffer.from(JSON.stringify({ selected, sequence: index })),
        });
        if (selected) expected.push(`${String(partition)}:${String(index)}`);
      }
    }
    const totalBytes = records.reduce(
      (sum, record) => sum + record.key.length + (record.value?.length ?? 0),
      0,
    );
    await seed(records);
    await connect();
    const initial = await pass("messages.start", {
      topic,
      mode: "earliest",
      maxMessages: 1_000,
      search: search("$.selected == true"),
    });
    expect(initial.coverage).toMatchObject({
      reason: "scan-limit",
      scannedRecords: 10_000,
      unavailableRecords: 0,
    });
    const firstToken = token(initial);
    await seed(
      [0, 1, 2].map((partition) => ({
        partition,
        key: Buffer.from("late"),
        value: Buffer.from('{"selected":true}'),
      })),
    );
    const final = await pass("messages.continue", { continuationId: firstToken });
    expect(final.coverage).toMatchObject({ reason: "range-complete", scannedRecords: 2_003 });
    expect(final.searchProgress).toEqual({
      pass: 2,
      scannedRecords: 12_003,
      scannedBytes: totalBytes,
      matchedRecords: expected.length,
      unavailableRecords: 0,
      continuation: null,
    });
    expect(final.coverage.partitions).toEqual(
      [0, 1, 2].map((partition) => ({
        partition,
        startOffset: "0",
        endOffset: "4001",
        nextOffset: "4001",
      })),
    );
    expect(messages.map(locator).sort()).toEqual(expected.sort());
    expect(new Set(messages.map(locator)).size).toBe(messages.length);
    expect(await execute("messages.continue", { continuationId: firstToken })).toMatchObject({
      ok: false,
    });
    expect(messages).toHaveLength(expected.length);
  });
}, 120_000);

it("resumes a confirmed stop from delivered records without skipping or duplicating matches", async () => {
  await withTopic(1, async ({ topic, seed, connect, pass, messages, execute, onBatch }) => {
    const records = Array.from({ length: 3_000 }, (_, index) => ({
      partition: 0,
      key: Buffer.from(`key-${String(index)}`),
      // The initial distinct writer payloads exercise cancellation during async
      // decoding; the remaining JSON positions keep this a read test, not a codec soak.
      value: index < 3 ? avroRecord(index) : Buffer.from('{"name":"ok"}'),
    }));
    await seed(records);
    await connect();
    let stop: ReturnType<typeof execute> | undefined;
    onBatch(() => {
      if (stop !== undefined) return;
      onBatch(undefined);
      stop = execute("messages.stop", {});
    });
    let state = await pass("messages.start", {
      topic,
      mode: "earliest",
      maxMessages: 1_000,
      search: search('$.name == "ok"'),
    });
    expect(await stop).toMatchObject({ ok: true });
    expect(state).toMatchObject({ state: "stopped", coverage: { reason: "cancelled" } });
    expect(messages.length).toBeGreaterThan(0);
    expect(messages.length).toBeLessThan(1_000);
    expect(state.searchProgress.matchedRecords).toBe(messages.length);
    expect(
      state.coverage.partitions.reduce((sum, partition) => sum + Number(partition.nextOffset), 0),
    ).toBe(messages.length);
    for (let attempt = 0; state.searchProgress.continuation !== null && attempt < 5; attempt += 1) {
      state = await pass("messages.continue", { continuationId: token(state) });
    }
    expect(state.coverage.reason).toBe("range-complete");
    expect(state.searchProgress).toMatchObject({
      scannedRecords: 3_000,
      matchedRecords: 3_000,
      unavailableRecords: 0,
      continuation: null,
    });
    expect(messages).toHaveLength(3_000);
    expect(new Set(messages.map(locator)).size).toBe(3_000);
    expect(messages.map(locator).sort()).toEqual(
      Array.from({ length: 3_000 }, (_, offset) => `0:${String(offset)}`).sort(),
    );
  });
}, 120_000);

it.each(["retention", "replacement", "partitions"] as const)(
  "rejects continuation after %s changes the captured broker ranges",
  async (change) => {
    await withTopic(1, async ({ topic, admin, seed, connect, pass, messages, execute }) => {
      const records = Array.from({ length: 6 }, (_, index) => ({
        partition: 0,
        key: Buffer.from(String(index)),
        value: Buffer.from('{"selected":true}'),
      }));
      await seed(records);
      await connect();
      const initial = await pass("messages.start", { topic, mode: "earliest", maxMessages: 1 });
      expect(initial.coverage.partitions).toEqual([
        { partition: 0, startOffset: "0", endOffset: "6", nextOffset: "1" },
      ]);
      if (change === "retention") {
        const deleted = await admin.deleteRecords({
          topics: [{ name: topic, partitions: [{ partition: 0, offset: 3n }] }],
        });
        expect(deleted[0]?.partitions[0]?.lowWatermark).toBe(3n);
      } else if (change === "replacement") {
        await admin.deleteTopics({ topics: [topic] });
        await vi.waitFor(async () => expect(await admin.listTopics()).not.toContain(topic), {
          timeout: 10_000,
        });
        await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
        await seed(records);
      } else {
        await admin.createPartitions({ topics: [{ name: topic, count: 2, assignments: null }] });
      }
      const continuationId = token(initial);
      const response = await execute("messages.continue", { continuationId });
      expect(response, JSON.stringify(response)).toMatchObject({
        ok: false,
        error: {
          code: "VALIDATION",
          stage: "validation",
          retryable: false,
          summary:
            change === "retention"
              ? "The remaining captured offsets are no longer retained by Kafka."
              : change === "replacement"
                ? "The Kafka cluster or topic identity changed since this read began."
                : "The topic partition inventory changed since this read began.",
          recovery: expect.stringContaining("Start a new read"),
        },
      });
      expect(messages).toHaveLength(1);
      expect(await execute("messages.continue", { continuationId })).toMatchObject({ ok: false });
    });
  },
  60_000,
);

it("preserves protected mixed-schema search and unavailable counts across result pages", async () => {
  await withTopic(1, async ({ topic, seed, connect, pass, messages, execute }) => {
    const secret = "9223372036854775807";
    const values = [
      Buffer.from(`{"id":${secret},"name":"ok"}`),
      Buffer.from('{"id":'),
      Buffer.from("0000000007feffffffffffffffff01046f6b", "hex"),
      Buffer.from("000000006301", "hex"),
      Buffer.from("00000000080008ffffffffffffffff7f12026f6b", "hex"),
      null,
      Buffer.alloc(0),
    ];
    await seed(
      values.map((value) => ({
        partition: 0,
        key: Buffer.from("key"),
        value,
        headers: new Map([
          [Buffer.from("cid"), Buffer.from("first")],
          [Buffer.from("cid"), Buffer.from(secret)],
        ]),
      })),
    );
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
    await connect();
    let state = await pass("messages.start", {
      topic,
      mode: "earliest",
      maxMessages: 1,
      search: search('$.name == "ok"'),
    });
    for (let attempt = 0; state.searchProgress.continuation !== null && attempt < 4; attempt += 1) {
      state = await pass("messages.continue", { continuationId: token(state) });
    }
    expect(state.coverage.reason).toBe("range-complete");
    expect(state.searchProgress).toMatchObject({
      pass: 4,
      scannedRecords: 7,
      matchedRecords: 3,
      unavailableRecords: 4,
      continuation: null,
    });
    expect(messages.map((message) => message.offset)).toEqual(["0", "2", "4"]);
    expect(messages.map((message) => message.payload)).toEqual(
      Array.from({ length: 3 }, () => '{"id":"[MASKED]","name":"ok"}'),
    );
    for (const message of messages) {
      expect(message.structured?.headers.map((header) => header.key)).toEqual(["cid", "cid"]);
      expect(message.original).toEqual({ state: "unavailable", reason: "masked" });
    }
    expect(messages.map((message) => message.structured?.value.writerSchema?.id ?? null)).toEqual([
      null,
      7,
      8,
    ]);
    expect(JSON.stringify(messages)).not.toContain(secret);
    messages.length = 0;
    const noSecret = await pass("messages.start", {
      topic,
      mode: "earliest",
      maxMessages: 1,
      search: search(`$.id == "${secret}"`),
    });
    expect(noSecret.searchProgress).toMatchObject({
      pass: 1,
      scannedRecords: 7,
      matchedRecords: 0,
      unavailableRecords: 4,
      continuation: null,
    });
    expect(messages).toEqual([]);
  });
}, 90_000);

it("revokes a previous continuation on a fresh read and on reconnect", async () => {
  await withTopic(1, async ({ topic, seed, connect, pass, execute, messages }) => {
    await seed(
      Array.from({ length: 3 }, (_, index) => ({
        partition: 0,
        key: Buffer.from(String(index)),
        value: Buffer.from("record"),
      })),
    );
    await connect();
    const request = { topic, mode: "earliest", maxMessages: 1 };
    const first = token(await pass("messages.start", request));
    const latest = token(await pass("messages.start", request));
    expect(await execute("messages.continue", { continuationId: first })).toMatchObject({
      ok: false,
    });
    expect(messages).toHaveLength(2);
    expect(await execute("connection.disconnect", {})).toMatchObject({ ok: true });
    await connect();
    expect(await execute("messages.continue", { continuationId: latest })).toMatchObject({
      ok: false,
    });
    expect(messages).toHaveLength(2);
  });
}, 60_000);
