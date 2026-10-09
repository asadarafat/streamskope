import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin, Consumer, Producer } from "@platformatic/kafka";
import { afterAll, beforeAll, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseHostEvent,
  type HostCommand,
  type HostEvent,
  type KafkaExploredMessage,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import {
  kafkaRecordLocator,
  type KafkaRecordLocator,
  type KafkaRecordLocatorOutcome,
} from "../../src/features/kafka/contracts/record-locator";
import { parseKafkaRecordLocatorOutcome } from "../../src/features/kafka/contracts/record-locator-protocol";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { fetchFixtureToken, loadFixtureConfig } from "../support/kafka-fixture";
import { kafkaFixtureFailure, waitForKafkaTopicOffsets } from "../support/kafka-topic-readiness";
import {
  disposeNativeFixtureResources,
  startNativeKafkaFixture,
} from "../support/native-kafka-fixture";
import { createSchemaRegistryProtocolFixture } from "../support/schema-registry-protocol-fixture";

type State = Extract<HostEvent, { event: "consumption.state" }>["payload"];
interface RecordInput {
  readonly partition: number;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers?: Map<Buffer, Buffer>;
}
let fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>>;
beforeAll(async () => {
  fixture = await startNativeKafkaFixture();
}, 180_000);
afterAll(async () => {
  await fixture?.dispose();
}, 30_000);

function saved(message: KafkaExploredMessage | undefined): KafkaRecordLocator {
  expect(message).toBeDefined();
  if (message === undefined) throw new Error("Expected a delivered fixture record.");
  const locator = kafkaRecordLocator(message);
  expect(
    locator,
    "A modern real Kafka fetch must establish durable source identity",
  ).not.toBeNull();
  if (locator === null) throw new Error("The delivered record has no verified source identity.");
  return locator;
}
async function withTopic(
  work: (context: {
    topic: string;
    topicId: string;
    clusterId: string;
    admin: Admin;
    messages: KafkaExploredMessage[];
    states: State[];
    execute: (
      command: HostCommand["command"],
      payload: unknown,
    ) => ReturnType<ReturnType<typeof createKafkaBackend>["execute"]>;
    connect: () => Promise<void>;
    seed: (records: readonly RecordInput[], topicId?: string, partitions?: number) => Promise<void>;
    read: () => Promise<readonly KafkaExploredMessage[]>;
    load: (locator: KafkaRecordLocator) => Promise<KafkaRecordLocatorOutcome>;
    recreate: () => Promise<string>;
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
    clientId: `locator-${randomUUID()}`,
    retries: 0,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    sasl: { mechanism: "OAUTHBEARER" as const, token: await fetchFixtureToken(connection, config) },
    tls: { ca: [caPem], rejectUnauthorized: true },
  };
  const admin = new Admin(options);
  const producer = new Producer<Buffer | null, Buffer | null, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
    repeatOnStaleMetadata: false,
  });
  const readiness = new Consumer({ ...options, groupId: randomUUID(), autocreateTopics: false });
  const backend = createKafkaBackend();
  const messages: KafkaExploredMessage[] = [];
  const states: State[] = [];
  backend.subscribe((wire) => {
    const event = parseHostEvent(JSON.parse(JSON.stringify(wire)));
    if (event.event === "messages.batch") messages.push(...event.payload.messages);
    if (event.event === "consumption.state") states.push(event.payload);
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
  const topic = `record-locators-${randomUUID()}`;
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
      name: "Durable record fixture",
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
    const createdTopics = await admin.createTopics({ topics: [topic], partitions: 3, replicas: 1 });
    created = true;
    const topicId = createdTopics.find((item) => item.name === topic)?.id;
    if (topicId === undefined) throw new Error("Fixture topic identity is missing.");
    await waitForKafkaTopicOffsets(readiness, topic, topicId, 3);
    const clusterId = (
      await admin.metadata({ topics: [topic], forceUpdate: true, autocreateTopics: false })
    ).id;
    await work({
      topic,
      topicId,
      clusterId,
      admin,
      messages,
      states,
      execute,
      connect: async (): Promise<void> => {
        expect(await execute("connection.connect", input)).toMatchObject({ ok: true });
      },
      seed: async (records, expectedTopicId = topicId, partitions = 3): Promise<void> => {
        const offsets = [
          ...(await waitForKafkaTopicOffsets(readiness, topic, expectedTopicId, partitions)),
        ];
        producer.clearMetadata();
        try {
          await producer.send({ messages: records.map((record) => ({ topic, ...record })) });
        } catch (error) {
          throw kafkaFixtureFailure("Locator fixture seed failed without replaying writes", error);
        }
        for (const record of records) offsets[record.partition]! += 1n;
        expect(
          await waitForKafkaTopicOffsets(readiness, topic, expectedTopicId, partitions),
        ).toEqual(offsets);
      },
      read: async (): Promise<readonly KafkaExploredMessage[]> => {
        const stateStart = states.length;
        const messageStart = messages.length;
        expect(
          await execute("messages.start", { topic, mode: "earliest", maxMessages: 1_000 }),
        ).toMatchObject({ ok: true });
        await vi.waitFor(
          () =>
            expect(
              states
                .slice(stateStart)
                .some(
                  (state) =>
                    state.request?.topic === topic &&
                    ["complete", "empty", "failed"].includes(state.state),
                ),
            ).toBe(true),
          { timeout: 30_000 },
        );
        expect(states.slice(stateStart).filter((state) => state.state === "failed")).toEqual([]);
        return messages.slice(messageStart);
      },
      load: async (locator): Promise<KafkaRecordLocatorOutcome> => {
        const result = await execute("records.locator.load", { requestId: randomUUID(), locator });
        expect(result, JSON.stringify(result)).toMatchObject({
          ok: true,
          command: "records.locator.load",
        });
        if (!result.ok || result.command !== "records.locator.load")
          throw new Error("The record reload command failed.");
        return parseKafkaRecordLocatorOutcome(JSON.parse(JSON.stringify(result.result.outcome)));
      },
      recreate: async (): Promise<string> => {
        await admin.deleteTopics({ topics: [topic] });
        await vi.waitFor(async () => expect(await admin.listTopics()).not.toContain(topic), {
          timeout: 15_000,
        });
        const replacement = await admin.createTopics({
          topics: [topic],
          partitions: 3,
          replicas: 1,
        });
        const replacementId = replacement.find((item) => item.name === topic)?.id;
        if (replacementId === undefined)
          throw new Error("Replacement fixture identity is missing.");
        expect(replacementId).not.toBe(topicId);
        await waitForKafkaTopicOffsets(readiness, topic, replacementId, 3);
        return replacementId;
      },
    });
  } finally {
    await disposeNativeFixtureResources([
      (): Promise<void> => backend.shutdown(),
      (): Promise<void> => producer.close(),
      (): Promise<void> => readiness.close(),
      async (): Promise<void> => {
        if (created && (await admin.listTopics()).includes(topic))
          await admin.deleteTopics({ topics: [topic] });
      },
      (): Promise<void> => admin.close(),
      (): Promise<void> => registry.close(),
    ]);
  }
}

it("reloads exact mixed records and applies the current protection after reconnect", async () => {
  await withTopic(async ({ topicId, clusterId, seed, connect, read, load, execute }) => {
    const secret = "9223372036854775807";
    const values = [
      Buffer.from(`{"id":${secret},"name":"ok"}`),
      Buffer.from("0000000007feffffffffffffffff01046f6b", "hex"),
      Buffer.from("00000000080008ffffffffffffffff7f12026f6b", "hex"),
      Buffer.from("000000006301", "hex"),
      null,
      Buffer.from([255, 254]),
    ];
    await seed(
      values.map((value, index) => ({
        partition: index % 3,
        key: index === 4 ? null : Buffer.from(`record-${index}`),
        value,
        headers: new Map([
          [Buffer.from("cid"), Buffer.from("first")],
          [Buffer.from("cid"), Buffer.from(secret)],
        ]),
      })),
    );
    await connect();
    const records = [...(await read())].sort(
      (a, b) => a.partition - b.partition || Number(BigInt(a.offset) - BigInt(b.offset)),
    );
    expect(records).toHaveLength(6);
    expect(records.map((message) => `${message.partition}:${message.offset}`)).toEqual([
      "0:0",
      "0:1",
      "1:0",
      "1:1",
      "2:0",
      "2:1",
    ]);
    for (const message of records) {
      const locator = saved(message);
      expect(locator).toMatchObject({ topicId, clusterId, leaderEpoch: 0 });
      const outcome = await load(locator);
      expect(outcome.state).toBe("loaded");
      if (outcome.state !== "loaded") throw new Error("Fixture position was not loaded.");
      expect(outcome.message.original).toEqual(message.original);
      expect(outcome.message.structured).toEqual(message.structured);
      expect(outcome.message.structured?.headers.map((header) => header.key)).toEqual([
        "cid",
        "cid",
      ]);
    }
    const locators = records.map(saved);
    expect(await load({ ...locators[0]!, leaderEpoch: 1 })).toMatchObject({
      state: "record-replaced",
    });
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
    await connect();
    const protectedResults = [];
    for (const locator of locators) {
      const outcome = await load(locator);
      expect(outcome.state).toBe("loaded");
      if (outcome.state !== "loaded") throw new Error("Protected fixture position was not loaded.");
      protectedResults.push(outcome);
      expect(outcome.message.original).toEqual({ state: "unavailable", reason: "masked" });
      expect(outcome.message.structured?.headers).toEqual([
        { key: "cid", value: "[MASKED]", error: null },
        { key: "cid", value: "[MASKED]", error: null },
      ]);
    }
    expect(JSON.stringify(protectedResults)).not.toContain(secret);
    for (const index of [0, 2, 4])
      expect(protectedResults[index]?.message.payload).toBe('{"id":"[MASKED]","name":"ok"}');
    expect(protectedResults[3]?.message.structured?.value.state).toBe("null");
  });
}, 90_000);

it("rejects expired and missing resources while partition growth preserves existing positions", async () => {
  await withTopic(async ({ topic, topicId, admin, seed, connect, read, load, recreate }) => {
    await seed(
      Array.from({ length: 3 }, (_, index) => ({
        partition: 0,
        key: Buffer.from(String(index)),
        value: Buffer.from(`original-${index}`),
      })),
    );
    await connect();
    const records = await read();
    const first = saved(records[0]);
    const last = saved(records[2]);
    await admin.createPartitions({ topics: [{ name: topic, count: 4, assignments: null }] });
    await vi.waitFor(
      async () => {
        const current = (
          await admin.metadata({ topics: [topic], forceUpdate: true, autocreateTopics: false })
        ).topics.get(topic);
        expect(current?.id).toBe(topicId);
        expect(current?.partitions).toHaveLength(4);
        for (const partition of current?.partitions ?? []) {
          expect(partition.leader).toBeGreaterThanOrEqual(0);
          expect(partition.isr).toContain(partition.leader);
        }
      },
      { timeout: 15_000 },
    );
    expect(await load(last)).toMatchObject({ state: "loaded", message: { payload: "original-2" } });
    const removed = await admin.deleteRecords({
      topics: [{ name: topic, partitions: [{ partition: 0, offset: 2n }] }],
    });
    expect(removed[0]?.partitions[0]?.lowWatermark).toBe(2n);
    expect(await load(first)).toMatchObject({ state: "expired" });
    const replacementId = await recreate();
    await seed(
      [{ partition: 0, key: Buffer.from("replacement"), value: Buffer.from("must-not-reload") }],
      replacementId,
    );
    expect(await load(last)).toMatchObject({ state: "resource-replaced" });
    await admin.deleteTopics({ topics: [topic] });
    await vi.waitFor(async () => expect(await admin.listTopics()).not.toContain(topic), {
      timeout: 15_000,
    });
    expect(await load(last)).toMatchObject({ state: "topic-missing" });
  });
}, 90_000);

it("fences an active tail from lending the original identity to a recreated topic", async () => {
  await withTopic(
    async ({
      topic,
      topicId,
      clusterId,
      seed,
      connect,
      messages,
      states,
      execute,
      load,
      recreate,
    }) => {
      await connect();
      expect(
        await execute("messages.start", { topic, mode: "tail", maxMessages: 1_000 }),
      ).toMatchObject({ ok: true });
      await seed(
        Array.from({ length: 250 }, (_, index) => ({
          partition: index % 3,
          key: Buffer.from(String(index)),
          value: Buffer.from(`before-replacement-${index}`),
        })),
      );
      await vi.waitFor(() => expect(messages).toHaveLength(250), { timeout: 30_000 });
      const positions = messages.map(saved);
      expect(
        positions.every(
          (locator) => locator.topicId === topicId && locator.clusterId === clusterId,
        ),
      ).toBe(true);
      const retained = saved(messages[0]);
      const replacementId = await recreate();
      await seed(
        Array.from({ length: 300 }, (_, index) => ({
          partition: index % 3,
          key: Buffer.from(String(index)),
          value: Buffer.from(`after-replacement-${index}`),
        })),
        replacementId,
      );
      await vi.waitFor(
        () =>
          expect(
            states.some((state) => state.request?.topic === topic && state.state === "failed"),
          ).toBe(true),
        { timeout: 30_000 },
      );
      expect(messages).toHaveLength(250);
      expect(messages.map(saved)).toEqual(positions);
      expect(messages.some((message) => message.payload?.startsWith("after-replacement-"))).toBe(
        false,
      );
      expect(await execute("messages.stop", {})).toMatchObject({ ok: true });
      expect(await load(retained)).toMatchObject({ state: "resource-replaced" });
    },
  );
}, 90_000);

it("keeps local notes with the original topic UUID across recreation and permits orphan cleanup", async () => {
  await withTopic(
    async ({ topic, topicId, clusterId, execute, connect, recreate, messages, states }) => {
      await connect();
      const identity = { clusterId, topicId, topic };
      const annotation = {
        identity,
        description: "Owned fixture notes",
        owner: "QA",
        labels: ["fixture"],
        links: [],
      };
      expect(await execute("catalog.load", { topic })).toMatchObject({
        ok: true,
        result: { snapshot: { identity, annotation: null } },
      });
      expect(await execute("catalog.put", { annotation, expected: null })).toMatchObject({
        ok: true,
        result: { snapshot: { annotation } },
      });
      // An otherwise valid resource description from another cluster must never be associated by name.
      expect(
        await execute("catalog.put", {
          annotation: { ...annotation, identity: { ...identity, clusterId: "other-cluster" } },
          expected: null,
        }),
      ).toMatchObject({ ok: false, error: { code: "QUERY_UNAVAILABLE" } });
      const replacementId = await recreate();
      expect(await execute("catalog.load", { topic })).toMatchObject({
        ok: true,
        result: {
          snapshot: { identity: { ...identity, topicId: replacementId }, annotation: null },
        },
      });
      expect(
        await execute("catalog.put", {
          annotation: { ...annotation, description: "stale" },
          expected: annotation,
        }),
      ).toMatchObject({ ok: false, error: { code: "QUERY_UNAVAILABLE" } });
      expect(await execute("catalog.list", {})).toMatchObject({
        ok: true,
        result: { snapshot: { topics: [annotation] } },
      });
      expect(await execute("connection.disconnect", {})).toMatchObject({ ok: true });
      expect(await execute("catalog.delete", { identity, expected: annotation })).toMatchObject({
        ok: true,
        result: { snapshot: { annotation: null } },
      });
      expect(await execute("catalog.list", {})).toMatchObject({
        ok: true,
        result: { snapshot: { topics: [] } },
      });
      expect(messages).toEqual([]);
      expect(states.some((state) => state.state === "streaming")).toBe(false);
    },
  );
}, 90_000);
