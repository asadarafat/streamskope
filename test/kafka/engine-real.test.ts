import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";

import { Admin, Producer, type BaseOptions } from "@platformatic/kafka";
import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  parseHostEvent,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { InMemoryKafkaOperationalPreferenceStore } from "../../src/features/kafka/application/in-memory-operational-preference-store";
import type {
  KafkaFetchRequest,
  KafkaMessage,
  SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import type { KafkaMessageStream } from "../../src/features/kafka/application";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  fetchFixtureToken,
  loadFixtureConfig,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
  type FixtureConfig,
  type FixtureConnection,
} from "../support/kafka-fixture";

function useLocalhost(endpoint: string): string {
  return endpoint.replace(/^127\.0\.0\.1:/u, "localhost:");
}

function useLocalhostUrl(endpoint: string): string {
  const url = new URL(endpoint);
  if (url.hostname === "127.0.0.1") {
    url.hostname = "localhost";
  }
  return url.toString();
}

async function secureConnectionInput(
  fixture: FixtureConnection,
  config: FixtureConfig,
): Promise<SecureConnectionInput> {
  return {
    brokers: [useLocalhost(fixture.kafkaEndpoint)],
    name: "Local aio",
    oauth: {
      clientId: config.oauthClientId,
      clientSecret: config.oauthClientSecret,
      scope: config.oauthScope,
      tokenEndpoint: useLocalhostUrl(fixture.oauthEndpoint),
    },
    tls: {
      caPem: await readFile(fixture.caPath, "utf8"),
      enabled: true,
    },
  };
}

async function fixtureClientOptions(
  fixture: FixtureConnection,
  config: FixtureConfig,
): Promise<BaseOptions> {
  return {
    bootstrapBrokers: [fixture.kafkaEndpoint],
    clientId: `streamskope-fetch-acceptance-${randomUUID()}`,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    retries: 0,
    sasl: {
      mechanism: "OAUTHBEARER",
      token: await fetchFixtureToken(fixture, config),
    },
    tls: {
      ca: [await readFile(fixture.caPath, "utf8")],
      rejectUnauthorized: true,
    },
  };
}

async function collectFiniteStream(
  stream: KafkaMessageStream,
  timeoutMs = 10_000,
): Promise<readonly KafkaMessage[]> {
  const messages: KafkaMessage[] = [];
  let timeoutId: NodeJS.Timeout | undefined;
  const collection = (async (): Promise<void> => {
    for await (const message of stream) {
      messages.push(message);
    }
  })();
  const timeout = new Promise<never>((_resolve, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error("Timed out waiting for a finite Kafka stream to complete."));
    }, timeoutMs);
  });

  try {
    await Promise.race([collection, timeout]);
    return messages;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    await stream.close();
    await Promise.allSettled([collection]);
  }
}

async function openAndCollect(
  request: KafkaFetchRequest,
  connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>>,
): Promise<readonly KafkaMessage[]> {
  return collectFiniteStream(
    await connection.openMessageStream(request, new AbortController().signal),
  );
}

describe("real StreamSkope Kafka engine", () => {
  it("creates a reviewed topic and produces exact binary/null/ordered-header records once", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const admin = new Admin(await fixtureClientOptions(fixture, config));
    const engine = new StreamSkopeKafkaEngine();
    const connection = await engine.openConnection(
      await secureConnectionInput(fixture, config),
      new AbortController().signal,
    );
    const topic = `streamskope-write-${randomUUID()}`;
    let created = false;
    try {
      const create = {
        kind: "topic",
        topic,
        partitions: 1,
        replicationFactor: 1,
        configs: [{ name: "cleanup.policy", value: "compact" }],
      } as const;
      await connection.reviewWrite!(create);
      expect(await admin.listTopics()).not.toContain(topic);
      const result = await connection.applyWrite!(create);
      created = result.state === "acknowledged";
      expect(result).toMatchObject({ state: "acknowledged", verification: "verified" });
      await expect(connection.reviewWrite!(create)).rejects.toThrow(/already exists/u);
      await expect(
        connection.reviewWrite!({ ...create, topic: `${topic}-invalid`, replicationFactor: 32 }),
      ).rejects.toThrow(/broker count/u);
      expect(await admin.listTopics()).not.toContain(`${topic}-invalid`);
      for (const [index, value] of ["AP8=", null, ""].entries()) {
        const record = {
          kind: "record",
          topic,
          partition: 0,
          record: {
            state: "complete",
            encoding: "base64",
            key: "a2V5",
            value,
            headers: [
              { key: "c291cmNl", value: "Zmlyc3Q=" },
              { key: "c291cmNl", value: null },
              { key: "YmluYXJ5", value: "AP8=" },
            ],
          },
        } as const;
        await connection.reviewWrite!(record);
        expect(await connection.applyWrite!(record)).toMatchObject({
          state: "acknowledged",
          verification: "verified",
          receipt: { topic, partition: 0, offset: String(index) },
        });
      }
      const records = await collectFiniteStream(
        await connection.openMessageStream(
          { mode: "earliest", topic, maxMessages: 10 },
          new AbortController().signal,
        ),
      );
      expect(records).toHaveLength(3);
      expect(records.map((record) => record.original)).toEqual(
        ["AP8=", null, ""].map((value) => ({
          state: "complete",
          encoding: "base64",
          key: "a2V5",
          value,
          headers: [
            { key: "c291cmNl", value: "Zmlyc3Q=" },
            { key: "c291cmNl", value: null },
            { key: "YmluYXJ5", value: "AP8=" },
          ],
        })),
      );
    } finally {
      await connection.close();
      if (created) await admin.deleteTopics({ topics: [topic] });
      await admin.close();
    }
  }, 60_000);

  it("masks real Kafka records at the host boundary and blocks direct writes", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const options = await fixtureClientOptions(fixture, config);
    const topic = `streamskope-protection-${randomUUID()}`;
    const admin = new Admin(options);
    const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      ...options,
      autocreateTopics: false,
    });
    const preferences = new InMemoryKafkaOperationalPreferenceStore(
      { durability: "session", state: "ready" },
      {
        ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
        protection: {
          readOnly: true,
          maskKey: true,
          maskHeaders: ["token"],
          valuePaths: ["/secret"],
        },
      },
    );
    const backend = createKafkaBackend({ preferenceStore: preferences });
    const records: KafkaMessage[] = [];
    backend.subscribe((event) => {
      const parsed = parseHostEvent(event);
      if (parsed.event === "messages.batch") records.push(...parsed.payload.messages);
    });
    let created = false;
    try {
      await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
      created = true;
      await producer.send({
        messages: [
          {
            topic,
            partition: 0,
            key: Buffer.from("hidden-key"),
            value: Buffer.from('{"secret":"hidden-value","visible":1}'),
            headers: new Map([[Buffer.from("token"), Buffer.from("hidden-header")]]),
          },
        ],
      });
      expect(
        await backend.execute({
          command: "connection.connect",
          id: "connect-protected",
          payload: await secureConnectionInput(fixture, config),
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({ ok: true });
      expect(
        await backend.execute({
          command: "messages.start",
          id: "read-protected",
          payload: { mode: "earliest", topic, maxMessages: 1 },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({ ok: true });
      await vi.waitFor(() => expect(records).toHaveLength(1), { timeout: 10_000 });
      expect(records[0]).toMatchObject({
        key: "[MASKED]",
        payload: '{"secret":"[MASKED]","visible":1}',
        headers: { token: "[MASKED]" },
        original: { state: "unavailable", reason: "masked" },
      });
      expect(JSON.stringify(records)).not.toContain("hidden-");
      expect(
        await backend.execute({
          command: "schemas.delete",
          id: "blocked-write",
          payload: {
            target: { kind: "subject", subject: "never-dispatched" },
            mode: "soft",
            confirmation: "never-dispatched",
          },
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({
        ok: false,
        error: { code: "AUTHORIZATION_DENIED", summary: "Read-only mode blocks this operation." },
      });
    } finally {
      await backend.shutdown();
      await producer.close();
      try {
        if (created) await admin.deleteTopics({ topics: [topic] });
      } finally {
        await admin.close();
      }
    }
  }, 30_000);

  it("retains binary bytes, tombstones, empty values and ordered duplicate/null headers from Kafka", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const options = await fixtureClientOptions(fixture, config);
    const topic = `streamskope-fidelity-${randomUUID()}`;
    const admin = new Admin(options);
    const producer = new Producer<Buffer, Buffer, Buffer, Buffer | undefined>({
      ...options,
      autocreateTopics: false,
    });
    let connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>> | undefined;
    let created = false;
    const headers = new Map<Buffer, Buffer | undefined>([
      [Buffer.from("same"), Buffer.from([255, 0])],
      [Buffer.from("same"), undefined],
      [Buffer.from("empty"), Buffer.alloc(0)],
      [Buffer.from("same"), Buffer.from("last")],
    ]);
    try {
      await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
      created = true;
      await producer.send({
        messages: [
          { topic, partition: 0, headers },
          { topic, partition: 0, key: Buffer.alloc(0), value: Buffer.alloc(0) },
          {
            topic,
            partition: 0,
            key: Buffer.from([0, 255]),
            value: Buffer.from([128, 0, 254]),
            headers,
          },
        ],
      });
      connection = await new StreamSkopeKafkaEngine().openConnection(
        await secureConnectionInput(fixture, config),
        new AbortController().signal,
      );
      const records = await openAndCollect({ mode: "earliest", topic, maxMessages: 3 }, connection);
      expect(records).toHaveLength(3);
      expect(records[0]?.original).toMatchObject({ state: "complete", key: null, value: null });
      expect(records[1]?.original).toMatchObject({ state: "complete", key: "", value: "" });
      expect(records[2]?.original).toEqual({
        state: "complete",
        encoding: "base64",
        key: "AP8=",
        value: "gAD+",
        headers: [
          { key: "c2FtZQ==", value: "/wA=" },
          { key: "c2FtZQ==", value: null },
          { key: "ZW1wdHk=", value: "" },
          { key: "c2FtZQ==", value: "bGFzdA==" },
        ],
      });
      expect(records.map((record) => record.offset)).toEqual(["0", "1", "2"]);
    } finally {
      try {
        await connection?.close();
      } finally {
        await producer.close();
        try {
          if (created) await admin.deleteTopics({ topics: [topic] });
        } finally {
          await admin.close();
        }
      }
    }
  }, 30_000);

  it("reads half-open historical intervals across partitions, caps results and respects removed history", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const options = await fixtureClientOptions(fixture, config);
    const topic = `streamskope-history-${randomUUID()}`;
    const admin = new Admin(options);
    const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      ...options,
      autocreateTopics: false,
    });
    let connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>> | undefined;
    let created = false;
    try {
      await admin.createTopics({ topics: [topic], partitions: 2, replicas: 1 });
      created = true;
      const startTimeMs = Date.now() - 3_600_000;
      const endTimeMs = startTimeMs + 10_000;
      await producer.send({
        messages: [0, 1].flatMap((partition) =>
          [startTimeMs - 1_000, startTimeMs, startTimeMs + 5_000, endTimeMs].map(
            (timestamp, index) => ({
              topic,
              partition,
              timestamp: BigInt(timestamp),
              key: Buffer.from(`partition-${partition}`),
              value: Buffer.from(JSON.stringify({ partition, index })),
            }),
          ),
        ),
      });
      connection = await new StreamSkopeKafkaEngine().openConnection(
        await secureConnectionInput(fixture, config),
        new AbortController().signal,
      );
      const request: KafkaFetchRequest = {
        mode: "time-window",
        topic,
        startTimeMs,
        endTimeMs,
        maxMessages: 100,
      };
      const records = await openAndCollect(request, connection);
      expect(records.map((record) => `${record.partition}:${record.offset}`).sort()).toEqual([
        "0:1",
        "0:2",
        "1:1",
        "1:2",
      ]);
      for (const record of records) {
        expect(Date.parse(record.timestamp)).toBeGreaterThanOrEqual(startTimeMs);
        expect(Date.parse(record.timestamp)).toBeLessThan(endTimeMs);
      }
      const filtered = await connection.openMessageStream(
        {
          ...request,
          search: {
            key: "",
            value: "",
            offset: "",
            timestamp: "",
            partition: null,
            expression: "$.index == 2",
          },
        },
        new AbortController().signal,
      );
      expect(
        (await collectFiniteStream(filtered))
          .map((record) => `${record.partition}:${record.offset}`)
          .sort(),
      ).toEqual(["0:2", "1:2"]);
      expect(filtered.coverage?.()).toMatchObject({
        reason: "range-complete",
        scannedRecords: 4,
        matchedRecords: 2,
        unavailableRecords: 0,
      });
      const capped = await openAndCollect({ ...request, maxMessages: 1 }, connection);
      expect(capped).toHaveLength(1);
      expect(records.map((record) => record.id)).toContain(capped[0]?.id);
      expect(
        await openAndCollect(
          { ...request, startTimeMs: endTimeMs + 1, endTimeMs: endTimeMs + 1_000 },
          connection,
        ),
      ).toEqual([]);

      // Remove only our fixture's prefix to prove that an old interval cannot
      // recreate deleted history. This never touches the sandbox's shared topic.
      await admin.deleteRecords({
        topics: [
          { name: topic, partitions: [0, 1].map((partition) => ({ partition, offset: 3n })) },
        ],
      });
      expect(await openAndCollect(request, connection)).toEqual([]);
    } finally {
      try {
        await connection?.close();
      } finally {
        try {
          await producer.close();
        } finally {
          try {
            if (created) await admin.deleteTopics({ topics: [topic] });
          } finally {
            await admin.close();
          }
        }
      }
    }
  }, 45_000);

  it("confirms the secure fixture through localhost without runtime warnings", async () => {
    const seededFixtureTopic = await provisionSeededFixtureTopic();
    const config = seededFixtureTopic.config;
    const fixture = await loadFixtureConnection();
    const input = await secureConnectionInput(fixture, config);
    const warnings: Error[] = [];
    const warningListener = (warning: Error): void => {
      warnings.push(warning);
    };
    process.on("warning", warningListener);

    try {
      const engine = new StreamSkopeKafkaEngine();
      await expect(engine.testConnection(input)).resolves.toMatchObject({
        checks: ["oauth", "tls", "kafka-authentication", "metadata"],
      });
      const connection = await engine.openConnection(input, new AbortController().signal);
      try {
        await expect(connection.listTopics()).resolves.toContain(config.topic);
        const stream = await connection.openMessageStream(
          {
            maxMessages: 1_000,
            mode: "tail",
            topic: config.topic,
          },
          new AbortController().signal,
        );
        try {
          const seed = (async (): Promise<string> => {
            for await (const record of stream) {
              if (record.payload === config.seedPayload) {
                return record.payload;
              }
            }
            throw new Error("Kafka message stream ended before the fixture seed.");
          })();
          await expect(
            Promise.race([
              seed,
              new Promise<never>((_resolve, reject) => {
                setTimeout(() => {
                  reject(new Error("Timed out waiting for the fixture seed."));
                }, 10_000).unref();
              }),
            ]),
          ).resolves.toBe(config.seedPayload);
        } finally {
          await stream.close();
        }
      } finally {
        await connection.close();
      }
      expect(warnings).toEqual([]);
    } finally {
      process.off("warning", warningListener);
      await seededFixtureTopic.dispose();
    }
  }, 20_000);

  it("enforces every fetch mode and snapshot boundary against timestamped broker records", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const clientOptions = await fixtureClientOptions(fixture, config);
    const topic = `streamskope-fetch-${randomUUID()}`;
    const admin = new Admin(clientOptions);
    const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      ...clientOptions,
      autocreateTopics: false,
    });
    const engine = new StreamSkopeKafkaEngine();
    let connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>> | undefined;
    let topicCreated = false;

    try {
      await admin.createTopics({ partitions: 1, replicas: 1, topics: [topic] });
      topicCreated = true;
      const baseTimestamp = Date.now() - 60_000;
      await producer.send({
        messages: Array.from({ length: 6 }, (_unused, index) => ({
          key: Buffer.from(`key-${index + 1}`, "utf8"),
          partition: 0,
          timestamp: BigInt(baseTimestamp + index * 1_000),
          topic,
          value: Buffer.from(`record-${index + 1}`, "utf8"),
        })),
      });

      connection = await engine.openConnection(
        await secureConnectionInput(fixture, config),
        new AbortController().signal,
      );

      const first = await openAndCollect({ maxMessages: 2, mode: "earliest", topic }, connection);
      expect(first.map((message) => message.payload)).toEqual(["record-1", "record-2"]);

      const searchStream = await connection.openMessageStream(
        {
          maxMessages: 2,
          mode: "earliest",
          topic,
          search: { key: "", value: "record-5", offset: "", timestamp: "", partition: null },
        },
        new AbortController().signal,
      );
      expect((await collectFiniteStream(searchStream)).map((message) => message.payload)).toEqual([
        "record-5",
      ]);
      expect(searchStream.coverage?.()).toMatchObject({
        reason: "range-complete",
        scannedRecords: 6,
        matchedRecords: 1,
        partitions: [{ partition: 0, startOffset: "0", endOffset: "6", nextOffset: "6" }],
      });

      const newestStream = await connection.openMessageStream(
        { maxMessages: 2, mode: "newest", topic },
        new AbortController().signal,
      );
      await producer.send({
        messages: [
          {
            key: Buffer.from("snapshot-later", "utf8"),
            partition: 0,
            timestamp: BigInt(baseTimestamp + 6_000),
            topic,
            value: Buffer.from("record-after-snapshot", "utf8"),
          },
        ],
      });
      const newest = await collectFiniteStream(newestStream);
      expect(newest.map((message) => message.payload)).toEqual(["record-5", "record-6"]);

      const timeWindow = await openAndCollect(
        {
          endTimeMs: baseTimestamp + 5_000,
          maxMessages: 3,
          mode: "time-window",
          startTimeMs: baseTimestamp + 2_000,
          topic,
        },
        connection,
      );
      expect(timeWindow.map((message) => message.payload)).toEqual([
        "record-3",
        "record-4",
        "record-5",
      ]);

      const tail = await connection.openMessageStream(
        { maxMessages: 2, mode: "tail", topic },
        new AbortController().signal,
      );
      const iterator = tail[Symbol.asyncIterator]();
      await expect(iterator.next()).resolves.toMatchObject({
        done: false,
        value: { payload: "record-6" },
      });
      await expect(iterator.next()).resolves.toMatchObject({
        done: false,
        value: { payload: "record-after-snapshot" },
      });
      await producer.send({
        messages: [
          {
            key: Buffer.from("tail-live", "utf8"),
            partition: 0,
            timestamp: BigInt(baseTimestamp + 7_000),
            topic,
            value: Buffer.from("record-after-tail-start", "utf8"),
          },
        ],
      });
      await expect(iterator.next()).resolves.toMatchObject({
        done: false,
        value: { payload: "record-after-tail-start" },
      });
      await tail.close();
      await expect(iterator.next()).resolves.toMatchObject({ done: true });
    } finally {
      await connection?.close();
      await producer.close();
      if (topicCreated) {
        await admin.deleteTopics({ topics: [topic] });
      }
      await admin.close();
    }
  }, 40_000);
});
