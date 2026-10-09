import { createServer, type Socket } from "node:net";
import { inspect } from "node:util";

import {
  Consumer,
  createRecordsBatch,
  consumerConsumesChannel,
  instancesChannel,
  MessagesStream,
  Writer,
  type FetchOptions,
  type fetchV17,
} from "@platformatic/kafka";
import { afterEach, expect, it } from "vitest";

import { RecordProvenanceConsumer } from "../../src/features/kafka/engine/record-provenance-consumer";
import { PlatformaticConsumerFactory } from "../../src/features/kafka/engine/platformatic-consumer";
import { waitForKafkaTopicOffsets } from "../support/kafka-topic-readiness";

const TOPIC_ID = "12345678-1234-1234-1234-123456789abc";
const REPLACEMENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const IDENTITY = { clusterId: "socket-cluster", topicId: TOPIC_ID, topic: "events" };
const owned: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of owned.splice(0).reverse()) await close();
});

/** A public TCP broker fixture: no SDK methods, private symbols or diagnostics are patched. */
interface BrokerFixture {
  readonly port: number;
  readonly requests: Array<{ key: number; version: number }>;
  readonly replaceResponse: () => void;
  readonly replaceMetadata: () => void;
  readonly replaceEpoch: () => void;
  readonly metadataError: (code: number, once?: boolean) => void;
}
async function broker(fetchVersion: 12 | 13): Promise<BrokerFixture> {
  let port = 0;
  let topicId = TOPIC_ID;
  let responseId = TOPIC_ID;
  let epoch = 3;
  let metadataError = 0;
  let metadataErrorOnce = false;
  const requests: Array<{ key: number; version: number }> = [];
  const sockets = new Set<Socket>();
  const errors: Error[] = [];
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 4 && pending.length >= pending.readInt32BE(0) + 4) {
        const bytes = pending.subarray(4, pending.readInt32BE(0) + 4);
        pending = pending.subarray(bytes.length + 4);
        const key = bytes.readInt16BE(0);
        const version = bytes.readInt16BE(2);
        requests.push({ key, version });
        const response = Writer.create().appendInt32(bytes.readInt32BE(4));
        if (key !== 18) response.appendTaggedFields();
        try {
          if (key === 18) {
            response.appendInt16(0).appendArray(
              [
                { key: 18, version: 3 },
                { key: 3, version: 12 },
                { key: 1, version: fetchVersion },
                { key: 10, version: 4 },
                { key: 11, version: 9 },
                { key: 14, version: 5 },
                { key: 13, version: 5 },
                { key: 12, version: 4 },
                { key: 2, version: 7 },
              ],
              (w, api) => {
                w.appendInt16(api.key).appendInt16(api.version).appendInt16(api.version);
              },
            );
            response.appendInt32(0).appendTaggedFields();
          } else if (key === 3) {
            response
              .appendInt32(0)
              .appendArray([1], (w) => {
                w.appendInt32(1).appendString("127.0.0.1").appendInt32(port).appendString(null);
              })
              .appendString(IDENTITY.clusterId)
              .appendInt32(1)
              .appendArray([1], (w) => {
                w.appendInt16(metadataError)
                  .appendString("events")
                  .appendUUID(topicId)
                  .appendBoolean(false)
                  .appendArray([1], (p) => {
                    p.appendInt16(0)
                      .appendInt32(0)
                      .appendInt32(1)
                      .appendInt32(9)
                      .appendArray(
                        [1],
                        (a, n) => {
                          a.appendInt32(n);
                        },
                        true,
                        false,
                      )
                      .appendArray(
                        [1],
                        (a, n) => {
                          a.appendInt32(n);
                        },
                        true,
                        false,
                      )
                      .appendArray([], () => undefined, true, false);
                  })
                  .appendInt32(0);
              })
              .appendTaggedFields();
            if (metadataErrorOnce) {
              metadataError = 0;
              metadataErrorOnce = false;
            }
          } else if (key === 11) {
            response
              .appendInt32(0)
              .appendInt16(0)
              .appendInt32(1)
              .appendString("consumer")
              .appendString("roundrobin")
              .appendString("leader")
              .appendBoolean(false)
              .appendString("member")
              .appendArray([], () => undefined)
              .appendTaggedFields();
          } else if (key === 14) {
            const assignment = Writer.create()
              .appendInt16(0)
              .appendArray(
                [1],
                (w) => {
                  w.appendString("events", false).appendArray(
                    [0],
                    (p, n) => {
                      p.appendInt32(n);
                    },
                    false,
                    false,
                  );
                },
                false,
                false,
              )
              .appendBytes(Buffer.alloc(0), false).buffer;
            response
              .appendInt32(0)
              .appendInt16(0)
              .appendString("consumer")
              .appendString("roundrobin")
              .appendBytes(assignment)
              .appendTaggedFields();
          } else if (key === 13) {
            response
              .appendInt32(0)
              .appendInt16(0)
              .appendArray([1], (w) => {
                w.appendString("member").appendString(null).appendInt16(0);
              })
              .appendTaggedFields();
          } else if (key === 12) {
            response.appendInt32(0).appendInt16(0).appendTaggedFields();
          } else if (key === 2) {
            response
              .appendInt32(0)
              .appendArray([1], (w) => {
                w.appendString("events").appendArray([1], (p) => {
                  p.appendInt32(0).appendInt16(0).appendInt64(-1n).appendInt64(1n).appendInt32(9);
                });
              })
              .appendTaggedFields();
          } else if (key === 10) {
            response
              .appendInt32(0)
              .appendArray([1], (w) => {
                w.appendString("record-provenance-test")
                  .appendInt32(1)
                  .appendString("127.0.0.1")
                  .appendInt32(port)
                  .appendInt16(0)
                  .appendString(null);
              })
              .appendTaggedFields();
          } else if (key === 1) {
            const records = createRecordsBatch(
              [
                {
                  topic: "events",
                  partition: 0,
                  timestamp: 1_700_000_000_000n,
                  value: Buffer.from("evidence"),
                },
              ],
              { partitionLeaderEpoch: epoch },
            ).buffer;
            response
              .appendInt32(0)
              .appendInt16(0)
              .appendInt32(0)
              .appendArray([1], (w) => {
                if (version <= 12) w.appendString("events");
                else w.appendUUID(responseId);
                w.appendArray([1], (p) => {
                  p.appendInt32(0)
                    .appendInt16(0)
                    .appendInt64(1n)
                    .appendInt64(1n)
                    .appendInt64(0n)
                    .appendArray(null, () => undefined)
                    .appendInt32(-1)
                    .appendBytes(records);
                });
              })
              .appendTaggedFields();
          } else throw new Error(`Unexpected fixture request ${String(key)}.`);
          socket.write(response.prependLength().buffer);
        } catch (error) {
          errors.push(error instanceof Error ? error : new Error("Fixture response failed."));
          socket.destroy();
        }
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Fixture address missing.");
  port = address.port;
  owned.push(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
    expect(errors).toEqual([]);
  });
  return {
    port,
    requests,
    replaceResponse: (): void => {
      responseId = REPLACEMENT_ID;
    },
    replaceMetadata: (): void => {
      topicId = REPLACEMENT_ID;
    },
    replaceEpoch: (): void => {
      epoch = 4;
    },
    metadataError: (code: number, once = false): void => {
      metadataError = code;
      metadataErrorOnce = once;
    },
  };
}

it("waits for a newly created topic after a real SDK unknown-topic metadata response", async () => {
  const fixture = await broker(13);
  fixture.metadataError(3, true);
  const consumer = new Consumer({
    clientId: "topic-readiness-test",
    groupId: "topic-readiness-test",
    bootstrapBrokers: [`127.0.0.1:${String(fixture.port)}`],
    retries: 0,
    connectTimeout: 1_000,
    requestTimeout: 1_000,
    autocreateTopics: false,
  });
  owned.push(() => consumer.close(true));
  await expect(waitForKafkaTopicOffsets(consumer, "events", TOPIC_ID, 1)).resolves.toEqual([1n]);
  expect(fixture.requests.filter(({ key }) => key === 3)).toHaveLength(2);
  expect(fixture.requests.filter(({ key }) => key === 2)).toHaveLength(1);
});

it("maps a real metadata UNKNOWN_TOPIC error after the SDK removes its typed cause", async () => {
  const fixture = await broker(13);
  fixture.metadataError(3);
  await expect(
    new PlatformaticConsumerFactory().open({
      brokers: [`127.0.0.1:${String(fixture.port)}`],
      tlsEnabled: false,
      groupId: "record-provenance-test",
      operationTimeoutMs: 2_000,
      request: {
        topic: "events",
        mode: "earliest",
        maxMessages: 1,
        search: { key: "", value: "", offset: "", timestamp: "", partition: 0, offsetExact: "0" },
      },
      expectedLocator: { schemaVersion: 1, ...IDENTITY, partition: 0, offset: "0", leaderEpoch: 3 },
    }),
  ).rejects.toMatchObject({ reason: "topic-missing" });
});

it.each(["creation", "handoff"] as const)(
  "owns a stream cancelled synchronously at SDK %s before deferred construction",
  async (stage) => {
    const fixture = await broker(13);
    const controller = new AbortController();
    let captured: MessagesStream<Buffer, Buffer, Buffer, Buffer> | undefined;
    const observer: Parameters<typeof consumerConsumesChannel.subscribe>[0] = {
      start: (): void => undefined,
      end: (): void => undefined,
      asyncEnd: (): void => undefined,
      error: (): void => undefined,
      asyncStart: (context): void => {
        if (stage === "handoff" && context.result instanceof MessagesStream) {
          captured = context.result;
          controller.abort();
        }
      },
    };
    const created = (context: unknown): void => {
      if (
        stage === "creation" &&
        context !== null &&
        typeof context === "object" &&
        "instance" in context &&
        context.instance instanceof MessagesStream
      ) {
        captured = context.instance;
        controller.abort();
      }
    };
    consumerConsumesChannel.subscribe(observer);
    instancesChannel.subscribe(created);
    try {
      const failure = await new PlatformaticConsumerFactory()
        .open({
          brokers: [`127.0.0.1:${String(fixture.port)}`],
          tlsEnabled: false,
          groupId: "record-provenance-test",
          operationTimeoutMs: 2_000,
          request: { topic: "events", mode: "tail", maxMessages: 1 },
          signal: controller.signal,
        })
        .catch((error: unknown) => error);
      expect(failure, inspect(failure, { depth: 6 })).toMatchObject({ name: "AbortError" });
      expect(captured).toBeDefined();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(captured?.closed).toBe(true);
      expect(captured?.consumer.streamsCount).toBe(0);
    } finally {
      consumerConsumesChannel.unsubscribe(observer);
      instancesChannel.unsubscribe(created);
    }
  },
);
async function client(port: number): Promise<RecordProvenanceConsumer> {
  const consumer = new RecordProvenanceConsumer({
    clientId: "record-provenance-test",
    groupId: "record-provenance-test",
    bootstrapBrokers: [`127.0.0.1:${String(port)}`],
    retries: 0,
    connectTimeout: 1_000,
    requestTimeout: 1_000,
  });
  owned.push(async () => {
    consumer.releaseRecordProvenance();
    await consumer.close(true);
  });
  consumer.topics.track("events");
  await consumer.metadata({ topics: ["events"], autocreateTopics: false });
  consumer.bindRecordIdentity(IDENTITY);
  return consumer;
}
function request(topicId = TOPIC_ID): FetchOptions<Buffer, Buffer, Buffer, Buffer> {
  return {
    node: 1,
    maxWaitTime: 0,
    topics: [
      {
        topicId,
        partitions: [
          {
            partition: 0,
            fetchOffset: 0n,
            currentLeaderEpoch: 9,
            lastFetchedEpoch: -1,
            partitionMaxBytes: 65_536,
          },
        ],
      },
    ],
  };
}

it.each([12, 13] as const)(
  "uses actual Fetch v%i wire identity despite SDK name remapping",
  async (version) => {
    const fixture = await broker(version);
    const consumer = await client(fixture.port);
    const response = await consumer.fetch(request());
    // Even the legacy response is rewritten to a UUID by the SDK. It must not authorize bookmarks.
    expect(response.responses[0]?.topicId).toBe(TOPIC_ID);
    const batch = response.responses[0]?.partitions[0]?.records?.[0];
    expect(batch?.partitionLeaderEpoch).toBe(3);
    expect(batch?.records[0]?.value?.toString()).toBe("evidence");
    expect(consumer.recordProvenance(batch!.partitionLeaderEpoch)).toEqual(
      version === 13
        ? { clusterId: IDENTITY.clusterId, topicId: TOPIC_ID, leaderEpoch: 3 }
        : undefined,
    );
    expect(fixture.requests.filter(({ key }) => key === 1)).toEqual([{ key: 1, version }]);
  },
);

it.each([12, 13] as const)(
  "carries the original batch epoch through the actual SDK message stream at Fetch v%i",
  async (version) => {
    const fixture = await broker(version);
    const consumer = await client(fixture.port);
    const stream = await consumer.consume({
      topics: ["events"],
      mode: "manual",
      autocommit: false,
      maxFetches: 1,
      maxWaitTime: 0,
      offsets: [{ topic: "events", partition: 0, offset: 0n }],
    });
    const records = [];
    for await (const record of stream) {
      records.push({
        value: record.value.toString(),
        offset: String(record.offset),
        epoch: record.leaderEpoch,
        provenance: consumer.recordProvenance(record.leaderEpoch),
      });
    }
    expect(records).toEqual([
      {
        value: "evidence",
        offset: "0",
        epoch: 3,
        provenance:
          version === 13
            ? { clusterId: IDENTITY.clusterId, topicId: TOPIC_ID, leaderEpoch: 3 }
            : undefined,
      },
    ]);
    await stream.close();
  },
);

it("fences replacement before invoking the public callback and preserves old-batch epoch on leadership changes", async () => {
  const fixture = await broker(13);
  const consumer = await client(fixture.port);
  const fetch = (): Promise<fetchV17.FetchResponse> =>
    new Promise<fetchV17.FetchResponse>((resolve, reject) => {
      consumer.fetch(request(), (error, response) => {
        if (error) reject(error);
        else if (response) resolve(response);
        else reject(new Error("No fetch response."));
      });
    });
  expect((await fetch()).responses[0]?.partitions[0]?.records?.[0]?.partitionLeaderEpoch).toBe(3);
  expect(consumer.currentMetadata?.topics.get("events")?.partitions[0]?.leaderEpoch).toBe(9);
  expect(consumer.recordProvenance(3)?.leaderEpoch).toBe(3);
  fixture.replaceResponse();
  await expect(fetch()).rejects.toMatchObject({ reason: "resource-replaced" });
});

it("does not lend another consumer's proven identity to legacy fetches or make metadata calls per record", async () => {
  const modernBroker = await broker(13);
  const legacyBroker = await broker(12);
  const modern = await client(modernBroker.port);
  const legacy = await client(legacyBroker.port);
  await Promise.all([modern.fetch(request()), legacy.fetch(request())]);
  const before = modernBroker.requests.filter(({ key }) => key === 3).length;
  for (let i = 0; i < 100; i++) {
    expect(modern.recordProvenance(3)).toBeDefined();
    expect(legacy.recordProvenance(3)).toBeUndefined();
  }
  expect(modernBroker.requests.filter(({ key }) => key === 3)).toHaveLength(before);
  modern.releaseRecordProvenance();
  expect(modern.recordProvenance(3)).toBeUndefined();
});

it("rejects refreshed same-name metadata and a request redirected to another UUID", async () => {
  const fixture = await broker(13);
  const consumer = await client(fixture.port);
  await consumer.fetch(request());
  fixture.replaceMetadata();
  await consumer.metadata({ topics: ["events"], forceUpdate: true, autocreateTopics: false });
  expect(consumer.recordProvenance(3)).toBeUndefined();
  await expect(consumer.fetch(request(REPLACEMENT_ID))).rejects.toMatchObject({
    reason: "resource-replaced",
  });
  expect(fixture.requests.filter(({ key }) => key === 1)).toHaveLength(1);
});
