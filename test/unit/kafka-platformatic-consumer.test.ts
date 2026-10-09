import { performance } from "node:perf_hooks";

import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  KAFKA_QUERY_LIMITS,
  type KafkaFetchRequest,
  type KafkaMessage,
  type KafkaReadCoverage,
} from "../../src/features/kafka/contracts";
import { KafkaReadOpenCleanupError } from "../../src/features/kafka/application/read-open-cleanup";
import type { KafkaRecordLocator } from "../../src/features/kafka/contracts/record-locator";
import { PlatformaticConsumerFactory } from "../../src/features/kafka/engine/platformatic-consumer";
import type { KafkaConsumerInput, KafkaRawMessage } from "../../src/features/kafka/engine/types";
import { translateKafkaRecord } from "../../src/features/kafka/engine/message-record";

interface FetchDiagnosticSubscriber {
  asyncEnd(context: unknown): void;
  asyncStart(context: unknown): void;
  end(context: unknown): void;
  error(context: unknown): void;
  start(context: unknown): void;
}

const kafkaState = vi.hoisted(() => ({
  closeCalls: [] as boolean[],
  closeFailures: [] as Error[],
  pendingClose: undefined as Promise<void> | undefined,
  onConsume: undefined as (() => void) | undefined,
  streamCloseFailures: [] as Error[],
  consumeCalls: [] as Array<Record<string, unknown>>,
  consumers: [] as unknown[],
  fetchSubscribers: new Set<FetchDiagnosticSubscriber>(),
  fetchUnsubscribeCalls: 0,
  listOffsetCalls: [] as Array<{
    readonly timestamp?: bigint;
    readonly topics: readonly string[];
  }>,
  messages: [] as KafkaRawMessage[],
  metadataCalls: [] as Array<Record<string, unknown>>,
  missingTopic: false,
  metadataFailure: undefined as Error | undefined,
  identities: [] as Array<{ clusterId: string; topicId: string; partitionCount?: number }>,
  offsets: new Map<string, readonly bigint[]>(),
  offsetFailure: undefined as Error | undefined,
  pendingOffsets: undefined as Promise<ReadonlyMap<string, readonly bigint[]>> | undefined,
  streamCloseCalls: 0,
}));

vi.mock("@platformatic/kafka", async (importOriginal) => {
  const { EventEmitter } = await import("node:events");
  const actual = await importOriginal<typeof import("@platformatic/kafka")>();
  class FakeMessagesStream extends EventEmitter implements AsyncIterable<KafkaRawMessage> {
    readonly offsetsToFetch = new Map<string, bigint>();

    close(): Promise<void> {
      kafkaState.streamCloseCalls += 1;
      const failure = kafkaState.streamCloseFailures.shift();
      return failure === undefined ? Promise.resolve() : Promise.reject(failure);
    }

    async *[Symbol.asyncIterator](): AsyncIterator<KafkaRawMessage> {
      for (const message of kafkaState.messages) {
        yield await Promise.resolve(message);
      }
    }
  }

  return {
    ...actual,
    Consumer: class {
      readonly currentMetadata = {
        brokers: new Map([[1, { host: "kafka-1", port: 9093 }]]),
      };

      constructor() {
        kafkaState.consumers.push(this);
      }

      close(force = false): Promise<void> {
        kafkaState.closeCalls.push(force);
        const failure = kafkaState.closeFailures.shift();
        if (kafkaState.pendingClose !== undefined) return kafkaState.pendingClose;
        return failure === undefined ? Promise.resolve() : Promise.reject(failure);
      }

      consume(
        options: Record<string, unknown>,
        callback: (error: Error | null, stream?: FakeMessagesStream) => void,
      ): void {
        kafkaState.consumeCalls.push(options);
        kafkaState.onConsume?.();
        callback(null, new FakeMessagesStream());
      }

      metadata(options: { readonly topics: readonly string[] }): Promise<unknown> {
        kafkaState.metadataCalls.push(options);
        if (kafkaState.metadataFailure !== undefined)
          return Promise.reject(kafkaState.metadataFailure);
        const identity = kafkaState.identities.shift() ?? {
          clusterId: "fixture-cluster",
          topicId: "fixture-topic",
        };
        return Promise.resolve({
          ...this.currentMetadata,
          id: identity.clusterId,
          topics: kafkaState.missingTopic
            ? new Map()
            : new Map([
                [
                  options.topics[0],
                  {
                    id: identity.topicId,
                    partitionsCount:
                      identity.partitionCount ?? kafkaState.offsets.get("-1")?.length ?? 1,
                  },
                ],
              ]),
        });
      }

      listOffsets(options: {
        readonly timestamp?: bigint;
        readonly topics: readonly string[];
      }): Promise<ReadonlyMap<string, readonly bigint[]>> {
        kafkaState.listOffsetCalls.push(options);
        if (kafkaState.offsetFailure !== undefined) {
          return Promise.reject(kafkaState.offsetFailure);
        }
        if (kafkaState.pendingOffsets !== undefined) return kafkaState.pendingOffsets;
        const timestamp = options.timestamp;
        const offsets =
          kafkaState.offsets.get(String(timestamp)) ?? kafkaState.offsets.get("recent") ?? [];
        return Promise.resolve(new Map([[options.topics[0] ?? "", offsets]]));
      }
    },
    consumerFetchesChannel: {
      subscribe(subscriber: FetchDiagnosticSubscriber): void {
        kafkaState.fetchSubscribers.add(subscriber);
      },
      unsubscribe(subscriber: FetchDiagnosticSubscriber): void {
        kafkaState.fetchSubscribers.delete(subscriber);
        kafkaState.fetchUnsubscribeCalls += 1;
      },
    },
  };
});

const clientInput = {
  brokers: ["localhost:19093"],
  caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
  groupId: "streamskope-test",
  operationTimeoutMs: 5_000,
} as const;

function input(request: KafkaFetchRequest): KafkaConsumerInput {
  return { ...clientInput, request };
}

function rawMessage(offset: bigint, timestamp = 1_722_000_000_000n + offset): KafkaRawMessage {
  return {
    headers: new Map(),
    offset,
    partition: 0,
    timestamp,
    topic: "orders",
    value: Buffer.from(String(offset)),
  };
}

async function collect(stream: AsyncIterable<KafkaRawMessage>): Promise<readonly bigint[]> {
  const offsets: bigint[] = [];
  for await (const message of stream) {
    offsets.push(message.offset);
  }
  return offsets;
}

beforeEach(() => {
  kafkaState.closeCalls.length = 0;
  kafkaState.closeFailures.length = 0;
  kafkaState.pendingClose = undefined;
  kafkaState.onConsume = undefined;
  kafkaState.streamCloseFailures.length = 0;
  kafkaState.consumeCalls.length = 0;
  kafkaState.consumers.length = 0;
  kafkaState.fetchSubscribers.clear();
  kafkaState.fetchUnsubscribeCalls = 0;
  kafkaState.listOffsetCalls.length = 0;
  kafkaState.messages.length = 0;
  kafkaState.metadataCalls.length = 0;
  kafkaState.missingTopic = false;
  kafkaState.metadataFailure = undefined;
  kafkaState.identities.length = 0;
  kafkaState.offsets.clear();
  kafkaState.offsetFailure = undefined;
  kafkaState.pendingOffsets = undefined;
  kafkaState.streamCloseCalls = 0;
});

describe("durable locator preflight and retained cleanup", () => {
  const expected: KafkaRecordLocator = {
    schemaVersion: 1,
    clusterId: "fixture-cluster",
    topicId: "12345678-1234-1234-1234-123456789abc",
    topic: "orders",
    partition: 0,
    offset: "7",
    leaderEpoch: 3,
  };
  const identity = { clusterId: expected.clusterId, topicId: expected.topicId };
  const guarded = (): KafkaConsumerInput => ({
    ...input({
      topic: "orders",
      mode: "earliest",
      maxMessages: 1,
      search: { key: "", value: "", offset: "", timestamp: "", partition: 0, offsetExact: "7" },
    }),
    expectedLocator: expected,
  });
  it("distinguishes an absent topic from a present topic without a stable UUID", async () => {
    kafkaState.missingTopic = true;
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toMatchObject({
      reason: "topic-missing",
    });
    kafkaState.missingTopic = false;
    kafkaState.identities.push({ ...identity, topicId: "orders" });
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toMatchObject({
      reason: "unavailable",
    });
    expect(kafkaState.listOffsetCalls).toHaveLength(0);
  });
  it("recognizes only the SDK's exact unknown-topic conversion and preserves authorization failures", async () => {
    kafkaState.metadataFailure = Object.assign(new Error("Unknown topic orders."), {
      code: "PLT_KFK_USER",
    });
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toMatchObject({
      reason: "topic-missing",
    });
    kafkaState.metadataFailure = Object.assign(new Error("denied"), {
      apiId: "TOPIC_AUTHORIZATION_FAILED",
    });
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toBe(
      kafkaState.metadataFailure,
    );
    kafkaState.metadataFailure = Object.assign(new Error("Unknown topic another."), {
      code: "PLT_KFK_USER",
    });
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toBe(
      kafkaState.metadataFailure,
    );
  });
  it("reports expiry only after confirming identity again after both bounds reads", async () => {
    kafkaState.identities.push(identity, identity);
    kafkaState.offsets.set("-2", [8n]);
    kafkaState.offsets.set("-1", [10n]);
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toMatchObject({
      reason: "expired",
    });
    expect(kafkaState.metadataCalls).toHaveLength(2);
    expect(kafkaState.consumeCalls).toHaveLength(0);
  });
  it("does not call replacement during bounds lookup retention expiry", async () => {
    kafkaState.identities.push(identity, {
      ...identity,
      topicId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    });
    kafkaState.offsets.set("-2", [8n]);
    kafkaState.offsets.set("-1", [10n]);
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toMatchObject({
      reason: "resource-replaced",
    });
  });
  it("does not call a truncated high watermark a missing record", async () => {
    kafkaState.identities.push(identity, identity);
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [7n]);
    await expect(new PlatformaticConsumerFactory().open(guarded())).rejects.toMatchObject({
      reason: "unavailable",
    });
  });
  it("allows partition growth while guarding an existing partition", async () => {
    kafkaState.identities.push(
      { ...identity, partitionCount: 1 },
      { ...identity, partitionCount: 2 },
    );
    kafkaState.offsets.set("-2", [0n, 0n]);
    kafkaState.offsets.set("-1", [10n, 2n]);
    const stream = await new PlatformaticConsumerFactory().open(guarded());
    expect(kafkaState.consumeCalls[0]).toMatchObject({
      offsets: [{ topic: "orders", partition: 0, offset: 7n }],
    });
    await stream.close();
  });
  it.each(["stream", "consumer"] as const)(
    "retries only failed %s cleanup and joins concurrent closes",
    async (failing) => {
      kafkaState.offsets.set("-2", [0n]);
      kafkaState.offsets.set("-1", [10n]);
      const stream = await new PlatformaticConsumerFactory().open(
        input({ topic: "orders", mode: "tail", maxMessages: 10 }),
      );
      (failing === "stream" ? kafkaState.streamCloseFailures : kafkaState.closeFailures).push(
        new Error("close failed"),
      );
      const first = stream.close();
      expect(stream.close()).toBe(first);
      await expect(first).rejects.toThrow("did not close cleanly");
      await stream.close();
      expect(kafkaState.streamCloseCalls).toBe(failing === "stream" ? 2 : 1);
      expect(kafkaState.closeCalls).toHaveLength(failing === "consumer" ? 2 : 1);
    },
  );
  it("retains a failed-open consumer behind an exact close-only retry handle", async () => {
    kafkaState.offsetFailure = new Error("planning failed");
    kafkaState.closeFailures.push(new Error("closing failed"));
    const failure = await new PlatformaticConsumerFactory()
      .open(input({ topic: "orders", mode: "earliest", maxMessages: 1 }))
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(KafkaReadOpenCleanupError);
    await (failure as KafkaReadOpenCleanupError).cleanup.close();
    expect(kafkaState.closeCalls).toEqual([false, true]);
    expect(kafkaState.consumers).toHaveLength(1);
  });
  it("joins the original pending consumer close when a cancelled consume returns its stream late", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [10n]);
    let finishClose!: () => void;
    kafkaState.pendingClose = new Promise<void>((resolve) => {
      finishClose = resolve;
    });
    const controller = new AbortController();
    kafkaState.onConsume = (): void => {
      controller.abort();
    };
    let settled = false;
    const opening = new PlatformaticConsumerFactory()
      .open({
        ...input({ topic: "orders", mode: "tail", maxMessages: 10 }),
        signal: controller.signal,
      })
      .catch((error: unknown) => {
        settled = true;
        return error;
      });
    await vi.waitFor(() => expect(kafkaState.closeCalls).toHaveLength(1));
    expect(settled).toBe(false);
    expect(kafkaState.streamCloseCalls).toBe(0);
    expect(kafkaState.closeCalls).toEqual([true]);
    finishClose();
    expect(await opening).toMatchObject({ name: "AbortError" });
    expect(kafkaState.closeCalls).toEqual([true]);
    expect(kafkaState.streamCloseCalls).toBe(1);
  });
});

it("retains the client's ordered header entries including duplicate names and null values", async () => {
  kafkaState.offsets.set("-2", [0n]);
  kafkaState.offsets.set("-1", [0n]);
  const headerEntries = [
    [Buffer.from("same"), Buffer.from("one")],
    [Buffer.from("same"), undefined],
  ] as const;
  kafkaState.messages.push({ ...rawMessage(0n), headerEntries });
  const stream = await new PlatformaticConsumerFactory().open(
    input({ mode: "tail", maxMessages: 10, topic: "orders" }),
  );
  try {
    const result = await stream[Symbol.asyncIterator]().next();
    if (result.done) throw new Error("Expected a Kafka record");
    expect(result.value.headerEntries).toEqual(headerEntries);
  } finally {
    await stream.close();
  }
});

describe("Platformatic Kafka fetch adapter", () => {
  it("reports finite scan progress for nonmatches at most once per 500 monotonic milliseconds", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [5n]);
    kafkaState.messages.push(...[0n, 1n, 2n, 3n, 4n].map((offset) => rawMessage(offset)));
    let now = 0;
    const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
    const times = [0, 499, 500, 999, 1000];
    const coverage: KafkaReadCoverage[] = [];
    const stream = await new PlatformaticConsumerFactory().open({
      ...input({
        mode: "earliest",
        topic: "orders",
        maxMessages: 5,
        search: { key: "", value: "absent", offset: "", timestamp: "", partition: null },
      }),
      prepareRecord: (record) => {
        now = times[Number(record.offset)]!;
        return Promise.resolve(translateKafkaRecord(record, "orders"));
      },
    });
    const unsubscribe = stream.subscribeCoverage?.((snapshot) => coverage.push(snapshot));
    try {
      expect(await collect(stream)).toEqual([]);
      expect(coverage.map((snapshot) => snapshot.scannedRecords)).toEqual([1, 3, 5]);
      expect(coverage.every((snapshot) => snapshot.matchedRecords === 0)).toBe(true);
      expect(coverage.at(-1)).toMatchObject({ reason: "range-complete" });
      expect(kafkaState.streamCloseCalls).toBe(1);
    } finally {
      unsubscribe?.();
      clock.mockRestore();
      await stream.close();
    }
  });

  it("unsubscribes finite progress listeners without changing the record read", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [2n]);
    kafkaState.messages.push(rawMessage(0n), rawMessage(1n));
    const stream = await new PlatformaticConsumerFactory().open(
      input({ mode: "earliest", topic: "orders", maxMessages: 2 }),
    );
    const listener = vi.fn();
    const unsubscribe = stream.subscribeCoverage?.(listener);
    unsubscribe?.();
    unsubscribe?.();
    expect(await collect(stream)).toEqual([0n, 1n]);
    expect(listener).not.toHaveBeenCalled();
  });

  it("fails and closes owned finite resources when a progress observer throws", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [2n]);
    kafkaState.messages.push(rawMessage(0n), rawMessage(1n));
    const stream = await new PlatformaticConsumerFactory().open(
      input({ mode: "earliest", topic: "orders", maxMessages: 2 }),
    );
    const failure = new Error("coverage observer failed");
    stream.subscribeCoverage?.(() => {
      throw failure;
    });
    await expect(collect(stream)).rejects.toBe(failure);
    expect(stream.coverage?.()).toMatchObject({ reason: "failed", scannedRecords: 1 });
    expect(kafkaState.closeCalls).toEqual([true]);
    expect(kafkaState.streamCloseCalls).toBe(1);
    await stream.close();
    expect(kafkaState.closeCalls).toEqual([true]);
  });

  it("checkpoints only acknowledged delivery, including a stop with a pending record", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [3n]);
    kafkaState.messages.push(rawMessage(0n), rawMessage(1n), rawMessage(2n));
    const stream = await new PlatformaticConsumerFactory().open(
      input({ mode: "earliest", topic: "orders", maxMessages: 3 }),
    );
    const iterator = stream[Symbol.asyncIterator]();
    try {
      const first = await iterator.next();
      if (first.done) throw new Error("Expected first fixture record");
      expect(stream.coverage?.()?.partitions[0]?.nextOffset).toBe("1");
      expect(stream.checkpoint?.()?.coverage).toMatchObject({
        scannedRecords: 0,
        matchedRecords: 0,
        partitions: [{ nextOffset: "0" }],
      });
      stream.acknowledge?.(first.value);
      expect(stream.checkpoint?.()?.coverage.partitions[0]?.nextOffset).toBe("1");
      const second = await iterator.next();
      if (second.done) throw new Error("Expected second fixture record");
      await stream.close();
      expect(stream.checkpoint?.()).toMatchObject({
        clusterId: "fixture-cluster",
        topicId: "fixture-topic",
        partitionCount: 1,
        coverage: {
          reason: "cancelled",
          scannedRecords: 1,
          matchedRecords: 1,
          partitions: [{ nextOffset: "1", endOffset: "3" }],
        },
      });
    } finally {
      await stream.close();
      await iterator.return?.();
    }
  });

  it("keeps ordinary finite reads usable when stable topic IDs are unavailable", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [1n]);
    kafkaState.identities.push({
      clusterId: "fixture-cluster",
      topicId: "00000000-0000-0000-0000-000000000000",
    });
    kafkaState.messages.push(rawMessage(0n));
    const stream = await new PlatformaticConsumerFactory().open(
      input({ mode: "earliest", topic: "orders", maxMessages: 1 }),
    );
    expect(await collect(stream)).toEqual([0n]);
    expect(stream.checkpoint?.()).toBeUndefined();
  });

  it("rejects identity changes during offset planning before starting a reader", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [2n]);
    kafkaState.identities.push(
      { clusterId: "fixture-cluster", topicId: "old-topic" },
      { clusterId: "fixture-cluster", topicId: "replacement-topic" },
    );
    await expect(
      new PlatformaticConsumerFactory().open(
        input({ mode: "earliest", topic: "orders", maxMessages: 1 }),
      ),
    ).rejects.toMatchObject({ name: "KafkaReadCheckpointError", reason: "identity-changed" });
    expect(kafkaState.consumeCalls).toEqual([]);
    expect(kafkaState.closeCalls).toEqual([false]);
  });

  it("rejects a resumed checkpoint belonging to another topic identity", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [2n]);
    await expect(
      new PlatformaticConsumerFactory().open({
        ...input({ mode: "earliest", topic: "orders", maxMessages: 1 }),
        checkpoint: {
          clusterId: "fixture-cluster",
          topicId: "deleted-topic",
          partitionCount: 1,
          coverage: {
            reason: "result-limit",
            scannedRecords: 1,
            scannedBytes: 1,
            matchedRecords: 1,
            unavailableRecords: 0,
            partitions: [{ partition: 0, startOffset: "0", endOffset: "2", nextOffset: "1" }],
          },
        },
      }),
    ).rejects.toMatchObject({ reason: "identity-changed" });
    expect(kafkaState.consumeCalls).toEqual([]);
  });
  it.each([false, true])(
    "cancels projection at the finite deadline (search=%s) without inventing failed coverage",
    async (search) => {
      vi.useFakeTimers();
      try {
        kafkaState.offsets.set("-2", [0n]);
        kafkaState.offsets.set("-1", [2n]);
        kafkaState.messages.push(rawMessage(0n));
        let entered!: () => void;
        const preparing = new Promise<void>((resolve) => {
          entered = resolve;
        });
        let revoked = false;
        const stream = await new PlatformaticConsumerFactory().open({
          ...input({
            mode: "earliest",
            topic: "orders",
            maxMessages: 10,
            ...(search
              ? { search: { key: "", value: "secret", offset: "", timestamp: "", partition: null } }
              : {}),
          }),
          prepareRecord: (_raw, signal): Promise<KafkaMessage> =>
            new Promise((_resolve, reject) => {
              if (!signal) throw new Error("Projection cancellation signal missing");
              signal.addEventListener(
                "abort",
                () => {
                  revoked = true;
                  reject(new Error("Projection cancelled"));
                },
                { once: true },
              );
              entered();
            }),
        });
        const pending = collect(stream);
        await preparing;
        await vi.advanceTimersByTimeAsync(KAFKA_QUERY_LIMITS.durationMs);
        expect(await pending).toEqual([]);
        expect(revoked).toBe(true);
        expect(stream.coverage?.()).toMatchObject({ reason: "deadline", scannedRecords: 0 });
      } finally {
        vi.useRealTimers();
      }
    },
  );
  it("awaits protected decoding before selecting bounded search matches", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [2n]);
    kafkaState.messages.push(
      { ...rawMessage(0n), value: Buffer.from("secret") },
      { ...rawMessage(1n), value: Buffer.from([0, 0, 0, 0, 7, 1]) },
    );
    const prepareRecord = vi.fn(async (raw: KafkaRawMessage) => {
      await Promise.resolve();
      return {
        ...translateKafkaRecord(raw, "orders"),
        payload: raw.offset === 0n ? "[MASKED]" : "secret",
      };
    });
    const stream = await new PlatformaticConsumerFactory().open({
      ...input({
        mode: "earliest",
        topic: "orders",
        maxMessages: 10,
        search: { key: "", value: "secret", offset: "", timestamp: "", partition: null },
      }),
      prepareRecord,
    });
    expect(await collect(stream)).toEqual([1n]);
    expect(prepareRecord).toHaveBeenCalledTimes(2);
    expect(stream.coverage?.()).toMatchObject({
      reason: "range-complete",
      scannedRecords: 2,
      matchedRecords: 1,
    });
  });
  it("closes a pending consumer immediately on cancellation and never starts a later fetch", async () => {
    let finish!: (offsets: ReadonlyMap<string, readonly bigint[]>) => void;
    kafkaState.pendingOffsets = new Promise((resolve) => {
      finish = resolve;
    });
    const controller = new AbortController();
    const opening = new PlatformaticConsumerFactory().open({
      ...input({ mode: "earliest", topic: "orders", maxMessages: 10 }),
      signal: controller.signal,
      onFetchSample: () => undefined,
    });
    const rejected = expect(opening).rejects.toMatchObject({ name: "AbortError" });
    controller.abort();
    expect(kafkaState.closeCalls).toEqual([true]);
    finish(new Map([["orders", [0n]]]));
    await rejected;
    expect(kafkaState.consumeCalls).toEqual([]);
    expect(kafkaState.closeCalls).toEqual([true]);
    expect(kafkaState.fetchSubscribers.size).toBe(0);
  });

  it("does not construct a consumer for an already cancelled read", async () => {
    await expect(
      new PlatformaticConsumerFactory().open({
        ...input({ mode: "earliest", topic: "orders", maxMessages: 10 }),
        signal: AbortSignal.abort(),
      }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(kafkaState.consumers).toEqual([]);
  });

  it("preserves cancelled coverage and closes an unopened iteration exactly once", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [100n]);
    const stream = await new PlatformaticConsumerFactory().open(
      input({ mode: "earliest", topic: "orders", maxMessages: 10 }),
    );
    await stream.close();
    await stream.close();
    expect(await collect(stream)).toEqual([]);
    expect(stream.coverage?.()).toMatchObject({
      reason: "cancelled",
      scannedRecords: 0,
      partitions: [{ nextOffset: "0", endOffset: "100" }],
    });
    expect(kafkaState.closeCalls).toEqual([true]);
  });

  it("opens a bounded manual First N stream and closes after sparse in-range records", async () => {
    kafkaState.offsets.set("-2", [5n]);
    kafkaState.offsets.set("-1", [10n]);
    kafkaState.messages.push(rawMessage(5n), rawMessage(7n), rawMessage(10n));
    const factory = new PlatformaticConsumerFactory();

    const stream = await factory.open(
      input({
        maxMessages: 2,
        mode: "earliest",
        topic: "orders",
      }),
    );

    await expect(collect(stream)).resolves.toEqual([5n, 7n]);
    expect(stream.coverage?.()).toMatchObject({ reason: "result-limit", scannedRecords: 2 });
    expect(kafkaState.consumeCalls).toEqual([
      expect.objectContaining({
        autocommit: false,
        fallbackMode: "fail",
        mode: "manual",
        offsets: [{ offset: 5n, partition: 0, topic: "orders" }],
        topics: ["orders"],
      }),
    ]);
    expect(kafkaState.consumeCalls[0]?.maxFetches).toEqual(expect.any(Number));
    expect(kafkaState.closeCalls).toEqual([true]);
    expect(kafkaState.streamCloseCalls).toBe(1);
  });

  it("enforces the topic-wide Newest N limit and excludes later offsets", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [3n]);
    kafkaState.offsets.set("recent", [1n]);
    kafkaState.messages.push(rawMessage(1n), rawMessage(2n), rawMessage(3n));
    const factory = new PlatformaticConsumerFactory();

    const stream = await factory.open(
      input({
        maxMessages: 2,
        mode: "newest",
        topic: "orders",
      }),
    );

    await expect(collect(stream)).resolves.toEqual([1n, 2n]);
    expect(kafkaState.closeCalls).toEqual([true]);
  });

  it("filters a time-window snapshot by its half-open Kafka timestamp interval", async () => {
    const startTimeMs = 1_722_000_000_000;
    const endTimeMs = startTimeMs + 120_000;
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [4n]);
    kafkaState.offsets.set(String(startTimeMs), [1n]);
    kafkaState.offsets.set(String(endTimeMs), [4n]);
    kafkaState.messages.push(
      rawMessage(1n, BigInt(startTimeMs - 1)),
      rawMessage(2n, BigInt(startTimeMs)),
      rawMessage(3n, BigInt(endTimeMs - 1)),
      rawMessage(4n, BigInt(startTimeMs)),
    );
    const factory = new PlatformaticConsumerFactory();

    const stream = await factory.open(
      input({
        endTimeMs,
        maxMessages: 10,
        mode: "time-window",
        startTimeMs,
        topic: "orders",
      }),
    );

    await expect(collect(stream)).resolves.toEqual([2n, 3n]);
  });

  it("keeps Tail continuous beyond the starting high watermark until explicitly closed", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [2n]);
    kafkaState.messages.push(rawMessage(0n), rawMessage(1n), rawMessage(2n));
    const factory = new PlatformaticConsumerFactory();

    const stream = await factory.open(
      input({
        maxMessages: 5,
        mode: "tail",
        topic: "orders",
      }),
    );

    await expect(collect(stream)).resolves.toEqual([0n, 1n, 2n]);
    expect(kafkaState.consumeCalls[0]).not.toHaveProperty("maxFetches");
    expect(kafkaState.consumeCalls[0]).toMatchObject({
      autocommit: false,
      highWaterMark: 200,
      maxBytes: 4 * 1024 * 1024,
      maxBytesPerPartition: 1024 * 1024,
      mode: "manual",
    });
    expect(kafkaState.consumeCalls[0]?.maxWaitTime).toEqual(expect.any(Number));
    expect(kafkaState.consumeCalls[0]?.maxWaitTime).toBeLessThan(clientInput.operationTimeoutMs);
    expect(kafkaState.closeCalls).toEqual([]);
    await stream.close();
    expect(kafkaState.closeCalls).toEqual([true]);
  });

  it("keeps the Tail broker wait below the smallest supported request timeout", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [0n]);
    const factory = new PlatformaticConsumerFactory();

    const stream = await factory.open({
      ...input({
        maxMessages: 1,
        mode: "tail",
        topic: "orders",
      }),
      operationTimeoutMs: 1,
    });

    expect(kafkaState.consumeCalls[0]?.maxWaitTime).toBe(0);
    expect(kafkaState.consumeCalls[0]?.maxWaitTime).toBeLessThan(1);
    await stream.close();
  });

  it("attributes fetch diagnostics to its own consumer and unsubscribes on close", async () => {
    kafkaState.offsets.set("-2", [0n]);
    kafkaState.offsets.set("-1", [1n]);
    kafkaState.messages.push(rawMessage(0n));
    const samples: Array<{ readonly broker: string; readonly durationMs: number }> = [];
    const factory = new PlatformaticConsumerFactory();

    const stream = await factory.open({
      ...input({
        maxMessages: 1,
        mode: "earliest",
        topic: "orders",
      }),
      onFetchSample: (sample): void => {
        samples.push(sample);
      },
    });
    const consumer = kafkaState.consumers[0];
    const subscriber = [...kafkaState.fetchSubscribers][0];
    expect(subscriber).toBeDefined();
    subscriber?.start({ client: {}, operationId: 1n });
    subscriber?.end({
      client: {},
      operationId: 1n,
      options: { node: 1 },
    });
    subscriber?.start({ client: consumer, operationId: 2n });
    subscriber?.end({
      client: consumer,
      operationId: 2n,
      options: { node: 1 },
    });

    expect(samples).toHaveLength(1);
    expect(samples[0]).toMatchObject({
      broker: "kafka-1:9093",
      nodeId: 1,
    });
    expect(samples[0]?.durationMs).toBeGreaterThanOrEqual(0);
    await expect(collect(stream)).resolves.toEqual([0n]);
    expect(kafkaState.fetchSubscribers.size).toBe(0);
    expect(kafkaState.fetchUnsubscribeCalls).toBe(1);
  });

  it("closes the partial consumer when offset planning fails", async () => {
    kafkaState.offsetFailure = new Error("offset lookup denied");
    const factory = new PlatformaticConsumerFactory();

    await expect(
      factory.open(
        input({
          maxMessages: 5,
          mode: "earliest",
          topic: "orders",
        }),
      ),
    ).rejects.toThrow("offset lookup denied");
    expect(kafkaState.consumeCalls).toEqual([]);
    expect(kafkaState.closeCalls).toEqual([false]);
  });
});
