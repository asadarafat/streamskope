import { performance } from "node:perf_hooks";

import {
  Consumer,
  consumerFetchesChannel,
  type ClusterMetadata,
  type Message,
  type MessagesStream,
} from "@platformatic/kafka";

import { KAFKA_MESSAGE_LIMITS, KAFKA_QUERY_LIMITS, type KafkaReadCoverage } from "../contracts";
import { KafkaReadCheckpointError, type KafkaReadCheckpoint } from "../application/read-checkpoint";

import { KafkaReadTracker } from "./read-coverage";
import { normalizeKafkaError } from "./failure";
import { resolveKafkaFetchPlan, type KafkaFetchPlan } from "./fetch-plan";
import { platformaticClientOptions } from "./platformatic-options";
import type {
  KafkaConsumerFactory,
  KafkaConsumerInput,
  KafkaRawMessage,
  KafkaRawMessageStream,
} from "./types";

const FINITE_MAX_FETCHES = 64;
const FINITE_MAX_WAIT_TIME_MS = 100;
const CONTINUOUS_MAX_WAIT_TIME_MS = 1_000;

type ReadIdentity = Omit<KafkaReadCheckpoint, "coverage">;

function readIdentity(metadata: ClusterMetadata, topic: string): ReadIdentity | undefined {
  const selected = metadata.topics.get(topic);
  const stable = (value: string | undefined): value is string =>
    typeof value === "string" &&
    value.trim().length > 0 &&
    value.length <= 256 &&
    !/^0+$/u.test(value.replaceAll("-", ""));
  if (!stable(metadata.id) || !stable(selected?.id) || !selected?.partitionsCount) return undefined;
  return { clusterId: metadata.id, topicId: selected.id, partitionCount: selected.partitionsCount };
}

function assertReadIdentity(actual: ReadIdentity | undefined, expected: ReadIdentity): void {
  if (actual?.clusterId !== expected.clusterId || actual.topicId !== expected.topicId)
    throw new KafkaReadCheckpointError("identity-changed");
  if (actual.partitionCount !== expected.partitionCount)
    throw new KafkaReadCheckpointError("partitions-changed");
}

function continuousMaxWaitTime(operationTimeoutMs: number): number {
  return Math.min(CONTINUOUS_MAX_WAIT_TIME_MS, Math.max(0, Math.floor(operationTimeoutMs / 2)));
}

function brokerEndpoint(host: string, port: number): string {
  return host.includes(":") ? `[${host}]:${String(port)}` : `${host}:${String(port)}`;
}

function fetchDiagnosticCleanup(
  consumer: Consumer<Buffer, Buffer, Buffer, Buffer>,
  observer: KafkaConsumerInput["onFetchSample"],
): () => void {
  if (observer === undefined) {
    return (): void => undefined;
  }
  const starts = new Map<bigint, number>();
  const subscribers: Parameters<typeof consumerFetchesChannel.subscribe>[0] = {
    asyncEnd: (): void => undefined,
    asyncStart: (): void => undefined,
    end: (context): void => {
      if (context.client !== consumer) {
        return;
      }
      const operationId = context.operationId;
      if (typeof operationId !== "bigint") {
        return;
      }
      const started = starts.get(operationId);
      starts.delete(operationId);
      const options =
        "options" in context && context.options !== null && typeof context.options === "object"
          ? (context.options as { readonly node?: unknown })
          : undefined;
      const node = options?.node;
      if (
        started === undefined ||
        context.error !== undefined ||
        typeof node !== "number" ||
        !Number.isSafeInteger(node) ||
        node < 0
      ) {
        return;
      }
      const nodeId = node;
      const broker = consumer.currentMetadata?.brokers.get(nodeId);
      if (broker === undefined) {
        return;
      }
      observer({
        broker: brokerEndpoint(broker.host, broker.port),
        durationMs: performance.now() - started,
        nodeId,
      });
    },
    error: (context): void => {
      if (context.client === consumer && typeof context.operationId === "bigint") {
        starts.delete(context.operationId);
      }
    },
    start: (context): void => {
      if (context.client === consumer && typeof context.operationId === "bigint") {
        starts.set(context.operationId, performance.now());
      }
    },
  };
  consumerFetchesChannel.subscribe(subscribers);
  return (): void => {
    consumerFetchesChannel.unsubscribe(subscribers);
    starts.clear();
  };
}

class PlatformaticMessageStream implements KafkaRawMessageStream {
  private readonly preparationController = new AbortController();
  private closePromise: Promise<void> | undefined;
  private readonly tracker: KafkaReadTracker | undefined;

  constructor(
    private readonly consumer: Consumer<Buffer, Buffer, Buffer, Buffer>,
    private readonly stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | null,
    private readonly plan: KafkaFetchPlan,
    private readonly cleanupDiagnostics: () => void,
    private readonly prepareRecord?: KafkaConsumerInput["prepareRecord"],
    private readonly identity?: ReadIdentity,
  ) {
    this.tracker = plan.continuous ? undefined : new KafkaReadTracker(plan);
  }

  coverage(): KafkaReadCoverage | undefined {
    return this.tracker?.snapshot();
  }

  acknowledge(message: KafkaRawMessage): void {
    this.tracker?.acknowledge(message);
  }

  checkpoint(): KafkaReadCheckpoint | undefined {
    return this.identity === undefined || this.tracker === undefined
      ? undefined
      : { ...this.identity, coverage: this.tracker.checkpoint() };
  }

  close(): Promise<void> {
    this.preparationController.abort();
    this.tracker?.finish("cancelled");
    this.closePromise ??= this.closeResources();
    return this.closePromise;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<KafkaRawMessage> {
    let iterationFailure: Error | undefined;
    let cleanupFailure: Error | undefined;
    const deadline = this.plan.continuous
      ? undefined
      : setTimeout(() => {
          this.tracker?.finish("deadline");
          void this.close().catch(() => undefined); // The finally block observes cleanup failure.
        }, KAFKA_QUERY_LIMITS.durationMs);
    deadline?.unref?.();
    try {
      if (this.stream !== null && !this.tracker?.finished) {
        for await (const message of this.stream) {
          const raw = this.toRawMessage(message);
          const accepted =
            this.tracker === undefined
              ? this.includes(raw)
              : this.tracker.accept(
                  raw,
                  this.prepareRecord
                    ? await this.prepareRecord(raw, this.preparationController.signal)
                    : undefined,
                );
          if (accepted) yield raw;
          if (this.tracker?.finished) break;
        }
      }
      this.tracker?.finish("fetch-limit");
    } catch (error) {
      if (!this.preparationController.signal.aborted) {
        this.tracker?.finish("failed");
        iterationFailure = normalizeKafkaError(error);
      }
    } finally {
      if (deadline !== undefined) clearTimeout(deadline);
      if (!this.plan.continuous) {
        try {
          await this.close();
        } catch (error) {
          this.tracker?.finish("failed");
          cleanupFailure = normalizeKafkaError(error);
        }
      }
    }
    if (iterationFailure !== undefined && cleanupFailure !== undefined) {
      throw new AggregateError(
        [iterationFailure, cleanupFailure],
        "The finite Kafka stream failed and did not close cleanly.",
        { cause: iterationFailure },
      );
    }
    if (iterationFailure !== undefined) {
      throw iterationFailure;
    }
    if (cleanupFailure !== undefined) {
      throw cleanupFailure;
    }
  }

  private async closeResources(): Promise<void> {
    const failures: unknown[] = [];
    this.cleanupDiagnostics();
    if (this.stream !== null) {
      try {
        await Promise.resolve(this.stream.close());
      } catch (error) {
        failures.push(error);
      }
    }
    try {
      await Promise.resolve(this.consumer.close(true));
    } catch (error) {
      failures.push(error);
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, "The Kafka consumer did not close cleanly.");
    }
  }

  private includes(message: KafkaRawMessage): boolean {
    const request = this.plan.request;
    if (message.topic !== request.topic) {
      return false;
    }
    const start = this.plan.startOffsets.get(message.partition);
    if (start === undefined || message.offset < start) {
      return false;
    }
    const end = this.plan.endOffsets?.get(message.partition);
    if (this.plan.endOffsets !== null && (end === undefined || message.offset >= end)) {
      return false;
    }
    return request.mode !== "time-window"
      ? true
      : message.timestamp >= BigInt(request.startTimeMs) &&
          message.timestamp < BigInt(request.endTimeMs);
  }

  private toRawMessage(message: Message<Buffer, Buffer, Buffer, Buffer>): KafkaRawMessage {
    return {
      headers: message.headers,
      ...(message.headerEntries === undefined ? {} : { headerEntries: message.headerEntries }),
      key: message.key,
      offset: message.offset,
      partition: message.partition,
      timestamp: message.timestamp,
      topic: message.topic,
      value: message.value,
    };
  }
}

export class PlatformaticConsumerFactory implements KafkaConsumerFactory {
  async open(input: KafkaConsumerInput): Promise<KafkaRawMessageStream> {
    input.signal?.throwIfAborted();
    const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
      ...platformaticClientOptions(input, `streamskope-consumer-${input.groupId}`),
      groupId: input.groupId,
      retries: 5,
      retryDelay: 200,
    });
    const cleanupDiagnostics = fetchDiagnosticCleanup(consumer, input.onFetchSample);
    let closing: Promise<void> | undefined;
    const close = (force = false): Promise<void> =>
      (closing ??= Promise.resolve(consumer.close(force)));
    const abort = (): void => {
      void close(true).catch(() => undefined);
    };
    input.signal?.addEventListener("abort", abort, { once: true });
    try {
      const captureIdentity = async (): Promise<ReadIdentity | undefined> =>
        readIdentity(
          await consumer.metadata({
            topics: [input.request.topic],
            forceUpdate: true,
            autocreateTopics: false,
          }),
          input.request.topic,
        );
      const identity = input.request.mode === "tail" ? undefined : await captureIdentity();
      if (input.checkpoint !== undefined) assertReadIdentity(identity, input.checkpoint);
      const plan = await resolveKafkaFetchPlan(
        {
          listTopicOffsets: async (topic, timestamp) => {
            const offsets = await consumer.listOffsets({
              timestamp,
              topics: [topic],
            });
            const topicOffsets = offsets.get(topic);
            if (topicOffsets === undefined) {
              throw new Error(`Kafka returned no offset metadata for topic ${topic}.`);
            }
            if (identity !== undefined && topicOffsets.length !== identity.partitionCount)
              throw new KafkaReadCheckpointError("partitions-changed");
            return topicOffsets;
          },
        },
        input.request,
        Date.now(),
        input.checkpoint,
      );
      if (identity !== undefined) assertReadIdentity(await captureIdentity(), identity);
      input.signal?.throwIfAborted();
      const offsets = [...plan.startOffsets].map(([partition, offset]) => ({
        offset,
        partition,
        topic: input.request.topic,
      }));
      const hasFiniteRecords =
        plan.endOffsets === null ||
        [...plan.startOffsets].some(
          ([partition, start]) => (plan.endOffsets?.get(partition) ?? start) > start,
        );
      if (!hasFiniteRecords) {
        return new PlatformaticMessageStream(
          consumer,
          null,
          plan,
          cleanupDiagnostics,
          input.prepareRecord,
          identity,
        );
      }
      const stream = await consumer.consume({
        autocommit: false,
        highWaterMark: KAFKA_MESSAGE_LIMITS.batchMessages,
        maxBytes: 4 * KAFKA_MESSAGE_LIMITS.messageBytes,
        maxBytesPerPartition: KAFKA_MESSAGE_LIMITS.messageBytes,
        fallbackMode: "fail",
        ...(plan.continuous
          ? {
              maxWaitTime: continuousMaxWaitTime(input.operationTimeoutMs),
            }
          : {
              maxFetches: FINITE_MAX_FETCHES,
              maxWaitTime: FINITE_MAX_WAIT_TIME_MS,
            }),
        mode: "manual",
        offsets,
        topics: [input.request.topic],
      });
      input.signal?.throwIfAborted();
      return new PlatformaticMessageStream(
        consumer,
        stream,
        plan,
        cleanupDiagnostics,
        input.prepareRecord,
        identity,
      );
    } catch (error) {
      cleanupDiagnostics();
      try {
        await close();
      } catch (cleanupError) {
        throw new AggregateError(
          [error, cleanupError],
          "The Kafka consumer failed to start and did not close cleanly.",
          { cause: cleanupError },
        );
      }
      throw error;
    } finally {
      input.signal?.removeEventListener("abort", abort);
    }
  }
}
