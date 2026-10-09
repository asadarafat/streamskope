import { performance } from "node:perf_hooks";

import {
  type Consumer,
  consumerFetchesChannel,
  type ClusterMetadata,
  type Message,
  type MessagesStream,
} from "@platformatic/kafka";

import { KAFKA_MESSAGE_LIMITS, KAFKA_QUERY_LIMITS, type KafkaReadCoverage } from "../contracts";
import { KafkaReadCheckpointError, type KafkaReadCheckpoint } from "../application/read-checkpoint";
import { parseKafkaRecordProvenance, type KafkaRecordLocator } from "../contracts/record-locator";
import { KafkaReadOpenCleanupError } from "../application/read-open-cleanup";
import { KafkaRecordLocatorError } from "../application/record-locator-errors";
import { connectionErrorChain } from "../application/connection-diagnostics";

import { RecordProvenanceConsumer } from "./record-provenance-consumer";
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
const COVERAGE_NOTIFICATION_INTERVAL_MS = 500;

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
  private readonly coverageListeners = new Set<(coverage: KafkaReadCoverage) => void>();
  private lastCoverageNotification: number | undefined;
  private messagesClosed = false;
  private consumerClosed = false;
  private streamFailure: Error | undefined;

  constructor(
    private readonly consumer: RecordProvenanceConsumer,
    private readonly stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | null,
    private readonly plan: KafkaFetchPlan,
    private readonly cleanupDiagnostics: () => void,
    private readonly closeConsumer: () => Promise<void>,
    private priorConsumerClose: Promise<void> | undefined,
    private readonly prepareRecord?: KafkaConsumerInput["prepareRecord"],
    private readonly identity?: ReadIdentity,
    private readonly expectedLocator?: KafkaRecordLocator,
  ) {
    this.tracker = plan.continuous ? undefined : new KafkaReadTracker(plan);
    if (stream !== null) {
      // Construction continues on a later tick. Own errors before the async iterator exists;
      // iteration still reports them and close still joins the original stream's cleanup.
      const observe = (error: Error): void => {
        this.streamFailure ??= error;
      };
      stream.on("error", observe);
      stream.once("close", (): void => {
        stream.removeListener("error", observe);
      });
    }
  }

  coverage(): KafkaReadCoverage | undefined {
    return this.tracker?.snapshot();
  }

  subscribeCoverage(listener: (coverage: KafkaReadCoverage) => void): () => void {
    if (this.tracker !== undefined && !this.preparationController.signal.aborted)
      this.coverageListeners.add(listener);
    return (): void => {
      this.coverageListeners.delete(listener);
    };
  }

  private notifyCoverage(): void {
    if (this.tracker === undefined || this.coverageListeners.size === 0) return;
    const now = performance.now();
    if (
      this.lastCoverageNotification !== undefined &&
      now - this.lastCoverageNotification < COVERAGE_NOTIFICATION_INTERVAL_MS
    )
      return;
    const coverage = this.tracker.snapshot();
    if (coverage.scannedRecords === 0) return;
    this.lastCoverageNotification = now;
    // Run inside the owned iteration so observer failure follows its cleanup path.
    for (const listener of this.coverageListeners) listener(coverage);
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
    this.coverageListeners.clear();
    this.preparationController.abort();
    this.tracker?.finish("cancelled");
    this.closePromise ??= this.closeResources().catch((error: unknown) => {
      this.closePromise = undefined;
      throw error;
    });
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
      if (this.streamFailure !== undefined) throw this.streamFailure;
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
          this.notifyCoverage();
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
    if (this.priorConsumerClose !== undefined) {
      // Consumer.close(force) may already be closing this SDK stream. Calling stream.close
      // concurrently makes the SDK resolve its callbacks before its close event. Join first.
      try {
        await this.priorConsumerClose;
      } catch (error) {
        failures.push(error);
      } finally {
        this.priorConsumerClose = undefined;
      }
    }
    if (this.stream !== null && !this.messagesClosed) {
      try {
        await Promise.resolve(this.stream.close());
        this.messagesClosed = true;
      } catch (error) {
        failures.push(error);
      }
    }
    try {
      if (!this.consumerClosed) {
        await this.closeConsumer();
        this.consumerClosed = true;
      }
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
    const provenance = this.consumer.recordProvenance(message.leaderEpoch);
    const expected = this.expectedLocator;
    if (expected !== undefined) {
      if (provenance === undefined) throw new KafkaRecordLocatorError("unavailable");
      if (
        provenance.clusterId !== expected.clusterId ||
        provenance.topicId !== expected.topicId ||
        message.topic !== expected.topic
      )
        throw new KafkaRecordLocatorError("resource-replaced");
      if (message.partition !== expected.partition)
        throw new KafkaRecordLocatorError("unavailable");
      if (
        message.offset.toString() === expected.offset &&
        provenance.leaderEpoch !== expected.leaderEpoch
      )
        throw new KafkaRecordLocatorError("record-replaced");
    }
    return {
      ...(provenance === undefined ? {} : { provenance }),
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
    const consumer = new RecordProvenanceConsumer({
      ...platformaticClientOptions(input, `streamskope-consumer-${input.groupId}`),
      groupId: input.groupId,
      retries: 5,
      retryDelay: 200,
    });
    const cleanupFetchDiagnostics = fetchDiagnosticCleanup(consumer, input.onFetchSample);
    const cleanupDiagnostics = (): void => {
      cleanupFetchDiagnostics();
      consumer.releaseRecordProvenance();
    };
    let openedStream: PlatformaticMessageStream | undefined;
    let closing: Promise<void> | undefined;
    const close = (force = false): Promise<void> => {
      if (closing !== undefined) return closing;
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      const work = new Promise<void>((complete, fail) => {
        resolve = complete;
        reject = fail;
      });
      closing = work.catch((error: unknown) => {
        closing = undefined;
        throw error;
      });
      try {
        Promise.resolve(consumer.close(force)).then(resolve, reject);
      } catch (error) {
        reject(error);
      }
      return closing;
    };
    const abort = (): void => {
      void (openedStream?.close() ?? close(true)).catch(() => undefined);
    };
    input.signal?.addEventListener("abort", abort, { once: true });
    try {
      const captureIdentity = async (): Promise<ReadIdentity | undefined> => {
        try {
          const metadata = await consumer.metadata({
            topics: [input.request.topic],
            forceUpdate: true,
            autocreateTopics: false,
          });
          if (input.expectedLocator !== undefined && !metadata.topics.get(input.request.topic))
            throw new KafkaRecordLocatorError("topic-missing");
          return readIdentity(metadata, input.request.topic);
        } catch (error) {
          if (
            input.expectedLocator !== undefined &&
            connectionErrorChain(error).some(
              (item) =>
                item !== null &&
                typeof item === "object" &&
                (("apiId" in item && item.apiId === "UNKNOWN_TOPIC_OR_PARTITION") ||
                  // The SDK replaces this specific broker error with a cause-less UserError.
                  // Match its exact generated signature only, never arbitrary remote text.
                  ("code" in item &&
                    item.code === "PLT_KFK_USER" &&
                    "message" in item &&
                    item.message === `Unknown topic ${input.request.topic}.`)),
            )
          )
            throw new KafkaRecordLocatorError("topic-missing");
          throw error;
        }
      };
      const identity = await captureIdentity();
      const assertExpected = (actual: ReadIdentity | undefined): void => {
        const expected = input.expectedLocator;
        if (expected === undefined) return;
        if (actual === undefined) throw new KafkaRecordLocatorError("unavailable");
        try {
          parseKafkaRecordProvenance({
            clusterId: actual.clusterId,
            topicId: actual.topicId,
            leaderEpoch: 0,
          });
        } catch {
          throw new KafkaRecordLocatorError("unavailable");
        }
        if (actual.clusterId !== expected.clusterId || actual.topicId !== expected.topicId)
          throw new KafkaRecordLocatorError("resource-replaced");
        if (
          input.request.topic !== expected.topic ||
          input.request.mode === "tail" ||
          input.request.search?.partition !== expected.partition ||
          input.request.search.offsetExact !== expected.offset
        )
          throw new KafkaRecordLocatorError("unavailable");
      };
      assertExpected(identity);
      let lower: readonly bigint[] | undefined;
      let upper: readonly bigint[] | undefined;
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
            if (timestamp === -2n) lower = topicOffsets;
            if (timestamp === -1n) upper = topicOffsets;
            if (
              input.expectedLocator === undefined &&
              identity !== undefined &&
              topicOffsets.length !== identity.partitionCount
            )
              throw new KafkaReadCheckpointError("partitions-changed");
            return topicOffsets;
          },
        },
        input.request,
        Date.now(),
        input.checkpoint,
      );
      if (identity !== undefined) {
        const after = await captureIdentity();
        assertExpected(after);
        if (input.expectedLocator === undefined) assertReadIdentity(after, identity);
      }
      if (input.expectedLocator !== undefined) {
        const expected = input.expectedLocator;
        const low = lower?.[expected.partition];
        const high = upper?.[expected.partition];
        if (low === undefined || high === undefined)
          throw new KafkaRecordLocatorError("unavailable");
        if (low > BigInt(expected.offset)) throw new KafkaRecordLocatorError("expired");
        if (high <= BigInt(expected.offset)) throw new KafkaRecordLocatorError("unavailable");
      }
      consumer.bindRecordIdentity(
        identity === undefined ? undefined : { ...identity, topic: input.request.topic },
      );
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
          () => close(true),
          closing,
          input.prepareRecord,
          identity,
          input.expectedLocator,
        );
      }
      const stream = await new Promise<PlatformaticMessageStream>((resolve, reject) => {
        consumer.consume(
          {
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
          },
          (error, raw): void => {
            if (error !== null) {
              reject(error);
              return;
            }
            if (raw === undefined) {
              reject(new Error("Kafka did not return a message stream."));
              return;
            }
            // Capture the late stream synchronously. A cancelled consume may still return a
            // stream after its consumer closed, before Node executes the stream's _construct.
            openedStream = new PlatformaticMessageStream(
              consumer,
              raw,
              plan,
              cleanupDiagnostics,
              () => close(true),
              closing,
              input.prepareRecord,
              identity,
              input.expectedLocator,
            );
            if (input.signal?.aborted) void openedStream.close().catch(() => undefined);
            resolve(openedStream);
          },
        );
      });
      input.signal?.throwIfAborted();
      return stream;
    } catch (error) {
      cleanupDiagnostics();
      try {
        await (openedStream?.close() ?? close());
      } catch (cleanupError) {
        throw new KafkaReadOpenCleanupError(error, cleanupError, {
          close: () => openedStream?.close() ?? close(true),
        });
      }
      throw error;
    } finally {
      input.signal?.removeEventListener("abort", abort);
    }
  }
}
