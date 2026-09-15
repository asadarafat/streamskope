import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

import { Producer, type MessageToProduce, type ProduceResult } from "@platformatic/kafka";

import {
  KAFKA_LATENCY_LIMITS,
  parseKafkaLatencyProbeRequest,
  type KafkaLatencyProbeIssue,
} from "../contracts";
import type { KafkaLatencyProbeMeasurement } from "../application";

import { KafkaEngineFailure, mapKafkaAdminFailure, normalizeKafkaError } from "./failure";
import { probeKafkaNetwork, type KafkaLatencyNetworkResult } from "./latency-network";
import { PlatformaticConsumerFactory } from "./platformatic-consumer";
import { platformaticClientOptions } from "./platformatic-options";
import type {
  KafkaConsumerFactory,
  KafkaConsumerInput,
  KafkaLatencyProbeInput,
  KafkaLatencyProbePort,
  KafkaRawMessage,
  KafkaRawMessageStream,
} from "./types";

const RUN_HEADER = "x-streamskope-latency-run";
const SAMPLE_HEADER = "x-streamskope-latency-sample";
const SOURCE_HEADER = "x-streamskope-source";

export interface PlatformaticLatencyProducer {
  close(force?: boolean): Promise<void>;
  send(options: {
    readonly acks: -1 | 0 | 1;
    readonly messages: MessageToProduce<Buffer, Buffer, Buffer, Buffer>[];
  }): Promise<ProduceResult>;
}

export interface PlatformaticLatencyProbeOptions {
  readonly createConsumer?: KafkaConsumerFactory["open"];
  readonly createProducer?: (input: KafkaLatencyProbeInput) => PlatformaticLatencyProducer;
  readonly createSampleId?: () => string;
  readonly monotonicNow?: () => number;
  readonly probeNetwork?: (
    input: KafkaLatencyProbeInput,
    signal: AbortSignal,
  ) => Promise<KafkaLatencyNetworkResult>;
}

class ProbeAborted extends Error {
  constructor(readonly timedOut: boolean) {
    super(timedOut ? "The latency probe timed out." : "The latency probe was cancelled.");
    this.name = "ProbeAborted";
  }
}

function header(message: KafkaRawMessage, name: string): string | undefined {
  for (const [key, value] of message.headers) {
    if (key.toString("utf8") === name) {
      return value.toString("utf8");
    }
  }
  return undefined;
}

function abortable<Value>(
  operation: Promise<Value>,
  signal: AbortSignal,
  timedOut: () => boolean,
): Promise<Value> {
  if (signal.aborted) {
    return Promise.reject(new ProbeAborted(timedOut()));
  }
  return new Promise<Value>((resolve, reject) => {
    const onAbort = (): void => {
      reject(new ProbeAborted(timedOut()));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(normalizeKafkaError(error));
      },
    );
  });
}

function settleBeforeAbort(operation: Promise<unknown>, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const finish = (): void => {
      signal.removeEventListener("abort", finish);
      resolve();
    };
    operation.then(finish, finish);
    if (signal.aborted) {
      queueMicrotask(finish);
    } else {
      signal.addEventListener("abort", finish, { once: true });
    }
  });
}

function cancelledFailure(target: string, cleanupCause?: unknown): KafkaEngineFailure {
  return new KafkaEngineFailure({
    cleanupCause,
    code: "CANCELLED",
    recovery:
      cleanupCause === undefined
        ? "Run another bounded latency probe when the connection is stable."
        : "Reconnect to close remaining probe resources before running another latency probe.",
    retryable: true,
    stage: "kafka",
    summary:
      cleanupCause === undefined
        ? "The Kafka latency probe was cancelled."
        : "The Kafka latency probe was cancelled, but owned resources did not close cleanly.",
    target,
  });
}

function timeoutFailure(target: string, cleanupCause?: unknown): KafkaEngineFailure {
  return new KafkaEngineFailure({
    cleanupCause,
    code: "TIMEOUT",
    recovery:
      cleanupCause === undefined
        ? "Increase the bounded probe timeout or inspect the Kafka write and consumer path."
        : "Reconnect to close remaining probe resources, then inspect the Kafka write and consumer path.",
    retryable: true,
    stage: "kafka",
    summary:
      cleanupCause === undefined
        ? "The Kafka latency probe timed out before producing valid evidence."
        : "The Kafka latency probe timed out and owned resources did not close cleanly.",
    target,
  });
}

function partialProduceFailure(
  error: unknown,
  target: string,
  produced: number,
): KafkaEngineFailure {
  const mapped = mapKafkaAdminFailure(error, target);
  return new KafkaEngineFailure({
    cause: error,
    code: mapped.code,
    recovery: `${mapped.recovery} ${String(
      produced,
    )} synthetic probe record(s) may remain in the topic.`,
    retryable: mapped.retryable,
    stage: mapped.stage,
    summary: `Kafka latency production failed after ${String(
      produced,
    )} completed call(s); published probe records may remain in the topic.`,
    target,
  });
}

function failureWithCleanup(
  error: unknown,
  target: string,
  cleanupCause: unknown,
): KafkaEngineFailure {
  const failure = mapKafkaAdminFailure(error, target);
  return new KafkaEngineFailure({
    cause: failure.cause,
    cleanupCause,
    code: failure.code,
    recovery: failure.recovery,
    retryable: failure.retryable,
    stage: failure.stage,
    summary: failure.message,
    ...(failure.target === undefined ? {} : { target: failure.target }),
  });
}

export class PlatformaticLatencyProbe implements KafkaLatencyProbePort {
  private readonly createConsumer;
  private readonly createProducer;
  private readonly createSampleId;
  private readonly monotonicNow;
  private readonly networkProbe;

  constructor(options: PlatformaticLatencyProbeOptions = {}) {
    const consumerFactory = new PlatformaticConsumerFactory();
    this.createConsumer = options.createConsumer ?? consumerFactory.open.bind(consumerFactory);
    this.createProducer =
      options.createProducer ??
      ((input: KafkaLatencyProbeInput): PlatformaticLatencyProducer =>
        new Producer<Buffer, Buffer, Buffer, Buffer>({
          ...platformaticClientOptions(input, `streamskope-latency-producer-${input.runId}`),
          autocreateTopics: false,
        }));
    this.createSampleId = options.createSampleId ?? randomUUID;
    this.monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.networkProbe = options.probeNetwork ?? probeKafkaNetwork;
  }

  async run(
    input: KafkaLatencyProbeInput,
    externalSignal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement> {
    const request = parseKafkaLatencyProbeRequest(input.request, "latency");
    const timeoutController = new AbortController();
    const lifecycleController = new AbortController();
    const timeout = setTimeout(() => {
      timeoutController.abort();
    }, request.timeoutMs);
    timeout.unref();
    const signal = AbortSignal.any([
      externalSignal,
      timeoutController.signal,
      lifecycleController.signal,
    ]);
    const timedOut = (): boolean => timeoutController.signal.aborted && !externalSignal.aborted;
    const target = `${input.brokers.join(", ")} / ${request.topic}`;
    let producer: PlatformaticLatencyProducer | undefined;
    let stream: KafkaRawMessageStream | undefined;
    const producerDurationsMs: number[] = [];
    const endToEndDurationsMs: number[] = [];
    const observedSampleIds: string[] = [];
    const fetchSamples: KafkaLatencyProbeMeasurement["fetchSamples"][number][] = [];
    const fetchBrokerIds = new Set<number>();
    const sentAt = new Map<string, number>();
    const expected = new Set<string>();
    const issues: KafkaLatencyProbeIssue[] = [];
    let observationFailure: unknown;
    let resolveObserved!: () => void;
    const allObserved = new Promise<void>((resolve) => {
      resolveObserved = resolve;
    });
    let observationPump: Promise<void> | undefined;
    const network = Promise.resolve()
      .then(() => this.networkProbe(input, signal))
      .catch((): KafkaLatencyNetworkResult => ({
        endpoint: input.brokers[0] ?? "unavailable",
        issues: [
          {
            recovery: "Verify the primary broker endpoint and TCP network path.",
            stage: "tcp",
            summary: "TCP connection latency is unavailable.",
          },
          {
            recovery: "Verify the broker certificate, hostname, CA trust, and TLS network path.",
            stage: "tls",
            summary: "TLS handshake latency is unavailable.",
          },
        ],
        tcpConnectMs: null,
        tlsHandshakeMs: null,
      }));
    let terminalError: unknown;
    try {
      producer = this.createProducer(input);
      const consumerInput: KafkaConsumerInput = {
        ...input,
        groupId: `streamskope-latency-${input.runId}`,
        onFetchSample: (sample): void => {
          if (
            fetchSamples.length >= KAFKA_LATENCY_LIMITS.maxMetricSamples ||
            (!fetchBrokerIds.has(sample.nodeId) &&
              fetchBrokerIds.size >= KAFKA_LATENCY_LIMITS.brokerMetrics)
          ) {
            return;
          }
          fetchBrokerIds.add(sample.nodeId);
          fetchSamples.push(sample);
        },
        request: {
          maxMessages: request.messageCount,
          mode: "tail",
          topic: request.topic,
        },
      };
      stream = await this.createConsumer(consumerInput);
      if (signal.aborted) {
        throw new ProbeAborted(timedOut());
      }
      observationPump = this.observe(
        stream,
        input.runId,
        expected,
        sentAt,
        observedSampleIds,
        endToEndDurationsMs,
        request.messageCount,
        resolveObserved,
      ).catch((error: unknown) => {
        observationFailure = error;
        resolveObserved();
      });

      for (let index = 0; index < request.messageCount; index += 1) {
        const sampleId = this.createSampleId();
        expected.add(sampleId);
        const sent = this.monotonicNow();
        sentAt.set(sampleId, sent);
        const payload = Buffer.from(
          JSON.stringify({
            kind: "streamskope-latency-probe",
            runId: input.runId,
            sampleId,
          }),
          "utf8",
        );
        const headers = new Map<Buffer, Buffer>([
          [Buffer.from(RUN_HEADER, "utf8"), Buffer.from(input.runId, "utf8")],
          [Buffer.from(SAMPLE_HEADER, "utf8"), Buffer.from(sampleId, "utf8")],
          [Buffer.from(SOURCE_HEADER, "utf8"), Buffer.from("StreamSkope", "utf8")],
        ]);
        try {
          await abortable(
            producer.send({
              acks: request.acknowledgements,
              messages: [
                {
                  headers,
                  key: Buffer.from(sampleId, "utf8"),
                  timestamp: BigInt(Date.now()),
                  topic: request.topic,
                  value: payload,
                },
              ],
            }),
            signal,
            timedOut,
          );
          producerDurationsMs.push(this.monotonicNow() - sent);
        } catch (error) {
          if (error instanceof ProbeAborted) {
            throw error;
          }
          throw partialProduceFailure(error, target, producerDurationsMs.length);
        }
      }

      if (observedSampleIds.length < request.messageCount) {
        await abortable(allObserved, signal, timedOut);
      }
      if (observationFailure !== undefined) {
        throw mapKafkaAdminFailure(observationFailure, target);
      }
    } catch (error) {
      terminalError = error;
    } finally {
      const ownedStream = stream;
      const ownedProducer = producer;
      const cleanup = await Promise.allSettled([
        ...(ownedStream === undefined ? [] : [Promise.resolve().then(() => ownedStream.close())]),
        ...(ownedProducer === undefined
          ? []
          : [Promise.resolve().then(() => ownedProducer.close(true))]),
      ]);
      if (observationPump !== undefined) {
        await settleBeforeAbort(observationPump, signal);
      }
      const cleanupFailures = cleanup.flatMap((result) =>
        result.status === "rejected" ? [result.reason as unknown] : [],
      );
      if (cleanupFailures.length > 0) {
        if (terminalError !== undefined) {
          const failure =
            terminalError instanceof ProbeAborted
              ? terminalError.timedOut
                ? timeoutFailure(target, cleanupFailures[0])
                : cancelledFailure(target, cleanupFailures[0])
              : failureWithCleanup(terminalError, target, cleanupFailures[0]);
          terminalError = failure;
        } else {
          issues.push({
            recovery: "Reconnect before running another latency probe.",
            stage: "cleanup",
            summary: "One or more probe resources did not close cleanly.",
          });
        }
      }
    }

    try {
      if (externalSignal.aborted) {
        throw terminalError instanceof KafkaEngineFailure
          ? terminalError
          : cancelledFailure(target);
      }
      if (terminalError instanceof ProbeAborted && terminalError.timedOut) {
        if (producerDurationsMs.length === 0) {
          throw timeoutFailure(target);
        }
        issues.push({
          recovery: "Increase the bounded timeout or inspect the topic consumer path.",
          stage: "end-to-end",
          summary: `${String(
            request.messageCount - observedSampleIds.length,
          )} probe record(s) were not observed before timeout.`,
        });
      } else if (terminalError !== undefined) {
        throw normalizeKafkaError(terminalError);
      }
      const networkResult = await network;
      if (externalSignal.aborted) {
        throw cancelledFailure(target);
      }
      issues.push(...networkResult.issues);
      return {
        endToEndDurationsMs,
        fetchSamples,
        issues,
        network: {
          endpoint: networkResult.endpoint,
          tcpConnectMs: networkResult.tcpConnectMs,
          tlsHandshakeMs: networkResult.tlsHandshakeMs,
        },
        observedSampleIds,
        producerDurationsMs,
      };
    } finally {
      clearTimeout(timeout);
      lifecycleController.abort();
      await network;
    }
  }

  private async observe(
    stream: KafkaRawMessageStream,
    runId: string,
    expected: ReadonlySet<string>,
    sentAt: ReadonlyMap<string, number>,
    observedSampleIds: string[],
    endToEndDurationsMs: number[],
    targetCount: number,
    resolveObserved: () => void,
  ): Promise<void> {
    const observed = new Set<string>();
    for await (const message of stream) {
      if (header(message, RUN_HEADER) !== runId) {
        continue;
      }
      const sampleId = header(message, SAMPLE_HEADER);
      if (
        sampleId === undefined ||
        !expected.has(sampleId) ||
        observed.has(sampleId) ||
        message.key?.toString("utf8") !== sampleId
      ) {
        continue;
      }
      const started = sentAt.get(sampleId);
      if (started === undefined) {
        continue;
      }
      observed.add(sampleId);
      observedSampleIds.push(sampleId);
      endToEndDurationsMs.push(this.monotonicNow() - started);
      if (observed.size === targetCount) {
        resolveObserved();
      }
    }
  }
}
