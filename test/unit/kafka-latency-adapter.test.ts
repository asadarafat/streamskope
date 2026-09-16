import { describe, expect, it, vi } from "vitest";

import { KAFKA_LATENCY_LIMITS } from "../../src/features/kafka/contracts";
import type {
  KafkaConsumerInput,
  KafkaLatencyNetworkResult,
  KafkaRawMessage,
  KafkaRawMessageStream,
  PlatformaticLatencyProducer,
} from "../../src/features/kafka/engine";
import { PlatformaticLatencyProbe } from "../../src/features/kafka/engine";

class RawQueue implements KafkaRawMessageStream {
  private closed = false;
  private readonly messages: KafkaRawMessage[] = [];
  private waiter: (() => void) | undefined;
  readonly close = vi.fn<KafkaRawMessageStream["close"]>((): Promise<void> => {
    this.closed = true;
    this.waiter?.();
    return Promise.resolve();
  });

  push(message: KafkaRawMessage): void {
    this.messages.push(message);
    this.waiter?.();
    this.waiter = undefined;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<KafkaRawMessage> {
    while (!this.closed) {
      const message = this.messages.shift();
      if (message !== undefined) {
        yield message;
        continue;
      }
      await new Promise<void>((resolve) => {
        this.waiter = resolve;
      });
    }
  }
}

function sampleIdSequence(...values: string[]): () => string {
  return (): string => {
    const value = values.shift();
    if (value === undefined) {
      throw new Error("The sample-ID fixture was exhausted.");
    }
    return value;
  };
}

function incrementingClock(): () => number {
  let value = 0;
  return (): number => ++value;
}

function successfulNetworkProbe(): Promise<KafkaLatencyNetworkResult> {
  return Promise.resolve({
    endpoint: "127.0.0.1:19093",
    issues: [],
    tcpConnectMs: 1,
    tlsHandshakeMs: 2,
  });
}

const request = {
  acknowledgements: -1,
  messageCount: 2,
  timeoutMs: 10_000,
  topic: "orders.events",
} as const;

const client = {
  brokers: ["127.0.0.1:19093"],
  caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
  operationTimeoutMs: 5_000,
} as const;

describe("Platformatic Kafka latency adapter", () => {
  it("forwards exact acknowledgements and measures only exact probe records", async () => {
    const queue = new RawQueue();
    let consumerInput: KafkaConsumerInput | undefined;
    const sent: Array<Parameters<PlatformaticLatencyProducer["send"]>[0]> = [];
    let offset = 0n;
    const close = vi.fn<PlatformaticLatencyProducer["close"]>(() => Promise.resolve());
    const send = vi.fn<PlatformaticLatencyProducer["send"]>((options) => {
      sent.push(options);
      const message = options.messages[0]!;
      queue.push({
        headers: new Map([[Buffer.from("unrelated", "utf8"), Buffer.from("value", "utf8")]]),
        key: Buffer.from("unrelated", "utf8"),
        offset: offset++,
        partition: 0,
        timestamp: BigInt(Date.now()),
        topic: request.topic,
        value: Buffer.from("ignore", "utf8"),
      });
      queue.push({
        headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
        ...(message.key === undefined ? {} : { key: message.key }),
        offset: offset++,
        partition: 0,
        timestamp: message.timestamp ?? BigInt(Date.now()),
        topic: message.topic,
        ...(message.value === undefined ? {} : { value: message.value }),
      });
      consumerInput?.onFetchSample?.({
        broker: "kafka-1:9093",
        durationMs: 3,
        nodeId: 1,
      });
      return Promise.resolve({});
    });
    const producer: PlatformaticLatencyProducer = {
      close,
      send,
    };
    const times = [0, 1, 4, 10, 11, 16, 20, 28];
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (input): Promise<KafkaRawMessageStream> => {
        consumerInput = input;
        return Promise.resolve(queue);
      },
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: sampleIdSequence("sample-1", "sample-2"),
      monotonicNow: (): number => times.shift() ?? 30,
      probeNetwork: successfulNetworkProbe,
    });

    const result = await probe.run(
      { ...client, request, runId: "run-123" },
      new AbortController().signal,
    );

    expect(sent).toHaveLength(2);
    expect(sent.every((call) => call.acks === -1)).toBe(true);
    expect(sent.every((call) => call.messages.length === 1)).toBe(true);
    expect(sent.map((call) => call.messages[0]?.key?.toString("utf8"))).toEqual([
      "sample-1",
      "sample-2",
    ]);
    expect(
      sent.map((call) =>
        [...(call.messages[0]?.headers as Map<Buffer, Buffer>)]
          .find(([key]) => key.toString("utf8") === "x-streamskope-latency-run")?.[1]
          .toString("utf8"),
      ),
    ).toEqual(["run-123", "run-123"]);
    expect(result.observedSampleIds).toEqual(["sample-1", "sample-2"]);
    expect(result.endToEndDurationsMs).toHaveLength(2);
    expect(result.producerDurationsMs).toHaveLength(2);
    expect(result.fetchSamples).toEqual([
      { broker: "kafka-1:9093", durationMs: 3, nodeId: 1 },
      { broker: "kafka-1:9093", durationMs: 3, nodeId: 1 },
    ]);
    expect(queue.close).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledOnce();
  });

  it("returns explicit partial network evidence without erasing Kafka samples", async () => {
    const queue = new RawQueue();
    let input: KafkaConsumerInput | undefined;
    const producer: PlatformaticLatencyProducer = {
      close: () => Promise.resolve(),
      send: (options) => {
        const message = options.messages[0]!;
        queue.push({
          headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
          ...(message.key === undefined ? {} : { key: message.key }),
          offset: 0n,
          partition: 0,
          timestamp: message.timestamp ?? 0n,
          topic: message.topic,
          ...(message.value === undefined ? {} : { value: message.value }),
        });
        input?.onFetchSample?.({ broker: "kafka-1:9093", durationMs: 5, nodeId: 1 });
        return Promise.resolve({});
      },
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (consumerInput): Promise<KafkaRawMessageStream> => {
        input = consumerInput;
        return Promise.resolve(queue);
      },
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: (): Promise<KafkaLatencyNetworkResult> =>
        Promise.resolve({
          endpoint: "127.0.0.1:19093",
          issues: [
            {
              recovery: "Verify TLS trust.",
              stage: "tls",
              summary: "TLS handshake evidence is unavailable.",
            },
          ],
          tcpConnectMs: 1,
          tlsHandshakeMs: null,
        }),
    });

    const result = await probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-partial",
      },
      new AbortController().signal,
    );

    expect(result.network.tlsHandshakeMs).toBeNull();
    expect(result.issues).toEqual([expect.objectContaining({ stage: "tls" })]);
    expect(result.producerDurationsMs).toHaveLength(1);
    expect(result.endToEndDurationsMs).toHaveLength(1);
  });

  it("bounds retained fetch diagnostics by sample and broker limits", async () => {
    const queue = new RawQueue();
    let input: KafkaConsumerInput | undefined;
    const producer: PlatformaticLatencyProducer = {
      close: () => Promise.resolve(),
      send: (options) => {
        for (let nodeId = 0; nodeId <= KAFKA_LATENCY_LIMITS.brokerMetrics; nodeId += 1) {
          input?.onFetchSample?.({
            broker: `kafka-${String(nodeId)}:9093`,
            durationMs: nodeId + 1,
            nodeId,
          });
        }
        for (let index = 0; index < KAFKA_LATENCY_LIMITS.maxMetricSamples; index += 1) {
          input?.onFetchSample?.({
            broker: "kafka-0:9093",
            durationMs: index + 1,
            nodeId: 0,
          });
        }
        const message = options.messages[0]!;
        queue.push({
          headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
          ...(message.key === undefined ? {} : { key: message.key }),
          offset: 0n,
          partition: 0,
          timestamp: message.timestamp ?? 0n,
          topic: message.topic,
          ...(message.value === undefined ? {} : { value: message.value }),
        });
        return Promise.resolve({});
      },
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (consumerInput): Promise<KafkaRawMessageStream> => {
        input = consumerInput;
        return Promise.resolve(queue);
      },
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: successfulNetworkProbe,
    });

    const result = await probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-bounded-fetch-diagnostics",
      },
      new AbortController().signal,
    );

    expect(result.fetchSamples).toHaveLength(KAFKA_LATENCY_LIMITS.maxMetricSamples);
    expect(new Set(result.fetchSamples.map((sample) => sample.nodeId))).toHaveLength(
      KAFKA_LATENCY_LIMITS.brokerMetrics,
    );
  });

  it("waits for the configured sample count after an early matching record", async () => {
    const queue = new RawQueue();
    let sendCount = 0;
    const producer: PlatformaticLatencyProducer = {
      close: () => Promise.resolve(),
      send: async (options) => {
        sendCount += 1;
        const message = options.messages[0]!;
        const rawMessage: KafkaRawMessage = {
          headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
          ...(message.key === undefined ? {} : { key: message.key }),
          offset: BigInt(sendCount),
          partition: 0,
          timestamp: message.timestamp ?? 0n,
          topic: message.topic,
          ...(message.value === undefined ? {} : { value: message.value }),
        };
        if (sendCount === 1) {
          queue.push(rawMessage);
          await new Promise<void>((resolve) => {
            setTimeout(resolve, 0);
          });
        } else {
          setTimeout(() => {
            queue.push(rawMessage);
          }, 20);
        }
        return {};
      },
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: sampleIdSequence("sample-1", "sample-2"),
      monotonicNow: incrementingClock(),
      probeNetwork: successfulNetworkProbe,
    });

    const result = await probe.run(
      {
        ...client,
        request: { ...request, timeoutMs: 1_000 },
        runId: "run-delayed-observation",
      },
      new AbortController().signal,
    );

    expect(result.observedSampleIds).toEqual(["sample-1", "sample-2"]);
    expect(result.endToEndDurationsMs).toHaveLength(2);
  });

  it("aborts and awaits network work after a fatal Kafka failure", async () => {
    const queue = new RawQueue();
    const close = vi.fn<PlatformaticLatencyProducer["close"]>(() => Promise.resolve());
    const producer: PlatformaticLatencyProducer = {
      close,
      send: () => Promise.reject(new Error("Kafka write rejected.")),
    };
    let networkSignal: AbortSignal | undefined;
    let networkSettled = false;
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: (): number => 1,
      probeNetwork: (_input, signal): Promise<KafkaLatencyNetworkResult> =>
        new Promise((resolve) => {
          networkSignal = signal;
          const complete = (): void => {
            networkSettled = true;
            resolve({
              endpoint: "127.0.0.1:19093",
              issues: [],
              tcpConnectMs: null,
              tlsHandshakeMs: null,
            });
          };
          if (signal.aborted) {
            complete();
          } else {
            signal.addEventListener("abort", complete, { once: true });
          }
        }),
    });

    await expect(
      probe.run(
        {
          ...client,
          request: { ...request, messageCount: 1 },
          runId: "run-fatal",
        },
        new AbortController().signal,
      ),
    ).rejects.toThrow("Kafka latency production failed");
    expect(networkSignal?.aborted).toBe(true);
    expect(networkSettled).toBe(true);
    expect(queue.close).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledOnce();
  });

  it("preserves owned-resource cleanup failure with the primary Kafka failure", async () => {
    const queue = new RawQueue();
    const cleanupFailure = new Error("Producer cleanup failed.");
    const producer: PlatformaticLatencyProducer = {
      close: () => Promise.reject(cleanupFailure),
      send: () => Promise.reject(new Error("Kafka write rejected.")),
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: (): number => 1,
      probeNetwork: successfulNetworkProbe,
    });

    let failure: unknown;
    try {
      await probe.run(
        {
          ...client,
          request: { ...request, messageCount: 1 },
          runId: "run-primary-and-cleanup-failure",
        },
        new AbortController().signal,
      );
    } catch (error) {
      failure = error;
    }

    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toContain("Kafka latency production failed");
    expect(failure).toMatchObject({ cleanupCause: cleanupFailure });
    expect(queue.close).toHaveBeenCalledOnce();
  });

  it("aborts parallel network work when producer construction fails synchronously", async () => {
    let networkSignal: AbortSignal | undefined;
    let networkSettled = false;
    const probe = new PlatformaticLatencyProbe({
      createProducer: (): never => {
        throw new Error("Producer construction failed.");
      },
      probeNetwork: (_input, signal): Promise<KafkaLatencyNetworkResult> =>
        new Promise((resolve) => {
          networkSignal = signal;
          signal.addEventListener(
            "abort",
            () => {
              networkSettled = true;
              resolve({
                endpoint: "127.0.0.1:19093",
                issues: [],
                tcpConnectMs: null,
                tlsHandshakeMs: null,
              });
            },
            { once: true },
          );
        }),
    });

    await expect(
      probe.run(
        {
          ...client,
          request: { ...request, messageCount: 1 },
          runId: "run-construction-failure",
        },
        new AbortController().signal,
      ),
    ).rejects.toThrow("Producer construction failed.");
    expect(networkSignal?.aborted).toBe(true);
    expect(networkSettled).toBe(true);
  });

  it("converts a synchronous network diagnostic failure into partial evidence", async () => {
    const queue = new RawQueue();
    const producer: PlatformaticLatencyProducer = {
      close: () => Promise.resolve(),
      send: (options) => {
        const message = options.messages[0]!;
        queue.push({
          headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
          ...(message.key === undefined ? {} : { key: message.key }),
          offset: 0n,
          partition: 0,
          timestamp: message.timestamp ?? 0n,
          topic: message.topic,
          ...(message.value === undefined ? {} : { value: message.value }),
        });
        return Promise.resolve({});
      },
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: (): never => {
        throw new Error("Network diagnostics failed.");
      },
    });

    const result = await probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-network-failure",
      },
      new AbortController().signal,
    );

    expect(result.network).toMatchObject({ tcpConnectMs: null, tlsHandshakeMs: null });
    expect(result.issues.map((issue) => issue.stage)).toEqual(["tcp", "tls"]);
    expect(result.observedSampleIds).toEqual(["sample-1"]);
  });

  it("reports a synchronous owned-resource close failure as partial evidence", async () => {
    const queue = new RawQueue();
    const producer: PlatformaticLatencyProducer = {
      close: (): never => {
        throw new Error("Producer close failed.");
      },
      send: (options) => {
        const message = options.messages[0]!;
        queue.push({
          headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
          ...(message.key === undefined ? {} : { key: message.key }),
          offset: 0n,
          partition: 0,
          timestamp: message.timestamp ?? 0n,
          topic: message.topic,
          ...(message.value === undefined ? {} : { value: message.value }),
        });
        return Promise.resolve({});
      },
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: successfulNetworkProbe,
    });

    const result = await probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-cleanup-failure",
      },
      new AbortController().signal,
    );

    expect(result.issues).toEqual([expect.objectContaining({ stage: "cleanup" })]);
    expect(result.observedSampleIds).toEqual(["sample-1"]);
    expect(queue.close).toHaveBeenCalledOnce();
  });

  it("returns partial evidence when observation reaches the bounded timeout", async () => {
    const queue = new RawQueue();
    const close = vi.fn<PlatformaticLatencyProducer["close"]>(() => Promise.resolve());
    const producer: PlatformaticLatencyProducer = {
      close,
      send: () => Promise.resolve({}),
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: successfulNetworkProbe,
    });

    const result = await probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1, timeoutMs: 1_000 },
        runId: "run-timeout",
      },
      new AbortController().signal,
    );

    expect(result.producerDurationsMs).toHaveLength(1);
    expect(result.observedSampleIds).toEqual([]);
    expect(result.issues).toEqual([expect.objectContaining({ stage: "end-to-end" })]);
    expect(queue.close).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledOnce();
  });

  it("aborts blocked I/O and closes every owned resource", async () => {
    const queue = new RawQueue();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const close = vi.fn<PlatformaticLatencyProducer["close"]>(() => {
      release();
      return Promise.resolve();
    });
    const send = vi.fn<PlatformaticLatencyProducer["send"]>(async () => {
      await blocked;
      return {};
    });
    const producer: PlatformaticLatencyProducer = {
      close,
      send,
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: (): number => 1,
      probeNetwork: successfulNetworkProbe,
    });
    const controller = new AbortController();
    const running = probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-cancel",
      },
      controller.signal,
    );
    controller.abort();

    await expect(running).rejects.toMatchObject({ code: "CANCELLED" });
    expect(queue.close).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledOnce();
  });

  it("does not wait forever when a failed stream close leaves iteration blocked", async () => {
    const cleanupFailure = new Error("Consumer stream close failed.");
    const stream: KafkaRawMessageStream = {
      close: () => Promise.reject(cleanupFailure),
      [Symbol.asyncIterator]: (): AsyncIterator<KafkaRawMessage> => ({
        next: () => new Promise<IteratorResult<KafkaRawMessage>>(() => undefined),
      }),
    };
    const send = vi.fn<PlatformaticLatencyProducer["send"]>(() => Promise.resolve({}));
    const producer: PlatformaticLatencyProducer = {
      close: () => Promise.resolve(),
      send,
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(stream),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: successfulNetworkProbe,
    });
    const controller = new AbortController();
    const running = probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-uncloseable-stream",
      },
      controller.signal,
    );
    await vi.waitFor(() => {
      expect(send).toHaveBeenCalledOnce();
    });
    controller.abort();
    let hangTimer: ReturnType<typeof setTimeout> | undefined;
    const outcome = await Promise.race([
      running.then(
        () => ({ kind: "resolved" as const }),
        (error: unknown) => ({ error, kind: "rejected" as const }),
      ),
      new Promise<{ readonly kind: "hung" }>((resolve) => {
        hangTimer = setTimeout(() => {
          resolve({ kind: "hung" });
        }, 100);
      }),
    ]);
    if (hangTimer !== undefined) {
      clearTimeout(hangTimer);
    }

    expect(outcome).toMatchObject({
      error: { cleanupCause: cleanupFailure, code: "CANCELLED" },
      kind: "rejected",
    });
  });

  it("does not return late partial evidence when cancellation arrives during network work", async () => {
    const queue = new RawQueue();
    let kafkaClosed!: () => void;
    const closed = new Promise<void>((resolve) => {
      kafkaClosed = resolve;
    });
    const producer: PlatformaticLatencyProducer = {
      close: () => {
        kafkaClosed();
        return Promise.resolve();
      },
      send: (options) => {
        const message = options.messages[0]!;
        queue.push({
          headers: (message.headers ?? new Map()) as ReadonlyMap<Buffer, Buffer>,
          ...(message.key === undefined ? {} : { key: message.key }),
          offset: 0n,
          partition: 0,
          timestamp: message.timestamp ?? 0n,
          topic: message.topic,
          ...(message.value === undefined ? {} : { value: message.value }),
        });
        return Promise.resolve({});
      },
    };
    const probe = new PlatformaticLatencyProbe({
      createConsumer: (): Promise<KafkaRawMessageStream> => Promise.resolve(queue),
      createProducer: (): PlatformaticLatencyProducer => producer,
      createSampleId: (): string => "sample-1",
      monotonicNow: incrementingClock(),
      probeNetwork: (_input, signal): Promise<KafkaLatencyNetworkResult> =>
        new Promise((resolve) => {
          signal.addEventListener(
            "abort",
            () => {
              resolve({
                endpoint: "127.0.0.1:19093",
                issues: [],
                tcpConnectMs: null,
                tlsHandshakeMs: null,
              });
            },
            { once: true },
          );
        }),
    });
    const controller = new AbortController();
    const running = probe.run(
      {
        ...client,
        request: { ...request, messageCount: 1 },
        runId: "run-cancelled-during-network",
      },
      controller.signal,
    );
    await closed;
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    controller.abort();

    await expect(running).rejects.toMatchObject({ code: "CANCELLED" });
    expect(queue.close).toHaveBeenCalledOnce();
  });
});
