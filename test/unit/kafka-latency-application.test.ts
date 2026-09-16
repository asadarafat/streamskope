import { describe, expect, it, vi } from "vitest";

import type {
  KafkaLatencyProbeMeasurement,
  KafkaLatencyProbeSessionPort,
} from "../../src/features/kafka/application";
import {
  KafkaLatencyProbeService,
  KafkaLatencyProbeValidationError,
  NoActiveKafkaConnectionError,
} from "../../src/features/kafka/application";

const request = {
  acknowledgements: -1,
  messageCount: 2,
  timeoutMs: 10_000,
  topic: "orders.events",
} as const;

const completeMeasurement: KafkaLatencyProbeMeasurement = {
  endToEndDurationsMs: [8, 12],
  fetchSamples: [
    { broker: "kafka-1:9093", durationMs: 3, nodeId: 1 },
    { broker: "kafka-1:9093", durationMs: 5, nodeId: 1 },
  ],
  issues: [],
  network: {
    endpoint: "127.0.0.1:19093",
    tcpConnectMs: 1,
    tlsHandshakeMs: 2,
  },
  observedSampleIds: ["sample-1", "sample-2"],
  producerDurationsMs: [4, 6],
};

interface Deferred<Value> {
  readonly promise: Promise<Value>;
  reject(error: unknown): void;
  resolve(value: Value): void;
}

function deferred<Value>(): Deferred<Value> {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

class FakeSession implements KafkaLatencyProbeSessionPort {
  context: ReturnType<KafkaLatencyProbeSessionPort["activeConnectionContext"]> = {
    connectionBrokers: ["127.0.0.1:19093"],
    connectionName: "local-aio",
    connectionTarget: "127.0.0.1:19093",
  };
  readonly run = vi.fn<KafkaLatencyProbeSessionPort["runLatencyProbe"]>(() =>
    Promise.resolve(completeMeasurement),
  );

  activeConnectionContext(): ReturnType<KafkaLatencyProbeSessionPort["activeConnectionContext"]> {
    return this.context;
  }

  runLatencyProbe(
    ...parameters: Parameters<KafkaLatencyProbeSessionPort["runLatencyProbe"]>
  ): Promise<KafkaLatencyProbeMeasurement> {
    return this.run(...parameters);
  }
}

function service(session: FakeSession): KafkaLatencyProbeService {
  const dates = [new Date("2026-07-25T16:00:00.000Z"), new Date("2026-07-25T16:00:01.000Z")];
  return new KafkaLatencyProbeService(session, {
    createRunId: () => "run-123",
    now: () => dates.shift() ?? new Date("2026-07-25T16:00:01.000Z"),
  });
}

describe("Kafka latency application service", () => {
  it("assembles deterministic ready evidence and nearest-rank p95", async () => {
    const session = new FakeSession();
    session.run.mockResolvedValue({
      ...completeMeasurement,
      endToEndDurationsMs: [1, 2],
      producerDurationsMs: [1, 2],
    });
    const result = await service(session).start(request);

    expect(session.run).toHaveBeenCalledWith(request, "run-123", expect.any(AbortSignal));
    expect(result.state).toBe("ready");
    expect(result.evidence).toMatchObject({
      connection: { endpoint: "127.0.0.1:19093", name: "local-aio" },
      endToEnd: { averageMs: 1.5, p95Ms: 2, samples: 2 },
      fetch: {
        perBroker: [
          {
            broker: "kafka-1:9093",
            nodeId: 1,
            summary: { averageMs: 4, p95Ms: 5, samples: 2 },
          },
        ],
        summary: { averageMs: 4, p95Ms: 5, samples: 2 },
      },
      observedMessages: 2,
      producer: {
        semantics: "acknowledged",
        summary: { averageMs: 1.5, p95Ms: 2, samples: 2 },
      },
      requestedMessages: 2,
      runId: "run-123",
      schema: "streamskope.kafka-latency.v1",
    });
  });

  it("labels no-response producer timing as send completion", async () => {
    const session = new FakeSession();
    const result = await service(session).start({
      ...request,
      acknowledgements: 0,
    });

    expect(session.run).toHaveBeenCalledWith(
      { ...request, acknowledgements: 0 },
      "run-123",
      expect.any(AbortSignal),
    );
    expect(result).toMatchObject({
      evidence: {
        acknowledgements: 0,
        producer: { semantics: "send-completion", summary: { samples: 2 } },
      },
      state: "ready",
    });
  });

  it("reports partial evidence with null missing values and exact observed counts", async () => {
    const session = new FakeSession();
    session.run.mockResolvedValue({
      ...completeMeasurement,
      endToEndDurationsMs: [8],
      issues: [
        {
          recovery: "Verify broker TLS access.",
          stage: "tls",
          summary: "TLS handshake evidence is unavailable.",
        },
        {
          recovery: "Increase timeout or inspect the consumer path.",
          stage: "end-to-end",
          summary: "One probe record was not observed before timeout.",
        },
      ],
      network: {
        ...completeMeasurement.network,
        tlsHandshakeMs: null,
      },
      observedSampleIds: ["sample-1"],
    });
    const result = await service(session).start(request);

    expect(result.state).toBe("partial");
    expect(result.evidence).toMatchObject({
      endToEnd: { samples: 1 },
      issues: [{ stage: "tls" }, { stage: "end-to-end" }],
      network: { tlsHandshakeMs: null },
      observedMessages: 1,
    });
  });

  it("retains only derived ready and partial summaries for the current connection", async () => {
    const session = new FakeSession();
    let run = 0;
    let tick = 0;
    const latency = new KafkaLatencyProbeService(session, {
      createRunId: (): string => `run-${String(++run)}`,
      now: (): Date => new Date(Date.UTC(2026, 6, 25, 16, 0, tick++)),
    });

    await latency.start(request);
    session.run.mockResolvedValue({
      ...completeMeasurement,
      endToEndDurationsMs: [8],
      issues: [
        {
          recovery: "Inspect the consumer path.",
          stage: "end-to-end",
          summary: "One probe record was not observed.",
        },
      ],
      observedSampleIds: ["sample-1"],
    });
    await latency.start(request);

    expect(latency.historySnapshot()).toEqual({
      connectionName: "local-aio",
      entries: [
        {
          acknowledgements: -1,
          completedAt: "2026-07-25T16:00:01.000Z",
          endToEnd: { averageMs: 10, p95Ms: 12 },
          fetch: { averageMs: 4, p95Ms: 5 },
          issueCount: 0,
          observedMessages: 2,
          producer: { averageMs: 5, p95Ms: 6 },
          requestedMessages: 2,
          runId: "run-1",
          state: "ready",
          topic: "orders.events",
        },
        {
          acknowledgements: -1,
          completedAt: "2026-07-25T16:00:03.000Z",
          endToEnd: { averageMs: 8, p95Ms: 8 },
          fetch: { averageMs: 4, p95Ms: 5 },
          issueCount: 1,
          observedMessages: 1,
          producer: { averageMs: 5, p95Ms: 6 },
          requestedMessages: 2,
          runId: "run-2",
          state: "partial",
          topic: "orders.events",
        },
      ],
    });
    expect(JSON.stringify(latency.historySnapshot())).not.toMatch(
      /sample-1|127\.0\.0\.1|kafka-1|recovery|summary/,
    );
  });

  it("evicts the oldest summary after twenty explicit completions", async () => {
    const session = new FakeSession();
    let run = 0;
    let tick = 0;
    const latency = new KafkaLatencyProbeService(session, {
      createRunId: (): string => `run-${String(++run)}`,
      now: (): Date => new Date(Date.UTC(2026, 6, 25, 16, 0, tick++)),
    });

    for (let index = 0; index < 21; index += 1) {
      await latency.start(request);
    }

    expect(latency.historySnapshot().entries).toHaveLength(20);
    expect(latency.historySnapshot().entries[0]?.runId).toBe("run-2");
    expect(latency.historySnapshot().entries.at(-1)?.runId).toBe("run-21");
  });

  it("retains probe completion order when the wall clock moves backward", async () => {
    const session = new FakeSession();
    let run = 0;
    const dates = [
      new Date("2026-07-25T16:00:00.000Z"),
      new Date("2026-07-25T16:00:01.000Z"),
      new Date("2026-07-25T15:59:58.000Z"),
      new Date("2026-07-25T15:59:59.000Z"),
    ];
    const latency = new KafkaLatencyProbeService(session, {
      createRunId: (): string => `run-${String(++run)}`,
      now: (): Date => dates.shift() ?? new Date("2026-07-25T15:59:59.000Z"),
    });

    await latency.start(request);
    await latency.start(request);

    expect(latency.historySnapshot().entries.map((entry) => entry.runId)).toEqual([
      "run-1",
      "run-2",
    ]);
    expect(latency.historySnapshot().entries.map((entry) => entry.completedAt)).toEqual([
      "2026-07-25T16:00:01.000Z",
      "2026-07-25T15:59:59.000Z",
    ]);
  });

  it("rejects overlapping starts and leaves the first run authoritative", async () => {
    const session = new FakeSession();
    const pending = deferred<KafkaLatencyProbeMeasurement>();
    session.run.mockReturnValue(pending.promise);
    const latency = service(session);
    const first = latency.start(request);

    await expect(latency.start(request)).rejects.toBeInstanceOf(KafkaLatencyProbeValidationError);
    expect(session.run).toHaveBeenCalledTimes(1);

    pending.resolve(completeMeasurement);
    await expect(first).resolves.toMatchObject({ state: "ready" });
  });

  it("cancels one run, waits for its cleanup and treats repeated stop as harmless", async () => {
    const session = new FakeSession();
    const pending = deferred<KafkaLatencyProbeMeasurement>();
    session.run.mockImplementation(async (_request, _runId, signal) => {
      await pending.promise;
      if (signal.aborted) {
        throw new DOMException("Cancelled", "AbortError");
      }
      return completeMeasurement;
    });
    const latency = service(session);
    const running = latency.start(request);
    const stopping = latency.stop();
    expect(session.run.mock.calls[0]?.[2].aborted).toBe(true);

    pending.resolve(completeMeasurement);
    await expect(running).rejects.toMatchObject({ name: "AbortError" });
    await expect(stopping).resolves.toEqual(request);
    await expect(latency.stop()).resolves.toBeNull();
    expect(latency.historySnapshot()).toEqual({
      connectionName: null,
      entries: [],
    });
  });

  it("does not report stop success when cancellation cleanup failed", async () => {
    const session = new FakeSession();
    const cleanupFailure = new Error("Consumer close failed.");
    session.run.mockImplementation(async (_request, _runId, signal) => {
      await new Promise<void>((resolve) => {
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
      throw Object.assign(new Error("The Kafka latency probe was cancelled."), {
        cleanupCause: cleanupFailure,
        code: "CANCELLED",
      });
    });
    const latency = service(session);
    const running = latency.start(request);
    const stopping = latency.stop();

    await expect(running).rejects.toMatchObject({
      cleanupCause: cleanupFailure,
      code: "CANCELLED",
    });
    await expect(stopping).rejects.toMatchObject({
      cleanupCause: cleanupFailure,
      code: "CANCELLED",
    });
  });

  it("prevents late evidence from crossing connection ownership", async () => {
    const session = new FakeSession();
    const pending = deferred<KafkaLatencyProbeMeasurement>();
    session.run.mockReturnValue(pending.promise);
    const latency = service(session);
    const running = latency.start(request);

    latency.invalidate();
    session.context = {
      connectionBrokers: ["other:9093"],
      connectionName: "other",
      connectionTarget: "other:9093",
    };
    pending.resolve(completeMeasurement);

    await expect(running).rejects.toMatchObject({
      name: "ConnectionAttemptSupersededError",
    });
    expect(latency.currentEvidence()).toBeNull();
    expect(latency.historySnapshot()).toEqual({
      connectionName: null,
      entries: [],
    });
  });

  it("exports only current ready or partial evidence as canonical bounded JSON", async () => {
    const session = new FakeSession();
    const latency = service(session);
    const result = await latency.start(request);
    const document = latency.exportDocument();

    expect(document.fileName).toBe("streamskope-latency-orders.events-run-123.json");
    expect(document.content).toBe(`${JSON.stringify(result.evidence, null, 2)}\n`);
    expect(document.byteSize).toBe(new TextEncoder().encode(document.content).byteLength);
    expect(document.content).not.toContain("clientSecret");

    latency.invalidate();
    expect(latency.staleEvidence()).toEqual(result.evidence);
    expect(() => latency.exportDocument()).toThrow(KafkaLatencyProbeValidationError);
  });

  it("rejects a request without an active connection before adapter work", async () => {
    const session = new FakeSession();
    session.context = null;
    const latency = service(session);

    await expect(latency.start(request)).rejects.toBeInstanceOf(NoActiveKafkaConnectionError);
    expect(session.run).not.toHaveBeenCalled();
  });
});
