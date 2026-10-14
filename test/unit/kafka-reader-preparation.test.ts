import { afterEach, describe, expect, it, vi } from "vitest";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";
import { KafkaReadOpenCleanupError } from "../../src/features/kafka/application/read-open-cleanup";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine/engine";
import { resolveKafkaFetchPlan } from "../../src/features/kafka/engine/fetch-plan";
import type {
  KafkaConsumerFactory,
  KafkaConsumerInput,
  KafkaRawMessage,
  KafkaRawMessageStream,
} from "../../src/features/kafka/engine/types";
import { RecordingAdmin, RecordingAdminFactory } from "../support/kafka-engine-admin-fixture";

const connection: SecureConnectionInput = {
  brokers: ["broker.example:9092"],
  name: "Remote broker",
  tls: { enabled: false },
};
const tail = { topic: "model-changes", mode: "tail", maxMessages: 1_000 } as const;

function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = (): void => {
      clearTimeout(timer);
      reject(new DOMException("The fixture request was aborted.", "AbortError"));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", abort, { once: true });
  });
}

class EmptyStream implements KafkaRawMessageStream {
  closeCalls = 0;

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  [Symbol.asyncIterator](): AsyncIterator<KafkaRawMessage> {
    return { next: () => Promise.resolve({ done: true, value: undefined }) };
  }
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Kafka reader preparation budget", () => {
  it("opens a healthy remote Tail whose successful offset probes collectively exceed one request timeout", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-14T12:00:00Z"));
    const started = Date.now();
    const raw = new EmptyStream();
    const inputs: KafkaConsumerInput[] = [];
    const timestamps: bigint[] = [];
    let readyAt: number | undefined;
    const consumerFactory: KafkaConsumerFactory = {
      open: async (input) => {
        inputs.push(input);
        input.onOpeningStage?.("topic-metadata");
        await delay(400, input.signal);
        input.onOpeningStage?.("starting-offsets");
        await resolveKafkaFetchPlan(
          {
            listTopicOffsets: async (_topic, timestamp) => {
              timestamps.push(timestamp);
              await delay(400, input.signal);
              if (timestamp === -2n) return [0n];
              if (timestamp === -1n) return [100_000n];
              return timestamp >= BigInt(started - 60 * 60 * 1_000) ? [99_500n] : [98_000n];
            },
          },
          input.request,
          started,
        );
        input.onOpeningStage?.("verify-topic");
        await delay(400, input.signal);
        input.onOpeningStage?.("start-reader");
        readyAt = Date.now();
        return raw;
      },
    };
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin([tail.topic])),
      consumerFactory,
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    const result = active.openMessageStream(tail, new AbortController().signal).then(
      (stream) => ({ stream }),
      (error: unknown) => ({ error }),
    );
    try {
      await vi.advanceTimersByTimeAsync(9_000);
      const settled = await result;
      expect(settled).toHaveProperty("stream");
      expect(inputs[0]?.operationTimeoutMs).toBe(5_000);
      expect((readyAt ?? started) - started).toBeGreaterThan(5_000);
      expect(timestamps.filter((timestamp) => timestamp >= 0n).length).toBeGreaterThan(10);
      if ("stream" in settled) await settled.stream.close();
      expect(raw.closeCalls).toBe(1);
    } finally {
      await active.close();
    }
  });

  it("bounds stalled offset preparation and freezes its phase before late cleanup diagnostics", async () => {
    vi.useFakeTimers();
    const inputs: KafkaConsumerInput[] = [];
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin([tail.topic])),
      consumerFactory: {
        open: async (input): Promise<KafkaRawMessageStream> => {
          inputs.push(input);
          input.onOpeningStage?.("starting-offsets");
          try {
            await delay(60_000, input.signal);
          } catch (error) {
            input.onOpeningStage?.("verify-topic");
            throw error;
          }
          return new EmptyStream();
        },
      },
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    let settled = false;
    const result = active
      .openMessageStream(tail, new AbortController().signal)
      .catch((error: unknown) => {
        settled = true;
        return error;
      });
    try {
      await vi.advanceTimersByTimeAsync(29_999);
      expect(settled).toBe(false);
      expect(inputs[0]?.signal?.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const failure = await result;
      expect(failure).toMatchObject({ code: "TIMEOUT", stage: "broker", retryable: true });
      expect(failure).toBeInstanceOf(Error);
      if (failure instanceof Error) {
        expect(failure.message).toMatch(/reader.*prepar|prepar.*reader/iu);
        expect(failure.message).toMatch(/offset/iu);
        expect(failure.message).not.toMatch(/metadata/iu);
      }
      expect(inputs[0]?.signal?.aborted).toBe(true);
      expect(inputs[0]?.operationTimeoutMs).toBe(5_000);
    } finally {
      await active.close();
    }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("applies an explicit preparation budget without changing individual request deadlines", async () => {
    vi.useFakeTimers();
    let observed: KafkaConsumerInput | undefined;
    const engine = new StreamSkopeKafkaEngine({
      operationTimeoutMs: 200,
      readPreparationTimeoutMs: 750,
      adminFactory: new RecordingAdminFactory(new RecordingAdmin([tail.topic])),
      consumerFactory: {
        open: async (input): Promise<KafkaRawMessageStream> => {
          observed = input;
          input.onOpeningStage?.("start-reader");
          await delay(1_000, input.signal);
          return new EmptyStream();
        },
      },
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    const result = active
      .openMessageStream(tail, new AbortController().signal)
      .catch((error: unknown) => error);
    try {
      await vi.advanceTimersByTimeAsync(749);
      expect(observed?.signal?.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(await result).toMatchObject({ code: "TIMEOUT", stage: "broker" });
      expect(observed?.operationTimeoutMs).toBe(200);
    } finally {
      await active.close();
    }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cancels preparation immediately instead of waiting for its longer deadline", async () => {
    vi.useFakeTimers();
    const started = Date.now();
    let observed: KafkaConsumerInput | undefined;
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin([tail.topic])),
      consumerFactory: {
        open: async (input): Promise<KafkaRawMessageStream> => {
          observed = input;
          input.onOpeningStage?.("starting-offsets");
          await delay(60_000, input.signal);
          return new EmptyStream();
        },
      },
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    const controller = new AbortController();
    const result = active
      .openMessageStream(tail, controller.signal)
      .catch((error: unknown) => error);
    try {
      controller.abort();
      expect(await result).toMatchObject({ code: "CANCELLED" });
      expect(observed?.signal?.aborted).toBe(true);
      expect(Date.now()).toBe(started);
    } finally {
      await active.close();
    }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retains failed cleanup through the original connection when preparation times out", async () => {
    vi.useFakeTimers();
    const cleanupFailure = new Error("The fixture consumer did not close.");
    let cleanupAttempts = 0;
    const cleanup = {
      close: (): Promise<void> => {
        cleanupAttempts += 1;
        return cleanupAttempts === 1 ? Promise.reject(cleanupFailure) : Promise.resolve();
      },
    };
    const admin = new RecordingAdmin([tail.topic]);
    const engine = new StreamSkopeKafkaEngine({
      readPreparationTimeoutMs: 100,
      adminFactory: new RecordingAdminFactory(admin),
      consumerFactory: {
        open: async (input): Promise<KafkaRawMessageStream> => {
          input.onOpeningStage?.("starting-offsets");
          try {
            await delay(1_000, input.signal);
            return new EmptyStream();
          } catch (cause) {
            try {
              await cleanup.close();
            } catch (failedCleanup) {
              throw new KafkaReadOpenCleanupError(cause, failedCleanup, cleanup);
            }
            throw cause;
          }
        },
      },
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    const result = active
      .openMessageStream(tail, new AbortController().signal)
      .catch((error: unknown) => error);
    try {
      await vi.advanceTimersByTimeAsync(100);
      const failure = await result;
      expect(failure).toBeInstanceOf(KafkaReadOpenCleanupError);
      expect(failure).toMatchObject({ code: "TIMEOUT", cleanupCause: cleanupFailure });
      expect(cleanupAttempts).toBe(1);
      expect(admin.closeCalls).toBe(0);
    } finally {
      await active.close();
    }
    expect(cleanupAttempts).toBe(2);
    expect(admin.closeCalls).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    "rejects an invalid reader preparation timeout (%s)",
    (readPreparationTimeoutMs) => {
      expect(() => new StreamSkopeKafkaEngine({ readPreparationTimeoutMs })).toThrow(RangeError);
    },
  );
});
