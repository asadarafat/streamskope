import { OBSERVATION_LIMITS as limits, type ObservationIssue } from "../contracts/observations";
import { parseObservationRecords, type ObservationRecords } from "../contracts/observation-records";
import type { KafkaMessage } from "../contracts";

import type { KafkaActiveConnection, KafkaMessageStream } from "./types";
import { observationAborted, observationIssue } from "./observation-errors";

function withinSamplingDeadline<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    void operation.catch(() => undefined);
    return Promise.reject(observationAborted(signal));
  }
  return new Promise<T>((resolve, reject) => {
    const abort = (): void => reject(observationAborted(signal));
    signal.addEventListener("abort", abort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", abort);
        reject(
          error instanceof Error ? error : new Error("Record sampling failed.", { cause: error }),
        );
      },
    );
  });
}

/** Prefer a complete recent window within the same fixed record/byte budget. */
export function observationRecordWindow(positionsPerSecond: number | null): number {
  if (
    positionsPerSecond === null ||
    !Number.isFinite(positionsPerSecond) ||
    positionsPerSecond <= 0
  )
    return 60_000;
  return [60_000, 10_000, 1_000, 100].find((ms) => (positionsPerSecond * ms) / 1000 <= 100) ?? 100;
}

/** Consumes the engine's protected records and returns aggregates/locators, never key bytes. */
export async function sampleObservationRecords(
  connection: Pick<KafkaActiveConnection, "openMessageStream">,
  topic: string,
  endTimeMs: number,
  signal: AbortSignal,
  visit?: (message: KafkaMessage) => void,
  options: {
    readonly windowMs?: number;
    readonly expectedPartitions?: readonly number[];
    readonly onIssue?: (issue: ObservationIssue) => void;
  } = {},
): Promise<ObservationRecords> {
  const windowMs = options.windowMs ?? 60_000;
  if (!Number.isInteger(windowMs) || windowMs < 100 || windowMs > 60_000)
    throw new Error("Invalid bounded record window.");
  const startTimeMs = endTimeMs - windowMs;
  const deadline = new AbortController();
  const timer = setTimeout(() => {
    deadline.abort(new DOMException("Record sampling exceeded its deadline.", "TimeoutError"));
  }, 5_000);
  timer.unref?.();
  const bounded = AbortSignal.any([signal, deadline.signal]);
  const sizes: number[] = [],
    partitions = new Map<number, number>();
  const keys = new Map<string, { count: number; partition: number; offset: string }>();
  const seen = new Set<string>();
  let bytes = 0,
    nullKeys = 0,
    unavailableKeys = 0,
    knownKeys = 0;
  let stream: KafkaMessageStream | undefined,
    reason: ObservationRecords["reason"] = "no-coverage";
  let iterator: AsyncIterator<KafkaMessage> | undefined;
  let closing: Promise<void> | undefined;
  let completedPartitions = 0;
  let issue: ObservationIssue | undefined;
  const close = (): void => {
    if (stream) closing ??= Promise.resolve().then(() => stream!.close());
    void closing?.catch(() => undefined);
  };
  bounded.addEventListener("abort", close, { once: true });
  try {
    const opening = connection
      .openMessageStream(
        { mode: "time-window", topic, startTimeMs, endTimeMs, maxMessages: limits.sampleRecords },
        bounded,
      )
      .then((opened) => {
        stream = opened;
        if (bounded.aborted) close();
        return opened;
      });
    stream = await withinSamplingDeadline(opening, bounded);
    if (bounded.aborted) close();
    iterator = stream[Symbol.asyncIterator]();
    for (;;) {
      const next = await withinSamplingDeadline(iterator.next(), bounded);
      if (next.done) break;
      const message = next.value;
      if (bounded.aborted) {
        reason = "timeout";
        break;
      }
      if (
        sizes.length >= limits.sampleRecords ||
        bytes + message.originalByteSize > limits.sampleBytes
      ) {
        reason = "limit";
        break;
      }
      const timestamp = Date.parse(message.timestamp),
        identity = JSON.stringify([message.partition, message.offset]);
      if (
        message.topic !== topic ||
        timestamp < startTimeMs ||
        timestamp >= endTimeMs ||
        !Number.isFinite(timestamp) ||
        seen.has(identity)
      )
        continue;
      seen.add(identity);
      visit?.(message);
      bytes += message.originalByteSize;
      sizes.push(message.originalByteSize);
      partitions.set(message.partition, (partitions.get(message.partition) ?? 0) + 1);
      const original = message.original;
      if (!original || original.state !== "complete") unavailableKeys++;
      else if (original.key === null) nullKeys++;
      else {
        knownKeys++;
        const previous = keys.get(original.key);
        keys.set(original.key, {
          count: (previous?.count ?? 0) + 1,
          partition: previous?.partition ?? message.partition,
          offset: previous?.offset ?? message.offset,
        });
      }
    }
    if (reason === "no-coverage")
      reason =
        stream.coverage?.()?.reason === "range-complete"
          ? "range-complete"
          : sizes.length >= limits.sampleRecords
            ? "limit"
            : "no-coverage";
    const coverage = stream.coverage?.();
    completedPartitions = (options.expectedPartitions ?? []).filter((partition) => {
      const p = coverage?.partitions.find((p) => p.partition === partition);
      return p && BigInt(p.nextOffset) >= BigInt(p.endOffset);
    }).length;
  } catch (error) {
    reason = bounded.aborted ? "timeout" : "read-failed";
    issue = observationIssue(
      bounded.aborted ? observationAborted(bounded, error) : error,
      "records",
    );
  } finally {
    bounded.removeEventListener("abort", close);
    try {
      close();
      // Return may wait for an unresponsive next(); cleanup must not prolong the deadline.
      void Promise.resolve()
        .then(() => iterator?.return?.())
        .catch(() => undefined);
      if (closing) await withinSamplingDeadline(closing, bounded);
    } catch (error) {
      reason = "read-failed";
      issue ??= observationIssue(
        bounded.aborted ? observationAborted(bounded, error) : error,
        "records",
      );
    } finally {
      clearTimeout(timer);
    }
  }
  signal.throwIfAborted();
  if (bounded.aborted) {
    reason = "timeout";
    issue ??= observationIssue(observationAborted(bounded), "records");
  }
  if (issue) options.onIssue?.(issue);
  sizes.sort((a, b) => a - b);
  return parseObservationRecords({
    analysisEligible:
      reason === "range-complete" &&
      (options.expectedPartitions?.length ?? 0) > 0 &&
      completedPartitions === options.expectedPartitions?.length,
    ...(options.expectedPartitions === undefined
      ? {}
      : {
          partitionCoverage: {
            expected: options.expectedPartitions.length,
            completed: completedPartitions,
          },
        }),
    source: "protected-kafka-record-sample",
    startTimeMs,
    endTimeMs,
    state: reason === "range-complete" ? "complete" : sizes.length ? "partial" : "unavailable",
    reason,
    count: sizes.length,
    bytes,
    meanBytes: sizes.length ? bytes / sizes.length : null,
    p95Bytes: sizes.length ? sizes[Math.ceil(sizes.length * 0.95) - 1] : null,
    knownKeys,
    nullKeys,
    unavailableKeys,
    distinctKeys: keys.size,
    topKeys: [...keys.values()]
      .sort(
        (a, b) =>
          b.count - a.count ||
          a.partition - b.partition ||
          (BigInt(a.offset) < BigInt(b.offset) ? -1 : 1),
      )
      .slice(0, 10),
    partitions: [...partitions]
      .sort(([a], [b]) => a - b)
      .map(([partition, count]) => ({ partition, count })),
  });
}
