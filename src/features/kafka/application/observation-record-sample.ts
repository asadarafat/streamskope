import { OBSERVATION_LIMITS as limits } from "../contracts/observations";
import { parseObservationRecords, type ObservationRecords } from "../contracts/observation-records";
import type { KafkaMessage } from "../contracts";

import type { KafkaActiveConnection, KafkaMessageStream } from "./types";

/** Consumes the engine's protected records and returns aggregates/locators, never key bytes. */
export async function sampleObservationRecords(
  connection: Pick<KafkaActiveConnection, "openMessageStream">,
  topic: string,
  endTimeMs: number,
  signal: AbortSignal,
  visit?: (message: KafkaMessage) => void,
): Promise<ObservationRecords> {
  const startTimeMs = endTimeMs - 60_000;
  const bounded = AbortSignal.any([signal, AbortSignal.timeout(5_000)]);
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
  let closing: Promise<void> | undefined;
  const close = (): void => {
    if (stream) closing ??= stream.close();
    void closing?.catch(() => undefined);
  };
  bounded.addEventListener("abort", close, { once: true });
  try {
    stream = await connection.openMessageStream(
      { mode: "time-window", topic, startTimeMs, endTimeMs, maxMessages: limits.sampleRecords },
      bounded,
    );
    if (bounded.aborted) close();
    for await (const message of stream) {
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
  } catch {
    reason = bounded.aborted ? "timeout" : "read-failed";
  } finally {
    bounded.removeEventListener("abort", close);
    try {
      close();
      await closing;
    } catch {
      reason = "read-failed";
    }
  }
  signal.throwIfAborted();
  if (bounded.aborted) reason = "timeout";
  sizes.sort((a, b) => a - b);
  return parseObservationRecords({
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
