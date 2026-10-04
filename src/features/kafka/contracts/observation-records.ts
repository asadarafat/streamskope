import { OBSERVATION_LIMITS as limits } from "./observations";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export interface ObservationRecords {
  /** Size analysis eligibility for this complete bounded window, never a topic-wide census. */
  readonly analysisEligible?: boolean;
  readonly partitionCoverage?: { readonly expected: number; readonly completed: number };
  readonly source: "protected-kafka-record-sample";
  readonly startTimeMs: number;
  readonly endTimeMs: number;
  readonly state: "complete" | "partial" | "unavailable";
  readonly reason: "range-complete" | "limit" | "timeout" | "read-failed" | "no-coverage";
  readonly count: number;
  readonly bytes: number;
  readonly meanBytes: number | null;
  readonly p95Bytes: number | null;
  readonly knownKeys: number;
  readonly nullKeys: number;
  readonly unavailableKeys: number;
  readonly distinctKeys: number;
  readonly topKeys: readonly {
    readonly count: number;
    readonly partition: number;
    readonly offset: string;
  }[];
  readonly partitions: readonly { readonly partition: number; readonly count: number }[];
}
function count(value: unknown, maximum: number): number {
  const n = nonNegativeInteger(value, "count");
  if (n > maximum) throw new Error("Record sample exceeds its bound.");
  return n;
}
export function parseObservationRecords(value: unknown): ObservationRecords {
  const p = record(value, "records");
  exactKeys(
    p,
    [
      "source",
      "startTimeMs",
      "endTimeMs",
      "state",
      "reason",
      "count",
      "bytes",
      "meanBytes",
      "p95Bytes",
      "knownKeys",
      "nullKeys",
      "unavailableKeys",
      "distinctKeys",
      "topKeys",
      "partitions",
      "analysisEligible",
      "partitionCoverage",
    ],
    "records",
  );
  const startTimeMs = count(p.startTimeMs, 8.64e15),
    endTimeMs = count(p.endTimeMs, 8.64e15);
  if (endTimeMs - startTimeMs < 100 || endTimeMs - startTimeMs > 60_000)
    throw new Error("Record sample window must be between 100 ms and one minute.");
  const sampleCount = count(p.count, limits.sampleRecords),
    knownKeys = count(p.knownKeys, sampleCount),
    nullKeys = count(p.nullKeys, sampleCount),
    unavailableKeys = count(p.unavailableKeys, sampleCount);
  if (knownKeys + nullKeys + unavailableKeys !== sampleCount)
    throw new Error("Key coverage differs from the sample.");
  if (
    !Array.isArray(p.topKeys) ||
    p.topKeys.length > 10 ||
    !Array.isArray(p.partitions) ||
    p.partitions.length > limits.partitions
  )
    throw new Error("Record sample exceeds its bound.");
  const size = (v: unknown): number | null => {
    if (v === null) return null;
    if (typeof v !== "number" || !Number.isFinite(v) || v < 0 || v > limits.sampleBytes)
      throw new Error("Invalid sample size.");
    return v;
  };
  const topKeys = (p.topKeys as unknown[]).map((item) => {
    const k = record(item, "key-count");
    exactKeys(k, ["count", "partition", "offset"], "key-count");
    const offset = text(k.offset, "offset", 20);
    if (!/^(0|[1-9][0-9]*)$/.test(offset) || BigInt(offset) > 9223372036854775807n)
      throw new Error("Invalid sample locator.");
    return { count: count(k.count, knownKeys), partition: count(k.partition, 2147483647), offset };
  });
  const partitions = (p.partitions as unknown[]).map((item) => {
    const v = record(item, "partition-count");
    exactKeys(v, ["partition", "count"], "partition-count");
    return { partition: count(v.partition, 2147483647), count: count(v.count, sampleCount) };
  });
  if (
    partitions.reduce((n, v) => n + v.count, 0) !== sampleCount ||
    new Set(partitions.map((p) => p.partition)).size !== partitions.length ||
    topKeys.reduce((n, v) => n + v.count, 0) > knownKeys
  )
    throw new Error("Invalid sample distribution.");
  const coverage =
    p.partitionCoverage === undefined
      ? undefined
      : record(p.partitionCoverage, "partition coverage");
  if (coverage) exactKeys(coverage, ["expected", "completed"], "partition coverage");
  const partitionCoverage = coverage
    ? {
        expected: count(coverage.expected, limits.partitions),
        completed: count(coverage.completed, limits.partitions),
      }
    : undefined;
  const analysisEligible =
    p.analysisEligible === undefined ? false : truth(p.analysisEligible, "analysis eligibility");
  if (partitionCoverage && partitionCoverage.completed > partitionCoverage.expected)
    throw new Error("Invalid record partition coverage.");
  if (
    analysisEligible &&
    (p.state !== "complete" ||
      p.reason !== "range-complete" ||
      !partitionCoverage?.expected ||
      partitionCoverage.completed !== partitionCoverage.expected)
  )
    throw new Error("Incomplete record samples cannot qualify analysis.");
  return {
    analysisEligible,
    ...(partitionCoverage === undefined ? {} : { partitionCoverage }),
    source: declaredValue(p.source, ["protected-kafka-record-sample"] as const, "source"),
    startTimeMs,
    endTimeMs,
    state: declaredValue(p.state, ["complete", "partial", "unavailable"] as const, "state"),
    reason: declaredValue(
      p.reason,
      ["range-complete", "limit", "timeout", "read-failed", "no-coverage"] as const,
      "reason",
    ),
    count: sampleCount,
    bytes: count(p.bytes, limits.sampleBytes),
    meanBytes: size(p.meanBytes),
    p95Bytes: size(p.p95Bytes),
    knownKeys,
    nullKeys,
    unavailableKeys,
    distinctKeys: count(p.distinctKeys, knownKeys),
    topKeys,
    partitions,
  };
}
