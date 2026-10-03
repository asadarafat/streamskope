import { parseObservationRecords } from "./observation-records";
import {
  declaredValue,
  exactKeys,
  nullableText,
  nonNegativeInteger,
  record,
  text,
  truth,
} from "./validation-primitives";
import {
  OBSERVATION_LIMITS as limits,
  type KafkaObservation,
  type ObservationHistory,
  type ObservationInput,
  type ObservationSeries,
  observationIdentity,
} from "./observations";

function number(value: unknown, maximum: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > maximum)
    throw new Error("Invalid bounded observation value.");
  return value;
}
function nullableNumber(value: unknown, maximum: number): number | null {
  return value === null ? null : number(value, maximum);
}
function offset(value: unknown): string | null {
  if (value === null) return null;
  const result = text(value, "offset", 20);
  if (!/^(0|[1-9][0-9]*)$/.test(result) || BigInt(result) > 9223372036854775807n)
    throw new Error("Invalid offset.");
  return result;
}
function array(value: unknown, maximum: number): unknown[] {
  if (!Array.isArray(value) || value.length > maximum)
    throw new Error("Observation collection exceeds its limit.");
  return value as unknown[];
}
export function parseObservationInput(value: unknown): ObservationInput {
  const p = record(value, "observation");
  exactKeys(p, ["topic", "groupId", "thresholds", "sampleRecords"], "observation");
  const topic = text(p.topic, "topic", 249);
  if (!/^[A-Za-z0-9._-]+$/.test(topic) || topic === "." || topic === "..")
    throw new Error("Select one valid topic.");
  const t = record(p.thresholds, "thresholds");
  exactKeys(t, ["lag", "requestMs"], "thresholds");
  return {
    topic,
    sampleRecords: p.sampleRecords === undefined ? false : truth(p.sampleRecords, "sampleRecords"),
    groupId: nullableText(p.groupId, "groupId", 512),
    thresholds: {
      lag: nullableNumber(t.lag, Number.MAX_SAFE_INTEGER),
      requestMs: nullableNumber(t.requestMs, 60_000),
    },
  };
}
export function parseObservation(value: unknown): KafkaObservation {
  const p = record(value, "sample");
  exactKeys(
    p,
    [
      "id",
      "segmentId",
      "startedAt",
      "observedAt",
      "source",
      "requestMs",
      "providerCalls",
      "state",
      "groupState",
      "members",
      "brokerCount",
      "controllerKnown",
      "groupCoverage",
      "partitions",
      "alerts",
      "records",
    ],
    "sample",
  );
  const startedAt = number(p.startedAt, 8.64e15),
    observedAt = number(p.observedAt, 8.64e15);
  if (observedAt < startedAt || observedAt - startedAt > limits.deadlineMs + 1000)
    throw new Error("Invalid observation interval.");
  const partitions = array(p.partitions, limits.partitions).map((item) => {
    const v = record(item, "partition");
    exactKeys(
      v,
      ["partition", "leader", "replicas", "inSyncReplicas", "endOffset", "committedOffset", "lag"],
      "partition",
    );
    return {
      partition: nonNegativeInteger(v.partition, "partition"),
      leader: v.leader === null ? null : nonNegativeInteger(v.leader, "leader"),
      replicas: nonNegativeInteger(v.replicas, "replicas"),
      inSyncReplicas: nonNegativeInteger(v.inSyncReplicas, "inSyncReplicas"),
      endOffset: offset(v.endOffset),
      committedOffset: offset(v.committedOffset),
      lag: offset(v.lag),
    };
  });
  if (!partitions.length || new Set(partitions.map((v) => v.partition)).size !== partitions.length)
    throw new Error("Invalid observation partition coverage.");
  return {
    id: text(p.id, "id", 128),
    segmentId: text(p.segmentId, "segmentId", 128),
    startedAt,
    observedAt,
    source: declaredValue(p.source, ["kafka-api"] as const, "source"),
    requestMs: number(p.requestMs, limits.deadlineMs + 1000),
    providerCalls: number(p.providerCalls, 4),
    state: declaredValue(p.state, ["ready", "partial"] as const, "state"),
    groupState: nullableText(p.groupState, "groupState", 64),
    members: nullableNumber(p.members, 1000),
    brokerCount: nonNegativeInteger(p.brokerCount, "brokerCount"),
    controllerKnown: truth(p.controllerKnown, "controllerKnown"),
    groupCoverage: declaredValue(
      p.groupCoverage,
      ["complete", "partial", "unavailable", "not-selected"] as const,
      "groupCoverage",
    ),
    partitions,
    records:
      p.records === null || p.records === undefined ? null : parseObservationRecords(p.records),
    alerts: array(p.alerts, 2).map((item) => {
      const a = record(item, "alert");
      exactKeys(a, ["metric", "observed", "threshold"], "alert");
      return {
        metric: declaredValue(a.metric, ["lag", "requestMs"] as const, "metric"),
        observed: number(a.observed, Number.MAX_SAFE_INTEGER),
        threshold: number(a.threshold, Number.MAX_SAFE_INTEGER),
      };
    }),
  };
}
export function parseObservationSeries(value: unknown): ObservationSeries {
  const p = record(value, "series");
  exactKeys(p, ["clusterId", "topicId", "topic", "groupId", "samples"], "series");
  const samples = array(p.samples, limits.samples).map(parseObservation);
  if (samples.some((s, i) => i > 0 && s.observedAt <= samples[i - 1]!.observedAt))
    throw new Error("History must be chronological.");
  return {
    clusterId: text(p.clusterId, "clusterId", 512),
    topicId: text(p.topicId, "topicId", 128),
    topic: text(p.topic, "topic", 249),
    groupId: nullableText(p.groupId, "groupId", 512),
    samples,
  };
}
export function parseObservationHistory(value: unknown): ObservationHistory {
  const p = record(value, "history");
  exactKeys(p, ["schemaVersion", "series"], "history");
  if (
    p.schemaVersion !== 1 ||
    new TextEncoder().encode(JSON.stringify(value)).length > limits.fileBytes
  )
    throw new Error("Unsupported or oversized observation history.");
  const series = array(p.series, limits.series).map(parseObservationSeries);
  if (new Set(series.map(observationIdentity)).size !== series.length)
    throw new Error("Duplicate history identity.");
  return { schemaVersion: 1, series };
}
