import { parseObservationRecords } from "./observation-records";
import { parseObservationRollups, parseObservationSettings } from "./observation-retention";
import {
  observationNumber as number,
  nullableObservationNumber as nullableNumber,
  observationArray as array,
} from "./observation-values";
export { parseObservationInput } from "./observation-input";
import { HOST_ERROR_CODES } from "./types";
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
  type ObservationSeries,
  type ObservationIssue,
  observationIdentity,
} from "./observations";

function offset(value: unknown): string | null {
  if (value === null) return null;
  const result = text(value, "offset", 20);
  if (!/^(0|[1-9][0-9]*)$/.test(result) || BigInt(result) > 9223372036854775807n)
    throw new Error("Invalid offset.");
  return result;
}
export function parseObservationIssues(value: unknown): readonly ObservationIssue[] {
  if (value === undefined) return [];
  return array(value, 8).map((item) => {
    const p = record(item, "observation issue");
    exactKeys(p, ["measurement", "code", "summary", "recovery", "retryable"], "observation issue");
    return {
      measurement: declaredValue(
        p.measurement,
        ["end-offsets", "group-offsets", "group-members", "records"] as const,
        "measurement",
      ),
      code: declaredValue(p.code, HOST_ERROR_CODES, "issue code"),
      summary: text(p.summary, "issue summary", 512),
      recovery: text(p.recovery, "issue recovery", 1024),
      retryable: truth(p.retryable, "issue retryable"),
    };
  });
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
      "issues",
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
    issues: parseObservationIssues(p.issues),
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
  exactKeys(
    p,
    p.schemaVersion === 2
      ? ["schemaVersion", "series", "settings", "rollups"]
      : ["schemaVersion", "series"],
    "history",
  );
  if (
    (p.schemaVersion !== 1 && p.schemaVersion !== 2) ||
    new TextEncoder().encode(JSON.stringify(value)).length > limits.fileBytes
  )
    throw new Error("Unsupported or oversized observation history.");
  const series = array(p.series, limits.series).map(parseObservationSeries);
  if (new Set(series.map(observationIdentity)).size !== series.length)
    throw new Error("Duplicate history identity.");
  if (p.schemaVersion === 1) return { schemaVersion: 1, series };
  const rollups = parseObservationRollups(p.rollups);
  if (new Set([...series, ...rollups].map(observationIdentity)).size > limits.series)
    throw new Error("Observation history exceeds its identity bound.");
  return { schemaVersion: 2, series, settings: parseObservationSettings(p.settings), rollups };
}
