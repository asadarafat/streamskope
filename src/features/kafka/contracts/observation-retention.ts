import { parseObservationInput } from "./observation-input";
import {
  observationNumber as number,
  nullableObservationNumber as nullableNumber,
  observationArray,
} from "./observation-values";
import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  type ObservationRollup,
  type ObservationSettings,
} from "./observations";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export function parseObservationSettings(value: unknown): ObservationSettings | null {
  if (value === null) return null;
  const p = record(value, "observation settings");
  exactKeys(
    p,
    ["input", "connectionName", "clusterId", "topicId", "savedAt"],
    "observation settings",
  );
  return {
    input: parseObservationInput(p.input),
    connectionName: text(p.connectionName, "settings connection", 256),
    clusterId: text(p.clusterId, "settings cluster", 512),
    topicId: text(p.topicId, "settings topic", 128),
    savedAt: number(p.savedAt, 8.64e15),
  };
}

function parseRollup(value: unknown): ObservationRollup {
  const p = record(value, "observation rollup");
  exactKeys(
    p,
    [
      "clusterId",
      "topicId",
      "topic",
      "groupId",
      "source",
      "bucketStart",
      "bucketEnd",
      "firstObservedAt",
      "lastObservedAt",
      "samples",
      "partial",
      "lagKnown",
      "lagMin",
      "lagMax",
      "lastLag",
      "requestMin",
      "requestMax",
      "lastRequest",
      "boundary",
      "firstSampleId",
      "lastSampleId",
    ],
    "observation rollup",
  );
  const r: ObservationRollup = {
    clusterId: text(p.clusterId, "rollup cluster", 512),
    topicId: text(p.topicId, "rollup topic identity", 128),
    topic: text(p.topic, "rollup topic", 249),
    groupId: nullableText(p.groupId, "rollup group", 512),
    source: declaredValue(p.source, ["kafka-api"], "rollup source"),
    bucketStart: number(p.bucketStart, 8.64e15),
    bucketEnd: number(p.bucketEnd, 8.64e15),
    firstObservedAt: number(p.firstObservedAt, 8.64e15),
    lastObservedAt: number(p.lastObservedAt, 8.64e15),
    samples: positiveBoundedInteger(p.samples, "rollup samples", Number.MAX_SAFE_INTEGER),
    partial: nonNegativeInteger(p.partial, "rollup partial samples"),
    lagKnown: nonNegativeInteger(p.lagKnown, "rollup known lag samples"),
    lagMin: nullableNumber(p.lagMin, Number.MAX_SAFE_INTEGER),
    lagMax: nullableNumber(p.lagMax, Number.MAX_SAFE_INTEGER),
    lastLag: nullableNumber(p.lastLag, Number.MAX_SAFE_INTEGER),
    requestMin: number(p.requestMin, limits.deadlineMs + 1000),
    requestMax: number(p.requestMax, limits.deadlineMs + 1000),
    lastRequest: number(p.lastRequest, limits.deadlineMs + 1000),
    boundary: declaredValue(
      p.boundary,
      [
        "initial",
        "window",
        "unverified",
        "incomplete",
        "group-state",
        "restart",
        "clock-gap",
        "cooldown",
        "topology",
        "offset-reset",
      ],
      "rollup boundary",
    ),
    firstSampleId: text(p.firstSampleId, "rollup first sample", 128),
    lastSampleId: text(p.lastSampleId, "rollup last sample", 128),
  };
  if (
    r.bucketStart % limits.rollupMs !== 0 ||
    r.bucketEnd !== r.bucketStart + limits.rollupMs ||
    r.firstObservedAt < r.bucketStart ||
    r.lastObservedAt >= r.bucketEnd ||
    r.lastObservedAt < r.firstObservedAt ||
    r.partial > r.samples ||
    r.lagKnown > r.samples ||
    (r.lagKnown === 0) !== (r.lagMin === null && r.lagMax === null) ||
    (r.lagMin === null) !== (r.lagMax === null) ||
    (r.lagMin !== null && r.lagMax !== null && r.lagMin > r.lagMax) ||
    (r.lastLag !== null &&
      (r.lagMin === null || r.lagMax === null || r.lastLag < r.lagMin || r.lastLag > r.lagMax)) ||
    r.requestMin > r.requestMax ||
    r.lastRequest < r.requestMin ||
    r.lastRequest > r.requestMax ||
    (r.samples === 1 &&
      (r.firstObservedAt !== r.lastObservedAt || r.firstSampleId !== r.lastSampleId)) ||
    (r.samples > 1 && (r.firstObservedAt >= r.lastObservedAt || r.firstSampleId === r.lastSampleId))
  )
    throw new Error("Inconsistent observation rollup coverage.");
  return r;
}

export function parseObservationRollups(value: unknown): readonly ObservationRollup[] {
  const rollups = observationArray(value, limits.series * limits.rollupsPerSeries).map(parseRollup);
  const identities = new Map<string, number>();
  const buckets = new Set<string>();
  for (const rollup of rollups) {
    const identity = observationIdentity(rollup);
    const bucket = JSON.stringify([identity, rollup.bucketStart, rollup.firstSampleId]);
    if (buckets.has(bucket)) throw new Error("Duplicate observation summary.");
    buckets.add(bucket);
    identities.set(identity, (identities.get(identity) ?? 0) + 1);
  }
  if (
    identities.size > limits.series ||
    [...identities.values()].some((v) => v > limits.rollupsPerSeries)
  )
    throw new Error("Observation summaries exceed their identity bounds.");
  return rollups;
}
