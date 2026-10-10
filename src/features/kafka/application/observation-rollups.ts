import { observationContinuityBreak } from "../contracts/observation-continuity";
import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  observationLag,
  type KafkaObservation,
  type ObservationRollup,
  type ObservationRollupBoundary,
  type ObservationSeries,
} from "../contracts/observations";

/** Add actual measurements once; split on unverified continuity and never interpolate. */
export function appendObservationRollup(
  rollups: readonly ObservationRollup[],
  series: ObservationSeries,
  sample: KafkaObservation,
  before?: KafkaObservation,
): readonly ObservationRollup[] {
  const bucketStart = Math.floor(sample.observedAt / limits.rollupMs) * limits.rollupMs;
  const key = observationIdentity(series);
  const previous = rollups.filter((r) => observationIdentity(r) === key).at(-1);
  if (previous?.lastSampleId === sample.id) return rollups;
  let boundary: ObservationRollupBoundary = "initial";
  if (previous) {
    boundary =
      !before || before.id !== previous.lastSampleId
        ? "unverified"
        : (observationContinuityBreak(before, sample) ??
          (before.state === "partial" || sample.state === "partial" ? "incomplete" : null) ??
          (before.groupState !== sample.groupState ? "group-state" : null) ??
          "window");
  }
  const compatible =
    previous &&
    before?.id === previous.lastSampleId &&
    boundary === "window" &&
    previous.bucketStart === bucketStart;
  const lag = observationLag(sample);
  const next: ObservationRollup = compatible
    ? {
        ...previous,
        lastObservedAt: sample.observedAt,
        samples: previous.samples + 1,
        partial: previous.partial + (sample.state === "partial" ? 1 : 0),
        lagKnown: previous.lagKnown + (lag === null ? 0 : 1),
        lagMin: lag === null ? previous.lagMin : Math.min(previous.lagMin ?? lag, lag),
        lagMax: lag === null ? previous.lagMax : Math.max(previous.lagMax ?? lag, lag),
        lastLag: lag,
        requestMin: Math.min(previous.requestMin, sample.requestMs),
        requestMax: Math.max(previous.requestMax, sample.requestMs),
        lastRequest: sample.requestMs,
        lastSampleId: sample.id,
      }
    : {
        clusterId: series.clusterId,
        topicId: series.topicId,
        topic: series.topic,
        groupId: series.groupId,
        source: "kafka-api",
        bucketStart,
        bucketEnd: bucketStart + limits.rollupMs,
        firstObservedAt: sample.observedAt,
        lastObservedAt: sample.observedAt,
        samples: 1,
        partial: sample.state === "partial" ? 1 : 0,
        lagKnown: lag === null ? 0 : 1,
        lagMin: lag,
        lagMax: lag,
        lastLag: lag,
        requestMin: sample.requestMs,
        requestMax: sample.requestMs,
        lastRequest: sample.requestMs,
        boundary,
        firstSampleId: sample.id,
        lastSampleId: sample.id,
      };
  return [...rollups.filter((r) => !compatible || r !== previous), next];
}

/** Legacy history can summarize only the raw measurements actually present in that archive. */
export function summarizeLegacyObservations(
  series: readonly ObservationSeries[],
): readonly ObservationRollup[] {
  let rollups: readonly ObservationRollup[] = [];
  for (const resource of series)
    for (const [index, sample] of resource.samples.entries())
      rollups = appendObservationRollup(rollups, resource, sample, resource.samples[index - 1]);
  return rollups;
}
