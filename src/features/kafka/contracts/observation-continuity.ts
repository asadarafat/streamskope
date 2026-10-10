import { OBSERVATION_LIMITS as limits, type KafkaObservation } from "./observations";

export type ObservationContinuityBreak =
  "restart" | "clock-gap" | "cooldown" | "topology" | "offset-reset";

/** Shared comparability rules for raw analysis and retained measurement summaries. */
export function observationContinuityBreak(
  before: KafkaObservation,
  after: KafkaObservation,
): ObservationContinuityBreak | null {
  if (before.segmentId !== after.segmentId) return "restart";
  if (
    after.observedAt <= before.observedAt ||
    after.observedAt - before.observedAt > limits.staleMs
  )
    return "clock-gap";
  if (after.startedAt - before.startedAt < limits.intervalMs) return "cooldown";
  const old = new Map(before.partitions.map((p) => [p.partition, p]));
  if (old.size !== after.partitions.length || after.partitions.some((p) => !old.has(p.partition)))
    return "topology";
  if (
    after.partitions.some((p) => {
      const previous = old.get(p.partition)!;
      return ["endOffset", "committedOffset"].some((key) => {
        const field = key as "endOffset" | "committedOffset";
        return (
          previous[field] !== null &&
          p[field] !== null &&
          BigInt(p[field]) < BigInt(previous[field])
        );
      });
    })
  )
    return "offset-reset";
  return null;
}
