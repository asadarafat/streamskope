import { exactKeys, nullableText, record, text, truth } from "./validation-primitives";
import { nullableObservationNumber } from "./observation-values";
import type { ObservationInput } from "./observations";

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
      lag: nullableObservationNumber(t.lag, Number.MAX_SAFE_INTEGER),
      requestMs: nullableObservationNumber(t.requestMs, 60_000),
    },
  };
}
