import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  type ObservationHistory,
  type ObservationSeries,
  type RetainedObservationHistory,
  emptyObservationHistory,
} from "../contracts/observations";
import { parseObservationHistory } from "../contracts/observation-validation";

import { appendObservationRollup, summarizeLegacyObservations } from "./observation-rollups";

export interface ObservationStore {
  readonly durability: "session" | "durable";
  load(): Promise<ObservationHistory>;
  commit(history: ObservationHistory): Promise<void>;
  /** Explicit discard only, after the host has stopped and joined original work. */
  clear?(): Promise<void>;
}
export class MemoryObservationStore implements ObservationStore {
  readonly durability = "session" as const;
  private value: ObservationHistory = emptyObservationHistory();
  load(): Promise<ObservationHistory> {
    return Promise.resolve(structuredClone(this.value));
  }
  commit(history: ObservationHistory): Promise<void> {
    this.value = parseObservationHistory(structuredClone(history));
    return Promise.resolve();
  }
}
export function retainObservations(
  history: ObservationHistory,
  now: number,
  next?: ObservationSeries,
): RetainedObservationHistory {
  let series = history.series.filter(
    (s) => !next || observationIdentity(s) !== observationIdentity(next),
  );
  if (next) series = [...series, next];
  // Future timestamps remain bounded historical evidence after clock rollback; never current.
  series = series
    .map((s) => ({
      ...s,
      samples: s.samples.filter((v) => v.observedAt >= now - limits.retentionMs),
    }))
    .filter((s) => s.samples.length > 0)
    .sort((a, b) => (a.samples.at(-1)?.observedAt ?? 0) - (b.samples.at(-1)?.observedAt ?? 0));
  let rollups = history.schemaVersion === 1 ? summarizeLegacyObservations(series) : history.rollups;
  if (history.schemaVersion === 2 && next?.samples.at(-1))
    rollups = appendObservationRollup(rollups, next, next.samples.at(-1)!, next.samples.at(-2));
  // Drop a whole bucket when any of its measurements has expired. No partial bucket is invented.
  rollups = rollups.filter((r) => r.firstObservedAt >= now - limits.retentionMs);
  const resources = new Map<string, number>();
  for (const item of [
    ...series.map((s) => ({ resource: s, at: s.samples.at(-1)!.observedAt })),
    ...rollups.map((r) => ({ resource: r, at: r.lastObservedAt })),
  ]) {
    const key = observationIdentity(item.resource);
    resources.set(key, Math.max(resources.get(key) ?? 0, item.at));
  }
  const retained = new Set(
    [...resources]
      .sort((a, b) => a[1] - b[1])
      .slice(-limits.series)
      .map(([key]) => key),
  );
  series = series
    .filter((s) => retained.has(observationIdentity(s)))
    .map((s) => ({ ...s, samples: s.samples.slice(-limits.samples) }));
  const counts = new Map<string, number>();
  rollups = [...rollups]
    .sort((a, b) => b.lastObservedAt - a.lastObservedAt)
    .filter((r) => {
      const key = observationIdentity(r),
        count = (counts.get(key) ?? 0) + 1;
      counts.set(key, count);
      return retained.has(key) && count <= limits.rollupsPerSeries;
    })
    .reverse();
  const document = (): RetainedObservationHistory => ({
    schemaVersion: 2,
    series,
    settings: history.schemaVersion === 2 ? history.settings : null,
    rollups,
  });
  while (new TextEncoder().encode(JSON.stringify(document())).length > limits.fileBytes - 1) {
    const rawCount = series.reduce((count, s) => count + s.samples.length, 0);
    if (rawCount > 1) {
      const oldest = series.reduce((a, s) =>
        s.samples[0]!.observedAt < a.samples[0]!.observedAt ? s : a,
      );
      series = series
        .map((s) => (s === oldest ? { ...s, samples: s.samples.slice(1) } : s))
        .filter((s) => s.samples.length > 0);
    } else if (rollups.length) rollups = rollups.slice(1);
    else throw new Error("An observation exceeds the active history byte bound.");
  }
  return document();
}
