import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  type ObservationHistory,
  type ObservationSeries,
} from "../contracts/observations";
import { parseObservationHistory } from "../contracts/observation-validation";

export interface ObservationStore {
  readonly durability: "session" | "durable";
  load(): Promise<ObservationHistory>;
  commit(history: ObservationHistory): Promise<void>;
}
export class MemoryObservationStore implements ObservationStore {
  readonly durability = "session" as const;
  private value: ObservationHistory = { schemaVersion: 1, series: [] };
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
): ObservationHistory {
  let series = history.series.filter(
    (s) => !next || observationIdentity(s) !== observationIdentity(next),
  );
  if (next) series = [...series, next];
  series = series
    .map((s) => ({
      ...s,
      samples: s.samples
        .filter((v) => v.observedAt >= now - limits.retentionMs && v.observedAt <= now)
        .slice(-limits.samples),
    }))
    .filter((s) => s.samples.length > 0)
    .sort((a, b) => (a.samples.at(-1)?.observedAt ?? 0) - (b.samples.at(-1)?.observedAt ?? 0))
    .slice(-limits.series);
  while (
    new TextEncoder().encode(JSON.stringify({ schemaVersion: 1, series })).length >
    limits.fileBytes - 1
  ) {
    const oldest = series.reduce((a, s) =>
      s.samples[0]!.observedAt < a.samples[0]!.observedAt ? s : a,
    );
    series = series
      .map((s) => (s === oldest ? { ...s, samples: s.samples.slice(1) } : s))
      .filter((s) => s.samples.length > 0);
  }
  return { schemaVersion: 1, series };
}
