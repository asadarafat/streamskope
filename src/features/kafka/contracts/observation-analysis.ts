import {
  OBSERVATION_LIMITS as limits,
  observationLag,
  type KafkaObservation,
  type ObservationSeries,
} from "./observations";

export interface ObservationInterval {
  readonly start: KafkaObservation;
  readonly end: KafkaObservation;
  readonly seconds: number;
  readonly appendedPerSecond: number | null;
  readonly committedPerSecond: number | null;
  readonly partitionAppends: readonly { readonly partition: number; readonly positions: number }[];
}
export interface LagForecast {
  readonly state: "ready" | "insufficient";
  readonly reason: string;
  readonly horizonSeconds: number;
  readonly estimate: number | null;
  readonly lower: number | null;
  readonly upper: number | null;
  readonly slope: number | null;
  readonly backtestMae: number | null;
  readonly evidence: readonly string[];
}
export interface ObservationAnomaly {
  readonly metric: "offset-growth" | "request-time" | "record-size";
  readonly unit: string;
  readonly state: "baseline" | "anomaly" | "ordinary" | "baseline-change";
  readonly observed: number | null;
  readonly median: number | null;
  readonly threshold: number | null;
  readonly samples: number;
  readonly evidence: readonly string[];
}
export interface ObservationHint {
  readonly title: string;
  readonly detail: string;
  readonly evidence: readonly string[];
}
export interface ObservationAnalysis {
  readonly fresh: boolean;
  readonly contiguousSamples: number;
  readonly interval: ObservationInterval | null;
  readonly forecast: LagForecast;
  readonly anomalies: readonly ObservationAnomaly[];
  readonly hints: readonly ObservationHint[];
  readonly skew: {
    readonly source: string;
    readonly maximumShare: number;
    readonly total: number;
    readonly partitions: number;
    readonly suspected: boolean;
  } | null;
  readonly hotKey: {
    readonly share: number;
    readonly known: number;
    readonly total: number;
    readonly suspected: boolean;
  } | null;
}
function topology(sample: KafkaObservation): string {
  return sample.partitions
    .map((p) => p.partition)
    .sort((a, b) => a - b)
    .join(",");
}
function difference(before: string | null, after: string | null): number | null {
  if (before === null || after === null) return null;
  const d = BigInt(after) - BigInt(before);
  return d >= 0n && d <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(d) : null;
}
/** A failed read, restart, reset, topology change or clock gap ends the usable suffix. */
export function contiguousObservations(
  samples: readonly KafkaObservation[],
): readonly KafkaObservation[] {
  let start = samples.length - 1;
  for (; start > 0; start--) {
    const a = samples[start - 1]!,
      b = samples[start]!;
    if (
      a.segmentId !== b.segmentId ||
      b.observedAt <= a.observedAt ||
      b.startedAt - a.startedAt < limits.intervalMs ||
      b.observedAt - a.observedAt > limits.staleMs ||
      topology(a) !== topology(b)
    )
      break;
    if (
      b.partitions.some((p) => {
        const old = a.partitions.find((v) => v.partition === p.partition)!;
        return (
          (old.endOffset !== null &&
            p.endOffset !== null &&
            BigInt(p.endOffset) < BigInt(old.endOffset)) ||
          (old.committedOffset !== null &&
            p.committedOffset !== null &&
            BigInt(p.committedOffset) < BigInt(old.committedOffset))
        );
      })
    )
      break;
  }
  return samples.slice(Math.max(0, start));
}
export function observationInterval(
  a: KafkaObservation,
  b: KafkaObservation,
): ObservationInterval | null {
  if (contiguousObservations([a, b]).length !== 2) return null;
  const seconds = (b.observedAt - a.observedAt) / 1000;
  const values = b.partitions.map((p) => {
    const previous = a.partitions.find((v) => v.partition === p.partition)!;
    return {
      partition: p.partition,
      appended: difference(previous.endOffset, p.endOffset),
      committed: difference(previous.committedOffset, p.committedOffset),
    };
  });
  const sum = (field: "appended" | "committed"): number | null => {
    if (values.some((p) => p[field] === null)) return null;
    const value = values.reduce((n, p) => n + p[field]!, 0);
    return Number.isSafeInteger(value) ? value / seconds : null;
  };
  return {
    start: a,
    end: b,
    seconds,
    appendedPerSecond: sum("appended"),
    committedPerSecond:
      a.groupCoverage === "complete" && b.groupCoverage === "complete" ? sum("committed") : null,
    partitionAppends: values.flatMap((p) =>
      p.appended === null ? [] : [{ partition: p.partition, positions: p.appended }],
    ),
  };
}
function mean(values: readonly number[]): number {
  return values.reduce((n, v) => n + v, 0) / values.length;
}
function median(values: readonly number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length % 2
    ? sorted[Math.floor(sorted.length / 2)]!
    : (sorted[sorted.length / 2 - 1]! + sorted[sorted.length / 2]!) / 2;
}
function fit(values: readonly { x: number; y: number }[]): {
  predict(x: number): number;
  slope: number;
} {
  const mx = mean(values.map((v) => v.x)),
    my = mean(values.map((v) => v.y));
  const denominator = values.reduce((n, v) => n + (v.x - mx) ** 2, 0);
  const slope =
    denominator === 0 ? 0 : values.reduce((n, v) => n + (v.x - mx) * (v.y - my), 0) / denominator;
  return { slope, predict: (x) => my + slope * (x - mx) };
}
export function forecastLag(samples: readonly KafkaObservation[], now: number): LagForecast {
  const unavailable = (reason: string): LagForecast => ({
    state: "insufficient",
    reason,
    horizonSeconds: 60,
    estimate: null,
    lower: null,
    upper: null,
    slope: null,
    backtestMae: null,
    evidence: [],
  });
  const recent = contiguousObservations(samples).slice(-20),
    last = recent.at(-1);
  if (!last || now < last.observedAt || now - last.observedAt > limits.staleMs)
    return unavailable("Recent evidence is missing or stale.");
  if (last.groupState === null)
    return unavailable(
      "Consumer group state is unavailable; comparable lag observations cannot be established.",
    );
  // Use only a contiguous suffix of complete lag observations in the current group state.
  let start = recent.length - 1;
  while (
    start >= 0 &&
    observationLag(recent[start]!) !== null &&
    recent[start]!.groupState === last.groupState
  )
    start--;
  const usable = recent.slice(start + 1);
  if (usable.length < 9 || usable.at(-1)!.observedAt - usable[0]!.observedAt < 60_000)
    return unavailable(
      "Need nine continuous, complete lag samples over at least one minute in the same group state.",
    );
  const firstAt = usable[0]!.observedAt;
  const points = usable.map((s) => ({ x: (s.observedAt - firstAt) / 1000, y: observationLag(s)! }));
  const training = points.slice(0, -3),
    holdout = points.slice(-3),
    validation = fit(training);
  const errors = holdout.map((p) => Math.abs(p.y - validation.predict(p.x))),
    mae = mean(errors);
  if (mae > Math.max(5, mean(points.map((p) => p.y)) * 0.2))
    return unavailable(
      "Recent held-out errors exceed 20% of mean lag (minimum five positions); workload is unstable.",
    );
  const model = fit(points),
    estimate = Math.max(0, model.predict(points.at(-1)!.x + 60));
  const residuals = points.map((p) => Math.abs(p.y - model.predict(p.x)));
  const spread =
    Math.max(1, Math.max(...errors) + 3 * median(residuals)) * (1 + 60 / points.at(-1)!.x);
  if (
    ![estimate, spread].every((n) => Number.isFinite(n) && n <= Number.MAX_SAFE_INTEGER) ||
    estimate + spread > Number.MAX_SAFE_INTEGER
  )
    return unavailable("Projection exceeds the safe numeric range.");
  return {
    state: "ready",
    reason:
      "A 60-second linear scenario if the observed workload continues. Range scales held-out error and residuals; it is not a statistical confidence interval or a processing guarantee.",
    horizonSeconds: 60,
    estimate,
    lower: Math.max(0, estimate - spread),
    upper: estimate + spread,
    slope: model.slope,
    backtestMae: mae,
    evidence: usable.map((s) => s.id),
  };
}
interface Point {
  readonly value: number | null;
  readonly id: string;
}
function anomaly(
  metric: ObservationAnomaly["metric"],
  unit: string,
  input: readonly Point[],
  floor: number,
): ObservationAnomaly {
  const last = input.at(-1),
    recent = input.slice(-13),
    previous = recent.slice(0, -1);
  const result = {
    metric,
    unit,
    observed: last?.value ?? null,
    median: null,
    threshold: null,
    samples: previous.length,
    evidence: recent.map((p) => p.id),
  };
  if (previous.length < 8 || recent.some((p) => p.value === null))
    return { ...result, state: "baseline" };
  const values = previous.map((p) => p.value!),
    center = median(values),
    deviation = median(values.map((v) => Math.abs(v - center)));
  const threshold = Math.max(floor, Math.abs(center) * 0.5, 6 * deviation);
  const outside = Math.abs(last!.value! - center) > threshold;
  // Repeated step changes are labelled a changing baseline, not repeated incident discoveries.
  const older = input
    .slice(-16, -3)
    .filter((p) => p.value !== null)
    .map((p) => p.value!);
  const olderCenter = older.length >= 8 ? median(older) : null;
  const olderThreshold =
    olderCenter === null
      ? null
      : Math.max(
          floor,
          Math.abs(olderCenter) * 0.5,
          6 * median(older.map((v) => Math.abs(v - olderCenter))),
        );
  const step =
    olderCenter !== null &&
    olderThreshold !== null &&
    input
      .slice(-3)
      .every((p) => p.value !== null && Math.abs(p.value - olderCenter) > olderThreshold);
  return {
    ...result,
    median: center,
    threshold,
    state: step ? "baseline-change" : outside ? "anomaly" : "ordinary",
  };
}
export function analyzeObservations(series: ObservationSeries, now: number): ObservationAnalysis {
  const recent = contiguousObservations(series.samples).slice(-24),
    latest = recent.at(-1);
  const fresh =
    latest !== undefined && now >= latest.observedAt && now - latest.observedAt <= limits.staleMs;
  const intervals = recent.slice(1).map((s, i) => observationInterval(recent[i]!, s));
  const interval = fresh ? (intervals.at(-1) ?? null) : null;
  const hints: ObservationHint[] = [];
  const evidence = recent.slice(-3).map((s) => s.id);
  if (fresh && latest) {
    if (latest.members === 0 && observationLag(latest) !== null && observationLag(latest)! > 0)
      hints.push({
        title: "No visible consumer members",
        detail:
          "The selected group has lag and no visible members. Check whether the application is intentionally stopped; this does not prove a consumer failure.",
        evidence: [latest.id],
      });
    if (
      interval?.appendedPerSecond !== null &&
      interval?.committedPerSecond !== null &&
      interval &&
      interval.appendedPerSecond > interval.committedPerSecond &&
      observationLag(latest) !== null
    )
      hints.push({
        title: "Append positions outpace commits",
        detail:
          "Lag growth is consistent with commits advancing more slowly than new offset positions. Check consumer capacity and commit policy; offsets do not measure successful processing.",
        evidence: [interval.start.id, interval.end.id],
      });
    if (
      recent.length >= 3 &&
      recent.slice(-3).every((s) => observationLag(s) !== null && observationLag(s)! > 0) &&
      intervals.slice(-2).every((v) => v?.committedPerSecond === 0)
    )
      hints.push({
        title: "Commits appear stalled",
        detail:
          "Lag remains while committed positions do not advance. A stopped/slow consumer, batching or a poison record are possible; consumer logs are required to distinguish them.",
        evidence,
      });
    const transitions = recent
      .slice(1)
      .filter(
        (s, i) =>
          s.groupState !== null &&
          recent[i]!.groupState !== null &&
          s.groupState !== recent[i]!.groupState,
      ).length;
    if (transitions >= 2)
      hints.push({
        title: "Observed group-state changes",
        detail:
          "Repeated sampled state changes may accompany rebalances. Ten-second sampling can miss transitions and cannot establish a rebalance storm or its cause.",
        evidence: recent.map((s) => s.id),
      });
    if (latest.partitions.some((p) => p.leader === null || p.inSyncReplicas < p.replicas))
      hints.push({
        title: "Replication evidence needs attention",
        detail:
          "Selected-topic metadata reports a missing leader or fewer in-sync than assigned replicas. Check broker availability and replication metrics; CPU/disk causes are unknown.",
        evidence: [latest.id],
      });
  }
  const appends = interval?.partitionAppends ?? [],
    total = appends.reduce((n, p) => n + p.positions, 0);
  const share = total > 0 ? Math.max(...appends.map((p) => p.positions)) / total : 0;
  const skew =
    appends.length >= 2 &&
    appends.length === latest?.partitions.length &&
    total >= 20 &&
    Number.isSafeInteger(total)
      ? {
          source: "Kafka end-offset position changes",
          maximumShare: share,
          total,
          partitions: appends.length,
          suspected: share >= 0.8 && share >= 1.5 / appends.length,
        }
      : null;
  if (skew?.suspected && interval)
    hints.push({
      title: "Uneven partition growth",
      detail:
        "At least 80% of observed offset-position growth is concentrated in one partition and exceeds 1.5 times the uniform share. Investigate key distribution and partitioning; compaction/control records can affect positions.",
      evidence: [interval.start.id, interval.end.id],
    });
  const records = fresh ? latest?.records : null;
  const hotKey =
    records &&
    records.analysisEligible === true &&
    records.unavailableKeys === 0 &&
    records.knownKeys >= 20
      ? {
          share: (records.topKeys[0]?.count ?? 0) / records.knownKeys,
          known: records.knownKeys,
          total: records.count,
          suspected: (records.topKeys[0]?.count ?? 0) / records.knownKeys >= 0.5,
        }
      : null;
  // Keep latency baselines separate when optional record sampling changes the work performed.
  const comparable = latest
    ? recent.slice(
        recent
          .map(
            (s) =>
              Boolean(s.records) === Boolean(latest.records) &&
              (s.records?.endTimeMs ?? 0) - (s.records?.startTimeMs ?? 0) ===
                (latest.records?.endTimeMs ?? 0) - (latest.records?.startTimeMs ?? 0),
          )
          .lastIndexOf(false) + 1,
      )
    : [];
  const anomalies = fresh
    ? [
        anomaly(
          "offset-growth",
          "offset positions/s",
          intervals.map((v, i) => ({ value: v?.appendedPerSecond ?? null, id: recent[i + 1]!.id })),
          1,
        ),
        anomaly(
          "request-time",
          "client elapsed ms",
          comparable.map((s) => ({ value: s.requestMs, id: s.id })),
          10,
        ),
        anomaly(
          "record-size",
          "sample mean bytes",
          comparable.map((s) => ({
            value:
              s.records?.analysisEligible === true && s.records.count >= 20
                ? s.records.meanBytes
                : null,
            id: s.id,
          })),
          32,
        ),
      ]
    : [];
  return {
    fresh,
    contiguousSamples: recent.length,
    interval,
    forecast: forecastLag(series.samples, now),
    anomalies,
    hints,
    skew,
    hotKey,
  };
}
