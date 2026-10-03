import { expect, it } from "vitest";

import {
  analyzeObservations,
  contiguousObservations,
  forecastLag,
  observationInterval,
} from "../../src/features/kafka/contracts/observation-analysis";
import { observation, observationSeries } from "../support/observation-fixture";

const linear = (count = 9): ReturnType<typeof observation>[] =>
  Array.from({ length: count }, (_, i) => observation(i));
it("backtests rising, flat and falling known lag series before giving a bounded-horizon scenario", () => {
  for (const slope of [10, 0, -5]) {
    const samples = linear().map((s, i) => ({
      ...s,
      partitions: s.partitions.map((p) => ({
        ...p,
        lag: String(100 + i * slope),
        committedOffset: String(900 + i * (20 - slope)),
      })),
    }));
    const forecast = forecastLag(samples, samples.at(-1)!.observedAt);
    expect(forecast.state).toBe("ready");
    expect(forecast.estimate).toBeCloseTo(Math.max(0, 100 + 14 * slope));
    expect(forecast.backtestMae).toBeCloseTo(0);
    expect(forecast.lower).toBeLessThanOrEqual(forecast.estimate!);
    expect(forecast.upper).toBeGreaterThan(forecast.estimate!);
    expect(forecast.horizonSeconds).toBe(60);
    expect(forecast.evidence).toHaveLength(9);
  }
});
it("refuses projection across missing/stale data, resets, restarts, topology and group-state changes", () => {
  const samples = linear(),
    last = samples.at(-1)!;
  for (const changed of [
    { ...last, segmentId: "restart" },
    { ...last, observedAt: last.observedAt + 50_000, startedAt: last.startedAt + 50_000 },
    { ...last, groupCoverage: "unavailable" as const },
    { ...last, groupState: "rebalancing" },
    { ...last, partitions: last.partitions.map((p) => ({ ...p, committedOffset: "0" })) },
    { ...last, partitions: last.partitions.map((p) => ({ ...p, partition: 1 })) },
  ])
    expect(forecastLag([...samples.slice(0, -1), changed], changed.observedAt).state).toBe(
      "insufficient",
    );
  expect(forecastLag(samples, last.observedAt + 45_001).state).toBe("insufficient");
  expect(forecastLag(samples, last.observedAt - 1).state).toBe("insufficient");
  expect(forecastLag(samples.slice(0, 8), last.observedAt).state).toBe("insufficient");
  expect(contiguousObservations([last, last])).toHaveLength(1);
});
it("rejects a forecast whose held-out behavior no longer fits its training period", () => {
  const samples = linear().map((s, i) =>
    i < 6
      ? s
      : { ...s, partitions: s.partitions.map((p) => ({ ...p, lag: String(500 + i * 100) })) },
  );
  expect(forecastLag(samples, samples.at(-1)!.observedAt).reason).toContain("unstable");
});
it("distinguishes ordinary fixture jitter, a spike and a sustained baseline change without classifying gaps", () => {
  const baseline = linear(13).map((s, i) => ({ ...s, requestMs: [19, 20, 21][i % 3]! }));
  for (let end = 9; end <= baseline.length; end++) {
    const result = analyzeObservations(
      observationSeries(baseline.slice(0, end)),
      baseline[end - 1]!.observedAt,
    );
    expect(result.anomalies.find((a) => a.metric === "request-time")?.state).toBe("ordinary");
  }
  const spike = [...baseline, observation(13, { requestMs: 200 })];
  expect(
    analyzeObservations(observationSeries(spike), spike.at(-1)!.observedAt).anomalies.find(
      (a) => a.metric === "request-time",
    )?.state,
  ).toBe("anomaly");
  const step = [...spike, observation(14, { requestMs: 200 }), observation(15, { requestMs: 200 })];
  expect(
    analyzeObservations(observationSeries(step), step.at(-1)!.observedAt).anomalies.find(
      (a) => a.metric === "request-time",
    )?.state,
  ).toBe("baseline-change");
  const restarted = [...spike, observation(14, { segmentId: "new" })];
  expect(
    analyzeObservations(observationSeries(restarted), restarted.at(-1)!.observedAt).anomalies.every(
      (a) => a.state === "baseline",
    ),
  ).toBe(true);
  expect(
    analyzeObservations(observationSeries(step), step.at(-1)!.observedAt + 50_000).anomalies,
  ).toEqual([]);
});
it("reports distribution from complete bounded evidence and treats offset rates as positions, not messages", () => {
  const first = observation(0),
    second = observation(1);
  const partitions = (growth: number): typeof first.partitions =>
    [0, 1].map((partition) => ({
      ...first.partitions[0]!,
      partition,
      endOffset: String(1000 + (partition === 0 ? growth : 0)),
    }));
  const samples = [
    { ...first, partitions: partitions(0) },
    { ...second, partitions: partitions(100) },
  ];
  const result = analyzeObservations(observationSeries(samples), second.observedAt);
  expect(result.interval?.appendedPerSecond).toBe(10);
  expect(result.skew).toMatchObject({ maximumShare: 1, total: 100, suspected: true });
  expect(result.hints.find((h) => h.title === "Uneven partition growth")?.evidence).toEqual([
    first.id,
    second.id,
  ]);
  const missing = {
    ...samples[1]!,
    partitions: samples[1]!.partitions.map((p) => ({ ...p, endOffset: null })),
  };
  expect(observationInterval(samples[0]!, missing)?.appendedPerSecond).toBeNull();
  expect(
    analyzeObservations(observationSeries([samples[0]!, missing]), second.observedAt).skew,
  ).toBeNull();
});
it("labels stalled commits and sampled group-state changes as hypotheses with traceable evidence", () => {
  const samples = linear(4).map((s, i) => ({
    ...s,
    groupState: i % 2 ? "preparing-rebalance" : "stable",
    partitions: s.partitions.map((p) => ({ ...p, committedOffset: "900" })),
  }));
  const result = analyzeObservations(observationSeries(samples), samples.at(-1)!.observedAt);
  expect(result.hints.find((h) => h.title === "Commits appear stalled")?.detail).toContain(
    "logs are required",
  );
  expect(
    result.hints.find((h) => h.title === "Observed group-state changes")?.evidence,
  ).toHaveLength(4);
  expect(
    result.hints.every((h) => h.evidence.every((id) => samples.some((s) => s.id === id))),
  ).toBe(true);
  expect(
    analyzeObservations(observationSeries(samples), samples.at(-1)!.observedAt + 50_000).hints,
  ).toEqual([]);
});
