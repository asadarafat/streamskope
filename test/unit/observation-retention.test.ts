import { expect, it } from "vitest";

import { retainObservations } from "../../src/features/kafka/application/observation-store";
import { parseObservationHistory } from "../../src/features/kafka/contracts/observation-validation";
import {
  OBSERVATION_LIMITS as limits,
  emptyObservationHistory,
  observationResources,
} from "../../src/features/kafka/contracts/observations";
import { OBSERVED_AT, observation, observationSeries } from "../support/observation-fixture";

it("summarizes only present measurements, keeps unknown lag distinct from zero and never invents a rate", () => {
  const samples = [
    observation(0, { groupCoverage: "unavailable", state: "partial" }),
    observation(1),
    observation(2, { partitions: [{ ...observation(2).partitions[0]!, lag: "0" }] }),
  ];
  const history = retainObservations(
    { schemaVersion: 1, series: [observationSeries(samples)] },
    samples[2]!.observedAt,
  );
  expect(history.rollups).toMatchObject([
    {
      samples: 1,
      partial: 1,
      lagKnown: 0,
      lagMin: null,
      lagMax: null,
      lastLag: null,
      boundary: "initial",
    },
    {
      samples: 2,
      partial: 0,
      lagKnown: 2,
      lagMin: 0,
      lagMax: 110,
      lastLag: 0,
      boundary: "incomplete",
      firstSampleId: "sample-1",
      lastSampleId: "sample-2",
    },
  ]);
  expect(JSON.stringify(history.rollups)).not.toMatch(/rate|forecast|estimate/);
  expect(parseObservationHistory(history)).toMatchObject({
    schemaVersion: 2,
    rollups: history.rollups,
    settings: null,
  });
});

it.each([
  ["restart", observation(1, { segmentId: "new-process" })],
  ["clock-gap", observation(6)],
  [
    "topology",
    observation(1, {
      partitions: [
        ...observation(1).partitions,
        { ...observation(1).partitions[0]!, partition: 1 },
      ],
    }),
  ],
  [
    "offset-reset",
    observation(1, {
      partitions: [
        { ...observation(1).partitions[0]!, endOffset: "10", committedOffset: "0", lag: "10" },
      ],
    }),
  ],
  ["incomplete", observation(1, { state: "partial" })],
  ["group-state", observation(1, { groupState: "empty" })],
] as const)("starts a separate summary at %s within the same clock window", (boundary, sample) => {
  const history = retainObservations(
    { schemaVersion: 1, series: [observationSeries([observation(0), sample])] },
    sample.observedAt,
  );
  expect(history.rollups).toHaveLength(2);
  expect(history.rollups[1]).toMatchObject({ samples: 1, boundary, firstSampleId: sample.id });
  expect(history.rollups[0]!.bucketStart).toBe(history.rollups[1]!.bucketStart);
  expect(() => parseObservationHistory(history)).not.toThrow();
});

it("appends a capture once, splits a new window and refuses to infer continuity without original raw evidence", () => {
  const samples = [observation(0), observation(1), observation(30)];
  let history = emptyObservationHistory();
  history = retainObservations(history, samples[0]!.observedAt, observationSeries([samples[0]!]));
  history = retainObservations(
    history,
    samples[1]!.observedAt,
    observationSeries(samples.slice(0, 2)),
  );
  expect(history.rollups[0]!.samples).toBe(2);
  expect(
    retainObservations(history, samples[1]!.observedAt, observationSeries(samples.slice(0, 2))),
  ).toEqual(history);
  history = retainObservations(history, samples[2]!.observedAt, observationSeries([samples[2]!]));
  expect(history.rollups[1]).toMatchObject({ samples: 1, boundary: "unverified" });
  expect(history.rollups[1]!.bucketStart).toBe(OBSERVED_AT + limits.rollupMs);
});

it("expires whole summaries and raw observations without erasing bounded future evidence after clock rollback", () => {
  const settings = {
    input: {
      topic: "events",
      groupId: "workers",
      thresholds: { lag: 10, requestMs: 25 },
      sampleRecords: true,
    },
    connectionName: "Fixture",
    clusterId: "fixture-cluster",
    topicId: "fixture-topic",
    savedAt: OBSERVED_AT + 20,
  };
  const first = retainObservations(
    { schemaVersion: 1, series: [observationSeries([observation(0), observation(1)])] },
    OBSERVED_AT + 10020,
  );
  const saved = { ...first, settings };
  expect(retainObservations(saved, OBSERVED_AT - 1000)).toEqual(saved);
  const expired = retainObservations(saved, OBSERVED_AT + 10021 + limits.retentionMs);
  expect(expired).toEqual({ ...emptyObservationHistory(), settings });
});

it("bounds raw and summary resources together and retains summary-only resources for inspection", () => {
  let history = emptyObservationHistory();
  for (let i = 0; i < 12; i++)
    history = retainObservations(history, observation(i).observedAt, {
      ...observationSeries([observation(i)]),
      topicId: `topic-${i}`,
      topic: `events-${i}`,
    });
  expect(observationResources(history)).toHaveLength(8);
  expect(observationResources({ ...history, series: [] })).toHaveLength(8);
  expect(observationResources(history).map((r) => r.topic)).not.toContain("events-0");
  expect(() => parseObservationHistory(history)).not.toThrow();
});

it("bounds independent summary segments to 288 and raw samples to 240", () => {
  const samples = Array.from({ length: 400 }, (_, i) =>
    observation(i, { segmentId: `segment-${i}` }),
  );
  const retained = retainObservations(
    { schemaVersion: 1, series: [observationSeries(samples)] },
    samples.at(-1)!.observedAt,
  );
  expect(retained.series[0]!.samples).toHaveLength(240);
  expect(retained.rollups).toHaveLength(288);
  expect(retained.rollups[0]!.firstSampleId).toBe("sample-112");
  expect(Buffer.byteLength(JSON.stringify(retained))).toBeLessThan(limits.fileBytes);
});

it("rejects inconsistent summary evidence, duplicate segments and unsupported secret-bearing settings", () => {
  const history = retainObservations(
    { schemaVersion: 1, series: [observationSeries([observation(0)])] },
    OBSERVED_AT + 20,
  );
  const row = history.rollups[0]!;
  for (const changes of [
    { lagKnown: 0 },
    { lastLag: 200 },
    { samples: 2 },
    { bucketEnd: row.bucketEnd + 1 },
    { partial: 2 },
    { boundary: "assumed-healthy" },
  ])
    expect(() =>
      parseObservationHistory({ ...history, rollups: [{ ...row, ...changes }] }),
    ).toThrow();
  expect(() => parseObservationHistory({ ...history, rollups: [row, row] })).toThrow();
  expect(() =>
    parseObservationHistory({ ...history, settings: { input: {}, password: "secret" } }),
  ).toThrow();
});

it("keeps the newest actual measurement and direct summaries within the byte bound for a full-partition history", () => {
  const samples = Array.from({ length: limits.samples }, (_, i) => {
    const sample = observation(i);
    return {
      ...sample,
      partitions: Array.from({ length: limits.partitions }, (_, partition) => ({
        ...sample.partitions[0]!,
        partition,
        endOffset: String(9_000_000_000_000_000_000n + BigInt(1000 + i * 20)),
        committedOffset: String(9_000_000_000_000_000_000n + BigInt(900 + i * 10)),
      })),
    };
  });
  const source = { schemaVersion: 1 as const, series: [observationSeries(samples)] };
  expect(Buffer.byteLength(JSON.stringify(source))).toBeGreaterThan(limits.fileBytes);
  const retained = retainObservations(source, samples.at(-1)!.observedAt);
  expect(Buffer.byteLength(JSON.stringify(retained))).toBeLessThan(limits.fileBytes);
  expect(retained.series[0]!.samples.length).toBeLessThan(samples.length);
  expect(retained.series[0]!.samples.at(-1)).toEqual(samples.at(-1));
  expect(retained.rollups.reduce((count, row) => count + row.samples, 0)).toBe(samples.length);
  expect(() => parseObservationHistory(retained)).not.toThrow();
});
