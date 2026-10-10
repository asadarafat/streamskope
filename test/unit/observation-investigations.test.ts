import { expect, it } from "vitest";

import {
  findingEvidence,
  prepareObservationInvestigation,
} from "../../src/features/kafka/ui/observation-investigations";
import { observation, observationSeries } from "../support/observation-fixture";

it("resolves only actual unique support and refuses stale, future, missing and duplicate IDs", () => {
  const series = observationSeries([observation(0), observation(1)]);
  const last = series.samples[1]!;
  expect(findingEvidence(series, ["sample-0", "sample-1"], last.observedAt)).toMatchObject({
    complete: true,
    fresh: true,
    firstAt: series.samples[0]!.observedAt,
    lastAt: last.observedAt,
  });
  for (const ids of [[], ["absent"], ["sample-0", "absent"], ["sample-1", "sample-1"]])
    expect(findingEvidence(series, ids, last.observedAt)).toMatchObject({
      complete: false,
      fresh: false,
    });
  expect(findingEvidence(series, [last.id], last.observedAt - 1).fresh).toBe(false);
  expect(findingEvidence(series, [last.id], last.observedAt + 45_001).fresh).toBe(false);
});

it("prepares bounded unloaded topic controls from the actual comparable segment without mutating evidence", () => {
  const old = observation(0),
    next = observation(1, { segmentId: "restart" });
  const series = observationSeries([old, next]);
  const original = JSON.stringify(series);
  const prepared = prepareObservationInvestigation(series, "topic");
  expect(prepared.resource).toBe("events · messages");
  expect(prepared.settings.configuration?.request).toEqual({
    topic: "events",
    mode: "time-window",
    maxMessages: 100,
    startTimeMs: next.startedAt,
    endTimeMs: next.observedAt,
  });
  expect(prepared.settings.view.destination).toEqual({ kind: "topic", workspace: "messages" });
  expect(prepared.settings.records).toEqual({ selected: null, comparison: null, bookmarks: [] });
  expect(JSON.stringify(series)).toBe(original);
  expect(prepared).not.toHaveProperty("clusterId");
  expect(prepared).not.toHaveProperty("profileId");
});

it("preserves a sampled record's declared window without claiming its partial contents are complete", () => {
  const sample = observation(0, {
    records: {
      source: "protected-kafka-record-sample",
      startTimeMs: observation(0).startedAt - 1000,
      endTimeMs: observation(0).startedAt,
      state: "partial",
      reason: "limit",
      analysisEligible: false,
      partitionCoverage: { expected: 1, completed: 0 },
      count: 1,
      bytes: 20,
      meanBytes: 20,
      p95Bytes: 20,
      knownKeys: 1,
      nullKeys: 0,
      unavailableKeys: 0,
      distinctKeys: 1,
      topKeys: [{ count: 1, partition: 0, offset: "0" }],
      partitions: [{ partition: 0, count: 1 }],
    },
  });
  const prepared = prepareObservationInvestigation(observationSeries([sample]), "topic");
  expect(prepared.settings.configuration?.request).toMatchObject({
    startTimeMs: sample.records!.startTimeMs,
    endTimeMs: sample.records!.endTimeMs,
  });
  expect(prepared.settings.records.bookmarks).toEqual([]);
  expect(prepared.settings.configuration).not.toHaveProperty("records");
});

it("prepares a standalone group and refuses absent resources without inventing a topic", () => {
  const prepared = prepareObservationInvestigation(
    observationSeries([observation(0)]),
    "consumer-group",
  );
  expect(prepared.settings.configuration).toBeNull();
  expect(prepared.settings.view.destination).toEqual({
    kind: "consumer-group",
    groupId: "workers",
  });
  expect(() => prepareObservationInvestigation(observationSeries([]), "topic")).toThrow(/capture/);
  expect(() =>
    prepareObservationInvestigation(
      { ...observationSeries([observation(0)]), groupId: null },
      "consumer-group",
    ),
  ).toThrow(/choose a consumer group/);
});

it("refuses support across a reset, restart or clock gap while preserving the new segment", () => {
  const first = observation(0);
  for (const next of [
    observation(1, { segmentId: "restart" }),
    observation(1, { observedAt: first.observedAt + 50_000, startedAt: first.startedAt + 50_000 }),
    observation(1, {
      partitions: [{ ...observation(1).partitions[0]!, committedOffset: "0", lag: "1020" }],
    }),
  ]) {
    const series = observationSeries([first, next]);
    expect(findingEvidence(series, [first.id, next.id], next.observedAt).complete).toBe(false);
    expect(findingEvidence(series, [next.id], next.observedAt)).toMatchObject({
      complete: true,
      fresh: true,
    });
  }
});
