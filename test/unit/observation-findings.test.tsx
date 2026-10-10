// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { analyzeObservations } from "../../src/features/kafka/contracts/observation-analysis";
import type { PreparedKafkaInvestigation } from "../../src/features/kafka/ui/investigation-view-settings";
import { ObservationFindings } from "../../src/features/kafka/ui/ObservationFindings";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { observation, observationSeries } from "../support/observation-fixture";

afterEach(cleanup);
const series = observationSeries([observation(0), observation(1, { members: 0 })]);
const now = series.samples[1]!.observedAt;
function show(overrides: Partial<Parameters<typeof ObservationFindings>[0]> = {}): {
  onSaveInvestigation: ReturnType<typeof vi.fn<(value: PreparedKafkaInvestigation) => void>>;
  onOpenTopic: ReturnType<typeof vi.fn<() => void>>;
} {
  const onSaveInvestigation = vi.fn<(value: PreparedKafkaInvestigation) => void>(),
    onOpenTopic = vi.fn();
  render(
    <StreamSkopeThemeProvider>
      <ObservationFindings
        series={series}
        analysis={analyzeObservations(series, now)}
        now={now}
        actionable
        settings={{
          input: {
            topic: series.topic,
            groupId: series.groupId,
            thresholds: { lag: 15, requestMs: 500 },
          },
          connectionName: "fixture",
          clusterId: series.clusterId,
          topicId: series.topicId,
          savedAt: now,
        }}
        onSaveInvestigation={onSaveInvestigation}
        onOpenTopic={onOpenTopic}
        {...overrides}
      />
    </StreamSkopeThemeProvider>,
  );
  return { onSaveInvestigation, onOpenTopic };
}

it("shows identities, exact time, sample counts, coverage and declared rules before resource actions", () => {
  const callbacks = show();
  const evidence = screen.getByRole("region", { name: "Observation evidence" });
  expect(evidence).toHaveTextContent("fixture-cluster");
  expect(evidence).toHaveTextContent("fixture-topic");
  expect(evidence).toHaveTextContent(new Date(now).toISOString());
  expect(evidence).toHaveTextContent("2 retained raw samples; 2 recent comparable samples");
  expect(evidence).toHaveTextContent("lag 1/1 known");
  expect(evidence).toHaveTextContent("Lag: 15 positions; collection time: 500 ms");
  expect(screen.getByText(/Known selected-topic lag above zero/)).toBeVisible();
  expect(screen.getByText(/Support IDs: sample-0, sample-1/)).toBeVisible();
  const save = screen.getByRole("button", { name: "Save topic investigation" });
  expect(evidence.compareDocumentPosition(save) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  fireEvent.click(save);
  expect(callbacks.onSaveInvestigation).toHaveBeenCalledOnce();
  expect(callbacks.onSaveInvestigation.mock.calls[0]![0].settings.records.bookmarks).toEqual([]);
  expect(callbacks.onOpenTopic).not.toHaveBeenCalled();
});

it.each([{ actionable: false }, { now: now + 45_001 }, { now: now - 1 }])(
  "disables resource actions for unverified, stale or future evidence: %j",
  (change) => {
    show(change);
    for (const button of screen.getAllByRole("button")) expect(button).toBeDisabled();
  },
);

it("refuses an unsupported finding rather than substituting available observations", () => {
  const analysis = analyzeObservations(series, now);
  show({
    analysis: { ...analysis, hints: [{ ...analysis.hints[0]!, evidence: ["not-retained"] }] },
  });
  expect(screen.getByText(/Supporting observations are unavailable/)).toBeVisible();
  expect(screen.getByText(/Evidence incomplete/)).toBeVisible();
  for (const button of screen.getAllByRole("button")) expect(button).toBeDisabled();
});

it("keeps unknown values and unmatched settings explicit", () => {
  const partial = observationSeries([
    observation(0, {
      members: null,
      groupCoverage: "unavailable",
      partitions: [{ ...observation(0).partitions[0]!, committedOffset: null, lag: null }],
    }),
  ]);
  show({
    series: partial,
    now: partial.samples[0]!.observedAt,
    analysis: analyzeObservations(partial, partial.samples[0]!.observedAt),
    settings: null,
  });
  const evidence = within(screen.getByRole("region", { name: "Observation evidence" }));
  expect(evidence.getByText(/lag 0\/1 known/)).toBeVisible();
  expect(evidence.getByText(/unknown visible members/)).toBeVisible();
  expect(evidence.getByText(/Captured settings unavailable/)).toBeVisible();
  expect(evidence.getByText(/No recorded record sample/)).toBeVisible();
  expect(
    screen.queryByRole("heading", { name: "No visible consumer members" }),
  ).not.toBeInTheDocument();
});
