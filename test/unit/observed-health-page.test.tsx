// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { StrictMode } from "react";
import {
  act,
  cleanup,
  fireEvent,
  render,
  renderHook,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { HostCommand, HostError, StreamSkopeHost } from "../../src/features/kafka/contracts";
import type {
  ObservationSeries,
  ObservationSnapshot,
} from "../../src/features/kafka/contracts/observations";
import { analyzeObservations } from "../../src/features/kafka/contracts/observation-analysis";
import {
  ObservedHealthPage,
  type ObservedHealthPageProperties,
} from "../../src/features/kafka/ui/ObservedHealthPage";
import { ObservationPartitionTable } from "../../src/features/kafka/ui/ObservationPartitionTable";
import { ObservationAnalysisPanel } from "../../src/features/kafka/ui/ObservationAnalysisPanel";
import { ObservationSummary } from "../../src/features/kafka/ui/ObservationSummary";
import { useObservedHealth } from "../../src/features/kafka/ui/use-observed-health";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";
import { observation, observationSeries } from "../support/observation-fixture";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

interface ObservationFixture {
  readonly host: StreamSkopeHost;
  readonly commands: HostCommand[];
  readonly setHistoryError: (error?: HostError) => void;
  readonly setCaptureError: (error?: HostError) => void;
  readonly deferCapture: (wait: () => Promise<void>) => void;
}
function fixture(initial: readonly ObservationSeries[] = []): ObservationFixture {
  const commands: HostCommand[] = [];
  let series = [...initial];
  let historyError: HostError | undefined;
  let captureError: HostError | undefined;
  let pending: (() => Promise<void>) | undefined;
  let count = 0;
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
    execute: testHostExecute(async (command) => {
      commands.push(command);
      const base = { command: command.command, id: command.id, version: command.version };
      const snapshot: ObservationSnapshot = { schemaVersion: 1, durability: "session", series };
      if (command.command === "observations.history")
        return historyError
          ? { ...base, ok: false, error: historyError }
          : { ...base, ok: true, result: { correlationId: "c", snapshot } };
      if (command.command === "observations.capture") {
        if (pending) await pending();
        if (captureError) return { ...base, ok: false, error: captureError };
        const sample = observation(count++, {
          startedAt: Date.now(),
          observedAt: Date.now(),
          id: `capture-${count}`,
        });
        const value = observationSeries([...(series[0]?.samples ?? []), sample]);
        series = [value];
        return {
          ...base,
          ok: true,
          result: { correlationId: "c", capture: { durability: "session", series: value } },
        };
      }
      if (command.command === "observations.clear")
        return {
          ...base,
          ok: true,
          result: { correlationId: "c", snapshot: { ...snapshot, series: [] } },
        };
      return { ...base, ok: true, result: { correlationId: "c" } };
    }),
  };
  return {
    host,
    commands,
    setHistoryError: (error?: HostError): void => {
      historyError = error;
    },
    setCaptureError: (error?: HostError): void => {
      captureError = error;
    },
    deferCapture: (wait: () => Promise<void>): void => {
      pending = wait;
    },
  };
}
function show(
  f: ReturnType<typeof fixture>,
  props: Partial<ObservedHealthPageProperties> = {},
): ReturnType<typeof render> {
  return render(
    <StreamSkopeThemeProvider>
      <ObservedHealthPage
        host={f.host}
        topics={["events", "orders"]}
        connectionName="Test Kafka"
        onOpenTopic={vi.fn()}
        onOpenGroup={vi.fn()}
        {...props}
      />
    </StreamSkopeThemeProvider>,
  );
}
async function ready(): Promise<void> {
  await act(async () => {
    if (vi.isFakeTimers()) await vi.advanceTimersByTimeAsync(0);
    else await Promise.resolve();
  });
}
function select(): void {
  fireEvent.change(screen.getByLabelText("Observed topic"), { target: { value: "events" } });
  fireEvent.change(screen.getByLabelText("Observed consumer group (optional)"), {
    target: { value: "workers" },
  });
}
const timeout: HostError = {
  code: "TIMEOUT",
  stage: "kafka",
  correlationId: "c",
  retryable: true,
  activeStateChanged: false,
  summary: "The Kafka observation timed out.",
  recovery: "Check the broker endpoint and retry.",
};

it("keeps measured summary values outside the document heading outline", () => {
  const now = Date.now();
  const series = observationSeries([observation(0, { startedAt: now, observedAt: now })]);
  render(
    <StreamSkopeThemeProvider>
      <ObservationSummary
        series={series}
        current
        fresh
        connectionName="Test Kafka"
        analysis={analyzeObservations(series, now)}
      />
    </StreamSkopeThemeProvider>,
  );
  const summary = within(screen.getByRole("region", { name: "Observation summary" }));
  expect(summary.getByText("100")).toBeVisible();
  expect(summary.getAllByRole("heading")).toHaveLength(1);
  expect(summary.getByRole("heading", { level: 2, name: "events" })).toBeVisible();
});

it("rejects out-of-range collection settings before issuing a request or entering cooldown", async () => {
  const f = fixture();
  show(f);
  await ready();
  select();
  fireEvent.click(screen.getByRole("button", { name: "History and collection settings" }));
  fireEvent.change(screen.getByLabelText("Request time alert threshold, ms (optional)"), {
    target: { value: "60001" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Capture observation" }));
  expect(screen.getByText("Request time threshold must be between zero and 60,000.")).toBeVisible();
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    0,
  );
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeEnabled();
});

it("opens the sampled partition and exact offset with its original bounded window and blocks stale reads", () => {
  const now = Date.now();
  const sample = observation(0, {
    startedAt: now,
    observedAt: now,
    records: {
      source: "protected-kafka-record-sample",
      startTimeMs: now - 60_000,
      endTimeMs: now,
      state: "complete",
      reason: "range-complete",
      count: 20,
      bytes: 40,
      meanBytes: 2,
      p95Bytes: 2,
      knownKeys: 20,
      nullKeys: 0,
      unavailableKeys: 0,
      distinctKeys: 1,
      topKeys: [{ count: 20, partition: 0, offset: "12" }],
      partitions: [{ partition: 0, count: 20 }],
      analysisEligible: true,
      partitionCoverage: { expected: 1, completed: 1 },
    },
  });
  const series = observationSeries([sample]);
  const onOpenRecord = vi.fn();
  const view = render(
    <StreamSkopeThemeProvider>
      <ObservationAnalysisPanel series={series} fresh onOpenRecord={onOpenRecord} />
    </StreamSkopeThemeProvider>,
  );
  fireEvent.click(
    screen.getByRole("button", { name: "Find sampled record partition 0 offset 12" }),
  );
  expect(onOpenRecord).toHaveBeenCalledWith({
    topic: "events",
    partition: 0,
    offset: "12",
    startTimeMs: now - 60_000,
    endTimeMs: now,
  });
  view.rerender(
    <StreamSkopeThemeProvider>
      <ObservationAnalysisPanel series={series} fresh={false} onOpenRecord={onOpenRecord} />
    </StreamSkopeThemeProvider>,
  );
  expect(
    screen.getByRole("button", { name: "Find sampled record partition 0 offset 12" }),
  ).toBeDisabled();
});

it("does not collect on entry, requires explicit clear confirmation and cancels on navigation", async () => {
  const f = fixture();
  const view = show(f);
  await waitFor(() => expect(f.commands).toHaveLength(1));
  expect(f.commands[0]?.command).toBe("observations.history");
  fireEvent.click(screen.getByRole("button", { name: "History and collection settings" }));
  expect(screen.getByRole("button", { name: "Clear all observation history" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Clear all history confirmation"), {
    target: { value: "CLEAR HISTORY" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Clear all observation history" }));
  await waitFor(() =>
    expect(f.commands.some((command) => command.command === "observations.clear")).toBe(true),
  );
  expect(f.commands.some((command) => command.command === "observations.capture")).toBe(false);
  view.unmount();
  expect(f.commands.at(-1)?.command).toBe("observations.cancel");
});

it("shows measured lag and blocks an immediate second capture with a visible cooldown", async () => {
  vi.useFakeTimers();
  const f = fixture();
  show(f);
  await ready();
  select();
  fireEvent.click(screen.getByRole("button", { name: "Capture observation" }));
  await ready();
  expect(
    within(screen.getByRole("region", { name: "Observation summary" })).getByText("100"),
  ).toBeVisible();
  expect(screen.getByText("Recent evidence")).toBeVisible();
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeDisabled();
  expect(screen.getByRole("status", { name: "Observation collection status" })).toHaveTextContent(
    "Next capture available in 10 seconds",
  );
  fireEvent.click(screen.getByRole("button", { name: "Capture observation" }));
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    1,
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(9_000);
  });
  expect(screen.getByRole("status", { name: "Observation collection status" })).toHaveTextContent(
    "Next capture available in 1 second",
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(999);
  });
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeDisabled();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1);
  });
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeEnabled();
});

it("owns one absolute clock deadline only while cooldown or freshness can change", async () => {
  vi.useFakeTimers();
  const f = fixture();
  const { result } = renderHook(() => useObservedHealth(f.host));
  await ready();
  expect(vi.getTimerCount()).toBe(0);
  await act(async () => {
    await result.current.capture({
      topic: "events",
      groupId: "workers",
      thresholds: { lag: null, requestMs: null },
    });
  });
  expect(result.current.cooldownSeconds).toBe(10);
  expect(vi.getTimerCount()).toBe(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10_000);
  });
  expect(result.current.cooldownSeconds).toBe(0);
  expect(result.current.fresh).toBe(true);
  expect(vi.getTimerCount()).toBe(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(35_000);
  });
  expect(result.current.fresh).toBe(true);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1);
  });
  expect(result.current.fresh).toBe(false);
  expect(vi.getTimerCount()).toBe(0);
});

it("disposes its cooldown and collection deadlines on navigation without another capture", async () => {
  vi.useFakeTimers();
  const f = fixture();
  const { result, unmount } = renderHook(() => useObservedHealth(f.host));
  await ready();
  act(() => {
    result.current.start({
      topic: "events",
      groupId: "workers",
      thresholds: { lag: null, requestMs: null },
    });
  });
  await ready();
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    1,
  );
  expect(vi.getTimerCount()).toBe(2);
  unmount();
  expect(vi.getTimerCount()).toBe(0);
  await vi.advanceTimersByTimeAsync(60_000);
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    1,
  );
});

it("runs only opted-in captures and stops the timer immediately", async () => {
  vi.useFakeTimers();
  const f = fixture();
  show(f);
  await ready();
  select();
  fireEvent.click(screen.getByRole("button", { name: "Start observing" }));
  await ready();
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    1,
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10_000);
  });
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    2,
  );
  fireEvent.click(screen.getByRole("button", { name: "Stop observing" }));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    2,
  );
  expect(f.commands.some((command) => command.command === "observations.cancel")).toBe(true);
});

it("separates retained evidence from this connection and disables stale drilldowns", async () => {
  vi.useFakeTimers();
  const now = Date.now();
  const retained = observationSeries([
    observation(0, { startedAt: now - 10_000, observedAt: now - 9_980 }),
  ]);
  const f = fixture([retained]);
  const onOpenGroup = vi.fn();
  show(f, { onOpenGroup });
  await ready();
  expect(screen.getByText("Retained evidence")).toBeVisible();
  expect(screen.getByRole("button", { name: "Inspect consumer group workers" })).toBeDisabled();
  fireEvent.click(screen.getByRole("button", { name: "Capture observation" }));
  await ready();
  expect(screen.getByRole("button", { name: "Inspect consumer group workers" })).toBeEnabled();
  fireEvent.click(screen.getByRole("button", { name: "Inspect consumer group workers" }));
  expect(onOpenGroup).toHaveBeenCalledWith("workers");
  await act(async () => {
    await vi.advanceTimersByTimeAsync(45_001);
  });
  expect(screen.getByText("Stale evidence")).toBeVisible();
  expect(screen.getByRole("button", { name: "Inspect consumer group workers" })).toBeDisabled();
  expect(
    screen.getByText("Refresh this connection’s evidence to evaluate current findings."),
  ).toBeVisible();
});

it("stops failed polling, keeps its previous measured evidence and offers a specific retry", async () => {
  vi.useFakeTimers();
  const f = fixture();
  show(f);
  await ready();
  select();
  fireEvent.click(screen.getByRole("button", { name: "Capture observation" }));
  await ready();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10_000);
  });
  f.setCaptureError(timeout);
  fireEvent.click(screen.getByRole("button", { name: "Start observing" }));
  await ready();
  expect(screen.getByText("The Kafka observation timed out.")).toBeVisible();
  expect(screen.getByText("Check the broker endpoint and retry.")).toBeVisible();
  expect(screen.getByRole("button", { name: "Stop observing" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Retry observation" })).toBeDisabled();
  expect(
    within(screen.getByRole("region", { name: "Observation summary" })).getByText("100"),
  ).toBeVisible();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20_000);
  });
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    2,
  );
  f.setCaptureError();
  fireEvent.click(screen.getByRole("button", { name: "Retry observation" }));
  await ready();
  expect(screen.queryByText("The Kafka observation timed out.")).not.toBeInTheDocument();
  expect(
    within(screen.getByRole("region", { name: "Observation summary" })).getByText("110"),
  ).toBeVisible();
});

it("does not publish a late capture after Stop", async () => {
  const f = fixture();
  let release!: () => void;
  f.deferCapture(
    () =>
      new Promise<void>((resolve) => {
        release = resolve;
      }),
  );
  show(f);
  await ready();
  select();
  fireEvent.click(screen.getByRole("button", { name: "Capture observation" }));
  await ready();
  expect(screen.getByRole("status", { name: "Observation collection status" })).toHaveTextContent(
    "Collecting observation",
  );
  fireEvent.click(screen.getByRole("button", { name: "Stop observing" }));
  await act(async () => {
    release();
    await Promise.resolve();
  });
  expect(screen.queryByRole("region", { name: "Observation summary" })).not.toBeInTheDocument();
  expect(screen.queryByText("Recent evidence")).not.toBeInTheDocument();
});

it("keeps capture disabled after unreadable history until an explicit reload succeeds", async () => {
  const f = fixture();
  f.setHistoryError({
    ...timeout,
    code: "OBSERVATION_HISTORY_UNAVAILABLE",
    stage: "storage",
    summary: "Observation history cannot be read.",
    recovery: "Restore the private history file or explicitly clear it.",
  });
  show(f);
  await ready();
  select();
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeDisabled();
  expect(screen.getByText("Observation history cannot be read.")).toBeVisible();
  f.setHistoryError();
  fireEvent.click(screen.getByRole("button", { name: "Reload history" }));
  await ready();
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeEnabled();
  expect(f.commands.filter((command) => command.command === "observations.capture")).toHaveLength(
    0,
  );
});

it("loads history under StrictMode and ignores the discarded mount request", async () => {
  const f = fixture();
  render(
    <StrictMode>
      <StreamSkopeThemeProvider>
        <ObservedHealthPage host={f.host} />
      </StreamSkopeThemeProvider>
    </StrictMode>,
  );
  await ready();
  select();
  expect(screen.getByRole("button", { name: "Capture observation" })).toBeEnabled();
  expect(f.commands.filter((command) => command.command === "observations.history")).toHaveLength(
    1,
  );
});

it("ranks replication problems above backlog and filters partitions without inventing zero lag", () => {
  const sample = observation(0, {
    partitions: [
      {
        partition: 0,
        leader: 1,
        replicas: 2,
        inSyncReplicas: 2,
        endOffset: "120",
        committedOffset: "120",
        lag: "0",
      },
      {
        partition: 1,
        leader: 1,
        replicas: 2,
        inSyncReplicas: 2,
        endOffset: "999999999999999999",
        committedOffset: "0",
        lag: "999999999999999999",
      },
      {
        partition: 2,
        leader: null,
        replicas: 2,
        inSyncReplicas: 1,
        endOffset: null,
        committedOffset: null,
        lag: null,
      },
    ],
  });
  render(
    <StreamSkopeThemeProvider>
      <ObservationPartitionTable sample={sample} />
    </StreamSkopeThemeProvider>,
  );
  const table = screen.getByRole("table", { name: "Observed partition positions" });
  expect(
    within(table)
      .getAllByRole("row")
      .slice(1)
      .map((row) => within(row).getAllByRole("cell")[0]?.textContent),
  ).toEqual(["2", "1", "0"]);
  fireEvent.click(screen.getByRole("checkbox", { name: "Only partitions with gaps or lag" }));
  expect(within(table).getAllByRole("row")).toHaveLength(3);
  expect(within(table).getAllByText("Unknown").length).toBeGreaterThan(0);
  fireEvent.change(screen.getByLabelText("Filter partitions"), {
    target: { value: "under-replicated" },
  });
  expect(within(table).getAllByRole("row")).toHaveLength(2);
  expect(
    within(table).getByText(
      "Leader unknown · Under-replicated · End position unknown · Lag unknown",
    ),
  ).toBeVisible();
});
