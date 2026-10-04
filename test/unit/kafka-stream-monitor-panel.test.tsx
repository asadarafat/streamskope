// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ThemeProvider } from "@mui/material";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  type HostError,
  type KafkaStreamMonitorSnapshot,
} from "../../src/features/kafka/contracts";
import {
  StreamMonitorPanel,
  type StreamMonitorPanelProperties,
} from "../../src/features/kafka/ui/StreamMonitorPanel";
import {
  createRendererStreamMonitorObserver,
  initialRendererStreamMonitorSample,
  type RendererStreamMonitorSnapshot,
} from "../../src/features/kafka/ui/stream-monitor-observer";
import { kafkaConsumptionStopLabel } from "../../src/features/kafka/ui/workbench-status";
import { streamSkopeTheme } from "../../src/platform/ui/createStreamSkopeTheme";

const now = Date.parse("2026-07-26T12:00:30.000Z");
const stamp = (seconds: number): string => new Date(now - seconds * 1_000).toISOString();
function hostSnapshot(
  overrides: Partial<KafkaStreamMonitorSnapshot> = {},
): KafkaStreamMonitorSnapshot {
  return {
    operationId: "request-1",
    connectionName: "Local Kafka",
    request: { topic: "orders", maxMessages: 1000, mode: "tail" },
    sampledAt: stamp(0),
    state: "streaming",
    status: "nominal",
    delivery: {
      batchCount: 2,
      batchSize: 200,
      publishedMessages: 8,
      historySamples: 50,
      intervalMs: 20,
      lastBatchMessages: 3,
      messagesPerSecond: 25.5,
      rateSampledAt: stamp(0),
      rateWindowMs: 1000,
      publicationDurationMs: 0.75,
      publicationSampledAt: stamp(3),
      queueWaitMs: 1.25,
      queueWaitSampledAt: stamp(3),
      receivedMessages: 10,
      tuningSource: "confirmed",
    },
    queue: {
      capacityBytes: 16_777_216,
      capacityMessages: 1000,
      currentBytes: 2048,
      currentMessages: 2,
      droppedMessages: 0,
      droppedPerSecond: 0,
      droppedSincePrevious: 0,
      oldestMessageAgeMs: 30,
      peakBytes: 4096,
      peakMessages: 3,
      pressureReasons: [],
      dropReasons: { countCapacity: 0, byteCapacity: 0, oversized: 0, terminalDiscarded: 0 },
    },
    ...overrides,
  };
}
const rendererSample: RendererStreamMonitorSnapshot = {
  ...initialRendererStreamMonitorSample,
  operationId: "request-1",
  messagesMounted: false,
  eventToCommitMs: 2.5,
  eventSampledAt: stamp(1),
  filterDurationMs: 0.25,
  filterSampledAt: stamp(5),
  renderDurationMs: 4.75,
  renderSampledAt: stamp(5),
  fps: 58,
  fpsSampledAt: stamp(0),
  fpsWindowMs: 1000,
  samplingState: "ready",
  sampledAt: stamp(0),
  rendererDroppedMessages: 1,
  rendererWindowEvictions: 10,
  retainedMessages: 8,
  visibleMessages: 7,
  history: [],
};
function renderPanel(
  overrides: Partial<StreamMonitorPanelProperties> = {},
  renderer = rendererSample,
): ReturnType<typeof render> & {
  readonly onOpenActivity: ReturnType<typeof vi.fn>;
  readonly onOpenObservedHealth: ReturnType<typeof vi.fn>;
  readonly onStop: ReturnType<typeof vi.fn>;
} {
  const observer = createRendererStreamMonitorObserver();
  vi.spyOn(observer, "getSnapshot").mockReturnValue(renderer);
  vi.spyOn(observer, "subscribe").mockImplementation(() => (): void => undefined);
  const actions = { onOpenActivity: vi.fn(), onOpenObservedHealth: vi.fn(), onStop: vi.fn() };
  const properties: StreamMonitorPanelProperties = {
    activeConnectionName: "Local Kafka",
    stopActionLabel: "Stop tail",
    consumptionStopping: false,
    consumptionError: null,
    history: [],
    rendererObserver: observer,
    selectedTopic: "orders",
    snapshot: hostSnapshot(),
    ...actions,
    ...overrides,
  };
  const view = render(
    <ThemeProvider theme={streamSkopeTheme}>
      <StreamMonitorPanel {...properties} />
    </ThemeProvider>,
  );
  return { ...actions, ...view };
}
beforeEach(() => {
  vi.spyOn(Date, "now").mockReturnValue(now);
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("operator Stream Monitor", () => {
  it("leads with delivery, buffering, historical omissions and freshness; technical evidence is collapsed", () => {
    renderPanel();
    expect(screen.getByLabelText("Current stream measurements")).toHaveTextContent(
      "Published rate25.5 msg/s",
    );
    expect(screen.getByLabelText("Current stream measurements")).toHaveTextContent("2 / 1,000");
    expect(screen.getByLabelText("Current stream measurements")).not.toHaveTextContent("FPS");
    expect(screen.getByLabelText("Current stream measurements")).not.toHaveTextContent("React");
    expect(screen.getByText(/Tail.*1,000 record limit/u)).toBeVisible();
    expect(screen.getByText("Diagnostics").closest("details")).not.toHaveAttribute("open");
    expect(screen.getByLabelText("Display omission reasons")).toHaveTextContent(
      "Renderer overload omissions1",
    );
    expect(screen.getByLabelText("Display omission reasons")).toHaveTextContent(
      "Display retention evictions10",
    );
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Delivering",
    );
  });
  it("explains unavailable evidence without manufactured metric rows", () => {
    renderPanel({
      stopActionLabel: null,
      snapshot: hostSnapshot({
        operationId: null,
        request: null,
        delivery: null,
        queue: null,
        sampledAt: null,
        state: "unavailable",
        status: "unavailable",
      }),
    });
    expect(screen.getByText("No live sample for orders.")).toBeVisible();
    expect(screen.queryByLabelText("Current stream measurements")).not.toBeInTheDocument();
  });
  it("exposes the existing stop callback and cluster observations", async () => {
    const actions = renderPanel();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Stop tail orders" }));
    await user.click(screen.getByRole("button", { name: "Observed health" }));
    expect(actions.onStop).toHaveBeenCalledOnce();
    expect(actions.onOpenObservedHealth).toHaveBeenCalledOnce();
  });
  it("supports cancellation while loading and disables duplicate stops", () => {
    renderPanel({
      consumptionStopping: true,
      stopActionLabel: "Cancel fetch",
      snapshot: hostSnapshot({
        state: "loading",
        request: { topic: "orders", maxMessages: 50, mode: "newest" },
      }),
    });
    expect(screen.getByRole("button", { name: "Cancel fetch orders" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Cancel fetch orders" })).toHaveTextContent(
      "Stopping…",
    );
  });
  it.each([
    { state: "complete", expected: "Complete" },
    { state: "stopped", expected: "Stopped" },
    { state: "failed", expected: "Failed" },
    { state: "stale", expected: "Stale evidence" },
  ] as const)("keeps $state lifecycle separate from historical losses", ({ state, expected }) => {
    const base = hostSnapshot();
    renderPanel({
      stopActionLabel: null,
      snapshot: {
        ...base,
        state,
        queue: {
          ...base.queue!,
          droppedMessages: 4,
          dropReasons: { countCapacity: 4, byteCapacity: 0, oversized: 0, terminalDiscarded: 0 },
        },
      },
    });
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      expected,
    );
    expect(screen.getByLabelText("Last stream measurements")).toHaveTextContent(
      "Last delivery rate",
    );
    expect(screen.queryByRole("button", { name: /Stop tail/u })).not.toBeInTheDocument();
  });
  it("names current transport pressure without turning historical omissions into pressure", () => {
    const base = hostSnapshot();
    renderPanel({
      snapshot: { ...base, queue: { ...base.queue!, pressureReasons: ["transport"] } },
    });
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Buffer pressure",
    );
    expect(screen.getByText("Display transport is paused")).toBeVisible();
  });
  it("advances historical age text from visible application frame observations", () => {
    let monotonicTime = 0;
    let frame: ((timestamp: number) => void) | undefined;
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => monotonicTime,
      wallNow: () => new Date(Date.now()),
      isDocumentVisible: () => true,
      requestFrame: (callback) => {
        frame = callback;
        return 1;
      },
      cancelFrame: () => undefined,
    });
    observer.setOperation("request-1");
    const view = renderPanel({
      snapshot: hostSnapshot({ state: "stopped" }),
      stopActionLabel: null,
      rendererObserver: observer,
    });
    act(() => {
      vi.mocked(Date.now).mockReturnValue(now + 1000);
      monotonicTime = 1000;
      frame?.(monotonicTime);
    });
    expect(screen.getByLabelText("Last stream measurements")).toHaveTextContent("Measured 1s ago");
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Stopped",
    );
    view.unmount();
    observer.dispose();
  });

  it("marks aged host evidence stale and preserves the last chart window", () => {
    const current = hostSnapshot({
      sampledAt: stamp(20),
      delivery: { ...hostSnapshot().delivery!, rateSampledAt: stamp(20) },
    });
    renderPanel({ snapshot: current });
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Stale evidence",
    );
    expect(screen.getByRole("region", { name: /^Delivery rate trend/u })).toHaveTextContent(
      "12:00:10",
    );
  });
  it("uses a shared time domain and all retained samples within it instead of twelve rows", async () => {
    const history = Array.from({ length: 20 }, (_, i) =>
      hostSnapshot({
        sampledAt: stamp(20 - i),
        delivery: { ...hostSnapshot().delivery!, rateSampledAt: stamp(20 - i) },
      }),
    );
    renderPanel({ history });
    const rate = screen.getByRole("region", { name: /^Delivery rate trend/u });
    const buffer = screen.getByRole("region", { name: /^Buffer depth trend/u });
    expect(rate).toHaveAccessibleName(/21 samples/u);
    expect(buffer).toHaveAccessibleName(/21 samples/u);
    for (const chart of [rate, buffer]) {
      expect(chart).toHaveTextContent("11:59:30");
      expect(chart).toHaveTextContent("12:00:30");
    }
    const plot = within(rate).getByRole("group", { name: "Delivery rate trend plot" });
    plot.focus();
    await userEvent.setup().keyboard("{End}");
    expect(within(rate).getByRole("status")).toHaveTextContent("25.5 msg/s");
  });
  it("keeps last render timings and their ages in Diagnostics, labels application FPS", async () => {
    renderPanel();
    await userEvent.setup().click(screen.getByText("Diagnostics"));
    expect(screen.getByText(/Messages workspace unmounted/u)).toBeVisible();
    const metrics = screen.getByLabelText("Renderer metrics");
    expect(metrics).toHaveTextContent("Last message workspace render4.75 ms · Measured 5s ago");
    expect(metrics).toHaveTextContent("Application frame rate58 FPS");
    expect(metrics).toHaveTextContent("Unavailable while Messages is unmounted");
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent(
      "Last host publication0.75 ms · Measured 3s ago",
    );
  });
  it("never combines a previous request's renderer losses or plots", () => {
    renderPanel(
      {},
      { ...rendererSample, operationId: "prior-request", rendererDroppedMessages: 99 },
    );
    expect(screen.getByLabelText("Display omission reasons")).not.toHaveTextContent("99");
  });
  it("retains the shared retry-stop action for a cleanup timeout", async () => {
    const error: HostError = {
      activeStateChanged: true,
      code: "TIMEOUT",
      correlationId: "cleanup",
      recovery: "Cleanup is still pending. Retry stop to wait again.",
      retryable: true,
      stage: "kafka",
      summary: "Stop cleanup timed out.",
      target: "kafka-consumption-cleanup",
    };
    const current = hostSnapshot({ state: "failed" });
    const actions = renderPanel({
      snapshot: current,
      consumptionError: error,
      stopActionLabel: kafkaConsumptionStopLabel("failed", current.request, error),
    });
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Failed",
    );
    expect(screen.getByText(/Cleanup is still pending/u)).toBeVisible();
    await userEvent.setup().click(screen.getByRole("button", { name: "Retry stop orders" }));
    expect(actions.onStop).toHaveBeenCalledOnce();
  });

  it("keeps failure recovery linked to Activity", async () => {
    const actions = renderPanel({
      snapshot: hostSnapshot({ state: "failed" }),
      stopActionLabel: null,
    });
    await userEvent.setup().click(screen.getByRole("button", { name: "Open activity" }));
    expect(actions.onOpenActivity).toHaveBeenCalledOnce();
  });
});
