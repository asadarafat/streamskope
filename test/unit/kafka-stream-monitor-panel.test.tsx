// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ThemeProvider } from "@mui/material";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  KAFKA_MESSAGE_LIMITS,
  type HostError,
  type KafkaStreamMonitorSnapshot,
} from "../../src/kafka/contracts";
import { StreamMonitorPanel } from "../../src/kafka/ui/StreamMonitorPanel";
import type {
  RendererStreamMonitorObserver,
  RendererStreamMonitorSnapshot,
} from "../../src/kafka/ui/stream-monitor-observer";
import { streamSkopeTheme } from "../../src/ui/createStreamSkopeTheme";
import { streamSkopeTypography } from "../../src/ui/typographyContract";

const unavailableRenderer: RendererStreamMonitorSnapshot = {
  eventBacklog: 0,
  eventToCommitMs: null,
  filterDurationMs: null,
  fps: null,
  history: [],
  rendererDroppedMessages: 0,
  renderDurationMs: null,
  retainedMessages: 0,
  sampledAt: null,
  samplingState: "unavailable",
  visibleMessages: 0,
};

const measuredRenderer: RendererStreamMonitorSnapshot = {
  eventBacklog: 1,
  eventToCommitMs: 2.5,
  filterDurationMs: 0.25,
  fps: 58,
  history: [],
  rendererDroppedMessages: 1,
  renderDurationMs: 4.75,
  retainedMessages: 8,
  sampledAt: "2026-07-26T12:00:01.000Z",
  samplingState: "ready",
  visibleMessages: 7,
};

function observer(snapshot: RendererStreamMonitorSnapshot): RendererStreamMonitorObserver {
  return {
    commit: vi.fn(),
    dispose: vi.fn(),
    eventReceived: vi.fn(),
    getSnapshot: () => snapshot,
    recordFilterDuration: vi.fn(),
    recordRenderDuration: vi.fn(),
    setPresentationActive: vi.fn(),
    subscribe: () => () => undefined,
  };
}

function hostSnapshot(
  overrides: Partial<KafkaStreamMonitorSnapshot> = {},
): KafkaStreamMonitorSnapshot {
  return {
    connectionName: "Local aio-kafka",
    delivery: {
      batchCount: 2,
      batchSize: 200,
      deliveredMessages: 8,
      historySamples: 50,
      intervalMs: 20,
      lastBatchMessages: 3,
      messagesPerSecond: 25.5,
      publicationDurationMs: 0.75,
      queueWaitMs: 1.25,
      receivedMessages: 10,
      tuningSource: "confirmed",
    },
    queue: {
      capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
      capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
      currentBytes: 2_048,
      currentMessages: 2,
      droppedMessages: 0,
      droppedPerSecond: 0,
      droppedSincePrevious: 0,
      peakBytes: 4_096,
      peakMessages: 3,
    },
    request: {
      maxMessages: 1_000,
      mode: "tail",
      topic: "orders.events",
    },
    sampledAt: "2026-07-26T12:00:00.000Z",
    state: "streaming",
    status: "nominal",
    ...overrides,
  };
}

const unavailableHost: KafkaStreamMonitorSnapshot = {
  connectionName: null,
  delivery: null,
  queue: null,
  request: null,
  sampledAt: null,
  state: "unavailable",
  status: "unavailable",
};

const consumptionFailure: HostError = {
  activeStateChanged: true,
  code: "BROKER_UNREACHABLE",
  correlationId: "correlation-monitor",
  recovery: "Verify the broker route and retry consumption.",
  retryable: true,
  stage: "broker",
  summary: "The broker closed the fetch connection.",
  target: "orders.events",
};

interface RenderPanelResult {
  readonly onOpenActivity: ReturnType<typeof vi.fn<() => void>>;
}

function renderPanel({
  activeConnectionName = "Local aio-kafka",
  current = hostSnapshot(),
  error = null,
  history = [],
  renderer = measuredRenderer,
}: {
  readonly activeConnectionName?: string | null;
  readonly current?: KafkaStreamMonitorSnapshot;
  readonly error?: HostError | null;
  readonly history?: readonly KafkaStreamMonitorSnapshot[];
  readonly renderer?: RendererStreamMonitorSnapshot;
} = {}): RenderPanelResult {
  const onOpenActivity = vi.fn<() => void>();
  render(
    <ThemeProvider theme={streamSkopeTheme}>
      <StreamMonitorPanel
        activeConnectionName={activeConnectionName}
        consumptionError={error}
        history={history}
        onOpenActivity={onOpenActivity}
        rendererObserver={observer(renderer)}
        selectedTopic="orders.events"
        snapshot={current}
      />
    </ThemeProvider>,
  );
  return { onOpenActivity };
}

afterEach(() => {
  cleanup();
});

describe("Material UI Stream Monitor", () => {
  it("shows one selected-topic empty state without unavailable metric rows or duplicate navigation", () => {
    renderPanel({
      current: unavailableHost,
      renderer: unavailableRenderer,
    });

    expect(screen.getByRole("heading", { name: "Stream Monitor" })).toBeVisible();
    expect(screen.queryByText("Live telemetry", { exact: true })).not.toBeInTheDocument();
    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Unavailable",
    );
    expect(screen.getByText("No live sample for orders.events.")).toBeVisible();
    expect(screen.getAllByText("Unavailable")).toHaveLength(1);
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Open topics" })).not.toBeInTheDocument();
  });

  it.each([
    {
      expected: "Sampling",
      name: "loading",
      snapshot: hostSnapshot({
        delivery: null,
        queue: null,
        sampledAt: null,
        state: "loading",
        status: "idle",
      }),
    },
    {
      expected: "Idle",
      name: "idle",
      snapshot: hostSnapshot({
        delivery: {
          batchCount: 0,
          batchSize: 200,
          deliveredMessages: 0,
          historySamples: 50,
          intervalMs: 20,
          lastBatchMessages: 0,
          messagesPerSecond: null,
          publicationDurationMs: null,
          queueWaitMs: null,
          receivedMessages: 0,
          tuningSource: "confirmed",
        },
        queue: {
          capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
          capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
          currentBytes: 0,
          currentMessages: 0,
          droppedMessages: 0,
          droppedPerSecond: null,
          droppedSincePrevious: 0,
          peakBytes: 0,
          peakMessages: 0,
        },
        state: "streaming",
        status: "idle",
      }),
    },
    { expected: "Nominal", name: "nominal", snapshot: hostSnapshot() },
    {
      expected: "Backpressure",
      name: "backpressure",
      snapshot: hostSnapshot({
        delivery: {
          ...hostSnapshot().delivery!,
          deliveredMessages: 7,
        },
        queue: {
          ...hostSnapshot().queue!,
          droppedMessages: 1,
          droppedPerSecond: 2,
          droppedSincePrevious: 1,
        },
        status: "backpressure",
      }),
    },
    {
      expected: "Degraded",
      name: "degraded",
      snapshot: hostSnapshot({
        delivery: null,
        queue: null,
        state: "failed",
        status: "degraded",
      }),
    },
    {
      expected: "Complete · Nominal",
      name: "terminal",
      snapshot: hostSnapshot({ state: "complete" }),
    },
    {
      expected: "Stale",
      name: "stale",
      snapshot: hostSnapshot({ state: "stale", status: "stale" }),
    },
  ])("labels $name evidence honestly", ({ expected, snapshot }) => {
    renderPanel({
      current: snapshot,
      renderer:
        snapshot.status === "nominal"
          ? { ...measuredRenderer, rendererDroppedMessages: 0 }
          : unavailableRenderer,
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      expected,
    );
  });

  it("shows exact current host and renderer measurements before bounded history", () => {
    const history = Array.from({ length: 15 }, (_unused, index) =>
      hostSnapshot({
        sampledAt: new Date(Date.UTC(2026, 6, 26, 12, 0, index)).toISOString(),
      }),
    );
    const rendererHistory = Array.from({ length: 15 }, (_unused, index) => ({
      ...measuredRenderer,
      sampledAt: new Date(Date.UTC(2026, 6, 26, 12, 1, index)).toISOString(),
    }));
    renderPanel({
      history,
      renderer: { ...measuredRenderer, history: rendererHistory },
    });

    expect(screen.getByRole("region", { name: /Queue depth trend/u })).toHaveAccessibleName(
      /12 samples.*latest 2 messages/u,
    );
    expect(screen.getByRole("region", { name: /Delivery rate trend/u })).toHaveAccessibleName(
      /12 samples.*latest 25.5 msg\/s/u,
    );
    expect(screen.getByRole("region", { name: /Renderer work trend/u })).toHaveAccessibleName(
      /Event to commit.*12 samples.*2.5 ms/u,
    );
    const overview = screen.getByLabelText("Current stream measurements");
    const queueTable = screen.getByLabelText("Host queue metrics");
    expect(
      overview.compareDocumentPosition(queueTable) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();

    expect(screen.getByLabelText("Host queue metrics")).toHaveTextContent("2 / 1,000 messages");
    expect(screen.getByLabelText("Host queue metrics")).toHaveTextContent("2 KiB / 16 MiB");
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent("25.5 msg/s");
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent("0.75 ms");
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent(
      "Confirmed preferences",
    );
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent(
      "Effective batch200 messages",
    );
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent(
      "Shaping interval20 ms",
    );
    expect(screen.getByLabelText("Host delivery metrics")).toHaveTextContent(
      "History limit50 samples",
    );
    expect(screen.getByLabelText("Renderer metrics")).toHaveTextContent("2.5 ms");
    expect(screen.getByLabelText("Renderer metrics")).toHaveTextContent("58 FPS");
    expect(screen.getByRole("table", { name: "Recent host samples" })).not.toHaveTextContent(
      "12:00:02",
    );
    expect(screen.getByRole("table", { name: "Recent host samples" })).toHaveTextContent(
      "12:00:14",
    );
    const hostSamples = screen.getByRole("table", { name: "Recent host samples" });
    const timeHeader = within(hostSamples).getByRole("columnheader", { name: "Sample" });
    const latestHostSample = within(hostSamples).getByText(/12:00:14/u);
    const compactFontSize = `${String(
      streamSkopeTypography.roles.compact.size / streamSkopeTypography.rootSize,
    )}rem`;
    expect(getComputedStyle(timeHeader).fontFamily).toContain("system-ui");
    expect(getComputedStyle(timeHeader).fontSize).toBe(compactFontSize);
    expect(getComputedStyle(latestHostSample).fontFamily).toContain("system-ui");
    expect(getComputedStyle(latestHostSample).fontSize).toBe(compactFontSize);
    expect(screen.getByRole("table", { name: "Recent renderer samples" })).not.toHaveTextContent(
      "12:01:02",
    );
    expect(screen.getByRole("table", { name: "Recent renderer samples" })).toHaveTextContent(
      "12:01:14",
    );
  });

  it("nests every evidence body under one named section with explicit key and value rows", () => {
    renderPanel({
      history: [hostSnapshot()],
      renderer: { ...measuredRenderer, history: [measuredRenderer] },
    });

    for (const sectionName of ["Context", "Host queue", "Delivery", "Renderer"]) {
      const section = screen.getByRole("region", { name: sectionName });
      const terms = section.querySelectorAll("dt");
      const definitions = section.querySelectorAll("dd");

      expect(terms.length).toBeGreaterThan(0);
      expect(definitions).toHaveLength(terms.length);
      expect(within(section).queryByRole("table")).not.toBeInTheDocument();
    }

    const context = screen.getByRole("region", { name: "Context" });
    expect(context.querySelector("dt")?.textContent).toBe("Active cluster");
    expect(context.querySelector("dd")?.textContent).toBe("Local aio-kafka");

    for (const sectionName of ["Recent host samples", "Recent renderer samples"]) {
      const section = screen.getByRole("region", { name: sectionName });
      expect(within(section).getByRole("table", { name: sectionName })).toBeVisible();
    }
  });

  it("identifies renderer loss as backpressure without hiding the affected stage", () => {
    renderPanel();

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Backpressure",
    );
    expect(screen.getByText("Renderer evictions confirm record loss.")).toBeVisible();
    expect(screen.getByLabelText("Renderer metrics")).toHaveTextContent("1 message");
  });

  it("identifies measured slow renderer work as backpressure instead of nominal health", () => {
    renderPanel({
      renderer: {
        ...measuredRenderer,
        renderDurationMs: 25,
        rendererDroppedMessages: 0,
      },
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Backpressure",
    );
    expect(screen.getByText("Message workspace render work exceeded 24 ms.")).toBeVisible();
    expect(screen.getByLabelText("Renderer metrics")).toHaveTextContent("25 ms");
  });

  it.each([
    {
      explanation: "Renderer event backlog reached its 512-event observation bound.",
      renderer: { eventBacklog: 512 },
    },
    {
      explanation: "Host-event-to-commit time exceeded 120 ms.",
      renderer: { eventToCommitMs: 121 },
    },
    {
      explanation: "Message filtering work exceeded 24 ms.",
      renderer: { filterDurationMs: 25 },
    },
  ])("identifies bounded $explanation pressure evidence", (scenario) => {
    renderPanel({
      renderer: {
        ...measuredRenderer,
        rendererDroppedMessages: 0,
        ...scenario.renderer,
      },
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Backpressure",
    );
    expect(screen.getByText(scenario.explanation)).toBeVisible();
  });

  it("does not call the exact 24 ms renderer boundary pressure", () => {
    renderPanel({
      renderer: {
        ...measuredRenderer,
        eventBacklog: 0,
        eventToCommitMs: 120,
        filterDurationMs: 24,
        renderDurationMs: 24,
        rendererDroppedMessages: 0,
      },
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Nominal",
    );
  });

  it("retains terminal lifecycle while naming prior renderer pressure", () => {
    renderPanel({
      current: hostSnapshot({ state: "stopped" }),
      renderer: {
        ...measuredRenderer,
        eventToCommitMs: 121,
        rendererDroppedMessages: 0,
      },
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Stopped · Backpressure",
    );
    expect(
      screen.getByText(
        "The stopped operation is no longer sampling. Last renderer evidence: Host-event-to-commit time exceeded 120 ms.",
      ),
    ).toBeVisible();
  });

  it("does not claim nominal health before visible renderer sampling is complete", () => {
    renderPanel({
      renderer: {
        ...measuredRenderer,
        fps: null,
        rendererDroppedMessages: 0,
        samplingState: "sampling",
      },
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      "Sampling",
    );
    expect(
      screen.getByText("Host delivery is current; visible-frame sampling is still in progress."),
    ).toBeVisible();
  });

  it.each([
    {
      explanation: "Host delivery is current; renderer frame evidence is unavailable.",
      label: "Renderer unavailable",
      samplingState: "unavailable" as const,
    },
    {
      explanation: "Host delivery is current; renderer frame sampling is paused while hidden.",
      label: "Renderer hidden",
      samplingState: "hidden" as const,
    },
  ])("labels $samplingState renderer evidence instead of nominal health", (scenario) => {
    renderPanel({
      renderer: {
        ...measuredRenderer,
        fps: null,
        rendererDroppedMessages: 0,
        samplingState: scenario.samplingState,
      },
    });

    expect(screen.getByRole("status", { name: "Stream monitor status" })).toHaveTextContent(
      scenario.label,
    );
    expect(screen.getByText(scenario.explanation)).toBeVisible();
  });

  it("states nominal status once instead of repeating decorative status text", () => {
    renderPanel({
      renderer: { ...measuredRenderer, rendererDroppedMessages: 0 },
    });

    expect(screen.getAllByText("Nominal")).toHaveLength(1);
    expect(screen.getByRole("heading", { level: 3, name: "Trends" })).toBeVisible();
  });

  it("keeps exact failure and recovery evidence in Activity", async () => {
    const user = userEvent.setup();
    const { onOpenActivity } = renderPanel({
      current: hostSnapshot({
        delivery: null,
        queue: null,
        state: "failed",
        status: "degraded",
      }),
      error: consumptionFailure,
      renderer: unavailableRenderer,
    });

    expect(screen.getByText(consumptionFailure.summary)).toBeVisible();
    expect(screen.getByText(consumptionFailure.recovery)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Open activity" }));
    expect(onOpenActivity).toHaveBeenCalledOnce();
  });

  it("retains the evidence owner when its active connection has ended", () => {
    renderPanel({
      activeConnectionName: null,
      current: hostSnapshot({
        connectionName: "Previous production cluster",
        state: "stale",
        status: "stale",
      }),
      renderer: unavailableRenderer,
    });

    expect(screen.getByLabelText("Stream context")).toHaveTextContent(
      "Previous production cluster",
    );
  });
});
