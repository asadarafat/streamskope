// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type HostTextDocument,
  type ExternalUrlOpenResult,
  type KafkaLatencyHistorySnapshot,
  type KafkaOperationalPreferenceSnapshot,
  type KafkaLatencyProbeEvidence,
  type KafkaLatencySnapshot,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  LatencyWorkspace,
  type LatencyWorkspaceTransferPort,
} from "../../src/features/kafka/ui/LatencyWorkspace";

const request = {
  acknowledgements: -1,
  messageCount: 20,
  timeoutMs: 10_000,
  topic: "orders.events",
} as const;

const evidence: KafkaLatencyProbeEvidence = {
  acknowledgements: -1,
  completedAt: "2026-07-25T16:00:01.000Z",
  connection: { endpoint: "127.0.0.1:19093", name: "local-aio" },
  endToEnd: { averageMs: 5, p95Ms: 6, samples: 2 },
  fetch: {
    perBroker: [
      {
        broker: "kafka-1:9093",
        nodeId: 1,
        summary: { averageMs: 2, p95Ms: 3, samples: 2 },
      },
    ],
    summary: { averageMs: 2, p95Ms: 3, samples: 2 },
  },
  issues: [],
  network: {
    endpoint: "127.0.0.1:19093",
    tcpConnectMs: 1,
    tlsHandshakeMs: 2,
  },
  observedMessages: 2,
  producer: {
    semantics: "acknowledged",
    summary: { averageMs: 3, p95Ms: 4, samples: 2 },
  },
  requestedMessages: 2,
  runId: "run-123",
  sampleIds: ["sample-1", "sample-2"],
  schema: "streamskope.kafka-latency.v1",
  startedAt: "2026-07-25T16:00:00.000Z",
  topic: "orders.events",
};

const documentValue: HostTextDocument = {
  byteSize: 20,
  content: `${JSON.stringify(evidence, null, 2)}\n`,
  fileName: "streamskope-latency-orders.events-run-123.json",
  mediaType: "application/json",
};

const history: KafkaLatencyHistorySnapshot = {
  connectionName: "local-aio",
  entries: [
    {
      acknowledgements: -1,
      completedAt: evidence.completedAt,
      endToEnd: { averageMs: 5, p95Ms: 6 },
      fetch: { averageMs: 2, p95Ms: 3 },
      issueCount: 0,
      observedMessages: 2,
      producer: { averageMs: 3, p95Ms: 4 },
      requestedMessages: 2,
      runId: evidence.runId,
      state: "ready",
      topic: evidence.topic,
    },
  ],
};

const preferences: KafkaOperationalPreferenceSnapshot = {
  preferences: {
    fetch: { maxMessages: 100, mode: "newest" },
    latency: {
      acknowledgements: 1,
      messageCount: 50,
      runbookUrl: "https://runbooks.example.test/kafka/latency",
      timeoutMs: 30_000,
    },
    rules: {
      logLevel: "warn",
      loggingEnabled: true,
      notificationsEnabled: false,
    },
    stream: {
      batchSize: 50,
      historySamples: 100,
      intervalMs: 50,
      queueDepth: 500,
    },
  },
  store: { durability: "durable", state: "ready" },
};

function accepted(command: Parameters<StreamSkopeHost["execute"]>[0]): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result:
      command.command === "latency.export"
        ? { correlationId: "latency-correlation", document: documentValue }
        : { correlationId: "latency-correlation" },
    version: HOST_PROTOCOL_VERSION,
  };
}

function setup(
  snapshot: KafkaLatencySnapshot = { evidence: null, request: null, state: "idle" },
  selectedTopic: string | null = "orders.events",
  connectionName: string | null = "local-aio",
  preferenceSnapshot: KafkaOperationalPreferenceSnapshot | null = null,
  latencyHistory: KafkaLatencyHistorySnapshot = { connectionName: null, entries: [] },
): {
  readonly download: ReturnType<typeof vi.fn<LatencyWorkspaceTransferPort["download"]>>;
  readonly execute: ReturnType<typeof vi.fn<StreamSkopeHost["execute"]>>;
  readonly openExternalUrl: ReturnType<typeof vi.fn<StreamSkopeHost["openExternalUrl"]>>;
  readonly onOpenActivity: ReturnType<typeof vi.fn<() => void>>;
} {
  const execute = vi.fn<StreamSkopeHost["execute"]>((command) =>
    Promise.resolve(accepted(command)),
  );
  const download = vi.fn<LatencyWorkspaceTransferPort["download"]>(() => Promise.resolve());
  const openExternalUrl = vi.fn<StreamSkopeHost["openExternalUrl"]>(
    (): Promise<ExternalUrlOpenResult> =>
      Promise.resolve({ state: "accepted", version: HOST_PROTOCOL_VERSION }),
  );
  const onOpenActivity = vi.fn<() => void>();
  render(
    <LatencyWorkspace
      connectionName={connectionName}
      history={latencyHistory}
      host={{
        execute,
        openExternalUrl,
        subscribe: (): (() => void) => (): void => undefined,
      }}
      onOpenActivity={onOpenActivity}
      preferences={preferenceSnapshot}
      selectedTopic={selectedTopic}
      snapshot={snapshot}
      transfer={{ copy: (): Promise<void> => Promise.resolve(), download }}
    />,
  );
  return { download, execute, onOpenActivity, openExternalUrl };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Kafka latency Material UI workflow", () => {
  it("requests the exact confirmed runbook externally and reports only platform acceptance", async () => {
    const user = userEvent.setup();
    const { execute, openExternalUrl } = setup(
      { evidence: null, request: null, state: "idle" },
      "orders.events",
      "local-aio",
      preferences,
    );

    expect(screen.getByText("https://runbooks.example.test/kafka/latency")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Open runbook" }));

    expect(openExternalUrl).toHaveBeenCalledExactlyOnceWith(
      "https://runbooks.example.test/kafka/latency",
    );
    expect(execute).not.toHaveBeenCalled();
    expect(screen.getByRole("status", { name: "Latency operation status" })).toHaveTextContent(
      "Runbook request accepted by the platform.",
    );

    openExternalUrl.mockRejectedValueOnce(new Error("platform rejected request"));
    await user.click(screen.getByRole("button", { name: "Open runbook" }));
    expect(screen.getByText(/The platform did not accept the runbook request/u)).toBeVisible();
  });

  it("does not expose an active runbook action without a ready confirmed destination", () => {
    setup({ evidence: null, request: null, state: "idle" }, "orders.events", "local-aio", {
      ...preferences,
      store: {
        durability: "durable",
        recovery: "Reset Kafka operational preferences.",
        state: "unavailable",
      },
    });

    expect(screen.queryByRole("button", { name: "Open runbook" })).not.toBeInTheDocument();
    expect(screen.getByText(/Configure a latency runbook in Workbench Preferences/u)).toBeVisible();
  });

  it("initializes only the idle request draft from confirmed latency preferences", async () => {
    const user = userEvent.setup();
    const { execute } = setup(
      { evidence: null, request: null, state: "idle" },
      "orders.events",
      "local-aio",
      preferences,
    );

    expect(screen.getByLabelText("Probe records")).toHaveTextContent("50");
    expect(screen.getByLabelText("Kafka acknowledgements")).toHaveTextContent("Leader");
    expect(screen.getByLabelText("Probe timeout")).toHaveTextContent("30 seconds");
    expect(screen.getByText(/Confirmed durable preferences/u)).toBeVisible();

    await user.click(screen.getByRole("button", { name: "Run latency probe" }));
    await user.click(
      within(screen.getByRole("dialog")).getByRole("button", { name: "Run latency probe" }),
    );
    await waitFor(() => {
      expect(execute).toHaveBeenCalledOnce();
    });
    const startCommand = execute.mock.calls[0]?.[0];
    expect(startCommand?.command).toBe("latency.start");
    if (startCommand?.command !== "latency.start") {
      throw new Error("Expected one latency.start command.");
    }
    expect(startCommand.payload).toEqual({
      acknowledgements: 1,
      messageCount: 50,
      timeoutMs: 30_000,
      topic: "orders.events",
    });
  });

  it("keeps an active request immutable when confirmed defaults change", () => {
    const properties = {
      connectionName: "local-aio",
      history: { connectionName: null, entries: [] } satisfies KafkaLatencyHistorySnapshot,
      host: {
        execute: vi.fn<StreamSkopeHost["execute"]>((hostCommand) =>
          Promise.resolve(accepted(hostCommand)),
        ),
        openExternalUrl: (): Promise<never> =>
          Promise.reject(new Error("External URL action was not expected.")),
        subscribe: (): (() => void) => (): void => undefined,
      },
      onOpenActivity: vi.fn<() => void>(),
      selectedTopic: "orders.events",
      transfer: {
        copy: (): Promise<void> => Promise.resolve(),
        download: (): Promise<void> => Promise.resolve(),
      },
    } as const;
    const view = render(
      <LatencyWorkspace
        {...properties}
        preferences={preferences}
        snapshot={{ evidence: null, request, state: "running" }}
      />,
    );

    expect(screen.getByLabelText("Probe records")).toHaveTextContent("20");
    expect(screen.getByLabelText("Kafka acknowledgements")).toHaveTextContent(
      "All in-sync replicas",
    );
    expect(screen.getByLabelText("Probe timeout")).toHaveTextContent("10 seconds");

    view.rerender(
      <LatencyWorkspace
        {...properties}
        preferences={{
          ...preferences,
          preferences: {
            ...preferences.preferences,
            latency: {
              ...preferences.preferences.latency,
              acknowledgements: 0,
              messageCount: 100,
              timeoutMs: 60_000,
            },
          },
        }}
        snapshot={{ evidence: null, request, state: "running" }}
      />,
    );

    expect(screen.getByLabelText("Probe records")).toHaveTextContent("20");
    expect(screen.getByLabelText("Kafka acknowledgements")).toHaveTextContent(
      "All in-sync replicas",
    );
    expect(screen.getByLabelText("Probe timeout")).toHaveTextContent("10 seconds");
  });

  it("shows a bounded summary history without full evidence or private identifiers", () => {
    setup(
      { evidence, request: null, state: "ready" },
      "orders.events",
      "local-aio",
      preferences,
      history,
    );

    const table = screen.getByRole("table", { name: "Latency probe history" });
    expect(table).toBeVisible();
    expect(within(table).getByRole("row", { name: /orders.events Ready 2 \/ 2/u })).toBeVisible();
    expect(table).not.toHaveTextContent(evidence.connection.endpoint);
    expect(table).not.toHaveTextContent(evidence.sampleIds[0]!);
    expect(table).not.toHaveTextContent(evidence.runId);
    expect(within(table).getByText("2026-07-25 · 16:00:01 UTC")).toHaveStyle({
      whiteSpace: "nowrap",
    });
  });

  it("explains missing context and does not expose an executable probe", () => {
    setup({ evidence: null, request: null, state: "unavailable" }, null, null);

    expect(
      screen.getByText("Connect a Kafka cluster and select a topic to run a latency probe."),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: "Run latency probe" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Export latency JSON" })).toBeDisabled();
  });

  it("confirms the exact irreversible side effect before one bounded start", async () => {
    const user = userEvent.setup();
    const { execute } = setup();

    await user.click(screen.getByLabelText("Probe records"));
    await user.click(screen.getByRole("option", { name: "5" }));
    await user.click(screen.getByLabelText("Kafka acknowledgements"));
    await user.click(screen.getByRole("option", { name: "Leader" }));
    await user.click(screen.getByLabelText("Probe timeout"));
    await user.click(screen.getByRole("option", { name: "30 seconds" }));
    const run = screen.getByRole("button", { name: "Run latency probe" });
    await user.click(run);

    const dialog = screen.getByRole("dialog", { name: "Run latency probe?" });
    expect(dialog).toHaveTextContent("local-aio");
    expect(dialog).toHaveTextContent("orders.events");
    expect(dialog).toHaveTextContent("5 synthetic records");
    expect(dialog).toHaveTextContent("cannot remove those records");
    expect(execute).not.toHaveBeenCalled();

    await user.click(within(dialog).getByRole("button", { name: "Run latency probe" }));
    await waitFor(() => {
      expect(execute).toHaveBeenCalledOnce();
      expect(execute.mock.calls[0]?.[0]).toMatchObject({
        command: "latency.start",
        payload: {
          acknowledgements: 1,
          messageCount: 5,
          timeoutMs: 30_000,
          topic: "orders.events",
        },
        version: HOST_PROTOCOL_VERSION,
      });
      expect(execute.mock.calls[0]?.[0].id).toMatch(/^[\da-f-]+$/u);
    });
    await waitFor(() => {
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
      expect(run).toHaveFocus();
    });
  });

  it("shows exact running context and offers only the stop operation", async () => {
    const { execute } = setup({
      evidence: null,
      request: {
        acknowledgements: 0,
        messageCount: 20,
        timeoutMs: 10_000,
        topic: "orders.events",
      },
      state: "running",
    });

    expect(screen.getByRole("status", { name: "Latency operation status" })).toHaveTextContent(
      "Running 20-record probe on orders.events",
    );
    expect(screen.getByText(/send completion without a broker response/u)).toBeVisible();
    expect(screen.queryByRole("button", { name: "Run latency probe" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Stop latency probe" }));
    await waitFor(() => {
      expect(execute).toHaveBeenCalledWith(
        expect.objectContaining({ command: "latency.stop", payload: {} }),
      );
    });
  });

  it("keeps stop available while the start command is awaiting probe completion", async () => {
    const user = userEvent.setup();
    let resolveStart: ((response: HostCommandResponse) => void) | undefined;
    const startResponse = new Promise<HostCommandResponse>((resolve) => {
      resolveStart = resolve;
    });
    const execute = vi.fn<StreamSkopeHost["execute"]>((hostCommand) =>
      hostCommand.command === "latency.start"
        ? startResponse
        : Promise.resolve(accepted(hostCommand)),
    );
    const properties = {
      connectionName: "local-aio",
      history: { connectionName: null, entries: [] } satisfies KafkaLatencyHistorySnapshot,
      host: {
        execute,
        openExternalUrl: (): Promise<never> =>
          Promise.reject(new Error("External URL action was not expected.")),
        subscribe: (): (() => void) => (): void => undefined,
      },
      onOpenActivity: vi.fn<() => void>(),
      preferences: null,
      selectedTopic: "orders.events",
      transfer: {
        copy: (): Promise<void> => Promise.resolve(),
        download: (): Promise<void> => Promise.resolve(),
      },
    } as const;
    const view = render(
      <LatencyWorkspace
        {...properties}
        snapshot={{ evidence: null, request: null, state: "idle" }}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Run latency probe" }));
    await user.click(
      within(screen.getByRole("dialog")).getByRole("button", {
        name: "Run latency probe",
      }),
    );
    await waitFor(() => {
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    });
    view.rerender(
      <LatencyWorkspace
        {...properties}
        snapshot={{
          evidence: null,
          request,
          state: "running",
        }}
      />,
    );

    const stop = screen.getByRole("button", { name: "Stop latency probe" });
    expect(stop).toBeEnabled();
    await user.click(stop);
    expect(execute.mock.calls.map(([hostCommand]) => hostCommand.command)).toEqual([
      "latency.start",
      "latency.stop",
    ]);
    resolveStart?.({
      command: "latency.start",
      error: {
        activeStateChanged: false,
        code: "CANCELLED",
        correlationId: "latency-correlation",
        recovery: "Run another probe when ready.",
        retryable: true,
        stage: "kafka",
        summary: "The probe was cancelled.",
        target: "orders.events",
      },
      id: execute.mock.calls[0]![0].id,
      ok: false,
      version: HOST_PROTOCOL_VERSION,
    });
    await waitFor(() => {
      expect(screen.queryByRole("alert")).not.toHaveTextContent("The probe was cancelled.");
    });
  });

  it("presents dense ready evidence and transfers only the host export", async () => {
    const latencyHistory: KafkaLatencyHistorySnapshot = {
      connectionName: "local-aio",
      entries: [
        {
          ...history.entries[0]!,
          completedAt: "2026-07-25T15:58:01.000Z",
          endToEnd: { averageMs: 7, p95Ms: 9 },
          runId: "run-121",
        },
        {
          ...history.entries[0]!,
          completedAt: "2026-07-25T15:59:01.000Z",
          endToEnd: null,
          runId: "run-122",
          state: "partial",
        },
        history.entries[0]!,
      ],
    };
    const { download, execute } = setup(
      { evidence, request: null, state: "ready" },
      "orders.events",
      "local-aio",
      null,
      latencyHistory,
    );

    expect(screen.getByRole("status", { name: "Latency state" })).toHaveTextContent("Ready");
    expect(screen.getByRole("status", { name: "Latency operation status" })).toHaveTextContent(
      "Current latency evidence",
    );
    expect(screen.queryByText("Performance diagnostic", { exact: true })).not.toBeInTheDocument();
    expect(
      screen.getByRole("region", { name: /Publish-to-observe latency trend/u }),
    ).toHaveAccessibleName(/Average.*2 samples.*latest 5 ms.*P95.*2 samples.*latest 6 ms/u);
    const metricsTable = screen.getByRole("table", { name: "Latency metrics" });
    expect(metricsTable).toBeVisible();
    expect(metricsTable.closest('[role="region"]')).toHaveAttribute(
      "aria-label",
      "Latency metrics scroll area",
    );
    expect(metricsTable.closest('[role="region"]')).toHaveAttribute("tabindex", "0");
    expect(screen.getByRole("row", { name: /Produce acknowledgement 2 3 ms 4 ms/u })).toBeVisible();
    expect(screen.getByRole("row", { name: /Publish to observe 2 5 ms 6 ms/u })).toBeVisible();
    const brokerTable = screen.getByRole("table", { name: "Fetch latency by broker" });
    expect(brokerTable).toBeVisible();
    expect(brokerTable.closest('[role="region"]')).toHaveAttribute(
      "aria-label",
      "Fetch latency by broker scroll area",
    );
    expect(brokerTable.closest('[role="region"]')).toHaveAttribute("tabindex", "0");
    expect(screen.getByText("kafka-1:9093")).toBeVisible();
    expect(screen.getByText(/2026-07-25\s+16:00:00 UTC/u)).toHaveStyle({
      whiteSpace: "pre-line",
    });
    const evidenceHeading = screen.getByRole("heading", {
      level: 3,
      name: "Latest completed probe",
    });
    const rerun = screen.getByRole("button", { name: "Run again" });
    expect(
      evidenceHeading.compareDocumentPosition(rerun) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(screen.queryByLabelText("Probe records")).not.toBeInTheDocument();
    fireEvent.click(rerun);
    expect(screen.getByLabelText("Probe records")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Export latency JSON" }));
    await waitFor(() => {
      expect(download).toHaveBeenCalledWith(documentValue);
    });
    expect(execute).toHaveBeenCalledWith(
      expect.objectContaining({ command: "latency.export", payload: {} }),
    );
    expect(screen.getByRole("status", { name: "Latency operation status" })).toHaveTextContent(
      "Latency JSON download started.",
    );
  });

  it("does not present one completed observation as a trend", () => {
    setup({ evidence, request: null, state: "ready" }, "orders.events", "local-aio", null, history);

    expect(
      screen.queryByRole("region", { name: /Publish-to-observe latency trend/u }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("table", { name: "Latency metrics" })).toBeVisible();
  });

  it("keeps partial measurements, labels unavailable evidence and links recovery to activity", () => {
    const partial: KafkaLatencyProbeEvidence = {
      ...evidence,
      issues: [
        {
          recovery: "Verify TLS trust.",
          stage: "tls",
          summary: "TLS handshake evidence is unavailable.",
        },
      ],
      network: { ...evidence.network, tlsHandshakeMs: null },
    };
    const { onOpenActivity } = setup({
      evidence: partial,
      request: null,
      state: "partial",
    });

    expect(screen.getByText("Partial latency evidence")).toBeVisible();
    expect(screen.getByText("TLS handshake evidence is unavailable.")).toBeVisible();
    expect(screen.getByText("Verify TLS trust.")).toBeVisible();
    expect(screen.getAllByText("Unavailable")).not.toHaveLength(0);
    fireEvent.click(screen.getByRole("button", { name: "Open activity" }));
    expect(onOpenActivity).toHaveBeenCalledOnce();
  });
});
