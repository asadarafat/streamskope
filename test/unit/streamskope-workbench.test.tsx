// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { pasteText } from "../support/paste-text";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaExploredMessage,
  type KafkaFetchRequest,
  type KafkaLiveRuleCapability,
  type KafkaLiveRuleEvaluation,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";
import { createRendererStreamMonitorObserver } from "../../src/features/kafka/ui/stream-monitor-observer";

class FakeHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  subscribeCalls = 0;
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `correlation-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.subscribeCalls += 1;
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

const readyRuleCapability: KafkaLiveRuleCapability = {
  applicableRules: 0,
  omittedRules: 0,
  state: "ready",
};

function publishProfilesReady(host: FakeHost, sequence = 1): void {
  act(() => {
    host.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: {
          durability: "session",
          protection: "memory",
          state: "ready",
        },
      },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

function tailRequest(topic = "test"): KafkaFetchRequest {
  return {
    maxMessages: 1_000,
    mode: "tail",
    topic,
  };
}

const evaluatedRuleResult: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(
  id: string,
  payload: string | null = '{"status":"ready"}',
  overrides: Partial<KafkaExploredMessage> = {},
): KafkaExploredMessage {
  return {
    headers: { "content-type": "application/json" },
    id,
    key: "order-1",
    offset: id,
    originalByteSize: payload?.length ?? 1_048_577,
    partition: 0,
    payload,
    preview: payload ?? "oversized preview",
    ruleEvaluation:
      payload === null
        ? {
            ...evaluatedRuleResult,
            reason: "payload-truncated",
            state: "unavailable",
          }
        : evaluatedRuleResult,
    timestamp: "2026-07-25T15:00:00.000Z",
    topic: "test",
    truncated: payload === null,
    ...overrides,
  };
}

afterEach(() => {
  cleanup();
  localStorage.removeItem("streamskope-color-mode");
  localStorage.removeItem("streamskope-color-scheme");
  localStorage.removeItem("streamskope-inspector-pane-width");
  localStorage.removeItem("streamskope-resource-pane-width");
  document.documentElement.removeAttribute("data-mui-color-scheme");
  vi.restoreAllMocks();
});

describe("StreamSkope workbench shell", () => {
  it("presents and keyboard-dismisses bounded Material UI rule notices in queue order", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);

    act(() => {
      host.emit({
        event: "rules.notification",
        payload: {
          activeMatchCount: 2,
          highestSeverity: "warn",
          matches: [{ count: 2, level: "warn", name: "High priority" }],
          omittedMatches: 0,
          topic: "orders",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "rules.notification",
        payload: {
          activeMatchCount: 1,
          highestSeverity: "error",
          matches: [{ count: 1, level: "error", name: "Payment failed" }],
          omittedMatches: 0,
          topic: "payments",
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "rules.notification",
        payload: {
          activeMatchCount: 1,
          highestSeverity: "error",
          matches: [],
          omittedMatches: 1,
          topic: "audit",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const first = screen.getByRole("alert", { name: "Rule match notification" });
    expect(first).toHaveTextContent("2 active rule matches on orders");
    expect(first).toHaveTextContent("High priority · warn · 2");
    const dismiss = within(first).getByRole("button", {
      name: "Dismiss rule notification",
    });
    dismiss.focus();
    await user.keyboard("{Enter}");

    await waitFor(() => {
      const second = screen.getByRole("alert", { name: "Rule match notification" });
      expect(second).toHaveTextContent("1 active rule match on payments");
      expect(second).toHaveTextContent("Payment failed · error · 1");
      expect(second).not.toHaveTextContent("High priority");
    });
    const secondDismiss = within(
      screen.getByRole("alert", { name: "Rule match notification" }),
    ).getByRole("button", { name: "Dismiss rule notification" });
    secondDismiss.focus();
    await user.keyboard("{Enter}");

    await waitFor(() => {
      const omitted = screen.getByRole("alert", { name: "Rule match notification" });
      expect(omitted).toHaveTextContent("1 active rule match on audit");
      expect(omitted).toHaveTextContent("1 additional match not shown in this notification");
      expect(omitted).not.toHaveTextContent("Payment failed");
    });
  });

  it("feeds host receipt, filter, render and post-commit boundaries to renderer monitoring", async () => {
    const host = new FakeHost();
    const observer = createRendererStreamMonitorObserver();
    const eventReceived = vi.spyOn(observer, "eventReceived");
    const recordFilterDuration = vi.spyOn(observer, "recordFilterDuration");
    const recordRenderDuration = vi.spyOn(observer, "recordRenderDuration");
    const setPresentationActive = vi.spyOn(observer, "setPresentationActive");
    const commit = vi.spyOn(observer, "commit");
    const dispose = vi.spyOn(observer, "dispose");
    const user = userEvent.setup();
    const view = render(<StreamSkopeWorkbench host={host} streamMonitorObserver={observer} />);

    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-26T12:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(await screen.findByRole("button", { name: "test" }));

    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 1,
          request: tailRequest(),
          ruleEvaluation: readyRuleCapability,
          state: "streaming",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [message("1")],
          topic: "test",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await waitFor(() => {
      expect(eventReceived).toHaveBeenCalledWith(
        expect.objectContaining({ event: "messages.batch", sequence: 4 }),
      );
      expect(commit).toHaveBeenLastCalledWith({
        lastSequence: 4,
        rendererDroppedMessages: 0,
        retainedMessages: 1,
        visibleMessages: 1,
      });
      expect(observer.getSnapshot()).toMatchObject({
        eventBacklog: 0,
        rendererDroppedMessages: 0,
        retainedMessages: 1,
        visibleMessages: 1,
      });
    });
    expect(recordRenderDuration).toHaveBeenCalled();
    const filterCalls = recordFilterDuration.mock.calls.length;
    const commandCount = host.commands.length;

    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    await user.type(screen.getByRole("textbox", { name: "Key contains" }), "order");
    await waitFor(() => {
      expect(recordFilterDuration.mock.calls.length).toBeGreaterThan(filterCalls);
    });
    expect(host.commands).toHaveLength(commandCount);
    expect(host.commands.map((candidate) => candidate.command)).not.toContain(
      "streamMetrics.report",
    );

    await user.click(
      within(screen.getByRole("tablist", { name: "Topic sections" })).getByRole("tab", {
        name: "Rules",
      }),
    );
    await waitFor(() => {
      expect(setPresentationActive).toHaveBeenLastCalledWith(false);
    });
    eventReceived.mockClear();
    recordFilterDuration.mockClear();
    recordRenderDuration.mockClear();
    commit.mockClear();
    const taskSwitchCommands = host.commands.length;
    act(() => {
      host.emit({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [message("2")],
          topic: "test",
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(eventReceived).not.toHaveBeenCalled();
    expect(recordFilterDuration).not.toHaveBeenCalled();
    expect(recordRenderDuration).not.toHaveBeenCalled();
    expect(commit).not.toHaveBeenCalled();
    expect(host.commands).toHaveLength(taskSwitchCommands);
    expect(host.commands.map((candidate) => candidate.command)).not.toContain("messages.stop");
    expect(host.subscribeCalls).toBe(1);

    await user.click(
      within(screen.getByRole("tablist", { name: "Topic sections" })).getByRole("tab", {
        name: "Messages",
      }),
    );
    await waitFor(() => {
      expect(setPresentationActive).toHaveBeenLastCalledWith(true);
      expect(commit).toHaveBeenLastCalledWith({
        lastSequence: 5,
        rendererDroppedMessages: 0,
        retainedMessages: 2,
        visibleMessages: 2,
      });
    });

    view.unmount();
    expect(dispose).toHaveBeenCalledOnce();
  });

  it("switches the selected topic between message, configuration and explicit latency workflows", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);

    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    await user.click(await screen.findByRole("button", { name: "test" }));
    const workspaceTabs = screen.getByRole("tablist", { name: "Topic sections" });
    expect(within(workspaceTabs).getByRole("tab", { name: "Messages" })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    await user.click(within(workspaceTabs).getByRole("tab", { name: "Configuration" }));
    await screen.findByRole(
      "region",
      { name: "Topic configuration workspace" },
      { timeout: 5_000 },
    );
    await waitFor(
      () => {
        expect(host.commands.at(-1)).toMatchObject({
          command: "topicConfiguration.load",
          payload: { topic: "test" },
        });
      },
      { timeout: 5_000 },
    );

    act(() => {
      host.emit({
        event: "topicConfiguration.changed",
        payload: {
          connectionName: "Local validation",
          entries: [
            {
              documentation: "Retention time",
              isDefault: false,
              isSensitive: false,
              name: "retention.ms",
              readOnly: false,
              source: "topic",
              synonyms: [],
              type: "long",
              value: "86400000",
            },
          ],
          refreshedAt: "2026-07-25T14:01:00.000Z",
          state: "ready",
          topic: "test",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    expect(screen.getByRole("region", { name: "Topic configuration workspace" })).toBeVisible();
    expect(screen.getByRole("gridcell", { name: "retention.ms" })).toBeVisible();

    await user.click(within(workspaceTabs).getByRole("tab", { name: "Latency" }));
    expect(
      await screen.findByRole("heading", { name: "Latency probe" }, { timeout: 5_000 }),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: "Run latency probe" })).toBeEnabled();
    expect(within(workspaceTabs).getByRole("tab", { name: "Latency" })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    await user.click(within(workspaceTabs).getByRole("tab", { name: "Messages" }));
    expect(screen.getByRole("region", { name: "Message workspace" })).toBeVisible();
    expect(within(workspaceTabs).getByRole("tab", { name: "Messages" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
  }, 15_000);

  it("submits Material UI fetch controls and preserves the active request", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(await screen.findByRole("button", { name: "test" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.start",
      payload: tailRequest(),
    });
    const fetchMode = screen.getByRole("combobox", { name: "Read mode" });
    expect(fetchMode).toHaveTextContent("Tail");
    await user.click(fetchMode);
    await user.click(screen.getByRole("option", { name: "Newest N" }));
    expect(fetchMode).toHaveTextContent("Newest N");

    const maximum = screen.getByRole("combobox", { name: "Record limit" });
    await user.click(maximum);
    await user.click(screen.getByRole("option", { name: "100" }));
    expect(maximum).toHaveTextContent("100");

    await user.click(screen.getByRole("button", { name: "Load messages test" }));
    const request: KafkaFetchRequest = {
      maxMessages: 100,
      mode: "newest",
      topic: "test",
    };
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.start",
      payload: request,
      version: HOST_PROTOCOL_VERSION,
    });

    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request,
          ruleEvaluation: readyRuleCapability,
          state: "fetching",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByLabelText("Consumption status")).toHaveTextContent("Fetching snapshot");
    expect(fetchMode).toHaveAttribute("aria-disabled", "true");
    expect(maximum).toHaveAttribute("aria-disabled", "true");
    expect(fetchMode).toHaveTextContent("Newest N");
    expect(maximum).toHaveTextContent("100");

    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 2,
          request,
          ruleEvaluation: readyRuleCapability,
          state: "complete",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByLabelText("Consumption status")).toHaveTextContent("Snapshot complete");
    expect(fetchMode).not.toHaveAttribute("aria-disabled", "true");
  });

  it("submits and displays the exact last-two-minutes time-window request", async () => {
    const now = Date.parse("2026-07-25T15:00:00.000Z");
    vi.spyOn(Date, "now").mockReturnValue(now);
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(await screen.findByRole("button", { name: "test" }));
    await user.click(screen.getByRole("combobox", { name: "Read mode" }));
    await user.click(screen.getByRole("option", { name: "Time window" }));
    await user.click(screen.getByRole("button", { name: "Load messages test" }));
    const request: KafkaFetchRequest = {
      endTimeMs: now,
      maxMessages: 1_000,
      mode: "time-window",
      startTimeMs: now - 120_000,
      topic: "test",
    };
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.start",
      payload: request,
    });

    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request,
          ruleEvaluation: readyRuleCapability,
          state: "fetching",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByText(/2026-07-25T14:58:00.000Z/)).toBeVisible();
    expect(screen.getByText(/2026-07-25T15:00:00.000Z/)).toBeVisible();
  });

  it("starts, displays, inspects and stops a selected topic through host commands", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(await screen.findByRole("button", { name: "test" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.start",
      payload: tailRequest(),
      version: HOST_PROTOCOL_VERSION,
    });

    const consumedMessage = message("42");
    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request: tailRequest(),
          ruleEvaluation: readyRuleCapability,
          state: "loading",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request: tailRequest(),
          ruleEvaluation: readyRuleCapability,
          state: "streaming",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [consumedMessage],
          topic: "test",
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const grid = await screen.findByRole("grid", { name: "Kafka messages" }, { timeout: 5_000 });
    expect(within(grid).getByText("order-1")).toBeVisible();
    expect(screen.getByLabelText("Consumption status")).toHaveTextContent("Streaming");
    expect(screen.getByRole("button", { name: "Show message filters" })).toHaveAttribute(
      "aria-expanded",
      "false",
    );
    expect(screen.queryByRole("textbox", { name: "Timestamp contains" })).not.toBeInTheDocument();
    const commandCount = host.commands.length;
    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    expect(screen.getByRole("textbox", { name: "Timestamp contains" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);
    await user.click(within(grid).getByText("order-1"));

    const inspector = await screen.findByRole(
      "complementary",
      {
        name: "Message inspector",
      },
      { timeout: 5_000 },
    );
    expect(inspector).toHaveTextContent("Partition");
    expect(inspector).toHaveTextContent("42");
    await user.click(within(inspector).getByRole("tab", { name: "Value" }));
    expect(inspector).toHaveTextContent('"status": "ready"');
    const separator = screen.getByRole("separator", {
      name: "Resize messages and inspector",
    });
    expect(separator).toHaveAttribute("aria-valuenow", "320");
    separator.focus();
    await user.keyboard("{ArrowLeft}");
    expect(separator).toHaveAttribute("aria-valuenow", "336");
    expect(localStorage.getItem("streamskope-inspector-pane-width")).toBe("336");
    await user.click(within(inspector).getByRole("tab", { name: "Raw" }));
    expect(inspector).toHaveTextContent('{"status":"ready"}');

    const streamAction = screen.getByRole("button", { name: "Stop tail test" });
    expect(streamAction).toHaveTextContent("Stop tail");
    await user.click(streamAction);
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.stop",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it("identifies truncated payloads without presenting a preview as complete raw content", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 1,
          request: tailRequest(),
          ruleEvaluation: readyRuleCapability,
          state: "streaming",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [message("7", null)],
          topic: "test",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(await screen.findByRole("button", { name: "test" }));
    const grid = await screen.findByRole("grid", { name: "Kafka messages" });
    await user.click(within(grid).getByText("oversized preview"));

    const inspector = await screen.findByRole("complementary", {
      name: "Message inspector",
    });
    await user.click(within(inspector).getByRole("tab", { name: "Value" }));
    expect(inspector).toHaveTextContent("Payload truncated");
    expect(inspector).toHaveTextContent("1,048,577 bytes");
    expect(within(inspector).queryByRole("tab", { name: "Raw" })).not.toBeInTheDocument();
  });

  it("clears an inspector selection when its row leaves the bounded result", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Local validation",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T14:00:00.000Z",
          state: "ready",
          topics: ["test"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 10,
          request: {
            maxMessages: 10,
            mode: "tail",
            topic: "test",
          },
          ruleEvaluation: readyRuleCapability,
          state: "streaming",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: Array.from({ length: 10 }, (_value, index) => {
            const id = String(index);
            return message(id, `{"sequence":${id}}`, { key: `key-${id}` });
          }),
          topic: "test",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(await screen.findByRole("button", { name: "test" }));
    const grid = await screen.findByRole("grid", { name: "Kafka messages" }, { timeout: 5_000 });
    await user.click(within(grid).getByText("key-0"));
    expect(await screen.findByRole("complementary", { name: "Message inspector" })).toBeVisible();

    act(() => {
      host.emit({
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [message("10", '{"sequence":10}', { key: "key-10" })],
          topic: "test",
        },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    expect(await screen.findByText("The selected message is no longer retained.")).toBeVisible();
    expect(
      screen.queryByRole("complementary", { name: "Message inspector" }),
    ).not.toBeInTheDocument();
  });

  it("distinguishes topic loading, authorization denial, and refresh", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: {
          connectionName: "Restricted cluster",
          state: "connected",
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    act(() => {
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: null,
          state: "loading",
          topics: [],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByRole("status", { name: "Topic list status" })).toHaveTextContent(
      "Loading topics",
    );
    expect(screen.getByRole("button", { name: "Refresh topics" })).toBeDisabled();

    act(() => {
      host.emit({
        event: "topics.changed",
        payload: {
          error: {
            activeStateChanged: false,
            code: "AUTHORIZATION_DENIED",
            correlationId: "correlation-denied",
            recovery: "Request metadata permission from the cluster administrator.",
            retryable: false,
            stage: "authorization",
            summary: "Kafka denied topic metadata access.",
          },
          refreshedAt: null,
          state: "denied",
          topics: ["residual.unauthorized.topic"],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByRole("alert")).toHaveTextContent("Topic access denied");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Request metadata permission from the cluster administrator.",
    );
    expect(screen.queryByText("Cluster contains no topics.")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "residual.unauthorized.topic" }),
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Refresh topics" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "topics.list",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
  });

  it("identifies every required profile connection field before invoking the host", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfilesReady(host);

    await user.click(screen.getByRole("button", { name: "Add profile" }));
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.click(within(dialog).getByRole("combobox", { name: "Trust material format" }));
    await user.click(screen.getByRole("option", { name: "JKS truststore" }));
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));

    expect(within(dialog).getByText("Profile name is required.")).toBeVisible();
    expect(within(dialog).getByText("Enter at least one bootstrap broker.")).toBeVisible();
    expect(within(dialog).getByText("Select certificate or truststore material.")).toBeVisible();
    expect(within(dialog).getByText("Truststore password is required.")).toBeVisible();
    expect(within(dialog).getByText("OAuth token endpoint is required.")).toBeVisible();
    expect(within(dialog).getByText("OAuth client ID is required.")).toBeVisible();
    expect(within(dialog).getByText("OAuth client secret is required.")).toBeVisible();
    expect(within(dialog).getByText("OAuth scope is required.")).toBeVisible();
    expect(host.commands.filter((command) => command.command === "profiles.test")).toHaveLength(0);
  });

  it("reveals, remasks and tests the exact profile secret without saving it", async () => {
    const host = new FakeHost();
    const user = userEvent.setup();
    const view = render(<StreamSkopeWorkbench host={host} />);
    publishProfilesReady(host);

    await user.click(screen.getByRole("button", { name: "Add profile" }));
    let dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.type(within(dialog).getByRole("textbox", { name: "Profile name" }), "Local aio");
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:9093",
    );
    await user.click(within(dialog).getByRole("combobox", { name: "Trust material format" }));
    await user.click(screen.getByRole("option", { name: "PEM certificate" }));
    await user.upload(
      within(dialog).getByLabelText("Trust material file"),
      new File(["-----BEGIN CERTIFICATE-----\nfixture-ca\n-----END CERTIFICATE-----"], "ca.pem", {
        type: "application/x-pem-file",
      }),
    );
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    await pasteText(
      user,
      within(dialog).getByRole("textbox", { name: "OAuth token endpoint" }),
      "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
    );
    await user.type(within(dialog).getByRole("textbox", { name: "OAuth client ID" }), "admin");
    await user.type(within(dialog).getByLabelText("OAuth client secret"), "NokiaNsp@");
    await user.type(within(dialog).getByRole("textbox", { name: "OAuth scope" }), "kafka");

    const secret = within(dialog).getByLabelText("OAuth client secret");
    const reveal = within(dialog).getByRole("button", { name: "Show OAuth client secret" });
    expect(secret).toHaveAttribute("type", "password");
    expect(reveal).toHaveAttribute("aria-pressed", "false");

    await user.click(reveal);
    expect(secret).toHaveAttribute("type", "text");
    expect(secret).toHaveValue("NokiaNsp@");
    expect(
      within(dialog).getByRole("button", { name: "Hide OAuth client secret" }),
    ).toHaveAttribute("aria-pressed", "true");
    expect(host.commands.filter((command) => command.command === "profiles.test")).toHaveLength(0);

    await user.click(within(dialog).getByRole("button", { name: "Test connection" }));
    expect(secret).toHaveAttribute("type", "password");
    expect(secret).toHaveValue("NokiaNsp@");
    expect(within(dialog).getByRole("status")).toHaveTextContent("Connection test passed");
    const connectionCommands = host.commands.filter(
      (command) => command.command === "profiles.test",
    );
    expect(connectionCommands).toHaveLength(1);
    expect(connectionCommands[0]).toMatchObject({
      command: "profiles.test",
      payload: {
        mode: "create",
        profile: {
          brokers: ["127.0.0.1:9093"],
          name: "Local aio",
          oauth: {
            clientId: "admin",
            clientSecret: { mode: "replace", value: "NokiaNsp@" },
            scope: "kafka",
            tokenEndpoint: "http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token",
          },
          trust: {
            kind: "pem",
            label: "ca.pem",
            material: {
              mode: "replace",
              value: "-----BEGIN CERTIFICATE-----\nfixture-ca\n-----END CERTIFICATE-----",
            },
            password: { mode: "clear" },
          },
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(
      host.commands.filter(
        (command) => command.command === "profiles.create" || command.command === "profiles.update",
      ),
    ).toHaveLength(0);

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await user.click(screen.getByRole("button", { name: "Add profile" }));
    dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    expect(within(dialog).getByLabelText("OAuth client secret")).toHaveValue("");

    view.unmount();
    render(<StreamSkopeWorkbench host={host} />);
    publishProfilesReady(host, 2);
    await user.click(screen.getByRole("button", { name: "Add profile" }));
    dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.click(within(dialog).getByRole("switch", { name: "Use OAuth OAUTHBEARER" }));
    expect(within(dialog).getByLabelText("OAuth client secret")).toHaveValue("");
  });
});
