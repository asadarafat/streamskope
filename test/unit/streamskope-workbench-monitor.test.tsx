// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";
import { createRendererStreamMonitorObserver } from "../../src/features/kafka/ui/stream-monitor-observer";
import { FakeHost, message, readyRuleCapability, tailRequest } from "../support/workbench-fixtures";

afterEach(() => {
  cleanup();
  localStorage.removeItem("streamskope-color-mode");
  localStorage.removeItem("streamskope-color-scheme");
  localStorage.removeItem("streamskope-inspector-pane-width");
  localStorage.removeItem("streamskope-resource-pane-width");
  document.documentElement.removeAttribute("data-mui-color-scheme");
  vi.restoreAllMocks();
});

describe("StreamSkope workbench monitor integration", () => {
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
    const subscriptions = host.subscribeCalls;
    expect(subscriptions).toBe(2);

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
        rendererWindowEvictions: 0,
        retainedMessages: 1,
        visibleMessages: 1,
      });
      expect(observer.getSnapshot()).toMatchObject({
        eventBacklog: 0,
        rendererDroppedMessages: 0,
        rendererWindowEvictions: 0,
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
    expect(host.subscribeCalls).toBe(subscriptions);

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
        rendererWindowEvictions: 0,
        retainedMessages: 2,
        visibleMessages: 2,
      });
    });

    expect(host.subscribeCalls).toBe(subscriptions);
    view.unmount();
    expect(dispose).toHaveBeenCalledOnce();
  });
});
