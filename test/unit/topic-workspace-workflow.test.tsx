// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaFetchRequest,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/features/kafka/ui/StreamSkopeApp";

class TopicWorkflowHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
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
      result: { correlationId: `topic-workflow-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

function publishReadyTopics(host: TopicWorkflowHost): void {
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
        topics: ["test", "audit.events"],
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

function messageStartRequests(host: TopicWorkflowHost): readonly KafkaFetchRequest[] {
  return host.commands.flatMap((command) =>
    command.command === "messages.start" ? [command.payload] : [],
  );
}

function topicConfigurationLoads(host: TopicWorkflowHost): readonly { readonly topic: string }[] {
  return host.commands.flatMap((command) =>
    command.command === "topicConfiguration.load" ? [command.payload] : [],
  );
}

afterEach(() => {
  cleanup();
  localStorage.removeItem("streamskope-color-mode");
  localStorage.removeItem("streamskope-color-scheme");
  localStorage.removeItem("streamskope-inspector-pane-width");
  localStorage.removeItem("streamskope-resource-pane-width");
  document.documentElement.removeAttribute("data-mui-color-scheme");
});

describe("topic-led workspace", () => {
  it("reveals one topic workspace and starts one configured read from activation", async () => {
    const host = new TopicWorkflowHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishReadyTopics(host);

    await waitFor(() => {
      expect(screen.getByRole("navigation", { name: "StreamSkope resources" })).toBeVisible();
    });
    expect(screen.queryByRole("tablist", { name: "Topic sections" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "test" })).toBeVisible();
    expect(screen.getByRole("button", { name: "audit.events" })).toBeVisible();
    expect(screen.getByText("2026-07-25 · 14:00:00 UTC")).toHaveAttribute(
      "datetime",
      "2026-07-25T14:00:00.000Z",
    );

    const resourceFilter = screen.getByRole("searchbox", { name: "Search topics" });
    await user.type(resourceFilter, "missing");
    expect(screen.getByText("No topics match “missing”.")).toBeVisible();
    expect(screen.queryByRole("button", { name: "test" })).not.toBeInTheDocument();

    await user.clear(resourceFilter);
    expect(messageStartRequests(host)).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "test" }));

    expect(screen.getByRole("navigation", { name: "Breadcrumb" })).toHaveTextContent(
      "Explore/Topics/test",
    );
    const selectedTopicLabel = screen.getByRole("heading", { level: 1, name: "test" });
    expect(getComputedStyle(selectedTopicLabel).fontFamily).toContain("system-ui");
    expect(getComputedStyle(selectedTopicLabel).fontSize).toBe("1.5rem");
    expect(getComputedStyle(selectedTopicLabel).lineHeight).toBe(String(32 / 24));
    const messageWorkspace = screen.getByRole("region", { name: "Message workspace" });
    expect(within(messageWorkspace).queryByText("Message explorer")).not.toBeInTheDocument();
    expect(
      within(messageWorkspace).queryByRole("heading", { level: 1, name: "Messages" }),
    ).not.toBeInTheDocument();
    expect(within(messageWorkspace).queryByText("test", { exact: true })).not.toBeInTheDocument();
    const topicWorkspace = screen.getByRole("tablist", { name: "Topic sections" });
    expect(within(topicWorkspace).getAllByRole("tab")).toHaveLength(5);
    expect(within(topicWorkspace).getByRole("tab", { name: "Monitor" })).toBeVisible();
    expect(within(topicWorkspace).getByRole("tab", { name: "Latency" })).toBeVisible();
    expect(within(topicWorkspace).getByRole("tab", { name: "Messages" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(messageStartRequests(host)).toEqual([
      {
        maxMessages: 1_000,
        mode: "tail",
        topic: "test",
      },
    ]);

    await user.click(within(topicWorkspace).getByRole("tab", { name: "Rules" }));
    const ruleWorkspace = screen.getByRole("region", { name: "Rule workspace" });
    expect(ruleWorkspace).toBeVisible();
    expect(
      within(ruleWorkspace).queryByRole("heading", { level: 1, name: "Rules" }),
    ).not.toBeInTheDocument();
    expect(messageStartRequests(host)).toHaveLength(1);

    await user.click(within(topicWorkspace).getByRole("tab", { name: "Configuration" }));
    const configurationWorkspace = await screen.findByRole(
      "region",
      { name: "Topic configuration workspace" },
      { timeout: 10_000 },
    );
    expect(configurationWorkspace).toBeVisible();
    expect(within(configurationWorkspace).queryByText("Topic: test")).not.toBeInTheDocument();
    expect(
      within(configurationWorkspace).queryByRole("heading", {
        level: 1,
        name: "Configuration",
      }),
    ).not.toBeInTheDocument();
    expect(
      within(configurationWorkspace).getByRole("button", { name: "Refresh configuration" }),
    ).toBeVisible();
    expect(
      within(configurationWorkspace).getByRole("button", { name: "Configuration history" }),
    ).toBeVisible();
    await waitFor(
      () => {
        expect(topicConfigurationLoads(host)).toContainEqual({ topic: "test" });
      },
      { timeout: 10_000 },
    );
    expect(messageStartRequests(host)).toHaveLength(1);
  });

  it("uses the displayed read settings exactly once for pointer and keyboard topic activation", async () => {
    const host = new TopicWorkflowHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    publishReadyTopics(host);

    const testTopic = await screen.findByRole("button", { name: "test" });
    await user.click(testTopic);
    expect(messageStartRequests(host)).toEqual([
      {
        maxMessages: 1_000,
        mode: "tail",
        topic: "test",
      },
    ]);

    await user.click(screen.getByRole("combobox", { name: "Read mode" }));
    await user.click(screen.getByRole("option", { name: "Newest N" }));
    await user.click(screen.getByRole("combobox", { name: "Record limit" }));
    await user.click(screen.getByRole("option", { name: "100" }));
    expect(
      within(screen.getByRole("tablist", { name: "Topic sections" })).getByRole("tab", {
        name: "Messages",
      }),
    ).toHaveAttribute("aria-selected", "true");

    await user.click(
      within(screen.getByRole("navigation", { name: "Breadcrumb" })).getByRole("button", {
        name: "Topics",
      }),
    );
    const auditTopic = await screen.findByRole("button", { name: "audit.events" });
    auditTopic.focus();
    await user.keyboard("{Enter}");
    expect(messageStartRequests(host)).toEqual([
      {
        maxMessages: 1_000,
        mode: "tail",
        topic: "test",
      },
      {
        maxMessages: 100,
        mode: "newest",
        topic: "audit.events",
      },
    ]);

    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 0,
          request: {
            maxMessages: 100,
            mode: "newest",
            topic: "audit.events",
          },
          ruleEvaluation: {
            applicableRules: 0,
            omittedRules: 0,
            state: "ready",
          },
          state: "fetching",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByLabelText("Consumption status")).toHaveTextContent("Fetching");
    expect(
      within(screen.getByRole("navigation", { name: "StreamSkope resources" })).queryByRole(
        "button",
        { name: /cancel fetch/iu },
      ),
    ).not.toBeInTheDocument();
    const streamAction = screen.getByRole("button", {
      name: "Cancel fetch audit.events",
    });
    expect(streamAction).toHaveTextContent("Cancel fetch");

    await user.click(streamAction);
    expect(host.commands.filter((command) => command.command === "messages.stop")).toHaveLength(1);
  });
});
