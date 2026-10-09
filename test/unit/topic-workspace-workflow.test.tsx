// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, within } from "@testing-library/react";
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
import { testHostAccepted } from "../support/host-response";

class TopicWorkflowHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve(testHostAccepted(command, `topic-workflow-${command.id}`));
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

afterEach(() => {
  cleanup();
  localStorage.removeItem("streamskope-color-mode");
  localStorage.removeItem("streamskope-color-scheme");
  localStorage.removeItem("streamskope-inspector-pane-width");
  localStorage.removeItem("streamskope-resource-pane-width");
  document.documentElement.removeAttribute("data-mui-color-scheme");
});

describe("topic-led workspace", () => {
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
      name: "Pause read audit.events",
    });
    expect(streamAction).toHaveTextContent("Pause read");

    await user.click(streamAction);
    expect(host.commands.filter((command) => command.command === "messages.stop")).toHaveLength(1);
  });
});
