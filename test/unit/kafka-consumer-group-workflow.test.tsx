// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode } from "react";
import { afterEach, describe, expect, it } from "vitest";

import { StreamSkopeApp } from "../../src/app/StreamSkopeApp";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type HostEventListener,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupInventorySnapshot,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";

class ConsumerGroupHost implements StreamSkopeHost {
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
      result: { correlationId: `groups-${command.id}` },
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

const denied: HostError = {
  activeStateChanged: false,
  code: "AUTHORIZATION_DENIED",
  correlationId: "groups-denied",
  recovery: "Grant DescribeGroups permission and retry.",
  retryable: false,
  stage: "broker",
  summary: "Kafka denied consumer-group access.",
};

const failed: HostError = {
  activeStateChanged: false,
  code: "TIMEOUT",
  correlationId: "groups-timeout",
  recovery: "Verify broker reachability and retry.",
  retryable: true,
  stage: "broker",
  summary: "Consumer-group metadata timed out.",
};

function inventoryEvent(payload: KafkaConsumerGroupInventorySnapshot, sequence: number): HostEvent {
  return {
    event: "consumerGroups.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function detailEvent(payload: KafkaConsumerGroupDetailSnapshot, sequence: number): HostEvent {
  return {
    event: "consumerGroup.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function connect(host: ConsumerGroupHost): void {
  act(() => {
    host.emit({
      event: "connection.state",
      payload: { connectionName: "Local AIO", state: "connected" },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    host.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-08-12T09:00:00.000Z",
        state: "ready",
        topics: ["orders.events"],
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

afterEach(() => {
  cleanup();
  localStorage.clear();
  document.documentElement.removeAttribute("data-mui-color-scheme");
});

describe("consumer-group workbench", () => {
  it("loads, searches, refreshes and selects a group through one non-destructive path", async () => {
    const host = new ConsumerGroupHost();
    const user = userEvent.setup();
    render(
      <StrictMode>
        <StreamSkopeApp host={host} />
      </StrictMode>,
    );

    const resources = await screen.findByRole("navigation", { name: "StreamSkope resources" });
    const groupsMode = within(resources).getByRole("button", {
      name: "Consumer Groups",
    });
    fireEvent.click(groupsMode);
    expect(
      host.commands.filter((command) => command.command === "consumerGroups.list"),
    ).toHaveLength(0);

    connect(host);
    await waitFor(() => {
      expect(within(resources).getByRole("button", { name: "Topics" })).toHaveAttribute(
        "aria-current",
        "page",
      );
    });
    await user.click(groupsMode);

    await waitFor(() => {
      expect(
        host.commands.filter((command) => command.command === "consumerGroups.list"),
      ).toHaveLength(1);
    });
    act(() => {
      host.emit(
        inventoryEvent(
          {
            connectionName: "Local AIO",
            groups: [],
            omittedGroups: 0,
            refreshedAt: null,
            state: "loading",
          },
          3,
        ),
      );
    });
    expect(screen.getByText("Loading consumer groups…")).toBeVisible();

    act(() => {
      host.emit(
        inventoryEvent(
          {
            connectionName: "Local AIO",
            groups: [
              {
                groupType: "consumer",
                id: "billing-workers",
                protocolType: "consumer",
                state: "empty",
              },
              {
                groupType: "consumer",
                id: "orders-workers",
                protocolType: "consumer",
                state: "stable",
              },
            ],
            omittedGroups: 0,
            refreshedAt: "2026-08-12T09:01:00.000Z",
            state: "ready",
          },
          4,
        ),
      );
    });

    expect(groupsMode).toHaveAttribute("aria-current", "page");
    expect(groupsMode).toHaveAccessibleName("Consumer Groups");
    expect(screen.getByRole("contentinfo")).toHaveTextContent("2 consumer groups");
    expect(screen.getByRole("grid", { name: "Kafka consumer groups" })).toBeVisible();
    expect(screen.getByTestId("consumer-group-inventory-data-plane")).toBeVisible();
    const search = screen.getByRole("searchbox", { name: "Search consumer groups" });
    await user.type(search, "orders");
    expect(screen.getByRole("button", { name: "orders-workers" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "billing-workers" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Refresh consumer groups" }));
    expect(
      host.commands.filter((command) => command.command === "consumerGroups.list"),
    ).toHaveLength(2);

    const group = screen.getByRole("button", { name: "orders-workers" });
    group.focus();
    await user.keyboard("{Enter}");
    expect(host.commands.filter((command) => command.command === "consumerGroups.load")).toEqual([
      expect.objectContaining({ payload: { groupId: "orders-workers" } }),
    ]);
    expect(host.commands.map((command) => command.command)).not.toContain("messages.start");

    act(() => {
      host.emit(
        detailEvent(
          {
            connectionName: "Local AIO",
            group: {
              id: "orders-workers",
              members: [
                {
                  assignments: [{ partitions: [0, 1], topic: "orders.events" }],
                  clientHost: "/10.0.0.8",
                  clientId: "orders-worker-1",
                  groupInstanceId: null,
                  id: "member-1",
                },
              ],
              offsets: [
                {
                  committedOffset: "9007199254740993",
                  endOffset: "9007199254741000",
                  lag: "7",
                  partition: 0,
                  topic: "orders.events",
                },
              ],
              omittedAssignments: 0,
              omittedMembers: 0,
              omittedOffsets: 0,
              protocol: "range",
              protocolType: "consumer",
              state: "stable",
            },
            groupId: "orders-workers",
            refreshedAt: "2026-08-12T09:01:01.000Z",
            state: "ready",
          },
          5,
        ),
      );
    });

    expect(
      within(screen.getByRole("navigation", { name: "Breadcrumb" })).getByRole("button", {
        name: "Consumer Groups",
      }),
    ).toBeVisible();
    expect(screen.getByRole("heading", { name: "orders-workers", level: 1 })).toBeVisible();
    const workspace = screen.getByRole("region", { name: "Consumer group workspace" });
    expect(within(workspace).getByRole("heading", { name: "Group status" })).toBeVisible();
    expect(within(workspace).getByRole("heading", { name: "Members" })).toBeVisible();
    expect(within(workspace).getByRole("heading", { name: "Offsets and lag" })).toBeVisible();
    expect(
      within(workspace).getByRole("grid", { name: "Consumer group members" }),
    ).toHaveTextContent("orders-worker-1");
    expect(getComputedStyle(screen.getByTestId("consumer-group-members-grid")).height).toBe("82px");
    expect(
      within(workspace).getByRole("grid", { name: "Consumer group offsets" }),
    ).toHaveTextContent("9007199254740993");
    expect(getComputedStyle(screen.getByTestId("consumer-group-offsets-grid")).height).toBe("82px");
    expect(
      within(workspace).queryByRole("button", { name: /reset|delete/i }),
    ).not.toBeInTheDocument();
  });

  it("distinguishes empty, denied and failed inventories without manufacturing groups", async () => {
    const host = new ConsumerGroupHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    connect(host);
    await user.click(
      within(await screen.findByRole("navigation", { name: "StreamSkope resources" })).getByRole(
        "button",
        { name: "Consumer Groups" },
      ),
    );

    act(() => {
      host.emit(
        inventoryEvent(
          {
            connectionName: "Local AIO",
            groups: [],
            omittedGroups: 0,
            refreshedAt: "2026-08-12T09:02:00.000Z",
            state: "empty",
          },
          3,
        ),
      );
    });
    expect(screen.getByText("No consumer groups found.")).toBeVisible();

    act(() => {
      host.emit(
        inventoryEvent(
          {
            connectionName: "Local AIO",
            error: denied,
            groups: [
              {
                groupType: "consumer",
                id: "residual-unauthorized-group",
                protocolType: "consumer",
                state: "stable",
              },
            ],
            omittedGroups: 0,
            refreshedAt: null,
            state: "denied",
          },
          4,
        ),
      );
    });
    expect(
      within(screen.getByRole("main", { name: "Consumer groups page" })).getByText(
        "Consumer-group access denied",
      ),
    ).toBeVisible();
    expect(screen.getByText(denied.recovery)).toBeVisible();
    expect(screen.queryByText("No consumer groups found.")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "residual-unauthorized-group" }),
    ).not.toBeInTheDocument();

    act(() => {
      host.emit(
        inventoryEvent(
          {
            connectionName: "Local AIO",
            error: failed,
            groups: [],
            omittedGroups: 0,
            refreshedAt: null,
            state: "failed",
          },
          5,
        ),
      );
    });
    expect(screen.getByText("Consumer groups unavailable")).toBeVisible();
    expect(screen.getByText(failed.summary)).toBeVisible();
    expect(host.commands.map((command) => command.command)).not.toEqual(
      expect.arrayContaining(["messages.start", "topicConfiguration.apply"]),
    );
  });

  it("presents an explicit not-found detail and keeps inventory refresh available", async () => {
    const host = new ConsumerGroupHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    connect(host);
    await user.click(await screen.findByRole("button", { name: "Consumer Groups" }));
    act(() => {
      host.emit(
        inventoryEvent(
          {
            connectionName: "Local AIO",
            groups: [
              {
                groupType: "consumer",
                id: "ephemeral-workers",
                protocolType: "consumer",
                state: "empty",
              },
            ],
            omittedGroups: 0,
            refreshedAt: "2026-08-12T09:03:00.000Z",
            state: "ready",
          },
          3,
        ),
      );
    });
    await user.click(screen.getByRole("button", { name: "ephemeral-workers" }));
    act(() => {
      host.emit(
        detailEvent(
          {
            connectionName: "Local AIO",
            error: {
              ...failed,
              code: "CONSUMER_GROUP_NOT_FOUND",
              summary: "The selected consumer group no longer exists.",
            },
            group: null,
            groupId: "ephemeral-workers",
            refreshedAt: null,
            state: "not-found",
          },
          4,
        ),
      );
    });

    const workspace = screen.getByRole("region", { name: "Consumer group workspace" });
    expect(within(workspace).getByText("Consumer group not found")).toBeVisible();
    expect(
      screen.getByRole("button", { name: "Refresh consumer group ephemeral-workers" }),
    ).toBeEnabled();
    expect(
      within(screen.getByRole("navigation", { name: "Breadcrumb" })).getByRole("button", {
        name: "Consumer Groups",
      }),
    ).toBeEnabled();
  });
});
