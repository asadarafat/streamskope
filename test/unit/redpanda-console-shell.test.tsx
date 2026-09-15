// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { StreamSkopeApp } from "../../src/app/StreamSkopeApp";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";

class ShellHost implements StreamSkopeHost {
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
      result: { correlationId: `redpanda-shell-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External navigation was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

afterEach(() => {
  cleanup();
  localStorage.clear();
  document.documentElement.removeAttribute("data-mui-color-scheme");
});

describe("Redpanda Console-equivalent StreamSkope shell", () => {
  it("uses persistent resource navigation and main inventory pages without placeholder resources", async () => {
    const host = new ShellHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);

    await waitFor(() => {
      expect(host.commands.map((command) => command.command)).toEqual(
        expect.arrayContaining(["profiles.list", "rules.list", "templates.list"]),
      );
    });
    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(
      within(screen.getByRole("banner", { name: "StreamSkope application bar" })).getByRole(
        "heading",
        { name: "StreamSkope" },
      ),
    ).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Overview" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Topics" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Consumer Groups" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Connection Profiles" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(within(navigation).getByRole("button", { name: "Schema Registry" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Transforms" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Access Control Lists" })).toBeVisible();
    expect(within(navigation).queryByText("Connect", { exact: true })).not.toBeInTheDocument();
    expect(screen.queryByRole("group", { name: "Kafka resource views" })).not.toBeInTheDocument();

    const profilesPage = screen.getByRole("main", { name: "Connection profiles page" });
    const pageTitle = within(profilesPage).getByRole("heading", {
      name: "Connection Profiles",
    });
    expect(pageTitle).toBeVisible();
    expect(getComputedStyle(pageTitle).fontSize).toBe(`${String(24 / 16)}rem`);
    const selectedResource = within(navigation).getByRole("button", {
      name: "Connection Profiles",
    });
    expect(getComputedStyle(selectedResource).borderLeftWidth).toBe("0px");
    expect(
      within(profilesPage).queryByRole("navigation", { name: "Breadcrumb" }),
    ).not.toBeInTheDocument();

    act(() => {
      host.emit({
        event: "connection.state",
        payload: { connectionName: "Local AIO", state: "connected" },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-08-12T09:01:00.000Z",
          state: "ready",
          topics: ["orders.events", "payments.events"],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    const commandCount = host.commands.length;
    await user.click(within(navigation).getByRole("button", { name: "Topics" }));
    const topicsPage = screen.getByRole("main", { name: "Topics page" });
    expect(within(topicsPage).getByRole("heading", { name: "Topics" })).toBeVisible();
    const topicSummary = within(topicsPage).getByRole("group", {
      name: "Topic inventory status",
    });
    expect(topicSummary).toHaveTextContent("2 topics");
    expect(topicSummary).toHaveTextContent("Current");
    expect(within(topicSummary).queryByText("Total topics")).not.toBeInTheDocument();
    expect(within(topicSummary).queryByText("Visible now")).not.toBeInTheDocument();
    expect(within(topicsPage).getByRole("searchbox", { name: "Search topics" })).toBeVisible();
    expect(within(topicsPage).getByRole("button", { name: "orders.events" })).toBeVisible();
    expect(
      within(topicsPage).queryByRole("columnheader", { name: "Status" }),
    ).not.toBeInTheDocument();
    await user.type(within(topicsPage).getByRole("searchbox", { name: "Search topics" }), "orders");
    expect(topicSummary).toHaveTextContent("1 of 2 topics");
    expect(host.commands).toHaveLength(commandCount);
  });

  it("gates Overview until connection confirmation and then presents confirmed state", async () => {
    const host = new ShellHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);

    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(within(navigation).getByRole("button", { name: "Overview" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    expect(screen.queryByRole("group", { name: "Overview summary" })).not.toBeInTheDocument();
    act(() => {
      host.emit({
        event: "connection.state",
        payload: { connectionName: "Local AIO", state: "connected" },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-08-12T09:01:00.000Z",
          state: "ready",
          topics: ["orders.events", "payments.events"],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(within(navigation).getByRole("button", { name: "Overview" }));

    const summary = screen.getByRole("group", { name: "Overview summary" });
    expect(summary).toHaveTextContent("SessionConnected");
    expect(summary).toHaveTextContent("Topics2");
    expect(summary).toHaveTextContent("Consumer groups0");
    expect(summary).toHaveTextContent("Saved profiles0");
    expect(summary).toHaveTextContent("Application hostChecking");
  });

  it("lets the live session override a stale active-profile summary", () => {
    const host = new ShellHost();
    render(<StreamSkopeApp host={host} />);

    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [
            {
              active: true,
              brokers: ["localhost:19093"],
              createdAt: "2026-08-12T09:00:00.000Z",
              id: "stale-active-profile",
              name: "Local AIO Kafka",
              oauth: {
                clientId: "admin",
                clientSecretPresent: true,
                scope: "kafka",
                tokenEndpoint: "http://localhost:15000/rest-gateway/rest/api/v1/auth/token",
              },
              trust: {
                kind: "pem",
                label: "Local fixture CA",
                materialPresent: true,
                passwordPresent: false,
              },
              updatedAt: "2026-08-12T09:00:00.000Z",
            },
          ],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    expect(
      screen.queryByRole("status", { name: "Selected profile state" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Connect profile Local AIO Kafka" })).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Disconnect profile Local AIO Kafka" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Connection context:/ })).not.toBeInTheDocument();
    expect(screen.getByRole("contentinfo")).toHaveTextContent("Disconnected");
  });

  it("opens one breadcrumb-led topic detail with object-scoped tabs", async () => {
    const host = new ShellHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);

    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "connection.state",
        payload: { connectionName: "Local AIO", state: "connected" },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-08-12T09:01:00.000Z",
          state: "ready",
          topics: ["orders.events"],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    await user.click(within(navigation).getByRole("button", { name: "Topics" }));
    await user.click(screen.getByRole("button", { name: "orders.events" }));

    const detail = screen.getByRole("main", { name: "Topic detail page" });
    expect(screen.getByRole("navigation", { name: "Breadcrumb" })).toHaveTextContent(
      "Explore/Topics/orders.events",
    );
    expect(within(detail).getByRole("heading", { name: "orders.events" })).toBeVisible();
    const tabs = within(detail).getByRole("tablist", { name: "Topic sections" });
    for (const name of ["Messages", "Monitor", "Latency", "Rules", "Configuration"]) {
      expect(within(tabs).getByRole("tab", { name })).toBeVisible();
    }
    expect(within(tabs).getByRole("tab", { name: "Messages" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    const messageControls = within(detail).getByRole("group", {
      name: "Message controls",
    });
    expect(messageControls).toBeVisible();
    expect(within(messageControls).getByRole("combobox", { name: "Read mode" })).toHaveTextContent(
      "Tail",
    );
    expect(within(messageControls).getByRole("combobox", { name: "Record limit" })).toBeVisible();
    const readAction = within(messageControls).getByRole("button", {
      name: "Start tail orders.events",
    });
    expect(readAction).toBeVisible();
    expect(getComputedStyle(readAction).whiteSpace).toBe("nowrap");
    expect(within(messageControls).queryByRole("radio")).not.toBeInTheDocument();
    expect(within(detail).queryByRole("group", { name: "Topic summary" })).not.toBeInTheDocument();
    expect(
      within(detail).queryByRole("button", { name: "Fetch messages orders.events" }),
    ).not.toBeInTheDocument();
    expect(within(detail).queryByRole("button", { name: "Fetch options" })).not.toBeInTheDocument();
    await user.click(within(tabs).getByRole("tab", { name: "Monitor" }));
    expect(
      within(detail).queryByRole("group", { name: "Message controls" }),
    ).not.toBeInTheDocument();
    expect(within(detail).queryByRole("heading", { name: "Messages" })).not.toBeInTheDocument();
  });
});
