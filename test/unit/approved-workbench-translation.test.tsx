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
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { StreamSkopeApp as StreamSkopeWorkbench } from "../../src/app/StreamSkopeApp";

class ApprovedWorkbenchHost implements StreamSkopeHost {
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
      result: { correlationId: `approved-${command.id}` },
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

afterEach(() => {
  cleanup();
  localStorage.clear();
});

async function waitForBootstrap(host: ApprovedWorkbenchHost): Promise<void> {
  await waitFor(() => {
    expect(host.commands.map((command) => command.command)).toEqual(
      expect.arrayContaining(["preferences.get", "profiles.list", "rules.list", "templates.list"]),
    );
  });
}

describe("approved production workbench translation", () => {
  it("uses saved profiles as resources and opens their safe detail without host work", async () => {
    const host = new ApprovedWorkbenchHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    await waitForBootstrap(host);

    act(() => {
      host.emit({
        event: "profiles.changed",
        payload: {
          profiles: [
            {
              active: false,
              brokers: ["127.0.0.1:9093"],
              createdAt: "2026-07-31T10:00:00.000Z",
              id: "local-aio",
              name: "Local AIO",
              oauth: {
                clientId: "admin",
                clientSecretPresent: true,
                scope: "openid",
                tokenEndpoint: "http://127.0.0.1:5000/token",
              },
              trust: {
                kind: "pem",
                label: "aio-ca.pem",
                materialPresent: true,
                passwordPresent: false,
              },
              updatedAt: "2026-07-31T10:00:00.000Z",
            },
          ],
          store: { durability: "session", protection: "memory", state: "ready" },
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const commandCount = host.commands.length;
    await user.click(screen.getByRole("button", { name: "Select profile Local AIO" }));

    const page = screen.getByRole("main", { name: "Connection profiles page" });
    const workspace = within(page).getByRole("region", { name: "Connection profile workspace" });
    expect(workspace).toHaveTextContent("Local AIO");
    expect(workspace).toHaveTextContent("127.0.0.1:9093");
    expect(workspace).toHaveTextContent("OAuth secret retained by host");
    const profileDetails = within(workspace).getByRole("region", {
      name: "Connection profile details",
    });
    expect(within(profileDetails).getByRole("heading", { name: "Local AIO" })).toBeVisible();
    expect(
      within(workspace).queryByRole("heading", { name: "Connection details" }),
    ).not.toBeInTheDocument();
    expect(
      within(profileDetails).getByRole("heading", {
        level: 3,
        name: "Transport and profile",
      }),
    ).toBeVisible();
    const brokerAddress = within(profileDetails).getByText("127.0.0.1:9093", { exact: true });
    expect(getComputedStyle(brokerAddress).fontFamily).toContain("ui-monospace");
    expect(getComputedStyle(brokerAddress).fontSize).toBe("0.75rem");
    expect(getComputedStyle(brokerAddress).lineHeight).toBe("1.5");
    expect(getComputedStyle(within(profileDetails).getByText("OAuth 2.0")).fontFamily).toContain(
      "system-ui",
    );
    expect(
      within(workspace).queryByRole("region", { name: "Transport and credentials" }),
    ).not.toBeInTheDocument();
    expect(workspace).not.toHaveTextContent(
      "One saved profile owns brokers, transport, authentication, testing, and protected values.",
    );
    expect(
      within(page).getByRole("button", {
        name: "Connect profile Local AIO",
      }),
    ).toBeVisible();
    expect(
      within(workspace).queryByRole("button", { name: "Connect profile Local AIO" }),
    ).not.toBeInTheDocument();
    await user.click(
      within(page).getByRole("button", { name: "More actions for profile Local AIO" }),
    );
    expect(
      within(screen.getByRole("menu", { name: "Profile actions for Local AIO" })).getByRole(
        "menuitem",
        { name: "Edit" },
      ),
    ).toBeEnabled();
    await user.keyboard("{Escape}");
    expect(
      within(workspace).queryByRole("status", { name: "Selected profile state" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Breadcrumb" })).toHaveTextContent(
      "Explore/Connection Profiles",
    );
    expect(host.commands).toHaveLength(commandCount);
  });

  it("owns active consumption stop once in the message toolbar", async () => {
    const host = new ApprovedWorkbenchHost();
    const user = userEvent.setup();
    render(<StreamSkopeWorkbench host={host} />);
    await waitForBootstrap(host);

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
          refreshedAt: "2026-07-31T10:01:00.000Z",
          state: "ready",
          topics: ["orders.events"],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    await user.click(screen.getByRole("button", { name: "Topics" }));
    await user.click(await screen.findByRole("button", { name: "orders.events" }));
    act(() => {
      host.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 12,
          request: { maxMessages: 1000, mode: "tail", topic: "orders.events" },
          ruleEvaluation: { applicableRules: 0, omittedRules: 0, state: "ready" },
          state: "streaming",
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      });
    });

    const topicPage = screen.getByRole("main", { name: "Topic detail page" });
    const streamControl = within(topicPage).getByRole("button", {
      name: "Stop tail orders.events",
    });
    expect(streamControl).toHaveTextContent("Stop tail");
    expect(within(topicPage).getByLabelText("Consumption status")).toHaveTextContent("Streaming");
    expect(
      within(screen.getByRole("navigation", { name: "StreamSkope resources" })).queryByRole(
        "button",
        { name: /stop/iu },
      ),
    ).not.toBeInTheDocument();

    await user.click(streamControl);
    expect(host.commands.at(-1)).toMatchObject({ command: "messages.stop", payload: {} });
  });
});
