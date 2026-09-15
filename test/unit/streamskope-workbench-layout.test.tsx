// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { StreamSkopeApp } from "../../src/app/StreamSkopeApp";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { streamSkopeTypography } from "../../src/ui/typographyContract";

class LayoutHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) listener(event);
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `layout-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}

const localProfile: ProfileSummary = {
  active: false,
  brokers: ["localhost:19093"],
  createdAt: "2026-08-05T18:00:00.000Z",
  id: "profile-local-aio",
  name: "Local AIO Kafka",
  trust: {
    kind: "pem",
    label: "Repository fixture CA",
    materialPresent: true,
    passwordPresent: false,
  },
  updatedAt: "2026-08-05T18:00:00.000Z",
};

const configuredProfile: ProfileSummary = {
  ...localProfile,
  createdAt: "2026-08-05T17:00:00.000Z",
  oauth: {
    clientId: "streamskope",
    clientSecretPresent: true,
    scope: "openid kafka",
    tokenEndpoint: "https://auth.example.test/oauth/token",
  },
  services: {
    redpandaAdmin: {
      authentication: "oauth",
      baseUrl: "https://admin.example.test:9644",
    },
    schemaRegistry: {
      authentication: "none",
      baseUrl: "https://schema.example.test:8081",
    },
  },
  trust: {
    kind: "pkcs12",
    label: "fixture.p12",
    materialPresent: true,
    passwordPresent: true,
  },
};

function publishProfiles(host: LayoutHost, profiles: readonly ProfileSummary[]): void {
  act(() => {
    host.emit({
      event: "profiles.changed",
      payload: {
        profiles,
        store: { durability: "session", protection: "memory", state: "ready" },
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

function publishTopics(host: LayoutHost, topics: readonly string[], sequence = 2): void {
  act(() => {
    host.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-08-05T18:01:00.000Z",
        state: "ready",
        topics,
      },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

function confirmConnection(
  host: LayoutHost,
  state: "connected" | "connecting",
  sequence = 3,
): void {
  act(() => {
    host.emit({
      event: "connection.state",
      payload: { connectionName: "Local AIO Kafka", state },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  });
}

async function readyHost(host: LayoutHost): Promise<void> {
  await waitFor(() => {
    expect(host.commands.map((command) => command.command)).toEqual(
      expect.arrayContaining(["profiles.list", "rules.list", "templates.list"]),
    );
  });
}

async function openTopic(
  host: LayoutHost,
  user: ReturnType<typeof userEvent.setup>,
  topic = "orders.events",
): Promise<void> {
  confirmConnection(host, "connected");
  publishTopics(host, [topic], 4);
  await user.click(await screen.findByRole("button", { name: topic }));
  expect(screen.getByRole("main", { name: "Topic detail page" })).toBeVisible();
}

afterEach(() => {
  cleanup();
  localStorage.clear();
  document.documentElement.removeAttribute("data-mui-color-scheme");
});

describe("StreamSkope resource-first workbench layout", () => {
  it("finds profiles by address and selects without connection work", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    publishProfiles(host, [
      localProfile,
      { ...localProfile, id: "remote", name: "Remote test", brokers: ["broker.example.test:9093"] },
    ]);
    const count = host.commands.length;
    await user.click(screen.getByRole("button", { name: "Search and commands" }));
    await user.type(screen.getByRole("searchbox"), "broker.example.test");
    const result = screen.getByRole("button", { name: "Select profile Remote test" });
    result.focus();
    await user.keyboard("{Enter}");
    await waitFor(() =>
      expect(screen.queryByRole("dialog", { name: "Search and commands" })).not.toBeInTheDocument(),
    );
    expect(screen.getByRole("region", { name: "Connection profile workspace" })).toHaveTextContent(
      "Remote test",
    );
    expect(
      host.commands
        .slice(count)
        .filter(
          (command) =>
            command.command === "profiles.connect" || command.command === "profiles.test",
        ),
    ).toHaveLength(0);
    expect(screen.getByRole("button", { name: "Connect profile Remote test" })).toHaveClass(
      "MuiButton-contained",
    );
    expect(screen.getByRole("button", { name: "Connect profile Local AIO Kafka" })).toHaveClass(
      "MuiButton-outlined",
    );
  });
  it("presents one coherent desktop shell with stable responsibility boundaries", async () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);

    const applicationBar = screen.getByRole("banner", {
      name: "StreamSkope application bar",
    });
    expect(within(applicationBar).getByRole("heading", { name: "StreamSkope" })).toBeVisible();
    expect(
      within(applicationBar).queryByRole("button", { name: /Connection context:/ }),
    ).not.toBeInTheDocument();
    expect(screen.getAllByRole("status", { name: "Connection status" })).toHaveLength(1);
    expect(
      screen.queryByRole("status", { name: "Selected profile state" }),
    ).not.toBeInTheDocument();
    expect(
      within(applicationBar).getByRole("button", { name: "Search and commands" }),
    ).toBeVisible();
    expect(within(applicationBar).getByRole("button", { name: "Theme" })).toBeVisible();
    expect(within(applicationBar).getByRole("button", { name: "Preferences" })).toBeVisible();

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(within(navigation).queryByText("StreamSkope", { exact: true })).not.toBeInTheDocument();
    expect(
      within(navigation).queryByText(
        "Event-streaming operations, reduced to the current decision.",
      ),
    ).not.toBeInTheDocument();
    expect(within(navigation).queryByRole("button", { name: "Activity" })).not.toBeInTheDocument();
    expect(
      within(navigation).queryByRole("button", { name: "Preferences" }),
    ).not.toBeInTheDocument();
    expect(
      within(navigation).queryByRole("button", { name: /^Connect profile/iu }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Expand Activity" })).toBeVisible();
    expect(
      within(navigation).queryByRole("searchbox", { name: "Filter Kafka resources" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Breadcrumb" })).toHaveTextContent(
      "Explore/Connection Profiles",
    );
    expect(screen.queryByRole("tablist", { name: "Open workspaces" })).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Activity dock" })).toBeVisible();
    expect(screen.getByRole("contentinfo")).toBeVisible();
  });

  it("places profiles first in Explore and gates cluster resources until connection confirmation", async () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    const commandCount = host.commands.length;
    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(within(navigation).getByRole("heading", { name: "Explore" })).toBeVisible();
    expect(within(navigation).queryByRole("heading", { name: "Desktop" })).not.toBeInTheDocument();
    const profiles = within(navigation).getByRole("button", { name: "Connection Profiles" });
    const overview = within(navigation).getByRole("button", { name: "Overview" });
    expect(profiles.compareDocumentPosition(overview) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(
      0,
    );
    expect(profiles).not.toHaveAttribute("aria-disabled", "true");
    for (const label of [
      "Overview",
      "Topics",
      "Consumer Groups",
      "Schema Registry",
      "Transforms",
      "Access Control Lists",
    ]) {
      expect(within(navigation).getByRole("button", { name: label })).toHaveAttribute(
        "aria-disabled",
        "true",
      );
    }

    fireEvent.click(within(navigation).getByRole("button", { name: "Topics" }));
    expect(screen.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);

    confirmConnection(host, "connected", 4);
    await waitFor(() => {
      expect(within(navigation).getByRole("button", { name: "Topics" })).not.toHaveAttribute(
        "aria-disabled",
      );
    });
  });

  it("uses one breadcrumb location instead of workspace tabs and keeps ancestor navigation passive", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    await openTopic(host, user);

    const breadcrumb = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(breadcrumb).toHaveTextContent("Explore/Topics/orders.events");
    expect(screen.queryByRole("tablist", { name: "Open workspaces" })).not.toBeInTheDocument();
    const starts = host.commands.filter((command) => command.command === "messages.start").length;
    const stops = host.commands.filter((command) => command.command === "messages.stop").length;

    await user.click(within(breadcrumb).getByRole("button", { name: "Topics" }));
    expect(screen.getByRole("main", { name: "Topics page" })).toBeVisible();
    expect(screen.getByRole("navigation", { name: "Breadcrumb" })).toHaveTextContent(
      "Explore/Topics",
    );
    expect(host.commands.filter((command) => command.command === "messages.start")).toHaveLength(
      starts,
    );
    expect(host.commands.filter((command) => command.command === "messages.stop")).toHaveLength(
      stops,
    );
  });

  it("uses resource navigation instead of a duplicate global connection control", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    confirmConnection(host, "connected", 7);
    await screen.findByRole("main", { name: "Topics page" });
    const commandCount = host.commands.length;

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    await user.click(within(navigation).getByRole("button", { name: "Connection Profiles" }));
    expect(screen.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);
    expect(screen.queryByRole("button", { name: /Connection context:/ })).not.toBeInTheDocument();
    expect(screen.getAllByRole("status", { name: "Connection status" })).toHaveLength(1);
    expect(screen.getByRole("status", { name: "Connection status" })).toHaveTextContent(
      "Connected · Local AIO Kafka",
    );
    expect(
      screen.queryByRole("status", { name: "Selected profile state" }),
    ).not.toBeInTheDocument();
  });

  it("keeps the bounded resource destinations visible without a redundant global filter", async () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    const commandCount = host.commands.length;
    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });

    expect(
      within(navigation).queryByRole("searchbox", { name: "Filter Kafka resources" }),
    ).not.toBeInTheDocument();
    for (const label of [
      "Connection Profiles",
      "Overview",
      "Topics",
      "Consumer Groups",
      "Schema Registry",
      "Transforms",
      "Access Control Lists",
    ]) {
      expect(within(navigation).getByRole("button", { name: label })).toBeVisible();
    }
    expect(screen.getByRole("searchbox", { name: "Search profiles" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);
  });

  it("uses one persistent Redpanda-style resource hierarchy without the obsolete split-pane modes", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(
      within(navigation).queryByRole("heading", { name: "StreamSkope" }),
    ).not.toBeInTheDocument();
    expect(within(navigation).getByRole("button", { name: "Overview" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Topics" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Consumer Groups" })).toBeVisible();
    expect(within(navigation).getByRole("button", { name: "Connection Profiles" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(screen.queryByRole("group", { name: "Kafka resource views" })).not.toBeInTheDocument();
    expect(
      screen.queryByRole("separator", { name: "Resize Kafka resources and workspace" }),
    ).not.toBeInTheDocument();

    confirmConnection(host, "connected", 4);
    await screen.findByRole("main", { name: "Topics page" });
    const commandCount = host.commands.length;
    await user.click(within(navigation).getByRole("button", { name: "Overview" }));
    expect(screen.getByRole("main", { name: "Overview page" })).toBeVisible();
    await user.click(within(navigation).getByRole("button", { name: "Topics" }));
    expect(screen.getByRole("main", { name: "Topics page" })).toBeVisible();
    await user.click(within(navigation).getByRole("button", { name: "Connection Profiles" }));
    expect(screen.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);
  });

  it("presents profiles and topics as searchable main-page inventories", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    publishProfiles(host, [localProfile]);

    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    const profilesLink = within(navigation).getByRole("button", { name: "Connection Profiles" });
    expect(within(profilesLink).queryByText("1")).not.toBeInTheDocument();
    const profilesPage = screen.getByRole("main", { name: "Connection profiles page" });
    expect(within(profilesPage).getByRole("searchbox", { name: "Search profiles" })).toBeVisible();
    expect(
      within(profilesPage).getByRole("button", { name: "Select profile Local AIO Kafka" }),
    ).toBeVisible();

    confirmConnection(host, "connected", 4);
    publishTopics(host, ["orders.events", "payments.events"], 5);
    await user.click(within(navigation).getByRole("button", { name: "Topics" }));
    const topicsPage = screen.getByRole("main", { name: "Topics page" });
    expect(screen.getByRole("navigation", { name: "Breadcrumb" })).toHaveTextContent(
      "Explore/Topics",
    );
    const topicPlane = within(topicsPage).getByTestId("topic-inventory-data-plane");
    expect(within(topicPlane).getByRole("grid", { name: "Kafka topics" })).toBeVisible();
    const search = within(topicsPage).getByRole("searchbox", { name: "Search topics" });
    await user.type(search, "orders");
    expect(within(topicsPage).getByRole("button", { name: "orders.events" })).toBeVisible();
    expect(
      within(topicsPage).queryByRole("button", { name: "payments.events" }),
    ).not.toBeInTheDocument();
    const topicText = within(topicsPage).getByText("orders.events", { exact: true });
    expect(getComputedStyle(topicText).fontFamily).toContain("ui-monospace");
    expect(
      Number.parseFloat(getComputedStyle(topicText).fontSize) * streamSkopeTypography.rootSize,
    ).toBe(streamSkopeTypography.monospace.size);
  });

  it("keeps connection setup and profile mutation actions on the Connection Profiles page", () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    publishProfiles(host, [localProfile]);

    const profilesPage = screen.getByRole("main", { name: "Connection profiles page" });
    expect(within(profilesPage).getByRole("button", { name: "Add profile" })).toBeVisible();
    expect(
      within(profilesPage).getByRole("button", { name: "Connect profile Local AIO Kafka" }),
    ).toHaveTextContent("Connect");
    expect(
      within(screen.getByRole("navigation", { name: "StreamSkope resources" })).queryByRole(
        "button",
        { name: "Connect profile Local AIO Kafka" },
      ),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("dialog", { name: "Configure Kafka connection" }),
    ).not.toBeInTheDocument();
  });

  it("shows every safe configured profile field without exposing protected values", () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    publishProfiles(host, [configuredProfile]);

    const workspace = screen.getByRole("region", { name: "Connection profile workspace" });
    for (const evidence of [
      "openid kafka",
      "OAuth secret retained by host",
      "PKCS12 trust material present",
      "Trust password retained by host",
      "https://schema.example.test:8081",
      "No HTTP authorization",
      "https://admin.example.test:9644",
      "Profile OAuth bearer token",
      "2026-08-05 · 17:00:00 UTC",
      "2026-08-05 · 18:00:00 UTC",
    ]) {
      expect(within(workspace).getByText(evidence, { exact: true })).toBeVisible();
    }
    expect(workspace).not.toHaveTextContent("fixture.p12");
  });

  it("states the disconnected empty condition without inventing cluster data", () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    publishProfiles(host, []);

    const page = screen.getByRole("main", { name: "Connection profiles page" });
    expect(within(page).getByRole("heading", { name: "Connection Profiles" })).toBeVisible();
    expect(within(page).getByText("Add a connection profile to connect to Kafka.")).toBeVisible();
    expect(within(page).getByRole("status", { name: "No profile selected" })).toHaveTextContent(
      "Choose a connection profile",
    );
    expect(screen.getByRole("status", { name: "Connection status" })).toHaveTextContent(
      "Disconnected",
    );
    expect(screen.getByRole("contentinfo")).toHaveTextContent("Topics not loaded");
    expect(screen.queryByRole("main", { name: "Message workspace" })).not.toBeInTheDocument();
    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    expect(within(navigation).getByRole("button", { name: "Topics" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
  });

  it("opens Topics only after the host confirms the profile connection", async () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    const navigation = screen.getByRole("navigation", { name: "StreamSkope resources" });
    const profiles = within(navigation).getByRole("button", { name: "Connection Profiles" });
    const topics = within(navigation).getByRole("button", { name: "Topics" });
    expect(profiles).toHaveAttribute("aria-current", "page");
    expect(topics).toHaveAttribute("aria-disabled", "true");

    confirmConnection(host, "connecting");
    expect(profiles).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("status", { name: "Connection status" })).toHaveTextContent(
      "Connecting",
    );
    expect(topics).toHaveAttribute("aria-disabled", "true");

    confirmConnection(host, "connected", 4);
    await waitFor(() => expect(topics).toHaveAttribute("aria-current", "page"));
    expect(topics).not.toHaveAttribute("aria-disabled");
    expect(screen.getByRole("main", { name: "Topics page" })).toBeVisible();
    expect(host.commands.at(-1)).toMatchObject({ command: "topics.list", payload: {} });
  });

  it("opens one topic-detail page and starts one configured read", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    expect(screen.queryByRole("group", { name: "Message controls" })).not.toBeInTheDocument();
    await openTopic(host, user);
    const detail = screen.getByRole("main", { name: "Topic detail page" });
    expect(within(detail).getByRole("heading", { name: "orders.events" })).toBeVisible();
    expect(within(detail).queryByRole("region", { name: "Topic summary" })).not.toBeInTheDocument();
    const tabs = within(detail).getByRole("tablist", { name: "Topic sections" });
    expect(within(tabs).getAllByRole("tab")).toHaveLength(5);
    expect(within(tabs).getByRole("tab", { name: "Messages" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    const messageWorkspace = within(detail).getByRole("region", { name: "Message workspace" });
    const controls = within(messageWorkspace).getByRole("group", { name: "Message controls" });
    expect(controls).toBeVisible();
    expect(within(controls).getByRole("combobox", { name: "Read mode" })).toHaveTextContent("Tail");
    expect(within(controls).getByRole("combobox", { name: "Record limit" })).toBeVisible();
    expect(host.commands.filter((command) => command.command === "messages.start")).toHaveLength(1);
    expect(host.commands.at(-1)).toMatchObject({
      command: "messages.start",
      payload: { maxMessages: 1_000, mode: "tail", topic: "orders.events" },
    });
  });

  it("switches topic tabs without hidden message consumption and loads configuration on demand", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    await openTopic(host, user);
    const tabs = screen.getByRole("tablist", { name: "Topic sections" });
    const commandCount = host.commands.length;

    await user.click(within(tabs).getByRole("tab", { name: "Monitor" }));
    expect(
      await screen.findByRole("heading", { name: "Stream Monitor" }, { timeout: 15_000 }),
    ).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);

    await user.click(within(tabs).getByRole("tab", { name: "Latency" }));
    expect(
      await screen.findByRole("heading", { name: "Latency probe" }, { timeout: 15_000 }),
    ).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);

    await user.click(within(tabs).getByRole("tab", { name: "Rules" }));
    expect(screen.getByRole("region", { name: "Rule workspace" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);

    await user.click(within(tabs).getByRole("tab", { name: "Configuration" }));
    expect(
      await screen.findByRole(
        "region",
        { name: "Topic configuration workspace" },
        { timeout: 15_000 },
      ),
    ).toBeVisible();
    await waitFor(() => {
      expect(host.commands).toContainEqual(
        expect.objectContaining({
          command: "topicConfiguration.load",
          payload: { topic: "orders.events" },
        }),
      );
    });
    expect(host.commands.filter((command) => command.command === "messages.start")).toHaveLength(1);
  });

  it("keeps Activity and Preferences in their single stable shell locations", async () => {
    const host = new LayoutHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    await readyHost(host);
    const commandCount = host.commands.length;
    await user.click(screen.getByRole("button", { name: "Expand Activity" }));
    expect(screen.getByRole("complementary", { name: "Activity log" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "Raw logs" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Collapse Activity" }));
    await user.click(screen.getByRole("button", { name: "Preferences" }));
    expect(await screen.findByRole("dialog", { name: "Workbench Preferences" })).toBeVisible();
    expect(host.commands).toHaveLength(commandCount);
  });

  it("distinguishes connecting, connected, and stale-host states", () => {
    const host = new LayoutHost();
    render(<StreamSkopeApp host={host} />);
    confirmConnection(host, "connecting");
    expect(screen.getByRole("status", { name: "Connection status" })).toHaveTextContent(
      "Connecting",
    );
    confirmConnection(host, "connected", 4);
    expect(screen.getByRole("status", { name: "Connection status" })).toHaveTextContent(
      "Connected · Local AIO Kafka",
    );

    act(() => {
      host.emit({
        event: "backend.availability",
        payload: { state: "unavailable" },
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(screen.getByRole("status", { name: "Connection status" })).toHaveTextContent(
      "Last confirmed connected · Local AIO Kafka",
    );
    expect(screen.getByRole("contentinfo")).toHaveTextContent("Data is stale");
  });
});
