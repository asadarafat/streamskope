// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { StrictMode } from "react";
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import { KafkaQueryLibrary } from "../../src/features/kafka/application";
import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostEvent,
  type KafkaInvestigationQuery,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { createKafkaWorkspaceRegistration } from "../../src/features/kafka/ui/provider-workspace";
import { ProviderApplication } from "../../src/platform/ui/ProviderApplication";
import type {
  ProviderProfilesSnapshot,
  ProviderWorkspaceRegistration,
} from "../../src/platform/ui/provider-workspaces";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostAccepted, testHostExecute } from "../support/host-response";

afterEach(cleanup);

const configuration: KafkaInvestigationQuery = {
  schemaVersion: 1,
  request: {
    topic: "orders",
    mode: "time-window",
    maxMessages: 100,
    startTimeMs: Date.parse("2026-10-08T14:00:00.000Z"),
    endTimeMs: Date.parse("2026-10-08T14:05:00.000Z"),
  },
  filters: { key: "incident", value: "", offset: "", timestamp: "", partition: null },
};

async function fixture(
  strict = false,
  customDefaults = false,
  withSibling = false,
): Promise<{
  commands: HostCommand[];
  user: ReturnType<typeof userEvent.setup>;
  setCleanupBlocked: (blocked: boolean) => void;
  deferDisconnect: () => void;
  releaseDisconnect: () => void;
}> {
  const commands: HostCommand[] = [];
  const library = new KafkaQueryLibrary();
  await library.put({ id: "incident", name: "Incident", profileId: "fixture", configuration });
  const listeners = new Set<(event: HostEvent) => void>();
  let sequence = 0;
  let cleanupBlocked = false;
  let deferDisconnect = false;
  let releaseDisconnect: (() => void) | undefined;
  let active: string | null = null;
  const profiles: readonly ProfileSummary[] = ["fixture", "other"].map((id) => ({
    id,
    revision: 1,
    name: id === "fixture" ? "Fixture" : "Other",
    brokers: ["localhost:9092"],
    transport: "plaintext",
    active: false,
    createdAt: "2026-10-08T12:00:00.000Z",
    updatedAt: "2026-10-08T12:00:00.000Z",
  }));
  const emit = (event: Omit<HostEvent, "sequence" | "version">): void => {
    for (const listener of listeners)
      listener({ ...event, sequence: ++sequence, version: HOST_PROTOCOL_VERSION } as HostEvent);
  };
  const inventory = (): void =>
    emit({
      event: "profiles.changed",
      payload: {
        profiles: profiles.map((profile) => ({ ...profile, active: profile.id === active })),
        store: { state: "ready", protection: "memory", durability: "session" },
      },
    });
  const host: StreamSkopeHost = {
    execute: testHostExecute(async (command) => {
      commands.push(command);
      if (["queries.list", "queries.put", "queries.delete"].includes(command.command)) {
        const snapshot = await library.list();
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId: command.id, snapshot },
        };
      }
      if (command.command === "profiles.list") inventory();
      if (command.command === "messages.stop" && cleanupBlocked)
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: false,
          error: {
            activeStateChanged: false,
            code: "BACKEND_UNAVAILABLE",
            correlationId: command.id,
            retryable: true,
            stage: "backend",
            summary: "Stop remains unconfirmed.",
            recovery: "Retry after cleanup is available.",
          },
        };
      if (command.command === "connection.disconnect") {
        active = null;
        const publishDisconnect = (): void => {
          emit({
            event: "connection.state",
            payload: { state: "disconnected", connectionName: null },
          });
          inventory();
        };
        if (deferDisconnect) releaseDisconnect = publishDisconnect;
        else publishDisconnect();
      }
      if (command.command === "profiles.connect") {
        active = command.payload.profileId;
        emit({
          event: "connection.state",
          payload: {
            state: "connected",
            connectionName: profiles.find((profile) => profile.id === active)?.name ?? null,
          },
        });
        inventory();
      }
      if (command.command === "topics.list")
        emit({
          event: "topics.changed",
          payload: { state: "ready", refreshedAt: "2026-10-08T14:10:00.000Z", topics: ["orders"] },
        });
      if (command.command === "preferences.get") {
        const snapshot = {
          preferences: customDefaults
            ? {
                ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
                fetch: { mode: "newest" as const, maxMessages: 25 },
              }
            : KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: { state: "ready" as const, durability: "session" as const },
        };
        emit({ event: "preferences.changed", payload: snapshot });
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId: command.id, snapshot },
        };
      }
      if (command.command === "plugins.list")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId: command.id, pluginSnapshot: { revision: 0, plugins: [] } },
        };
      return testHostAccepted(command, command.id);
    }),
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    openExternalUrl: () => Promise.reject(new Error("No external navigation is expected.")),
  };
  const workspaces = [createKafkaWorkspaceRegistration({ host })];
  if (withSibling) {
    const snapshot: ProviderProfilesSnapshot = {
      profiles: [
        {
          id: "nats",
          name: "NATS Fixture",
          endpoints: ["remote.example:4222"],
          authentication: "None",
          transport: "TLS",
          source: "NATS",
          active: false,
        },
      ],
      available: true,
      storageReady: true,
      storageLabel: "Session-only test profile",
      loading: false,
      failure: null,
      creationActions: [],
    };
    const sibling: ProviderWorkspaceRegistration = {
      id: "nats",
      label: "NATS",
      profiles: {
        getSnapshot: () => snapshot,
        subscribe: () => () => undefined,
        refresh: () => Promise.resolve(),
        connect: () => Promise.resolve({ ok: true }),
        renderManagement: () => null,
      },
      deactivate: () => Promise.resolve({ state: "ready" }),
      render: (controls): React.JSX.Element => (
        <div>
          <p>NATS workspace fixture</p>
          {controls.profilesPage}
        </div>
      ),
    };
    workspaces.push(sibling);
  }
  const application = (
    <StreamSkopeThemeProvider>
      <ProviderApplication workspaces={workspaces} />
    </StreamSkopeThemeProvider>
  );
  render(strict ? <StrictMode>{application}</StrictMode> : application);
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "Saved queries" }));
  await user.click(await screen.findByRole("combobox", { name: "Saved query" }));
  await user.click(await screen.findByRole("option", { name: "Incident" }));
  await user.click(screen.getByRole("button", { name: "Open query" }));
  expect(commands.some((command) => command.command === "profiles.connect")).toBe(false);
  expect(commands.some((command) => command.command === "messages.start")).toBe(false);
  return {
    commands,
    user,
    setCleanupBlocked: (blocked): void => {
      cleanupBlocked = blocked;
    },
    deferDisconnect: (): void => {
      deferDisconnect = true;
    },
    releaseDisconnect: (): void => {
      releaseDisconnect?.();
      releaseDisconnect = undefined;
      deferDisconnect = false;
    },
  };
}

it.each([
  [false, false],
  [true, false],
  [false, true],
])(
  "retains the reviewed query through same-profile connection and waits for an explicit read (StrictMode=%s, custom defaults=%s)",
  async (strict, customDefaults) => {
    const { commands, user } = await fixture(strict, customDefaults);
    await user.click(
      await screen.findByRole("button", { name: "Connect insecure plaintext profile Fixture" }),
    );
    expect(await screen.findByRole("textbox", { name: "Start time (inclusive)" })).toHaveValue(
      "2026-10-08T14:00:00.000Z",
    );
    expect(screen.getByRole("textbox", { name: "End time (exclusive)" })).toHaveValue(
      "2026-10-08T14:05:00.000Z",
    );
    expect(screen.getByRole("combobox", { name: "Read mode" })).toHaveTextContent("Time window");
    expect(screen.getByRole("combobox", { name: "Record limit" })).toHaveTextContent("100");
    expect(commands.filter((command) => command.command === "messages.start")).toEqual([]);
    await user.click(screen.getByRole("button", { name: "Show message filters" }));
    expect(screen.getByRole("textbox", { name: "Key contains" })).toHaveValue("incident");
    await user.click(screen.getByRole("button", { name: "Load messages orders" }));
    expect(commands.filter((command) => command.command === "messages.start")).toMatchObject([
      { payload: configuration.request },
    ]);
    expect(commands.filter((command) => command.command === "queries.put")).toEqual([]);
  },
);

it("does not transfer a reviewed query to a different Kafka profile", async () => {
  const { commands, user } = await fixture();
  await user.click(
    await screen.findByRole("button", { name: "Connect insecure plaintext profile Other" }),
  );
  await waitFor(() =>
    expect(screen.getByLabelText("Connection status")).toHaveTextContent("Connected"),
  );
  expect(screen.queryByRole("textbox", { name: "Start time (inclusive)" })).not.toBeInTheDocument();
  expect(commands.filter((command) => command.command === "messages.start")).toEqual([]);
});

it("keeps profile actions available when the confirmed disconnect event arrives after activation replacement", async () => {
  const { commands, user, deferDisconnect, releaseDisconnect } = await fixture();
  await user.click(
    await screen.findByRole("button", { name: "Connect insecure plaintext profile Fixture" }),
  );
  await screen.findByRole("textbox", { name: "Start time (inclusive)" });
  await user.click(
    within(screen.getByRole("navigation", { name: "StreamSkope resources" })).getByRole("button", {
      name: "Connection Profiles",
    }),
  );
  deferDisconnect();
  await user.click(screen.getByRole("button", { name: "Disconnect profile Fixture" }));
  await waitFor(() =>
    expect(screen.getByTestId("provider-workspace")).toHaveAttribute("aria-busy", "false"),
  );
  act(releaseDisconnect);
  const profiles = await screen.findByRole("main", { name: "Connection profiles page" });
  expect(within(profiles).getByRole("button", { name: "Profile actions Fixture" })).toBeEnabled();
  expect(screen.getByLabelText("Connection status")).toHaveTextContent("Disconnected");
  expect(commands.filter((command) => command.command === "messages.start")).toEqual([]);
});

it("preserves the query when cleanup blocks connection, then restores it after a confirmed retry", async () => {
  const { commands, user, setCleanupBlocked } = await fixture();
  setCleanupBlocked(true);
  await user.click(
    await screen.findByRole("button", { name: "Connect insecure plaintext profile Fixture" }),
  );
  expect(
    await screen.findByRole("dialog", { name: "Unable to complete connection change" }),
  ).toBeVisible();
  expect(commands.filter((command) => command.command === "profiles.connect")).toEqual([]);
  setCleanupBlocked(false);
  await user.click(screen.getByRole("button", { name: "Retry connection" }));
  expect(await screen.findByRole("textbox", { name: "Start time (inclusive)" })).toHaveValue(
    "2026-10-08T14:00:00.000Z",
  );
  expect(commands.filter((command) => command.command === "profiles.connect")).toHaveLength(1);
  expect(commands.filter((command) => command.command === "messages.start")).toEqual([]);
});

it("discards the pending Kafka query when leaving for another provider", async () => {
  const { commands, user } = await fixture(false, false, true);
  await user.click(await screen.findByRole("button", { name: "Connect profile NATS Fixture" }));
  expect(await screen.findByText("NATS workspace fixture")).toBeVisible();
  await user.click(
    screen.getByRole("button", { name: "Connect insecure plaintext profile Fixture" }),
  );
  await waitFor(() =>
    expect(screen.getByLabelText("Connection status")).toHaveTextContent("Connected"),
  );
  expect(screen.queryByRole("textbox", { name: "Start time (inclusive)" })).not.toBeInTheDocument();
  expect(commands.filter((command) => command.command === "messages.start")).toEqual([]);
});
