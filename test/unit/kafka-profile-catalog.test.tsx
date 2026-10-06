// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseKafkaClusterDetailsDocument,
  type HostCommand,
  type HostEvent,
  type KafkaClusterDiagnosticsSnapshot,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { KafkaProfileCatalog } from "../../src/features/kafka/ui/profile-catalog";
import { createKafkaProfileManagementHost } from "../../src/features/kafka/ui/KafkaProfileManagement";
import { StreamSkopeApp } from "../../src/features/kafka/ui/StreamSkopeApp";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import type {
  ProviderConnectionOutcome,
  ProviderProfileManagementControls,
} from "../../src/platform/ui/provider-workspaces";
import type { PluginManifest } from "../../src/plugins/contracts";
import type { PluginRenderer, PluginViewContext } from "../../src/plugins/renderer-api";
import { testHostAccepted, testHostExecute } from "../support/host-response";

afterEach(cleanup);

const profile: ProfileSummary = {
  id: "kafka-profile",
  revision: 2,
  name: "Kafka laboratory",
  brokers: ["127.0.0.1:19092"],
  transport: "plaintext",
  active: false,
  createdAt: "2026-10-06T12:00:00.000Z",
  updatedAt: "2026-10-06T12:00:00.000Z",
};
const manifest: PluginManifest = {
  id: "sample.connection",
  name: "Sample workflow",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const diagnostics: KafkaClusterDiagnosticsSnapshot = {
  state: "ready",
  profile: { id: profile.id, name: profile.name, brokers: profile.brokers },
  endpoint: "127.0.0.1:19092",
  fetchedAt: "2026-10-06T12:00:00.000Z",
  cluster: {
    clusterId: "catalog-cluster",
    controllerId: 1,
    configurationSourceBrokerId: 1,
    brokers: [{ nodeId: 1, host: "127.0.0.1", port: 19092, rack: null }],
    configuration: [],
  },
};
const clusterDocumentContent = `${JSON.stringify(
  parseKafkaClusterDetailsDocument(
    {
      cluster: diagnostics.cluster,
      endpoint: diagnostics.endpoint,
      fetchedAt: diagnostics.fetchedAt,
      profile: diagnostics.profile,
    },
    "fixture",
  ),
  null,
  2,
)}\n`;

function fixture(plugin?: PluginRenderer): {
  readonly host: StreamSkopeHost;
  readonly catalog: KafkaProfileCatalog;
  readonly commands: HostCommand[];
  readonly listeners: Set<(event: HostEvent) => void>;
  readonly emit: (event: HostEvent) => void;
  readonly inventory: (profiles: readonly ProfileSummary[]) => void;
} {
  const commands: HostCommand[] = [];
  const listeners = new Set<(event: HostEvent) => void>();
  let profiles: readonly ProfileSummary[] = [profile];
  let sequence = 0;
  const emit = (event: HostEvent): void => {
    for (const listener of listeners) listener(event);
  };
  const inventory = (next: readonly ProfileSummary[]): void => {
    profiles = next;
    emit({
      event: "profiles.changed",
      sequence: ++sequence,
      version: HOST_PROTOCOL_VERSION,
      payload: { profiles, store: { durability: "session", protection: "memory", state: "ready" } },
    });
  };
  const host: StreamSkopeHost = {
    execute: testHostExecute((command) => {
      commands.push(command);
      if (command.command === "profiles.list") inventory(profiles);
      if (command.command === "profiles.create") {
        profiles = [
          { ...profile, id: "saved-profile", name: command.payload.profile.name, revision: 1 },
        ];
        inventory(profiles);
        return Promise.resolve({
          ...testHostAccepted(command, command.id),
          result: { correlationId: command.id, profileId: "saved-profile" },
        });
      }
      if (command.command === "plugins.list")
        return Promise.resolve({
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: {
            correlationId: command.id,
            pluginSnapshot: {
              revision: 1,
              plugins:
                plugin === undefined
                  ? []
                  : [
                      {
                        id: manifest.id,
                        active: manifest,
                        installed: manifest,
                        activationId: "activation-one",
                        pending: null,
                        restartRequired: false,
                        rendererUrl: `/plugins/${manifest.id}/activation-one/renderer.js`,
                      },
                    ],
            },
          },
        });
      if (command.command === "profiles.connect")
        emit({
          event: "connection.state",
          sequence: ++sequence,
          version: HOST_PROTOCOL_VERSION,
          payload: { connectionName: profiles[0]?.name ?? "Kafka laboratory", state: "connected" },
        });
      if (command.command === "clusterDetails.load")
        emit({
          event: "clusterDetails.changed",
          sequence: ++sequence,
          version: HOST_PROTOCOL_VERSION,
          payload: diagnostics,
        });
      if (command.command === "clusterDetails.export") {
        const content = clusterDocumentContent;
        return Promise.resolve({
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: {
            correlationId: command.id,
            document: {
              content,
              byteSize: new TextEncoder().encode(content).byteLength,
              fileName: "catalog-cluster.json",
              mediaType: "application/json",
            },
          },
        });
      }
      return Promise.resolve(testHostAccepted(command, command.id));
    }),
    openExternalUrl: () => Promise.reject(new Error("Unexpected external URL")),
    subscribe: (listener) => {
      listeners.add(listener);
      return (): void => {
        listeners.delete(listener);
      };
    },
  };
  const catalog = new KafkaProfileCatalog({
    host,
    ...(plugin === undefined
      ? {}
      : {
          pluginImporter: (): Promise<{ default: PluginRenderer }> =>
            Promise.resolve({ default: plugin }),
        }),
  });
  return { host, catalog, commands, listeners, emit, inventory };
}

function management(
  catalog: KafkaProfileCatalog,
  controls: ProviderProfileManagementControls,
): React.JSX.Element {
  return (
    <StreamSkopeThemeProvider>{catalog.facet.renderManagement(controls)}</StreamSkopeThemeProvider>
  );
}

describe("Kafka profile catalog ownership", () => {
  it("refreshes and projects safe profiles independently of workspace activation without retaining message batches", async () => {
    const source = fixture();
    const release = source.catalog.facet.subscribe(() => undefined);
    await source.catalog.facet.refresh();
    const before = source.catalog.facet.getSnapshot();
    expect(before.profiles).toEqual([
      {
        id: profile.id,
        revision: 2,
        name: profile.name,
        endpoints: profile.brokers,
        authentication: "None",
        transport: "Plaintext · insecure",
        source: "Kafka",
        active: false,
        actions: [{ id: "cluster", label: "Cluster detail", available: false }],
      },
    ]);
    source.emit({
      event: "messages.batch",
      sequence: 20,
      version: HOST_PROTOCOL_VERSION,
      payload: { droppedMessages: 0, messages: [], topic: "orders" },
    });
    expect(source.catalog.facet.getSnapshot()).toBe(before);
    expect(source.catalog.getManagementSnapshot()).not.toHaveProperty("messages");
    expect(source.commands.map((command) => command.command)).toEqual(["profiles.list"]);
    release();
    expect(source.listeners.size).toBe(0);
  });

  it("rejects a stale profile revision before connection dispatch and caches pre-mount connection evidence", async () => {
    const source = fixture();
    const result = await source.catalog.facet.connect({ id: profile.id, revision: 1 });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("Expected revision rejection");
    expect(result.summary).toContain("changed");
    expect(source.commands.map((command) => command.command)).toEqual(["profiles.list"]);
    await expect(source.catalog.facet.connect({ id: profile.id, revision: 2 })).resolves.toEqual({
      ok: true,
    });
    render(
      <StreamSkopeApp
        host={source.host}
        initialConnectionEvent={source.catalog.initialConnectionEvent()}
        profilesPage={<div>Unified profiles</div>}
      />,
    );
    expect(screen.getByRole("button", { name: "Overview" })).not.toBeDisabled();
    expect(screen.getByRole("contentinfo")).toHaveTextContent(profile.name);
    await userEvent.setup().click(screen.getByRole("button", { name: "Connection Profiles" }));
    expect(screen.getByText("Unified profiles")).toBeInTheDocument();
  });

  it("projects a recoverable storage failure while retaining the active connection", async () => {
    const source = fixture();
    const release = source.catalog.facet.subscribe(() => undefined);
    source.inventory([{ ...profile, active: true }]);
    await source.host.execute({
      command: "profiles.connect",
      id: "connect-storage",
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: profile.id },
    });
    const connection = source.catalog.initialConnectionEvent();
    source.emit({
      event: "profiles.changed",
      sequence: 100,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profiles: [{ ...profile, active: true }],
        store: {
          state: "unavailable",
          durability: "durable",
          protection: "unavailable",
          recovery: "Unlock the OS credential vault and restart StreamSkope.",
        },
      },
    });
    const snapshot = source.catalog.facet.getSnapshot();
    expect(snapshot.storageReady).toBe(false);
    expect(snapshot.failure).toEqual({
      summary: "Profile storage unavailable",
      recovery: "Unlock the OS credential vault and restart StreamSkope.",
    });
    expect(snapshot.profiles[0]?.active).toBe(true);
    expect(snapshot.creationActions.find((action) => action.id === "direct")?.available).toBe(
      false,
    );
    expect(source.catalog.initialConnectionEvent()).toBe(connection);
    expect(source.commands.map((command) => command.command)).toEqual(["profiles.connect"]);
    release();
  });

  it("routes Save and connect and saved-profile retry through catalog authority while preserving the saved receipt", async () => {
    const source = fixture();
    const onConnect = vi
      .fn<ProviderProfileManagementControls["onConnect"]>()
      .mockResolvedValueOnce({
        ok: false,
        summary: "Original provider cleanup is blocked.",
        recovery: "Stop the active subscription and retry.",
      })
      .mockResolvedValueOnce({ ok: true });
    const onProfileReady = vi.fn();
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      management(source.catalog, {
        action: { kind: "create", actionId: "direct" },
        isInteractive: () => true,
        onClose,
        onProfileReady,
        onConnect,
      }),
    );
    const dialog = screen.getByRole("dialog", { name: "Add Kafka profile" });
    await user.click(within(dialog).getByRole("radio", { name: "Plaintext (insecure)" }));
    await user.type(
      within(dialog).getByRole("textbox", { name: "Profile name" }),
      "Saved Kafka profile",
    );
    await user.type(
      within(dialog).getByRole("textbox", { name: "Bootstrap brokers" }),
      "127.0.0.1:19092",
    );
    await user.click(within(dialog).getByRole("button", { name: "Save and connect" }));
    await waitFor(() =>
      expect(onConnect).toHaveBeenCalledWith({ id: "saved-profile", revision: 1 }),
    );
    expect(onProfileReady).toHaveBeenCalledWith("saved-profile");
    expect(
      screen.getByText(/Profile saved\. Connection failed: Original provider cleanup is blocked/u),
    ).toBeInTheDocument();
    expect(source.commands.map((command) => command.command)).not.toContain("profiles.connect");
    const saves = source.commands.filter((command) => command.command === "profiles.create");
    expect(saves).toHaveLength(1);
    await user.click(within(dialog).getByRole("button", { name: "Retry connection" }));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    expect(onConnect).toHaveBeenCalledTimes(2);
    expect(source.commands.filter((command) => command.command === "profiles.create")).toHaveLength(
      1,
    );
  });

  it("projects installed plugin creation choices while closed and retains connector activation fences", async () => {
    let context: PluginViewContext | undefined;
    const plugin: PluginRenderer = {
      id: manifest.id,
      apiVersion: 2,
      connectionActions: [{ id: "capture", label: "Create through sample" }],
      mount: (_element, next) => {
        context = next;
        return {
          update: (nextContext): void => {
            context = nextContext;
          },
          dispose: (): void => undefined,
        };
      },
    };
    const source = fixture(plugin);
    const controls: ProviderProfileManagementControls = {
      action: null,
      isInteractive: () => false,
      onClose: vi.fn(),
      onProfileReady: vi.fn(),
      onConnect: (): Promise<ProviderConnectionOutcome> => Promise.resolve({ ok: true }),
    };
    const view = render(management(source.catalog, controls));
    await source.catalog.facet.refresh();
    await waitFor(() =>
      expect(
        source.commands.filter((command) => command.command === "plugins.renderer.failed"),
      ).toEqual([]),
    );
    await waitFor(() =>
      expect(source.catalog.facet.getSnapshot().creationActions).toContainEqual(
        expect.objectContaining({
          id: `plugin:${manifest.id}:capture`,
          label: "Create through sample",
          kind: "connector",
          available: true,
        }),
      ),
    );
    expect(source.catalog.facet.getSnapshot().creationActions).toContainEqual(
      expect.objectContaining({ id: "manage-plugins", available: true }),
    );
    expect(source.commands.some((command) => command.command === "connection.disconnect")).toBe(
      false,
    );
    const action = { kind: "create", actionId: `plugin:${manifest.id}:capture` } as const;
    view.rerender(management(source.catalog, { ...controls, action, isInteractive: () => true }));
    await waitFor(() => expect(context).toBeDefined());
    const admitted = context;
    if (admitted === undefined) throw new Error("Expected plugin management context");
    act(() => admitted.onProfileReady(profile.id));
    expect(controls.onProfileReady).toHaveBeenCalledWith(profile.id);
    view.rerender(management(source.catalog, controls));
    await expect(
      admitted.host.execute({
        command: "profiles.delete",
        id: "retired-view",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: profile.id },
      }),
    ).rejects.toThrow("no longer active");
  });

  it("denies stream authority and stale management callbacks without rewriting admitted receipts", async () => {
    const source = fixture();
    let interactive = true;
    const host = createKafkaProfileManagementHost(source.host, () => interactive);
    const admitted = host.execute({
      command: "profiles.delete",
      id: "admitted-delete",
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: profile.id },
    });
    interactive = false;
    await expect(admitted).resolves.toMatchObject({ ok: true, id: "admitted-delete" });
    await expect(
      host.execute({
        command: "profiles.list",
        id: "retired-list",
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      }),
    ).rejects.toThrow("no longer active");
    interactive = true;
    await expect(
      host.execute({
        command: "profiles.connect",
        id: "bypass-connect",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: profile.id },
      }),
    ).rejects.toThrow("connection catalog");
    expect(source.commands).toHaveLength(1);
  });

  it("opens the Kafka editor for a plugin-provided existing destination and retires the connector view", async () => {
    let context: PluginViewContext | undefined;
    const plugin: PluginRenderer = {
      id: manifest.id,
      apiVersion: 2,
      connectionActions: [{ id: "capture", label: "Create through sample" }],
      mount: (_element, next) => {
        context = next;
        return { update: (): void => undefined, dispose: (): void => undefined };
      },
    };
    const source = fixture(plugin);
    render(
      management(source.catalog, {
        action: { kind: "create", actionId: `plugin:${manifest.id}:capture` },
        isInteractive: () => true,
        onClose: (): void => undefined,
        onProfileReady: (): void => undefined,
        onConnect: (): Promise<ProviderConnectionOutcome> => Promise.resolve({ ok: true }),
      }),
    );
    await waitFor(() => expect(context).toBeDefined());
    const connector = context;
    if (connector === undefined) throw new Error("Expected connector context");
    act(() =>
      connector.onExistingDestination({
        name: "Discovered broker",
        brokers: ["discovered.example:9093"],
      }),
    );
    const dialog = await screen.findByRole("dialog", { name: "Add Kafka profile" });
    expect(within(dialog).getByRole("textbox", { name: "Profile name" })).toHaveValue(
      "Discovered broker",
    );
    expect(within(dialog).getByRole("textbox", { name: "Bootstrap brokers" })).toHaveValue(
      "discovered.example:9093",
    );
    await expect(
      connector.host.execute({
        command: "profiles.delete",
        id: "retired-connector",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: profile.id },
      }),
    ).rejects.toThrow("no longer active");
  });

  it("retains active Kafka cluster detail and JSON export with connection-scoped diagnostic admission", async () => {
    const source = fixture();
    const release = source.catalog.facet.subscribe(() => undefined);
    source.inventory([{ ...profile, active: true }]);
    await source.host.execute({
      command: "profiles.connect",
      id: "connect-cluster",
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: profile.id },
    });
    expect(source.catalog.facet.getSnapshot().profiles[0]?.actions).toEqual([
      { id: "cluster", label: "Cluster detail", available: true },
    ]);
    const user = userEvent.setup();
    const copy = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
    render(
      management(source.catalog, {
        action: { kind: "provider", actionId: "cluster", profileId: profile.id },
        isInteractive: () => true,
        onClose: (): void => undefined,
        onProfileReady: (): void => undefined,
        onConnect: (): Promise<ProviderConnectionOutcome> => Promise.resolve({ ok: true }),
      }),
    );
    await screen.findByRole("dialog", { name: "Cluster details" });
    await screen.findByText("catalog-cluster");
    await user.click(screen.getByRole("button", { name: "Copy cluster details JSON" }));
    await waitFor(() => expect(copy).toHaveBeenCalledWith(clusterDocumentContent));
    const loads = source.commands.filter(
      (command) => command.command === "clusterDetails.load",
    ).length;
    act(() =>
      source.emit({
        event: "connection.state",
        sequence: 100,
        version: HOST_PROTOCOL_VERSION,
        payload: { connectionName: null, state: "disconnected" },
      }),
    );
    expect(source.catalog.facet.getSnapshot().profiles[0]?.actions?.[0]?.available).toBe(false);
    expect(source.catalog.getManagementSnapshot().clusterDiagnostics.state).toBe("unavailable");
    expect(screen.getByRole("button", { name: "Refresh cluster details" })).toBeDisabled();
    expect(
      source.commands.filter((command) => command.command === "clusterDetails.load"),
    ).toHaveLength(loads);
    release();
  });
});
