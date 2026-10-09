// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { StrictMode, isValidElement, useEffect, useMemo } from "react";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  ConnectionProfilesCatalogProperties,
  CatalogProfile,
} from "../../src/platform/ui/ConnectionProfilesCatalog";
import { ProviderApplication } from "../../src/platform/ui/ProviderApplication";
import type {
  ProviderDeactivationResult,
  ProviderConnectionOutcome,
  ProviderProfileManagementControls,
  ProviderProfilesFacet,
  ProviderProfilesSnapshot,
  ProviderWorkspaceControls,
  ProviderWorkspaceRegistration,
} from "../../src/platform/ui/provider-workspaces";
import { StudioButton, StudioDialog, StudioDialogTitle } from "../../src/platform/ui/controls";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import {
  createInteractiveKafkaHost,
  createKafkaWorkspaceRegistration,
} from "../../src/features/kafka/ui/provider-workspace";
import { useWorkbenchActivity } from "../../src/features/kafka/ui/use-workbench-activity";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  DESKTOP_PLATFORM_VERSION,
  type DesktopActionListener,
  type StreamSkopeDesktop,
} from "../../src/platform/desktop";
import { testHostAccepted, testHostExecute } from "../support/host-response";

afterEach(cleanup);

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
} {
  let resolve: (value: T) => void = () => {
    throw new Error("Deferred fixture was not initialized.");
  };
  let reject: (reason: unknown) => void = () => {
    throw new Error("Deferred fixture was not initialized.");
  };
  const promise = new Promise<T>((accept, fail) => {
    resolve = accept;
    reject = fail;
  });
  return { promise, resolve, reject };
}

const command: Extract<HostCommand, { command: "connection.disconnect" }> = {
  command: "connection.disconnect",
  id: "view-request",
  payload: {},
  version: HOST_PROTOCOL_VERSION,
};

function hostFixture(): {
  host: StreamSkopeHost;
  commands: HostCommand[];
  listeners: Set<(event: HostEvent) => void>;
} {
  const commands: HostCommand[] = [],
    listeners = new Set<(event: HostEvent) => void>();
  const host: StreamSkopeHost = {
    execute: testHostExecute((submitted) => {
      commands.push(submitted);
      return Promise.resolve(
        submitted.command === "plugins.list"
          ? {
              command: submitted.command,
              id: submitted.id,
              version: HOST_PROTOCOL_VERSION,
              ok: true,
              result: { correlationId: submitted.id, pluginSnapshot: { revision: 0, plugins: [] } },
            }
          : testHostAccepted(submitted, submitted.id),
      );
    }),
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    openExternalUrl: () => Promise.reject(new Error("Unexpected external URL.")),
  };
  return { host, commands, listeners };
}

function catalogRow(providerId: string, profileId = "shared-id"): CatalogProfile {
  return {
    id: JSON.stringify([providerId, profileId]),
    profileId,
    providerId,
    providerLabel: providerId === "kafka" ? "Kafka" : "Probe",
    revision: 1,
    name: `${providerId} profile`,
    endpoints: ["remote.example:1234"],
    source: "Direct",
    authentication: "None",
    transport: "TLS",
    active: false,
  };
}
function facetFixture(
  providerId: string,
  captures: ProviderProfileManagementControls[] = [],
  connect: ProviderProfilesFacet["connect"] = vi.fn(() =>
    Promise.resolve<ProviderConnectionOutcome>({ ok: true }),
  ),
): ProviderProfilesFacet {
  const row = catalogRow(providerId);
  const snapshot: ProviderProfilesSnapshot = {
    profiles: [{ ...row, id: row.profileId }],
    loading: false,
    available: true,
    storageReady: true,
    storageLabel: "Protected storage",
    failure: null,
    creationActions: [
      {
        id: "new",
        label: providerId === "kafka" ? "Kafka broker" : "NATS server",
        description: "Direct connection",
        kind: "direct",
        available: true,
      },
    ],
  };
  return {
    getSnapshot: () => snapshot,
    subscribe: () => () => undefined,
    refresh: () => Promise.resolve(),
    connect,
    renderManagement: (controls): React.JSX.Element | null => {
      captures.push(controls);
      return controls.action === null ? null : <div>{providerId} profile settings</div>;
    },
  };
}

interface Capture {
  readonly controls: ProviderWorkspaceControls;
  readonly proxy: StreamSkopeHost;
  readonly select: (id: string) => void;
}
function WorkspaceProbe({
  controls,
  host,
  captures,
  portal,
  desktop,
}: {
  readonly controls: ProviderWorkspaceControls;
  readonly host: StreamSkopeHost;
  readonly captures: Capture[];
  readonly portal: boolean;
  readonly desktop?: StreamSkopeDesktop;
}): React.JSX.Element {
  const proxy = useMemo(
    () => createInteractiveKafkaHost(host, controls.isInteractive),
    [host, controls.isInteractive],
  );
  useEffect(() => proxy.subscribe(() => undefined), [proxy]);
  const activity = useWorkbenchActivity(desktop, [], controls.isInteractive);
  if (!isValidElement<ConnectionProfilesCatalogProperties>(controls.profilesPage))
    throw new Error("Expected the real provider control.");
  const control = controls.profilesPage;
  captures.push({
    controls,
    proxy,
    select: (id): void => control.props.onConnect(catalogRow(id)),
  });
  const action = (
    <StudioButton
      onClick={(): void => {
        void proxy.execute(command).catch(() => undefined);
      }}
    >
      Portal broker action
    </StudioButton>
  );
  return (
    <>
      <div>Kafka fixture workspace</div>
      {controls.profilesPage}
      <span>{activity.commandPaletteOpen ? "Palette open" : "Palette closed"}</span>
      <span>{activity.preferenceDialogOpen ? "Preferences open" : "Preferences closed"}</span>
      {portal ? (
        <StudioDialog open disableAutoFocus disableEnforceFocus>
          <StudioDialogTitle>Plugin fixture portal</StudioDialogTitle>
          {action}
        </StudioDialog>
      ) : null}
    </>
  );
}

function workspaceFixtures(
  deactivate: () => Promise<ProviderDeactivationResult>,
  options: { readonly portal?: boolean; readonly desktop?: StreamSkopeDesktop } = {},
): {
  workspaces: readonly ProviderWorkspaceRegistration[];
  captures: Capture[];
  probeSelections: Array<(id: string) => void>;
  probeDeactivate: ReturnType<typeof vi.fn>;
  fixture: ReturnType<typeof hostFixture>;
} {
  const fixture = hostFixture(),
    captures: Capture[] = [],
    probeSelections: Array<(id: string) => void> = [];
  const probeDeactivate = vi.fn(() =>
    Promise.resolve<ProviderDeactivationResult>({ state: "ready" }),
  );
  return {
    fixture,
    captures,
    probeSelections,
    probeDeactivate,
    workspaces: [
      {
        id: "kafka",
        label: "Kafka",
        deactivate,
        profiles: facetFixture("kafka"),
        render: (controls) => (
          <WorkspaceProbe
            controls={controls}
            host={fixture.host}
            captures={captures}
            portal={options.portal ?? false}
            {...(options.desktop === undefined ? {} : { desktop: options.desktop })}
          />
        ),
      },
      {
        id: "probe",
        label: "Probe",
        deactivate: probeDeactivate,
        profiles: facetFixture("probe"),
        render: (controls): React.JSX.Element => {
          if (!isValidElement<ConnectionProfilesCatalogProperties>(controls.profilesPage))
            throw new Error("Expected the real provider control.");
          const control = controls.profilesPage;
          probeSelections.push((id): void => control.props.onConnect(catalogRow(id)));
          return <div>Probe fixture workspace{controls.profilesPage}</div>;
        },
      },
    ],
  };
}
function latest(captures: readonly Capture[]): Capture {
  const capture = captures.at(-1);
  if (capture === undefined) throw new Error("Expected a mounted fixture workspace.");
  return capture;
}
function renderApplication(
  workspaces: readonly ProviderWorkspaceRegistration[],
): ReturnType<typeof render> {
  return render(
    <StreamSkopeThemeProvider>
      <ProviderApplication workspaces={workspaces} />
    </StreamSkopeThemeProvider>,
  );
}

describe("connection catalog behavior", () => {
  it("shows profiles with colliding local IDs and lets users inspect/create the other provider without stopping the active workspace", async () => {
    const fixtures = workspaceFixtures(vi.fn(() => Promise.resolve({ state: "ready" as const })));
    const managers: ProviderProfileManagementControls[] = [];
    const first = fixtures.workspaces[0],
      second = fixtures.workspaces[1];
    if (!first || !second) throw new Error("Expected both providers.");
    const workspaces = [first, { ...second, profiles: facetFixture("probe", managers) }];
    renderApplication(workspaces);
    const original = latest(fixtures.captures);
    const page = original.controls.profilesPage;
    if (!isValidElement<ConnectionProfilesCatalogProperties>(page))
      throw new Error("Expected catalog.");
    expect(page.props.snapshots.map((snapshot) => snapshot.profiles[0]?.id)).toEqual([
      "shared-id",
      "shared-id",
    ]);
    act(() => page.props.onSelect(catalogRow("probe")));
    expect(screen.getByText("probe profile settings")).toBeInTheDocument();
    expect(first.deactivate).not.toHaveBeenCalled();
    expect(fixtures.probeDeactivate).not.toHaveBeenCalled();
    expect(original.controls.isInteractive()).toBe(true);
    act(() => page.props.onCreate("probe", "new"));
    const create = managers.at(-1);
    if (!create) throw new Error("Expected inactive-provider editor.");
    expect(create.action).toEqual({ kind: "create", actionId: "new" });
    expect(create.isInteractive()).toBe(true);
    act(() => create.onProfileReady("new-profile"));
    expect(create.isInteractive()).toBe(true);
    act(() => create.onClose());
    expect(create.isInteractive()).toBe(false);
    await expect(create.onConnect({ id: "shared-id", revision: 1 })).resolves.toMatchObject({
      ok: false,
    });
    expect(first.deactivate).not.toHaveBeenCalled();
    expect(original.controls.isInteractive()).toBe(true);
  });

  it("routes inactive-provider Save and connect through confirmed original-host cleanup and only then dispatches the destination", async () => {
    const cleanup = deferred<ProviderDeactivationResult>();
    const deactivate = vi.fn(() => cleanup.promise);
    const fixtures = workspaceFixtures(deactivate);
    const managers: ProviderProfileManagementControls[] = [];
    const connect = vi.fn(() => Promise.resolve({ ok: true as const }));
    const first = fixtures.workspaces[0],
      second = fixtures.workspaces[1];
    if (!first || !second) throw new Error("Expected both providers.");
    renderApplication([first, { ...second, profiles: facetFixture("probe", managers, connect) }]);
    const original = latest(fixtures.captures);
    const page = original.controls.profilesPage;
    if (!isValidElement<ConnectionProfilesCatalogProperties>(page))
      throw new Error("Expected catalog.");
    act(() => page.props.onCreate("probe", "new"));
    const manager = managers.at(-1);
    if (!manager) throw new Error("Expected editor.");
    let receipt: Promise<unknown> | undefined;
    act(() => {
      receipt = manager.onConnect({ id: "shared-id", revision: 1 });
    });
    expect(original.controls.isInteractive()).toBe(false);
    expect(connect).not.toHaveBeenCalled();
    await waitFor(() => expect(deactivate).toHaveBeenCalledOnce());
    await act(async () => {
      cleanup.resolve({ state: "ready" });
      await receipt;
    });
    expect(connect).toHaveBeenCalledExactlyOnceWith({ id: "shared-id", revision: 1 });
    expect(screen.getByText("Probe fixture workspace")).toBeInTheDocument();
    expect(manager.isInteractive()).toBe(false);
    expect(original.controls.isInteractive()).toBe(false);
  });

  it("preserves the actual admitted target receipt when the application unmounts during connection", async () => {
    const receipt = deferred<ProviderConnectionOutcome>();
    const fixtures = workspaceFixtures(() => Promise.resolve({ state: "ready" }));
    const first = fixtures.workspaces[0],
      second = fixtures.workspaces[1];
    if (!first || !second) throw new Error("Expected both providers.");
    const managers: ProviderProfileManagementControls[] = [];
    const connect = vi.fn(() => receipt.promise);
    const view = renderApplication([
      first,
      { ...second, profiles: facetFixture("probe", managers, connect) },
    ]);
    const page = latest(fixtures.captures).controls.profilesPage;
    if (!isValidElement<ConnectionProfilesCatalogProperties>(page))
      throw new Error("Expected catalog.");
    act(() => page.props.onCreate("probe", "new"));
    const manager = managers.at(-1);
    if (!manager) throw new Error("Expected editor.");
    let actual: Promise<ProviderConnectionOutcome> | undefined;
    act(() => {
      actual = manager.onConnect({ id: "shared-id", revision: 1 });
    });
    await waitFor(() => expect(connect).toHaveBeenCalledOnce());
    view.unmount();
    await act(async () => {
      receipt.resolve({ ok: true });
      await receipt.promise;
    });
    await expect(actual).resolves.toEqual({ ok: true });
    expect(manager.isInteractive()).toBe(false);
    expect(fixtures.probeSelections).toEqual([]);
  });

  it("does not dispatch the destination on uncertain cleanup and safely exposes a failed destination after confirmed cleanup", async () => {
    const blocked = vi.fn(() =>
      Promise.resolve({
        state: "blocked" as const,
        summary: "Stop not confirmed",
        recovery: "Retry stop",
      }),
    );
    const fixtures = workspaceFixtures(blocked);
    const first = fixtures.workspaces[0],
      second = fixtures.workspaces[1];
    if (!first || !second) throw new Error("Expected both providers.");
    const connect = vi.fn(() =>
      Promise.resolve({
        ok: false as const,
        summary: "Target authentication failed",
        recovery: "Edit the target profile",
      }),
    );
    const view = renderApplication([
      first,
      { ...second, profiles: facetFixture("probe", [], connect) },
    ]);
    act(() => latest(fixtures.captures).select("probe"));
    await screen.findByText(/Stop not confirmed/u);
    expect(connect).not.toHaveBeenCalled();
    expect(latest(fixtures.captures).controls.isInteractive()).toBe(true);
    view.unmount();
    const next = workspaceFixtures(() => Promise.resolve({ state: "ready" }));
    const source = next.workspaces[0],
      target = next.workspaces[1];
    if (!source || !target) throw new Error("Expected both providers.");
    renderApplication([source, { ...target, profiles: facetFixture("probe", [], connect) }]);
    const original = latest(next.captures);
    act(() => original.select("probe"));
    await screen.findByText(/Target authentication failed/u);
    expect(connect).toHaveBeenCalledOnce();
    expect(screen.getByText("Probe fixture workspace")).toBeInTheDocument();
    expect(original.controls.isInteractive()).toBe(false);
  });
});

describe("provider activation ownership", () => {
  it("retains the inert old workspace until cleanup confirms ready and synchronously rejects rapid selection", async () => {
    const pending = deferred<ProviderDeactivationResult>(),
      deactivate = vi.fn(() => pending.promise);
    const fixtures = workspaceFixtures(deactivate);
    renderApplication(fixtures.workspaces);
    const original = latest(fixtures.captures);
    act(() => {
      original.select("unknown");
    });
    expect(deactivate).not.toHaveBeenCalled();
    act(() => {
      original.select("probe");
      original.select("probe");
      original.select("unknown");
    });
    expect(original.controls.isInteractive()).toBe(false);
    expect(screen.getByText("Kafka fixture workspace")).toBeInTheDocument();
    expect(screen.queryByText("Probe fixture workspace")).not.toBeInTheDocument();
    expect(screen.getByTestId("provider-workspace")).toHaveAttribute("inert");
    expect(screen.queryByRole("combobox", { name: "Messaging provider" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Add connection" })).toBeDisabled();
    await waitFor(() => expect(deactivate).toHaveBeenCalledTimes(1));
    await act(async () => {
      pending.resolve({ state: "ready" });
      await pending.promise;
    });
    expect(screen.getByText("Probe fixture workspace")).toBeInTheDocument();
    expect(screen.getByTestId("provider-workspace")).not.toHaveAttribute("inert");
    expect(original.controls.isInteractive()).toBe(false);
  });

  it("keeps actual portalled broker callbacks denied during switching and permanently retires them after returning", async () => {
    const pending = deferred<ProviderDeactivationResult>(),
      deactivate = vi.fn(() => pending.promise);
    const fixtures = workspaceFixtures(deactivate, { portal: true });
    renderApplication(fixtures.workspaces);
    const original = latest(fixtures.captures),
      portalButton = screen.getByRole("button", { name: "Portal broker action" });
    expect(portalButton.closest('[data-testid="provider-workspace"]')).toBeNull();
    act(() => {
      original.select("probe");
      fireEvent.click(portalButton);
    });
    expect(fixtures.fixture.commands).toEqual([]);
    expect(portalButton.closest("[inert]")).toBeNull();
    await act(async () => {
      pending.resolve({ state: "ready" });
      await pending.promise;
    });
    await expect(original.proxy.execute(command)).rejects.toThrow("workspace is inactive");
    const selectProbe = fixtures.probeSelections.at(-1);
    if (selectProbe === undefined) throw new Error("Expected a probe provider control.");
    await act(async () => {
      selectProbe("kafka");
      await Promise.resolve();
    });
    const returned = latest(fixtures.captures);
    expect(returned.controls.isInteractive).not.toBe(original.controls.isInteractive);
    expect(returned.controls.isInteractive()).toBe(true);
    await returned.proxy.execute(command);
    expect(fixtures.fixture.commands).toEqual([command]);
    act(() => original.select("probe"));
    await expect(original.proxy.execute(command)).rejects.toThrow("workspace is inactive");
    const listeners = fixtures.fixture.listeners.size;
    original.proxy.subscribe(() => undefined)();
    expect(fixtures.fixture.listeners.size).toBe(listeners);
    expect(fixtures.fixture.commands).toEqual([command]);
    expect(deactivate).toHaveBeenCalledTimes(1);
    expect(fixtures.probeDeactivate).toHaveBeenCalledTimes(1);
    expect(returned.controls.isInteractive()).toBe(true);
  });

  it("restores the same activation and proxy after cleanup failure and allows an explicit retry", async () => {
    const first = deferred<ProviderDeactivationResult>(),
      second = deferred<ProviderDeactivationResult>();
    const deactivate = vi
      .fn()
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise);
    const fixtures = workspaceFixtures(deactivate);
    renderApplication(fixtures.workspaces);
    const original = latest(fixtures.captures);
    act(() => original.select("probe"));
    await act(async () => {
      first.resolve({
        state: "blocked",
        summary: "Actual cleanup is unconfirmed.",
        recovery: "Resolve the cleanup and retry.",
      });
      await first.promise;
    });
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Actual cleanup is unconfirmed. Resolve the cleanup and retry.",
    );
    expect(original.controls.isInteractive()).toBe(true);
    expect(latest(fixtures.captures).proxy).toBe(original.proxy);
    expect(fixtures.fixture.listeners.size).toBe(1);
    await original.proxy.execute(command);
    fireEvent.click(screen.getByRole("button", { name: "Retry connection" }));
    expect(original.controls.isInteractive()).toBe(false);
    await waitFor(() => expect(deactivate).toHaveBeenCalledTimes(2));
    await act(async () => {
      second.resolve({ state: "ready" });
      await second.promise;
    });
    expect(screen.getByText("Probe fixture workspace")).toBeInTheDocument();
    expect(original.controls.isInteractive()).toBe(false);
  });

  it("keeps keyboard and native provider dialogs closed during cleanup, then restores those same hooks after failure", async () => {
    const pending = deferred<ProviderDeactivationResult>(),
      desktopListeners = new Set<DesktopActionListener>();
    const desktop: StreamSkopeDesktop = {
      saveArtifact: () =>
        Promise.resolve({ state: "cancelled", version: DESKTOP_PLATFORM_VERSION }),
      saveTextDocument: () =>
        Promise.resolve({ state: "cancelled", version: DESKTOP_PLATFORM_VERSION }),
      subscribeActions: (listener) => {
        desktopListeners.add(listener);
        return () => {
          desktopListeners.delete(listener);
        };
      },
    };
    const fixtures = workspaceFixtures(() => pending.promise, { desktop });
    renderApplication(fixtures.workspaces);
    act(() => latest(fixtures.captures).select("probe"));
    const blockedKey = new KeyboardEvent("keydown", { key: "k", ctrlKey: true, cancelable: true });
    act(() => {
      globalThis.dispatchEvent(blockedKey);
      for (const listener of desktopListeners)
        listener({ action: "preferences.open", version: DESKTOP_PLATFORM_VERSION });
    });
    expect(blockedKey.defaultPrevented).toBe(false);
    expect(screen.getByText("Palette closed")).toBeInTheDocument();
    expect(screen.getByText("Preferences closed")).toBeInTheDocument();
    await act(async () => {
      pending.resolve({
        state: "blocked",
        summary: "Stop failed.",
        recovery: "Retry after recovery.",
      });
      await pending.promise;
    });
    fireEvent.click(screen.getByRole("button", { name: "Keep working" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    const restoredKey = new KeyboardEvent("keydown", { key: "k", ctrlKey: true, cancelable: true });
    act(() => {
      globalThis.dispatchEvent(restoredKey);
      for (const listener of desktopListeners)
        listener({ action: "preferences.open", version: DESKTOP_PLATFORM_VERSION });
    });
    expect(restoredKey.defaultPrevented).toBe(true);
    expect(screen.getByText("Palette open")).toBeInTheDocument();
    expect(screen.getByText("Preferences open")).toBeInTheDocument();
  });

  it("fences late cleanup completion after root unmount and uses safe rejection guidance", async () => {
    const pending = deferred<ProviderDeactivationResult>(),
      fixtures = workspaceFixtures(() => pending.promise);
    const view = renderApplication(fixtures.workspaces),
      original = latest(fixtures.captures);
    act(() => original.select("probe"));
    await act(async () => {
      await Promise.resolve();
    });
    view.unmount();
    expect(original.controls.isInteractive()).toBe(false);
    expect(fixtures.fixture.listeners.size).toBe(0);
    await act(async () => {
      pending.resolve({ state: "ready" });
      await pending.promise;
    });
    expect(fixtures.probeSelections).toEqual([]);
    await expect(original.proxy.execute(command)).rejects.toThrow("workspace is inactive");
    const next = workspaceFixtures(() =>
      Promise.reject(new Error("private-credential-never-display")),
    );
    renderApplication(next.workspaces);
    act(() => latest(next.captures).select("probe"));
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent("could not finish connection cleanup"),
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent("private-credential");
    expect(latest(next.captures).controls.isInteractive()).toBe(true);
  });

  it("keeps real Kafka workspace subscriptions balanced and its final authority active through StrictMode replay", async () => {
    const initialQueryImport = JSON.stringify({
      schemaVersion: 1,
      request: { mode: "tail", topic: "orders", maxMessages: 10 },
    });
    const fixture = hostFixture(),
      registration = createKafkaWorkspaceRegistration({ host: fixture.host, initialQueryImport });
    const authorities: ProviderWorkspaceControls[] = [];
    const wrapped: ProviderWorkspaceRegistration = {
      ...registration,
      profiles: facetFixture("kafka"),
      render: (controls) => {
        authorities.push(controls);
        return registration.render(controls);
      },
    };
    let probeControls: ProviderWorkspaceControls | undefined;
    const probe: ProviderWorkspaceRegistration = {
      id: "probe",
      label: "Probe",
      deactivate: () => Promise.resolve({ state: "ready" }),
      profiles: facetFixture("probe"),
      render: (controls) => {
        probeControls = controls;
        return <div>Strict replay probe{controls.profilesPage}</div>;
      },
    };
    const workspaces = [wrapped, probe];
    const view = render(
      <StrictMode>
        <StreamSkopeThemeProvider>
          <ProviderApplication workspaces={workspaces} />
        </StreamSkopeThemeProvider>
      </StrictMode>,
    );
    await waitFor(() =>
      expect(fixture.commands.some((submitted) => submitted.command === "plugins.list")).toBe(true),
    );
    expect(await screen.findByRole("textbox", { name: "Query JSON or link" })).toHaveValue(
      initialQueryImport,
    );
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    const firstAuthority = authorities.at(-1);
    if (firstAuthority === undefined)
      throw new Error("Expected initial Kafka workspace authority.");
    const control = firstAuthority.profilesPage;
    if (!isValidElement<ConnectionProfilesCatalogProperties>(control))
      throw new Error("Expected current provider selector.");
    await act(async () => {
      control.props.onConnect(catalogRow("probe"));
      await Promise.resolve();
    });
    expect(screen.getByText("Strict replay probe")).toBeInTheDocument();
    if (
      probeControls === undefined ||
      !isValidElement<ConnectionProfilesCatalogProperties>(probeControls.profilesPage)
    )
      throw new Error("Expected probe provider selector.");
    const returnControl = probeControls.profilesPage;
    await act(async () => {
      returnControl.props.onConnect(catalogRow("kafka"));
      await Promise.resolve();
    });
    expect(screen.queryByRole("textbox", { name: "Query JSON or link" })).not.toBeInTheDocument();
    expect(firstAuthority.isInteractive()).toBe(false);
    const current = authorities.at(-1);
    if (current === undefined) throw new Error("Expected current Kafka workspace authority.");
    expect(current.isInteractive()).toBe(true);
    expect(fixture.listeners.size).toBeGreaterThan(0);
    const proxy = createInteractiveKafkaHost(fixture.host, current.isInteractive);
    await expect(proxy.execute(command)).resolves.toMatchObject({ ok: true });
    expect(screen.getAllByRole("banner", { name: "StreamSkope application bar" })).toHaveLength(1);
    expect(
      screen
        .getByRole("heading", { name: "Connection Profiles" })
        .closest("header")
        ?.closest("main"),
    ).toBe(screen.getByRole("main", { name: "Connection profiles page" }));
    expect(screen.getAllByRole("contentinfo")).toHaveLength(1);
    view.unmount();
    expect(fixture.listeners.size).toBe(0);
    expect(current.isInteractive()).toBe(false);
    await expect(proxy.execute(command)).rejects.toThrow("workspace is inactive");
  });

  it("rejects an empty registry, duplicate IDs and an unknown initial provider without deactivating anything", () => {
    const deactivate = vi.fn(() => Promise.resolve<ProviderDeactivationResult>({ state: "ready" })),
      fixtures = workspaceFixtures(deactivate);
    const first = fixtures.workspaces[0];
    if (first === undefined) throw new Error("Expected registered Kafka fixture.");
    expect(() => render(<ProviderApplication workspaces={[]} />)).toThrow("At least one");
    expect(() => render(<ProviderApplication workspaces={[first, first]} />)).toThrow(
      "distinct valid",
    );
    expect(() =>
      render(<ProviderApplication workspaces={fixtures.workspaces} initialProviderId="unknown" />),
    ).toThrow("not registered");
    expect(deactivate).not.toHaveBeenCalled();
  });
});
