// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type {
  PluginInstallation,
  PluginManifest,
  PluginSnapshot,
} from "../../src/plugins/contracts";
import type {
  PluginRenderer,
  PluginViewContext,
  PluginViewMount,
} from "../../src/plugins/renderer-api";
import { PluginsProvider, usePlugins } from "../../src/features/kafka/ui/PluginsProvider";
import type { LoadedPluginRenderer } from "../../src/features/kafka/ui/PluginsProvider";
import { ProfilePanel } from "../../src/features/kafka/ui/ProfilePanel";
import { ProfileWorkspace } from "../../src/features/kafka/ui/ProfileWorkspace";
import { currentPluginHost, PluginView } from "../../src/features/kafka/ui/PluginView";
import { testHostExecute } from "../support/host-response";
import { formatPluginVersion } from "../../src/plugins/validation";
import { pluginTransition } from "../support/plugin-management-fixture";

afterEach(cleanup);

const manifest: PluginManifest = {
  id: "sample.connection",
  name: "Sample connection",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const plugin: PluginRenderer = {
  apiVersion: 2,
  id: manifest.id,
  connectionActions: [{ id: "connect", label: "Connect through sample" }],
  mount: () => ({ update: () => undefined, dispose: () => undefined }),
};
const profile: ProfileSummary = {
  active: false,
  id: "managed",
  name: "Saved managed connection",
  brokers: ["127.0.0.1:19092"],
  createdAt: "2026-09-30T10:00:00.000Z",
  updatedAt: "2026-09-30T10:00:00.000Z",
  transport: "plaintext",
  source: { kind: "plugin", pluginId: manifest.id, version: 1, data: { session: "preserve-me" } },
};

function hostFor(plugins: readonly PluginInstallation[]): StreamSkopeHost {
  return {
    openExternalUrl: () => Promise.reject(new Error("Unexpected external navigation")),
    subscribe: () => () => undefined,
    execute: testHostExecute((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId: command.id, pluginSnapshot: { revision: 0, plugins } },
      }),
    ),
  };
}

function Status(): React.JSX.Element {
  const { plugins, errors, loading } = usePlugins();
  return (
    <>
      <span>{loading ? "Loading plugins" : "Plugins loaded"}</span>
      {plugins.map((entry) => (
        <span key={entry.manifest.id}>{entry.manifest.name}</span>
      ))}
      {Object.values(errors).map((error) => (
        <p key={error} role="alert">
          {error}
        </p>
      ))}
    </>
  );
}

it.each([2, 3] as const)(
  "checks renderer registration against its API 3 manifest (renderer API %s)",
  async (apiVersion) => {
    const compatibility = {
      streamskope: { minimum: "v0.1.0+build.5" },
      target: { system: "nsp", minimum: "26.4.0", maximum: "26.4.0" },
    };
    const active: PluginManifest = {
      ...manifest,
      apiVersion: 3,
      compatibility,
      revision: 1,
      version: formatPluginVersion(compatibility, 1),
    };
    const importer = vi.fn(() => Promise.resolve({ default: { ...plugin, apiVersion } }));
    const host = hostFor([
      {
        id: manifest.id,
        active,
        installed: active,
        activationId: "api3",
        pending: null,
        restartRequired: false,
        rendererUrl: "/plugins/sample.connection/api3/renderer.js",
      },
    ]);
    render(
      <PluginsProvider host={host} importer={importer}>
        <Status />
      </PluginsProvider>,
    );
    await screen.findByText("Plugins loaded");
    expect(importer).toHaveBeenCalledOnce();
    if (apiVersion === 3) {
      expect(screen.getByText(manifest.name)).toBeVisible();
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    } else {
      expect(screen.getByRole("alert")).toBeVisible();
      expect(screen.queryByText(manifest.name)).not.toBeInTheDocument();
    }
  },
);

it("only loads active renderer assets", async () => {
  const importer = vi.fn(() => Promise.resolve({ default: plugin }));
  const host = hostFor([
    { id: manifest.id, installed: manifest, pending: "install", restartRequired: true },
  ]);
  render(
    <PluginsProvider host={host} importer={importer}>
      <Status />
    </PluginsProvider>,
  );
  await screen.findByText("Plugins loaded");
  expect(importer).not.toHaveBeenCalled();
  expect(screen.queryByText(manifest.name)).not.toBeInTheDocument();
});

it("rejects remote renderer URLs before executing downloaded code", async () => {
  const importer = vi.fn(() => Promise.resolve({ default: plugin }));
  const host = hostFor([
    {
      id: manifest.id,
      active: manifest,
      activationId: "one",
      installed: manifest,
      pending: null,
      restartRequired: false,
      rendererUrl: "https://untrusted.example/plugins/sample.connection/one/renderer.js",
    },
  ]);
  render(
    <PluginsProvider host={host} importer={importer}>
      <Status />
    </PluginsProvider>,
  );
  expect(await screen.findByRole("alert")).toHaveTextContent("installed application asset");
  expect(importer).not.toHaveBeenCalled();
});

it("adds connection actions only after a compatible installed renderer loads", async () => {
  const host = hostFor([
    {
      id: manifest.id,
      active: manifest,
      activationId: "one",
      installed: manifest,
      pending: null,
      restartRequired: false,
      rendererUrl: "/plugins/sample.connection/one/renderer.js",
    },
  ]);
  const importer = vi.fn(() => Promise.resolve({ default: plugin }));
  render(
    <PluginsProvider host={host} importer={importer}>
      <ProfilePanel
        host={host}
        activityOpen={false}
        connected={false}
        connectionOperation={null}
        filter=""
        loading={false}
        onFilterChange={() => undefined}
        onOpenActivity={() => undefined}
        onProfileAction={() => undefined}
        onSelectProfile={() => undefined}
        onToggleConnection={() => undefined}
        profiles={[]}
        selectedProfileId={null}
        store={{ state: "ready", durability: "session", protection: "memory" }}
      />
    </PluginsProvider>,
  );
  await waitFor(() => expect(importer).toHaveBeenCalledOnce());
  await userEvent.setup().click(screen.getByRole("button", { name: "Add connection" }));
  expect(await screen.findByRole("menuitem", { name: "Connect through sample" })).toBeVisible();
  expect(screen.getByRole("menuitem", { name: "Existing Kafka cluster" })).toBeVisible();
});

it("keeps missing-plugin profiles visible and guides installation without connecting the stale endpoint", async () => {
  const host = hostFor([]);
  const connect = vi.fn();
  const openPlugins = vi.fn();
  render(
    <>
      <ProfilePanel
        host={host}
        activityOpen={false}
        connected={false}
        connectionOperation={null}
        filter=""
        loading={false}
        onFilterChange={() => undefined}
        onOpenActivity={() => undefined}
        onProfileAction={() => undefined}
        onSelectProfile={() => undefined}
        onToggleConnection={connect}
        profiles={[profile]}
        selectedProfileId={profile.id}
        store={{ state: "ready", durability: "session", protection: "memory" }}
      />
      <ProfileWorkspace
        host={host}
        profile={profile}
        action={null}
        activityOpen={false}
        clusterDiagnostics={{
          cluster: null,
          endpoint: null,
          fetchedAt: null,
          profile: null,
          state: "unavailable",
        }}
        onActionClose={() => undefined}
        onOpenActivity={() => undefined}
        onOpenPlugins={openPlugins}
        transfer={{ copy: () => Promise.resolve(), download: () => Promise.resolve() }}
      />
    </>,
  );
  expect(
    screen.getByRole("button", { name: `Connect insecure plaintext profile ${profile.name}` }),
  ).toBeDisabled();
  expect(screen.getByText(/saved settings are retained/u)).toBeVisible();
  await userEvent.setup().click(screen.getByRole("button", { name: "Open plugins" }));
  expect(openPlugins).toHaveBeenCalledOnce();
  expect(connect).not.toHaveBeenCalled();
  expect(profile.source?.data).toEqual({ session: "preserve-me" });
});

it("keeps profile navigation usable when plugin profile contributions throw", async () => {
  const contribution = vi.fn(() => {
    throw new Error("Unsupported saved metadata");
  });
  const renderer = { ...plugin, profileLabel: contribution, profileSummary: contribution };
  const host = hostFor([
    {
      id: manifest.id,
      active: manifest,
      activationId: "one",
      installed: manifest,
      pending: null,
      restartRequired: false,
      rendererUrl: "/plugins/sample.connection/one/renderer.js",
    },
  ]);
  render(
    <PluginsProvider host={host} importer={() => Promise.resolve({ default: renderer })}>
      <Status />
      <ProfilePanel
        host={host}
        activityOpen={false}
        connected={false}
        connectionOperation={null}
        filter=""
        loading={false}
        onFilterChange={() => undefined}
        onOpenActivity={() => undefined}
        onProfileAction={() => undefined}
        onSelectProfile={() => undefined}
        onToggleConnection={() => undefined}
        profiles={[profile]}
        selectedProfileId={profile.id}
        store={{ state: "ready", durability: "session", protection: "memory" }}
      />
      <ProfileWorkspace
        host={host}
        profile={profile}
        action={null}
        activityOpen={false}
        clusterDiagnostics={{
          cluster: null,
          endpoint: null,
          fetchedAt: null,
          profile: null,
          state: "unavailable",
        }}
        onActionClose={() => undefined}
        onOpenActivity={() => undefined}
        transfer={{ copy: () => Promise.resolve(), download: () => Promise.resolve() }}
      />
    </PluginsProvider>,
  );
  await screen.findByText("Plugins loaded");
  expect(contribution).toHaveBeenCalled();
  expect(screen.getByText("Plugin-managed connection")).toBeVisible();
  expect(screen.getByText("Plugin-managed connection · 127.0.0.1:19092")).toBeVisible();
  expect(screen.getByRole("button", { name: `Select profile ${profile.name}` })).toBeEnabled();
  await userEvent.setup().click(screen.getByRole("button", { name: "Add connection" }));
  expect(screen.getByRole("menuitem", { name: "Existing Kafka cluster" })).toBeVisible();
  expect(profile.source?.data).toEqual({ session: "preserve-me" });
});

it.each(["profileLabel", "profileSummary"] as const)(
  "rejects an invalid optional %s hook before activating its renderer",
  async (contribution) => {
    const renderer = { ...plugin };
    Reflect.set(renderer, contribution, "unsupported");
    const host = hostFor([
      {
        id: manifest.id,
        active: manifest,
        activationId: "one",
        installed: manifest,
        pending: null,
        restartRequired: false,
        rendererUrl: "/plugins/sample.connection/one/renderer.js",
      },
    ]);
    render(
      <PluginsProvider host={host} importer={() => Promise.resolve({ default: renderer })}>
        <Status />
      </PluginsProvider>,
    );
    expect(await screen.findByRole("alert")).toHaveTextContent("renderer is incompatible");
    expect(screen.queryByText(manifest.name)).not.toBeInTheDocument();
  },
);

it("contains plugin disposal failures after the view is detached", () => {
  const tasks: VoidFunction[] = [];
  const queue = vi.spyOn(globalThis, "queueMicrotask").mockImplementation((task) => {
    tasks.push(task);
  });
  const dispose = vi.fn(() => {
    throw new Error("Plugin disposal failed");
  });
  const renderer: PluginRenderer = {
    ...plugin,
    mount: () => ({ update: () => undefined, dispose }),
  };
  const { unmount } = render(
    <PluginView
      renderer={renderer}
      context={{
        host: hostFor([]),
        view: "connection",
        actionId: "connect",
        profiles: [],
        onClose: () => undefined,
        onProfileReady: () => undefined,
        onExistingDestination: () => undefined,
      }}
    />,
  );
  unmount();
  expect(queue).toHaveBeenCalled();
  expect(() => {
    for (const task of tasks) task();
  }).not.toThrow();
  expect(dispose).toHaveBeenCalledOnce();
  queue.mockRestore();
});

function installation(activationId: string, value = manifest): PluginInstallation {
  return {
    id: value.id,
    installed: value,
    active: value,
    activationId,
    pending: null,
    restartRequired: false,
    rendererUrl: `/plugins/${value.id}/${activationId}/renderer.js`,
    stylesUrl: `/plugins/${value.id}/${activationId}/renderer.css`,
  };
}

function observableHost(initial: PluginSnapshot): {
  host: StreamSkopeHost;
  publish: (snapshot: PluginSnapshot) => void;
} {
  const listeners = new Set<(event: HostEvent) => void>();
  let snapshot = initial;
  const host: StreamSkopeHost = {
    ...hostFor([]),
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    execute: testHostExecute((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId: command.id, pluginSnapshot: snapshot },
      }),
    ),
  };
  return {
    host,
    publish: (next): void => {
      snapshot = next;
      for (const listener of listeners)
        listener({
          event: "plugins.changed",
          payload: next,
          sequence: next.revision,
          version: HOST_PROTOCOL_VERSION,
        });
    },
  };
}

function MountedPlugin({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const { plugins } = usePlugins();
  return (
    <>
      {plugins.map((entry) => (
        <PluginView
          key={entry.manifest.id}
          renderer={entry.renderer}
          lifetime={entry.lifetime}
          activationId={entry.activationId}
          context={{
            host,
            view: "connection",
            actionId: "connect",
            profiles: [],
            onClose: () => undefined,
            onProfileReady: () => undefined,
            onExistingDestination: () => undefined,
          }}
        />
      ))}
    </>
  );
}

it("preserves a pending renderer import across progress revisions and retains the latest host warning", async () => {
  let complete!: (value: { default: PluginRenderer }) => void;
  const importer = vi.fn(
    () =>
      new Promise<{ default: PluginRenderer }>((resolve) => {
        complete = resolve;
      }),
  );
  const installed = installation("pending");
  const { host, publish } = observableHost({ revision: 1, plugins: [installed] });
  render(
    <PluginsProvider host={host} importer={importer}>
      <Status />
    </PluginsProvider>,
  );
  await waitFor(() => expect(importer).toHaveBeenCalledOnce());
  await act(() =>
    Promise.resolve(
      publish({
        revision: 2,
        plugins: [{ ...installed, transition: pluginTransition({ state: "running" }) }],
      }),
    ),
  );
  await act(() =>
    Promise.resolve(
      publish({
        revision: 3,
        plugins: [
          {
            ...installed,
            transition: pluginTransition(),
            error: "Previous backend cleanup is still pending.",
          },
        ],
      }),
    ),
  );
  expect(importer).toHaveBeenCalledOnce();
  expect(screen.getByText("Loading plugins")).toBeVisible();
  await act(() => Promise.resolve(complete({ default: plugin })));
  expect(await screen.findByText("Plugins loaded")).toBeVisible();
  expect(screen.getByText(manifest.name)).toBeVisible();
  expect(screen.getByRole("alert")).toHaveTextContent("Previous backend cleanup is still pending.");
  expect(importer).toHaveBeenCalledOnce();
});

it("keeps mounted renderer authority across diagnostic updates and revokes it on actual removal", async () => {
  let current: LoadedPluginRenderer | undefined;
  function Authority(): React.JSX.Element {
    current = usePlugins().plugins[0];
    return <Status />;
  }
  const installed = installation("retained");
  const { host, publish } = observableHost({ revision: 1, plugins: [installed] });
  const importer = vi.fn(() => Promise.resolve({ default: plugin }));
  render(
    <PluginsProvider host={host} importer={importer}>
      <Authority />
    </PluginsProvider>,
  );
  await screen.findByText(manifest.name);
  const original = current!;
  const stylesheet = document.querySelector('link[href*="/retained/"]');
  await act(() =>
    Promise.resolve(
      publish({ revision: 2, plugins: [{ ...installed, transition: pluginTransition() }] }),
    ),
  );
  expect(current).toBe(original);
  expect(original.lifetime.aborted).toBe(false);
  expect(screen.getByText("Plugins loaded")).toBeVisible();
  expect(document.querySelector('link[href*="/retained/"]')).toBe(stylesheet);
  expect(importer).toHaveBeenCalledOnce();
  await act(() => Promise.resolve(publish({ revision: 3, plugins: [] })));
  expect(original.lifetime.aborted).toBe(true);
  expect(document.querySelector('link[href*="/retained/"]')).toBeNull();
});

it("hot installs, replaces and removes one renderer while preserving other views and styles", async () => {
  const stable = { ...manifest, id: "stable.connection", name: "Stable connection" };
  const stableMount = vi.fn(() => ({ update: vi.fn(), dispose: vi.fn() }));
  const firstDispose = vi.fn();
  const nextDispose = vi.fn();
  const firstMount = vi.fn(() => ({ update: vi.fn(), dispose: firstDispose }));
  const nextMount = vi.fn(() => ({ update: vi.fn(), dispose: nextDispose }));
  const importer = vi.fn((url: string) =>
    Promise.resolve({
      default: url.includes(stable.id)
        ? { ...plugin, id: stable.id, mount: stableMount }
        : url.includes("/next/")
          ? { ...plugin, mount: nextMount }
          : { ...plugin, mount: firstMount },
    }),
  );
  const { host, publish } = observableHost({
    revision: 0,
    plugins: [installation("stable", stable)],
  });
  render(
    <PluginsProvider host={host} importer={importer}>
      <MountedPlugin host={host} />
    </PluginsProvider>,
  );
  await waitFor(() => expect(stableMount).toHaveBeenCalledOnce());
  const stableStyle = document.querySelector(`link[href*="${stable.id}"]`);
  await act(() =>
    Promise.resolve(
      publish({ revision: 1, plugins: [installation("stable", stable), installation("first")] }),
    ),
  );
  await waitFor(() => expect(firstMount).toHaveBeenCalledOnce());
  await act(() =>
    Promise.resolve(
      publish({ revision: 2, plugins: [installation("stable", stable), installation("next")] }),
    ),
  );
  await waitFor(() => expect(nextMount).toHaveBeenCalledOnce());
  expect(firstDispose).toHaveBeenCalledOnce();
  expect(document.querySelector('link[href*="/first/"]')).toBeNull();
  expect(document.querySelector('link[href*="/next/"]')).not.toBeNull();
  expect(document.querySelector(`link[href*="${stable.id}"]`)).toBe(stableStyle);
  expect(stableMount).toHaveBeenCalledOnce();
  await act(() =>
    Promise.resolve(publish({ revision: 3, plugins: [installation("stable", stable)] })),
  );
  await waitFor(() => expect(nextDispose).toHaveBeenCalledOnce());
  expect(document.querySelector('link[href*="/next/"]')).toBeNull();
  expect(stableMount).toHaveBeenCalledOnce();
  expect(importer).toHaveBeenCalledTimes(3);
});

it("revokes old renderer requests, subscriptions and callbacks before importing its replacement", async () => {
  const oldContexts: PluginViewContext[] = [];
  const onClose = vi.fn();
  const onProfileReady = vi.fn();
  const onExistingDestination = vi.fn();
  const eventListener = vi.fn();
  let deliver: ((event: HostEvent) => void) | undefined;
  const unsubscribe = vi.fn();
  const execute = vi.fn(
    testHostExecute((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId: command.id, output: null },
      }),
    ),
  );
  const host: StreamSkopeHost = {
    ...hostFor([]),
    execute: testHostExecute(execute),
    subscribe: (listener) => {
      deliver = listener;
      return unsubscribe;
    },
  };
  const oldRenderer = {
    ...plugin,
    mount: (_element: HTMLElement, context: PluginViewContext): PluginViewMount => {
      oldContexts.push(context);
      context.host.subscribe(eventListener);
      return { update: vi.fn(), dispose: vi.fn() };
    },
  };
  const lifetime = new AbortController();
  render(
    <PluginView
      renderer={oldRenderer}
      lifetime={lifetime.signal}
      activationId="old"
      context={{
        host,
        view: "connection",
        actionId: "connect",
        profiles: [],
        onClose,
        onProfileReady,
        onExistingDestination,
      }}
    />,
  );
  const context = oldContexts[0];
  if (context === undefined) throw new Error("Plugin view did not mount");
  await context.host.execute({
    command: "plugin.execute",
    id: "before",
    version: HOST_PROTOCOL_VERSION,
    payload: { pluginId: manifest.id, method: "read", input: null },
  });
  expect(execute.mock.calls[0]?.[0].payload).toMatchObject({ activationId: "old" });
  lifetime.abort();
  await expect(
    context.host.execute({
      command: "plugins.list",
      id: "after",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    }),
  ).rejects.toThrow("no longer active");
  context.onClose();
  context.onProfileReady("late-profile");
  context.onExistingDestination({ name: "stale", brokers: ["localhost:9092"] });
  deliver?.({
    event: "plugins.changed",
    payload: { revision: 1, plugins: [] },
    sequence: 1,
    version: HOST_PROTOCOL_VERSION,
  });
  expect(execute).toHaveBeenCalledOnce();
  expect(unsubscribe).toHaveBeenCalledOnce();
  expect(eventListener).not.toHaveBeenCalled();
  expect(onClose).not.toHaveBeenCalled();
  expect(onProfileReady).not.toHaveBeenCalled();
  expect(onExistingDestination).not.toHaveBeenCalled();
});

it("ignores an older initial listing after a newer lifecycle event", async () => {
  let resolveList: ((value: unknown) => void) | undefined;
  const { host, publish } = observableHost({ revision: 0, plugins: [] });
  host.execute = testHostExecute(
    (command) =>
      new Promise((resolve) => {
        resolveList = (value): void =>
          resolve({
            command: command.command,
            id: command.id,
            ok: true,
            version: HOST_PROTOCOL_VERSION,
            result: { correlationId: command.id, pluginSnapshot: value },
          });
      }),
  );
  const importer = vi.fn(() => Promise.resolve({ default: plugin }));
  render(
    <PluginsProvider host={host} importer={importer}>
      <Status />
    </PluginsProvider>,
  );
  await act(() => Promise.resolve(publish({ revision: 2, plugins: [installation("current")] })));
  await screen.findByText(manifest.name);
  await act(() => Promise.resolve(resolveList?.({ revision: 0, plugins: [] })));
  expect(screen.getByText(manifest.name)).toBeVisible();
  expect(importer).toHaveBeenCalledOnce();
});

it("reports a failed renderer to the host and loads the restored activation", async () => {
  const restored = { ...manifest, version: "0.9.0" };
  const { host } = observableHost({ revision: 1, plugins: [installation("broken")] });
  const report = vi.fn();
  const original = host.execute.bind(host);
  host.execute = testHostExecute((command) => {
    if (command.command !== "plugins.renderer.failed") return original(command);
    report(command.payload);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      version: HOST_PROTOCOL_VERSION,
      result: {
        correlationId: command.id,
        pluginSnapshot: { revision: 2, plugins: [installation("restored", restored)] },
      },
    });
  });
  const importer = vi.fn((url: string) =>
    url.includes("/broken/")
      ? Promise.reject(new Error("Invalid module"))
      : Promise.resolve({ default: plugin }),
  );
  render(
    <PluginsProvider host={host} importer={importer}>
      <Status />
    </PluginsProvider>,
  );
  expect(await screen.findByText(manifest.name)).toBeVisible();
  expect(report).toHaveBeenCalledExactlyOnceWith({
    pluginId: manifest.id,
    activationId: "broken",
    error: "Invalid module",
  });
  expect(importer).toHaveBeenCalledTimes(2);
  expect(document.querySelector('link[href*="/broken/"]')).toBeNull();
  expect(document.querySelector('link[href*="/restored/"]')).not.toBeNull();
});

it("does not reopen a connection dialog when its plugin is removed and installed again", async () => {
  const mount = vi.fn(() => ({ update: vi.fn(), dispose: vi.fn() }));
  const { host, publish } = observableHost({ revision: 0, plugins: [installation("first")] });
  const importer = vi.fn(() => Promise.resolve({ default: { ...plugin, mount } }));
  render(
    <PluginsProvider host={host} importer={importer}>
      <ProfilePanel
        host={host}
        activityOpen={false}
        connected={false}
        connectionOperation={null}
        filter=""
        loading={false}
        onFilterChange={() => undefined}
        onOpenActivity={() => undefined}
        onProfileAction={() => undefined}
        onSelectProfile={() => undefined}
        onToggleConnection={() => undefined}
        profiles={[]}
        selectedProfileId={null}
        store={{ state: "ready", durability: "session", protection: "memory" }}
      />
    </PluginsProvider>,
  );
  await waitFor(() => expect(importer).toHaveBeenCalledOnce());
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Add connection" }));
  await user.click(await screen.findByRole("menuitem", { name: "Connect through sample" }));
  expect(mount).toHaveBeenCalledOnce();
  await act(() => Promise.resolve(publish({ revision: 1, plugins: [] })));
  await act(() => Promise.resolve(publish({ revision: 2, plugins: [installation("second")] })));
  await waitFor(() => expect(importer).toHaveBeenCalledTimes(2));
  expect(mount).toHaveBeenCalledOnce();
  await user.click(screen.getByRole("button", { name: "Add connection" }));
  expect(await screen.findByRole("menuitem", { name: "Connect through sample" })).toBeVisible();
});

it("reports a renderer mount failure once and revokes any host subscriptions it created", async () => {
  const originalHost = hostFor([]);
  const execute = vi.fn(originalHost.execute.bind(originalHost));
  const unsubscribe = vi.fn();
  const host: StreamSkopeHost = {
    ...hostFor([]),
    execute: testHostExecute(execute),
    subscribe: () => unsubscribe,
  };
  const renderer = {
    ...plugin,
    mount: (_element: HTMLElement, context: PluginViewContext): PluginViewMount => {
      context.host.subscribe(vi.fn());
      throw new Error("Rendering failed");
    },
  };
  render(
    <PluginView
      renderer={renderer}
      activationId="failed"
      context={{
        host,
        view: "connection",
        actionId: "connect",
        profiles: [],
        onClose: () => undefined,
        onProfileReady: () => undefined,
        onExistingDestination: () => undefined,
      }}
    />,
  );
  expect(await screen.findByRole("alert")).toHaveTextContent("Rendering failed");
  expect(execute).toHaveBeenCalledOnce();
  expect(execute.mock.calls[0]?.[0]).toMatchObject({
    command: "plugins.renderer.failed",
    payload: {
      pluginId: manifest.id,
      activationId: "failed",
      error: "Rendering failed",
    },
  });
  expect(unsubscribe).toHaveBeenCalledOnce();
});

it("discards an in-flight response after its renderer is retired", async () => {
  let complete: (() => void) | undefined;
  const host: StreamSkopeHost = {
    ...hostFor([]),
    execute: testHostExecute(
      (command) =>
        new Promise((resolve) => {
          complete = (): void =>
            resolve({
              command: command.command,
              id: command.id,
              ok: true,
              version: HOST_PROTOCOL_VERSION,
              result: { correlationId: command.id, pluginSnapshot: { revision: 1, plugins: [] } },
            });
        }),
    ),
  };
  const lifetime = new AbortController();
  const result = currentPluginHost(host, lifetime.signal).execute({
    command: "plugins.list",
    id: "delayed",
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  });
  lifetime.abort();
  complete?.();
  await expect(result).rejects.toThrow("no longer active");
});

it("retains a load error across unrelated events and permits an explicit renderer retry", async () => {
  const { host, publish } = observableHost({ revision: 1, plugins: [installation("retry")] });
  const original = host.execute.bind(host);
  host.execute = testHostExecute((command) => {
    if (command.command === "plugins.renderer.failed")
      throw new Error("Host temporarily unavailable");
    return original(command);
  });
  let available = false;
  const importer = vi.fn(() =>
    available
      ? Promise.resolve({ default: plugin })
      : Promise.reject(new Error("Renderer temporarily unavailable")),
  );
  function Retry(): React.JSX.Element {
    const { refresh } = usePlugins();
    return (
      <button
        onClick={() => {
          void refresh();
        }}
      >
        Retry controls
      </button>
    );
  }
  render(
    <PluginsProvider host={host} importer={importer}>
      <Status />
      <Retry />
    </PluginsProvider>,
  );
  expect(await screen.findByRole("alert")).toHaveTextContent("Renderer temporarily unavailable");
  await act(() => Promise.resolve(publish({ revision: 2, plugins: [installation("retry")] })));
  expect(screen.getByRole("alert")).toHaveTextContent("Renderer temporarily unavailable");
  expect(importer).toHaveBeenCalledOnce();
  available = true;
  await userEvent.setup().click(screen.getByRole("button", { name: "Retry controls" }));
  expect(await screen.findByText(manifest.name)).toBeVisible();
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});

it("gives a replacement view a fresh DOM node before the old root finishes disposal", () => {
  const pending: VoidFunction[] = [];
  const queue = vi.spyOn(globalThis, "queueMicrotask").mockImplementation((callback) => {
    pending.push(callback);
  });
  const targets: HTMLElement[] = [];
  const renderer = (label: string): PluginRenderer => ({
    ...plugin,
    mount: (element): PluginViewMount => {
      targets.push(element);
      element.textContent = label;
      return {
        update: () => undefined,
        dispose: (): void => {
          element.textContent = "";
        },
      };
    },
  });
  const context: PluginViewContext = {
    host: hostFor([]),
    view: "connection",
    actionId: "connect",
    profiles: [],
    onClose: () => undefined,
    onProfileReady: () => undefined,
    onExistingDestination: () => undefined,
  };
  try {
    const { rerender } = render(
      <PluginView renderer={renderer("Old controls")} context={context} />,
    );
    rerender(<PluginView renderer={renderer("New controls")} context={context} />);
    expect(targets[0]).not.toBe(targets[1]);
    expect(targets[0]?.isConnected).toBe(false);
    expect(screen.getByText("New controls")).toBeVisible();
    for (const callback of pending) callback();
    expect(screen.getByText("New controls")).toBeVisible();
  } finally {
    queue.mockRestore();
  }
});
