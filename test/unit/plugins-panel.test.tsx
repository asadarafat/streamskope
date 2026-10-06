// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type {
  PluginCatalogSnapshot,
  PluginManifest,
  PluginSnapshot,
} from "../../src/plugins/contracts";
import { PluginsPanel } from "../../src/features/kafka/ui/PluginsPanel";
import { PluginsProvider, usePlugins } from "../../src/features/kafka/ui/PluginsProvider";
import type { PluginRenderer, PluginViewMount } from "../../src/plugins/renderer-api";
import { OperationalPreferencesDialog } from "../../src/features/kafka/ui/OperationalPreferencesDialog";
import { testHostExecute } from "../support/host-response";
import { formatPluginVersion } from "../../src/plugins/validation";
import {
  fixture,
  manifest,
  catalogGate,
  approvePackage,
} from "../support/plugin-management-fixture";

afterEach(cleanup);
it("shows installed plugins and completes removal while a network refresh is still pending", async () => {
  const remote = catalogGate();
  const { host, commands } = fixture({
    catalog: (refresh): Promise<PluginCatalogSnapshot> =>
      refresh
        ? remote.promise
        : Promise.resolve({ plugins: [], source: "cache", checkedAt: "2026-10-05T14:00:00.000Z" }),
    snapshot: {
      revision: 1,
      plugins: [
        {
          id: manifest.id,
          installed: manifest,
          active: manifest,
          pending: null,
          restartRequired: false,
        },
      ],
    },
  });
  const user = userEvent.setup();
  render(<PluginsPanel host={host} />);
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(screen.getByText("Checking for plugin updates…")).toBeVisible();
  const installed = within(screen.getByRole("region", { name: "Installed plugins" }));
  expect(installed.getByRole("button", { name: "Remove" })).toBeEnabled();
  await user.click(installed.getByRole("button", { name: "Remove" }));
  const dialog = await screen.findByRole("dialog", { name: "Remove Sample connection?" });
  await user.click(within(dialog).getByRole("button", { name: "Remove plugin" }));
  expect(await screen.findByText("No plugins are installed.")).toBeVisible();
  expect(commands.some((command) => command.command === "plugins.remove")).toBe(true);
  expect(screen.getByText("Checking for plugin updates…")).toBeVisible();
  await act((): Promise<void> => {
    remote.resolve({ plugins: [], source: "live" });
    return Promise.resolve();
  });
});

it("retries installed controls before a pending network refresh completes", async () => {
  const remote = catalogGate();
  const { host, commands } = fixture({
    catalog: (refresh): Promise<PluginCatalogSnapshot> =>
      refresh ? remote.promise : Promise.resolve({ plugins: [], source: "unavailable" }),
    snapshot: {
      revision: 1,
      plugins: [
        {
          id: manifest.id,
          installed: manifest,
          pending: null,
          restartRequired: false,
          error: "Renderer activation failed.",
        },
      ],
    },
  });
  render(<PluginsPanel host={host} />);
  const retry = await screen.findByRole("button", { name: "Retry activation" });
  expect(retry).toBeEnabled();
  await userEvent.setup().click(retry);
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(screen.getByText("Checking for plugin updates…")).toBeVisible();
  expect(commands.find((command) => command.command === "plugins.change.prepare")?.payload).toEqual(
    {
      pluginId: manifest.id,
      operation: "retry",
    },
  );
  expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  await act((): Promise<void> => {
    remote.resolve({ plugins: [], source: "unavailable" });
    return Promise.resolve();
  });
});

it("explicitly reloads rejected renderer controls after a healthy backend retry and waits for them", async () => {
  const snapshot: PluginSnapshot = {
    revision: 1,
    plugins: [
      {
        id: manifest.id,
        installed: manifest,
        active: manifest,
        activationId: "retained-backend",
        rendererUrl: "/plugins/sample.connection/retained-backend/renderer.js",
        pending: null,
        restartRequired: false,
      },
    ],
  };
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    openExternalUrl: vi.fn(),
    subscribe: () => () => undefined,
    execute: testHostExecute((command): Promise<unknown> => {
      commands.push(command);
      // The host did not receive the earlier renderer failure and consequently
      // treats its healthy backend retry as a no-op with the same activation ID.
      if (command.command === "plugins.renderer.failed")
        throw new Error("The host could not receive the renderer error.");
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: {
          correlationId: command.id,
          ...(command.command === "plugins.delivery"
            ? { pluginDelivery: { fileInstallationAvailable: false, cachedPackages: [] } }
            : command.command === "plugins.catalog"
              ? { pluginCatalog: { plugins: [], source: "unavailable" } }
              : command.command === "plugins.change.prepare"
                ? { pluginChange: null }
                : { pluginSnapshot: snapshot }),
        },
      });
    }),
  };
  let completeRenderer!: (module: { default: PluginRenderer }) => void;
  const rendererReady = new Promise<{ default: PluginRenderer }>((complete) => {
    completeRenderer = complete;
  });
  const importer = vi
    .fn()
    .mockRejectedValueOnce(new Error("The plugin controls failed to load."))
    .mockImplementationOnce(() => rendererReady);
  function Controls(): React.JSX.Element {
    const { plugins } = usePlugins();
    return (
      <span>
        {plugins.length === 0 ? "Plugin controls unavailable" : "Plugin controls available"}
      </span>
    );
  }
  render(
    <PluginsProvider host={host} importer={importer}>
      <PluginsPanel host={host} />
      <Controls />
    </PluginsProvider>,
  );
  await screen.findByText("The plugin controls failed to load.");
  expect(importer).toHaveBeenCalledOnce();
  const catalogCalls = commands.filter((command) => command.command === "plugins.catalog").length;
  await userEvent.setup().click(screen.getByRole("button", { name: "Retry activation" }));
  await waitFor(() => expect(importer).toHaveBeenCalledTimes(2));
  expect(screen.getByText("Plugin controls unavailable")).toBeVisible();
  expect(screen.queryByText("Sample connection 2.0.0 is active.")).not.toBeInTheDocument();
  await act((): Promise<void> => {
    completeRenderer({
      default: {
        apiVersion: 2,
        id: manifest.id,
        connectionActions: [],
        mount: (): PluginViewMount => ({
          update: (): void => undefined,
          dispose: (): void => undefined,
        }),
      },
    });
    return Promise.resolve();
  });
  expect(await screen.findByText("Plugin controls available")).toBeVisible();
  expect(screen.getByText("Sample connection 2.0.0 is active.")).toBeVisible();
  expect(screen.queryByText("The plugin controls failed to load.")).not.toBeInTheDocument();
  expect(commands.some((command) => command.command === "plugins.retry")).toBe(true);
  expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  expect(commands.filter((command) => command.command === "plugins.catalog")).toHaveLength(
    catalogCalls,
  );
});

it("retains cached available plugins and the last successful check after a network failure", async () => {
  const checkedAt = "2026-10-05T14:00:00.000Z";
  let refreshes = 0;
  const { host } = fixture({
    catalog: (refresh): Promise<PluginCatalogSnapshot> => {
      if (refresh && ++refreshes > 1)
        return Promise.reject(new Error("The proxy requires authentication."));
      return Promise.resolve({
        plugins: [manifest],
        source: refresh ? "live" : "cache",
        checkedAt,
      });
    },
  });
  const user = userEvent.setup();
  render(<PluginsPanel host={host} />);
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Check for updates" })).toBeEnabled(),
  );
  expect(screen.getByText(/^Catalog checked/u)).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Check for updates" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("The proxy requires authentication");
  expect(screen.getByText(/^Cached catalog · Last checked/u)).toHaveTextContent(
    new Date(checkedAt).toLocaleString(),
  );
  const available = within(screen.getByRole("region", { name: "Available plugins" }));
  expect(available.getByText("Sample connection")).toBeVisible();
  expect(available.getByRole("button", { name: "Install" })).toBeEnabled();
  expect(screen.queryByText(/^Catalog checked/u)).not.toBeInTheDocument();
});

it("keeps a refreshed catalog usable when saving its local copy fails", async () => {
  const { host } = fixture({
    catalog: (refresh): Promise<PluginCatalogSnapshot> =>
      Promise.resolve(
        refresh
          ? {
              plugins: [manifest],
              source: "live",
              checkedAt: "2026-10-05T14:00:00.000Z",
              error: "The catalog cache is read-only.",
            }
          : { plugins: [], source: "unavailable" },
      ),
  });
  render(<PluginsPanel host={host} />);
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "The plugin catalog was refreshed, but its local copy could not be saved.",
  );
  expect(screen.getByRole("button", { name: "Install" })).toBeEnabled();
  expect(screen.getByText(/^Catalog checked/u)).toBeVisible();
  expect(screen.queryByText(/The plugin catalog is unavailable/u)).not.toBeInTheDocument();
});

it("accepts a fresh empty catalog instead of retaining stale available entries after a cache-save failure", async () => {
  const { host } = fixture({
    catalog: (refresh): Promise<PluginCatalogSnapshot> =>
      Promise.resolve({
        plugins: refresh ? [] : [manifest],
        source: refresh ? "live" : "cache",
        checkedAt: "2026-10-05T14:00:00.000Z",
        ...(refresh ? { error: "The catalog cache is read-only." } : {}),
      }),
  });
  render(<PluginsPanel host={host} />);
  expect(await screen.findByRole("alert")).toHaveTextContent("The plugin catalog was refreshed");
  expect(screen.queryByRole("button", { name: "Install" })).not.toBeInTheDocument();
  expect(screen.getByText(/^Catalog checked/u)).toBeVisible();
});

it.each([false, true])(
  "shows compatibility bounds for an available plugin (update=%s)",
  async (update) => {
    const compatibility = {
      streamskope: { minimum: "v0.1.0+build.5" },
      target: { system: "nsp", minimum: "26.4.0", maximum: "26.4.0" },
    };
    const available: PluginManifest = {
      ...manifest,
      apiVersion: 3,
      compatibility,
      revision: 1,
      version: formatPluginVersion(compatibility, 1),
    };
    const { host } = fixture({
      manifest: available,
      ...(update
        ? {
            snapshot: {
              revision: 0,
              plugins: [
                {
                  id: manifest.id,
                  installed: manifest,
                  active: manifest,
                  pending: null,
                  restartRequired: false,
                },
              ],
            },
          }
        : {}),
    });
    render(<PluginsPanel host={host} />);
    const bounds = await screen.findByText(/Requires StreamSkope v0.1.0\+build.5 or later/u);
    expect(bounds).toHaveTextContent("Supports NSP 26.4.0–26.4.0 (inclusive)");
    expect(bounds.textContent?.startsWith("Available update:")).toBe(update);
    expect(
      screen.getByRole("button", { name: update ? `Update to ${available.version}` : "Install" }),
    ).toBeEnabled();
  },
);

it("installs only on request and immediately activates the plugin", async () => {
  const { host, commands } = fixture();
  const user = userEvent.setup();
  render(<PluginsPanel host={host} />);
  await screen.findByRole("button", { name: "Install" });
  expect(commands.map((command) => command.command)).toEqual([
    "plugins.list",
    "plugins.catalog",
    "plugins.delivery",
    "plugins.catalog",
  ]);
  await user.click(screen.getByRole("button", { name: "Install" }));
  const review = await screen.findByRole("dialog", { name: "Review plugin" });
  expect(review).toHaveTextContent(/Selected version\s*2\.0\.0/u);
  expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  await user.click(within(review).getByRole("button", { name: "Install plugin" }));
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(screen.getByText("Sample connection 2.0.0 is installed.")).toBeVisible();
  expect(
    commands.find((command) => command.command === "plugins.package.install")?.payload,
  ).toEqual({
    candidateId: "review-sample",
  });
  expect(screen.queryByRole("button", { name: "Restart StreamSkope" })).not.toBeInTheDocument();
});

it.each([2, 3] as const)(
  "offers a SemVer migration from an installed API %s plugin and displays bounded compatibility",
  async (apiVersion) => {
    const legacyCompatibility = {
      streamskope: { minimum: "v0.1.0+build.1" },
      target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
    };
    const legacy: PluginManifest = {
      ...manifest,
      apiVersion,
      version: apiVersion === 2 ? "26.8.2" : formatPluginVersion(legacyCompatibility, 10),
      ...(apiVersion === 3 ? { compatibility: legacyCompatibility, revision: 10 } : {}),
    };
    const available: PluginManifest = {
      ...manifest,
      apiVersion: 4,
      version: "0.1.0",
      compatibility: {
        streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
        target: legacyCompatibility.target,
      },
    };
    const { host, commands } = fixture({
      manifest: available,
      snapshot: {
        revision: 1,
        plugins: [
          {
            id: legacy.id,
            installed: legacy,
            active: legacy,
            pending: null,
            restartRequired: false,
          },
        ],
      },
    });
    render(<PluginsPanel host={host} />);
    const update = await screen.findByRole("button", { name: "Update to 0.1.0" });
    expect(
      screen.getByText(/Requires StreamSkope 0.2.0 up to, but excluding, 0.3.0/u),
    ).toHaveTextContent("Supports EDA 26.8.2–26.8.2 (inclusive) · Plugin API 4");
    await userEvent.setup().click(update);
    await approvePackage("Update plugin");
    expect(await screen.findByText("Active version 0.1.0")).toBeVisible();
    expect(commands.some((command) => command.command === "plugins.package.install")).toBe(true);
  },
);

it("retries the installed SemVer locally when the catalog contains an older encoded version", async () => {
  const compatibility = {
    streamskope: { minimum: "v0.1.0+build.1" },
    target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
  };
  const legacy: PluginManifest = {
    ...manifest,
    apiVersion: 3,
    compatibility,
    revision: 99,
    version: formatPluginVersion(compatibility, 99),
  };
  const installed: PluginManifest = {
    ...manifest,
    apiVersion: 4,
    version: "0.1.0",
    compatibility: {
      streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
      target: compatibility.target,
    },
  };
  const { host } = fixture({
    manifest: legacy,
    snapshot: {
      revision: 1,
      plugins: [
        {
          id: installed.id,
          installed,
          pending: null,
          restartRequired: false,
          error: "Activation failed.",
        },
      ],
    },
  });
  render(<PluginsPanel host={host} />);
  expect(await screen.findByText(/The catalog offers older version/u)).toHaveTextContent(
    "Installed version 0.1.0 is newer",
  );
  expect(screen.queryByRole("button", { name: /Update to/u })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Retry activation" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
});

it("keeps the active version visible and offers retry when an update fails verification", async () => {
  const current = { ...manifest, version: "1.0.0" };
  const { host, commands } = fixture({
    failInstall: true,
    snapshot: {
      revision: 0,
      plugins: [
        {
          id: manifest.id,
          installed: current,
          active: current,
          pending: null,
          restartRequired: false,
        },
      ],
    },
  });
  render(<PluginsPanel host={host} />);
  await userEvent.setup().click(await screen.findByRole("button", { name: "Update to 2.0.0" }));
  await approvePackage("Update plugin");
  expect(await screen.findByRole("alert")).toHaveTextContent("failed verification");
  expect(screen.getByText("Active version 1.0.0")).toBeVisible();
  expect(screen.getByRole("button", { name: "Update to 2.0.0" })).toBeEnabled();
  expect(screen.queryByRole("button", { name: "Restart StreamSkope" })).not.toBeInTheDocument();
  expect(commands.filter((command) => command.command === "plugins.catalog")).toHaveLength(2);
});

it.each([false, true])(
  "does not offer a downgrade for a newer installed plugin when offline=%s",
  async (offline) => {
    const newer = { ...manifest, version: "3.0.0", description: "Installed version capabilities." };
    const { host, commands } = fixture({
      offline,
      snapshot: {
        revision: 0,
        plugins: [
          {
            id: manifest.id,
            installed: newer,
            active: newer,
            pending: null,
            restartRequired: false,
          },
        ],
      },
    });
    render(<PluginsPanel host={host} />);
    expect(await screen.findByText("Active version 3.0.0")).toBeVisible();
    expect(screen.getByText("Installed version capabilities.")).toBeVisible();
    expect(screen.queryByRole("button", { name: /Update to/u })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Install" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
    expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  },
);

it("supports confirmed removal when the catalog is offline and retains profiles", async () => {
  const { host, commands } = fixture({
    offline: true,
    snapshot: {
      revision: 0,
      plugins: [
        {
          id: manifest.id,
          installed: manifest,
          active: manifest,
          pending: null,
          restartRequired: false,
        },
      ],
    },
  });
  const user = userEvent.setup();
  render(<PluginsPanel host={host} />);
  await user.click(await screen.findByRole("button", { name: "Remove" }));
  const confirm = await screen.findByRole("dialog", { name: "Remove Sample connection?" });
  expect(confirm).toHaveTextContent("Saved connection settings are retained");
  expect(commands.some((command) => command.command === "plugins.remove")).toBe(false);
  await user.click(within(confirm).getByRole("button", { name: "Remove plugin" }));
  expect(
    await screen.findByText(
      "Sample connection has been removed. Saved connection settings are retained.",
    ),
  ).toBeVisible();
  expect(screen.queryByText("Active version 2.0.0")).not.toBeInTheDocument();
  expect(commands.some((command) => command.command === "profiles.delete")).toBe(false);
});

it("blocks installation when installed plugin state is corrupt", async () => {
  const { host } = fixture({
    snapshot: { revision: 0, plugins: [], error: "Plugin state is invalid." },
  });
  render(<PluginsPanel host={host} />);
  expect(await screen.findByRole("button", { name: "Install" })).toBeDisabled();
  expect(screen.getByRole("alert")).toHaveTextContent("Plugin state is invalid");
});

it("opens plugin management inside Preferences without requiring loaded workbench settings", async () => {
  const { host } = fixture();
  render(
    <OperationalPreferencesDialog
      host={host}
      initialSection="plugins"
      open
      snapshot={null}
      onClose={() => undefined}
      onOpenActivity={() => undefined}
    />,
  );
  expect(screen.getByRole("tab", { name: "Plugins" })).toHaveAttribute("aria-selected", "true");
  expect(await screen.findByRole("button", { name: "Install" })).toBeVisible();
  expect(screen.queryByRole("button", { name: "Save preferences" })).not.toBeInTheDocument();
  await userEvent.setup().click(screen.getByRole("tab", { name: "Workbench" }));
  expect(screen.getByRole("button", { name: "Save preferences" })).toBeDisabled();
});

it("requires explicit confirmation to stop active work for an update and leaves cancellation untouched", async () => {
  const current = { ...manifest, version: "1.0.0" };
  const { host, commands } = fixture({
    snapshot: {
      revision: 0,
      plugins: [
        {
          id: manifest.id,
          installed: current,
          active: current,
          pending: null,
          restartRequired: false,
        },
      ],
    },
    prompt: {
      pluginId: manifest.id,
      token: "confirm-active-capture",
      title: "Stop capture and update?",
      message: "Your active capture will stop.",
      detail: "Temporary resources will be cleaned up before updating.",
      confirmLabel: "Stop and update",
    },
  });
  const user = userEvent.setup();
  render(<PluginsPanel host={host} />);
  await user.click(await screen.findByRole("button", { name: "Update to 2.0.0" }));
  await approvePackage("Update plugin");
  let dialog = await screen.findByRole("dialog", { name: "Stop capture and update?" });
  expect(dialog).toHaveTextContent("Temporary resources will be cleaned up");
  await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
  expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  expect(screen.getByText("Active version 1.0.0")).toBeVisible();
  await user.click(await screen.findByRole("button", { name: "Update to 2.0.0" }));
  await approvePackage("Update plugin");
  dialog = await screen.findByRole("dialog", { name: "Stop capture and update?" });
  await user.click(within(dialog).getByRole("button", { name: "Stop and update" }));
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(
    commands.find((command) => command.command === "plugins.package.install")?.payload,
  ).toEqual({
    candidateId: "review-sample",
    confirmationToken: "confirm-active-capture",
  });
});

it.each([false, true])(
  "retries a verified installed package without downloading when offline=%s",
  async (offline) => {
    const { host, commands } = fixture({
      offline,
      snapshot: {
        revision: 0,
        plugins: [
          {
            id: manifest.id,
            installed: manifest,
            pending: null,
            restartRequired: false,
            error: "Renderer activation failed.",
          },
        ],
      },
    });
    render(<PluginsPanel host={host} />);
    await userEvent.setup().click(await screen.findByRole("button", { name: "Retry activation" }));
    expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
    expect(commands.some((command) => command.command === "plugins.retry")).toBe(true);
    expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  },
);

it.each([true, false])(
  "keeps retained failed-installation files removable when catalog offline=%s",
  async (offline) => {
    const { host, commands } = fixture({
      offline,
      snapshot: {
        revision: 1,
        plugins: [
          {
            id: manifest.id,
            pending: null,
            restartRequired: false,
            error: "The installed renderer failed verification.",
          },
        ],
      },
    });
    render(<PluginsPanel host={host} />);
    const user = userEvent.setup();
    if (!offline) await screen.findByText(manifest.name);
    await user.click(await screen.findByRole("button", { name: "Remove" }));
    const dialog = await screen.findByRole("dialog", {
      name: `Remove ${offline ? manifest.id : manifest.name}?`,
    });
    await user.click(within(dialog).getByRole("button", { name: "Remove plugin" }));
    expect(
      await screen.findByText(
        `${offline ? manifest.id : manifest.name} has been removed. Saved connection settings are retained.`,
      ),
    ).toBeVisible();
    expect(commands.find((command) => command.command === "plugins.remove")?.payload).toEqual({
      pluginId: manifest.id,
    });
  },
);

it("offers same-version activation retry when the backend kept active work after a renderer failure", async () => {
  const { host, commands } = fixture({
    snapshot: {
      revision: 1,
      plugins: [
        {
          id: manifest.id,
          installed: manifest,
          active: manifest,
          activationId: "old",
          pending: null,
          restartRequired: false,
          error: "The plugin controls failed while a capture remains active.",
        },
      ],
    },
    prompt: {
      pluginId: manifest.id,
      token: "retry-capture",
      title: "Stop capture and retry?",
      message: "Stop the active capture before reloading controls.",
      detail: "Saved connections remain available.",
      confirmLabel: "Stop and retry",
    },
  });
  render(<PluginsPanel host={host} />);
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "Retry activation" }));
  const dialog = await screen.findByRole("dialog", { name: "Stop capture and retry?" });
  expect(commands.some((command) => command.command === "plugins.retry")).toBe(false);
  await user.click(within(dialog).getByRole("button", { name: "Stop and retry" }));
  expect(await screen.findByText("Sample connection 2.0.0 is active.")).toBeVisible();
  expect(commands.find((command) => command.command === "plugins.retry")?.payload).toEqual({
    pluginId: manifest.id,
    confirmationToken: "retry-capture",
  });
});

it.each(["both", "installed", "active"] as const)(
  "does not downgrade a newer %s version to repair an activation error",
  async (state) => {
    const newer = { ...manifest, version: "3.0.0" };
    const { host, commands } = fixture({
      snapshot: {
        revision: 1,
        plugins: [
          {
            id: manifest.id,
            ...(state === "active" ? {} : { installed: newer }),
            ...(state === "installed" ? {} : { active: newer, activationId: "current" }),
            pending: null,
            restartRequired: false,
            error: "The plugin controls failed to load.",
          },
        ],
      },
    });
    render(<PluginsPanel host={host} />);
    expect(await screen.findByText(/The catalog offers older version 2.0.0/u)).toHaveTextContent(
      "Installed version 3.0.0 is newer",
    );
    expect(screen.getByText("The plugin controls failed to load.")).toBeVisible();
    if (state === "active")
      expect(screen.queryByRole("button", { name: "Retry activation" })).not.toBeInTheDocument();
    else {
      await userEvent.setup().click(screen.getByRole("button", { name: "Retry activation" }));
      expect(await screen.findByText("Active version 3.0.0")).toBeVisible();
      expect(commands.find((command) => command.command === "plugins.retry")?.payload).toEqual({
        pluginId: manifest.id,
      });
    }
    expect(screen.queryByRole("button", { name: "Install" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Update to/u })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
    expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  },
);
