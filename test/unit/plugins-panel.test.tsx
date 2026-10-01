// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type {
  PluginChangePrompt,
  PluginManifest,
  PluginSnapshot,
} from "../../src/plugins/contracts";
import { PluginsPanel } from "../../src/features/kafka/ui/PluginsPanel";
import { OperationalPreferencesDialog } from "../../src/features/kafka/ui/OperationalPreferencesDialog";
import { testHostExecute } from "../support/host-response";
import { formatPluginVersion } from "../../src/plugins/validation";

afterEach(cleanup);
const manifest: PluginManifest = {
  id: "sample.connection",
  name: "Sample connection",
  version: "2.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};

function fixture(
  options: {
    snapshot?: PluginSnapshot;
    offline?: boolean;
    failInstall?: boolean;
    prompt?: PluginChangePrompt;
    manifest?: PluginManifest;
  } = {},
): {
  host: StreamSkopeHost;
  commands: HostCommand[];
} {
  const availableManifest = options.manifest ?? manifest;
  let snapshot = options.snapshot ?? { revision: 0, plugins: [] };
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    openExternalUrl: vi.fn(),
    subscribe: () => () => undefined,
    execute: testHostExecute((command) => {
      commands.push(command);
      let result: object = { correlationId: command.id };
      switch (command.command) {
        case "plugins.catalog":
          if (options.offline) throw new Error("Network unavailable.");
          result = { ...result, pluginCatalog: { plugins: [availableManifest] } };
          break;
        case "plugins.change.prepare":
          result = { ...result, pluginChange: options.prompt ?? null };
          break;
        case "plugins.install":
          if (options.failInstall) throw new Error("The downloaded package failed verification.");
          snapshot = {
            revision: snapshot.revision + 1,
            plugins: [
              {
                id: availableManifest.id,
                installed: availableManifest,
                active: availableManifest,
                activationId: "new",
                pending: null,
                restartRequired: false,
              },
            ],
          };
          result = { ...result, pluginSnapshot: snapshot };
          break;
        case "plugins.remove":
          snapshot = { revision: snapshot.revision + 1, plugins: [] };
          result = { ...result, pluginSnapshot: snapshot };
          break;
        case "plugins.list":
          result = { ...result, pluginSnapshot: snapshot };
          break;
        case "plugins.restart":
          break;
        default:
          throw new Error(`Unexpected ${command.command}`);
      }
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result,
      });
    }),
  };
  return { host, commands };
}

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
  expect(commands.map((command) => command.command)).toEqual(["plugins.list", "plugins.catalog"]);
  await user.click(screen.getByRole("button", { name: "Install" }));
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(screen.getByText("Sample connection 2.0.0 is installed.")).toBeVisible();
  expect(commands.find((command) => command.command === "plugins.install")?.payload).toEqual({
    pluginId: manifest.id,
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
    expect(await screen.findByText("Active version 0.1.0")).toBeVisible();
    expect(commands.some((command) => command.command === "plugins.install")).toBe(true);
  },
);

it("does not offer a legacy encoded version to repair a newer SemVer installation", async () => {
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
    "Retry requires version 0.1.0 or newer",
  );
  expect(
    screen.queryByRole("button", { name: /Update to|Retry activation/u }),
  ).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
});

it("keeps the active version visible and offers retry when an update fails verification", async () => {
  const current = { ...manifest, version: "1.0.0" };
  const { host } = fixture({
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
  expect(await screen.findByRole("alert")).toHaveTextContent("failed verification");
  expect(screen.getByText("Active version 1.0.0")).toBeVisible();
  expect(screen.getByRole("button", { name: "Update to 2.0.0" })).toBeEnabled();
  expect(screen.queryByRole("button", { name: "Restart StreamSkope" })).not.toBeInTheDocument();
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
    expect(commands.some((command) => command.command === "plugins.install")).toBe(false);
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
  let dialog = await screen.findByRole("dialog", { name: "Stop capture and update?" });
  expect(dialog).toHaveTextContent("Temporary resources will be cleaned up");
  await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
  expect(commands.some((command) => command.command === "plugins.install")).toBe(false);
  expect(screen.getByText("Active version 1.0.0")).toBeVisible();
  await user.click(await screen.findByRole("button", { name: "Update to 2.0.0" }));
  dialog = await screen.findByRole("dialog", { name: "Stop capture and update?" });
  await user.click(within(dialog).getByRole("button", { name: "Stop and update" }));
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(commands.find((command) => command.command === "plugins.install")?.payload).toEqual({
    pluginId: manifest.id,
    confirmationToken: "confirm-active-capture",
  });
});

it("allows retrying a verified installed package whose activation failed", async () => {
  const { host, commands } = fixture({
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
  expect(commands.some((command) => command.command === "plugins.install")).toBe(true);
});

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
  expect(commands.some((command) => command.command === "plugins.install")).toBe(false);
  await user.click(within(dialog).getByRole("button", { name: "Stop and retry" }));
  expect(await screen.findByText("Sample connection 2.0.0 is installed.")).toBeVisible();
  expect(commands.find((command) => command.command === "plugins.install")?.payload).toEqual({
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
      "Retry requires version 3.0.0 or newer",
    );
    expect(screen.getByText("The plugin controls failed to load.")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Retry activation" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Install" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Update to/u })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
    expect(commands.some((command) => command.command === "plugins.install")).toBe(false);
  },
);
