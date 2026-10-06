// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type { PluginCatalogSnapshot, PluginPackageInspection } from "../../src/plugins/contracts";
import { PluginsPanel } from "../../src/features/kafka/ui/PluginsPanel";
import {
  fixture,
  manifest,
  catalogGate,
  approvePackage,
} from "../support/plugin-management-fixture";

afterEach(cleanup);

async function selectFile(user = userEvent.setup()): Promise<void> {
  const button = screen.getByRole("button", { name: "Install from file" });
  await waitFor(() => expect(button).toBeEnabled());
  await user.click(button);
}

it("always offers file installation and explains its native host requirement", async () => {
  const { host } = fixture({ offline: true });
  render(<PluginsPanel host={host} />);
  const button = await screen.findByRole("button", { name: "Install from file" });
  await screen.findByText("File installation requires the desktop app.");
  expect(button).toBeDisabled();
});

it("reviews a signed local package while remote discovery is pending and discards cancelled reviews", async () => {
  const remote = catalogGate();
  const { host, commands } = fixture({
    catalog: (refresh): Promise<PluginCatalogSnapshot> =>
      refresh ? remote.promise : Promise.resolve({ plugins: [], source: "unavailable" }),
    delivery: { fileInstallationAvailable: true, cachedPackages: [] },
  });
  const user = userEvent.setup();
  render(<PluginsPanel host={host} />);
  await selectFile(user);
  const review = await screen.findByRole("dialog", { name: "Review plugin" });
  expect(review).toHaveTextContent("Signed local file");
  expect(review).toHaveTextContent("Fixture publisher (fixture-publisher)");
  expect(review).toHaveTextContent("a".repeat(64));
  expect(
    commands.find((command) => command.command === "plugins.package.inspect")?.payload,
  ).toEqual({ source: "file" });
  expect(
    commands.some(
      (command) =>
        command.command === "plugins.package.change.prepare" ||
        command.command === "plugins.package.install",
    ),
  ).toBe(false);
  await user.click(within(review).getByRole("button", { name: "Cancel" }));
  await waitFor(() =>
    expect(
      commands.find((command) => command.command === "plugins.package.discard")?.payload,
    ).toEqual({ candidateId: "review-sample" }),
  );
  expect(screen.getByText("Checking for plugin updates…")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Install from file" }));
  await approvePackage();
  expect(await screen.findByText("Active version 2.0.0")).toBeVisible();
  expect(commands.filter((command) => command.command === "plugins.package.install")).toHaveLength(
    1,
  );
  await act((): Promise<void> => {
    remote.resolve({ plugins: [], source: "unavailable" });
    return Promise.resolve();
  });
});

it.each(["already-installed", "blocked"] as const)(
  "shows a %s package without prompting to stop active work",
  async (status) => {
    const { host, commands } = fixture({
      delivery: { fileInstallationAvailable: true, cachedPackages: [] },
      inspection: {
        candidateId: "read-only-review",
        manifest,
        sha256: "a".repeat(64),
        source: "file",
        publisher: { keyId: "fixture", name: "Fixture publisher" },
        trust: "publisher",
        expiresAt: new Date(Date.now() + 300_000).toISOString(),
        installedVersion: manifest.version,
        status,
        ...(status === "blocked"
          ? { reason: "This plugin requires a newer desktop version." }
          : {}),
      },
      prompt: {
        pluginId: manifest.id,
        token: "must-not-ask",
        title: "Stop active capture?",
        message: "Stop work.",
        detail: "No-op must not stop work.",
        confirmLabel: "Stop",
      },
    });
    render(<PluginsPanel host={host} />);
    await selectFile();
    const review = await screen.findByRole("dialog", { name: "Review plugin" });
    expect(review).toHaveTextContent(
      status === "blocked" ? "requires a newer desktop" : "already installed",
    );
    expect(
      within(review).queryByRole("button", { name: /Install plugin|Update plugin/u }),
    ).not.toBeInTheDocument();
    await userEvent.setup().click(within(review).getByRole("button", { name: "Close" }));
    expect(
      commands.some(
        (command) =>
          command.command === "plugins.package.change.prepare" ||
          command.command === "plugins.package.install",
      ),
    ).toBe(false);
    expect(
      commands.find((command) => command.command === "plugins.package.discard")?.payload,
    ).toEqual({ candidateId: "read-only-review" });
  },
);

it("uses an exact cached version without consulting an unavailable remote catalog", async () => {
  const cached = { ...manifest, version: "1.0.0" };
  const digest = "b".repeat(64);
  const { host, commands } = fixture({
    offline: true,
    delivery: {
      fileInstallationAvailable: false,
      cachedPackages: [
        {
          manifest: cached,
          sha256: digest,
          cachedAt: "2026-10-05T14:00:00.000Z",
          trust: "official",
        },
      ],
    },
  });
  render(<PluginsPanel host={host} />);
  await screen.findByText(/The plugin catalog is unavailable/u);
  const lookups = commands.filter((command) => command.command === "plugins.catalog").length;
  await userEvent.setup().click(screen.getByRole("button", { name: "Use cached package" }));
  const review = await screen.findByRole("dialog", { name: "Review plugin" });
  expect(review).toHaveTextContent("Verified local cache");
  expect(review).toHaveTextContent(/Selected version\s*1\.0\.0/u);
  expect(
    commands.find((command) => command.command === "plugins.package.inspect")?.payload,
  ).toEqual({ source: "cache", pluginId: cached.id, version: cached.version, sha256: digest });
  await approvePackage();
  expect(await screen.findByText("Active version 1.0.0")).toBeVisible();
  expect(commands.filter((command) => command.command === "plugins.catalog")).toHaveLength(lookups);
});

it("keeps picker cancellation and invalid file verification free of lifecycle changes", async () => {
  const cancel = fixture({
    delivery: { fileInstallationAvailable: true, cachedPackages: [] },
    inspection: null,
  });
  const first = render(<PluginsPanel host={cancel.host} />);
  await selectFile();
  expect(screen.queryByRole("dialog", { name: "Review plugin" })).not.toBeInTheDocument();
  expect(cancel.commands.some((command) => command.command === "plugins.package.install")).toBe(
    false,
  );
  first.unmount();
  const invalid = fixture({
    delivery: { fileInstallationAvailable: true, cachedPackages: [] },
    inspectFailure: "The package publisher signature is invalid.",
  });
  render(<PluginsPanel host={invalid.host} />);
  await selectFile();
  expect(await screen.findByRole("alert")).toHaveTextContent("publisher signature is invalid");
  expect(
    invalid.commands.some(
      (command) =>
        command.command === "plugins.package.change.prepare" ||
        command.command === "plugins.package.install",
    ),
  ).toBe(false);
});

it("discards a late file inspection result after plugin management is unmounted", async () => {
  let complete!: (candidate: PluginPackageInspection | null) => void;
  const inspection = new Promise<PluginPackageInspection | null>((resolve) => {
    complete = resolve;
  });
  const { host, commands } = fixture({
    delivery: { fileInstallationAvailable: true, cachedPackages: [] },
    inspectDeferred: (): Promise<PluginPackageInspection | null> => inspection,
  });
  const view = render(<PluginsPanel host={host} />);
  await selectFile();
  view.unmount();
  await act((): Promise<void> => {
    complete({
      candidateId: "late-file-review",
      manifest,
      sha256: "a".repeat(64),
      source: "file",
      publisher: { keyId: "fixture", name: "Fixture publisher" },
      trust: "publisher",
      expiresAt: new Date(Date.now() + 300_000).toISOString(),
      status: "install",
    });
    return Promise.resolve();
  });
  await waitFor(() =>
    expect(
      commands.find((command) => command.command === "plugins.package.discard")?.payload,
    ).toEqual({ candidateId: "late-file-review" }),
  );
  expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
});

it("does not expire or discard a reviewed package while its admitted installation is pending", async () => {
  vi.useFakeTimers();
  try {
    let complete!: () => void;
    const installing = new Promise<void>((resolve) => {
      complete = resolve;
    });
    const { host, commands } = fixture({
      delivery: { fileInstallationAvailable: true, cachedPackages: [] },
      inspection: {
        candidateId: "admitted-review",
        manifest,
        sha256: "a".repeat(64),
        source: "file",
        publisher: { keyId: "fixture", name: "Fixture publisher" },
        trust: "publisher",
        expiresAt: new Date(Date.now() + 5_000).toISOString(),
        status: "install",
      },
      installDeferred: (): Promise<void> => installing,
    });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    render(<PluginsPanel host={host} />);
    await act((): Promise<void> => Promise.resolve());
    await user.click(screen.getByRole("button", { name: "Install from file" }));
    const review = screen.getByRole("dialog", { name: "Review plugin" });
    await user.click(within(review).getByRole("button", { name: "Install plugin" }));
    await act((): Promise<void> => vi.advanceTimersByTimeAsync(5_001));
    expect(within(review).getByRole("button", { name: "Applying change…" })).toBeDisabled();
    expect(review).not.toHaveTextContent("This review expired");
    expect(commands.some((command) => command.command === "plugins.package.discard")).toBe(false);
    await act((): Promise<void> => {
      complete();
      return Promise.resolve();
    });
    expect(screen.getByText("Active version 2.0.0")).toBeVisible();
  } finally {
    vi.useRealTimers();
  }
});

it("expires a reviewed package without starting installation and releases its receipt", async () => {
  vi.useFakeTimers();
  try {
    const { host, commands } = fixture({
      delivery: { fileInstallationAvailable: true, cachedPackages: [] },
      inspection: {
        candidateId: "expiring-review",
        manifest,
        sha256: "a".repeat(64),
        source: "file",
        publisher: { keyId: "fixture", name: "Fixture publisher" },
        trust: "publisher",
        expiresAt: new Date(Date.now() + 5_000).toISOString(),
        status: "install",
      },
    });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    render(<PluginsPanel host={host} />);
    await act((): Promise<void> => Promise.resolve());
    await user.click(screen.getByRole("button", { name: "Install from file" }));
    const review = screen.getByRole("dialog", { name: "Review plugin" });
    expect(within(review).getByRole("button", { name: "Install plugin" })).toBeEnabled();
    await act((): Promise<void> => vi.advanceTimersByTimeAsync(5_001));
    expect(review).toHaveTextContent("This review expired");
    expect(
      within(review).queryByRole("button", { name: "Install plugin" }),
    ).not.toBeInTheDocument();
    expect(
      commands.find((command) => command.command === "plugins.package.discard")?.payload,
    ).toEqual({ candidateId: "expiring-review" });
    expect(commands.some((command) => command.command === "plugins.package.install")).toBe(false);
  } finally {
    vi.useRealTimers();
  }
});
