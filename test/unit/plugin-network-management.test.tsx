// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import type { PluginPackageInspection, PluginNetworkTestResult } from "../../src/plugins/contracts";
import { PluginsPanel } from "../../src/features/kafka/ui/PluginsPanel";
import {
  fixture,
  manifest,
  networkSnapshot,
  catalogGate,
} from "../support/plugin-management-fixture";

afterEach(cleanup);

async function settings(): Promise<ReturnType<typeof userEvent.setup>> {
  const user = userEvent.setup();
  await act((): Promise<void> => Promise.resolve());
  await user.click(screen.getByRole("button", { name: /^Plugin download settings/u }));
  await screen.findByRole("checkbox", { name: "Offline plugin downloads" });
  return user;
}

async function changeField(label: string, value: string): Promise<void> {
  const field = screen.getByLabelText(label);
  await act((): Promise<void> => {
    fireEvent.change(field, { target: { value } });
    return Promise.resolve();
  });
}

const installed = {
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
} as const;

it("preserves a failed remote-check progress state when the host returns usable cached catalog metadata", async () => {
  const remote = catalogGate();
  const { host, commands, emitProgress } = fixture({
    catalog: (refresh) =>
      refresh
        ? remote.promise
        : Promise.resolve({
            plugins: [manifest],
            source: "cache",
            checkedAt: "2026-10-05T14:00:00.000Z",
          }),
  });
  render(<PluginsPanel host={host} />);
  await act((): Promise<void> => Promise.resolve());
  await waitFor(() =>
    expect(
      commands.some(
        (entry) => entry.command === "plugins.catalog" && entry.payload.refresh === true,
      ),
    ).toBe(true),
  );
  const request = commands.find(
    (entry) => entry.command === "plugins.catalog" && entry.payload.refresh === true,
  )!;
  await act((): Promise<void> => {
    emitProgress({
      requestId: request.id,
      operation: "catalog",
      phase: "catalog",
      state: "failed",
    });
    remote.resolve({
      plugins: [manifest],
      source: "cache",
      checkedAt: "2026-10-05T14:00:00.000Z",
      error: "The remote catalog is unavailable.",
    });
    return Promise.resolve();
  });
  expect(await screen.findByText(/Update check:.*failed/u)).toBeVisible();
  expect(screen.queryByText(/Update check:.*succeeded/u)).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Install" })).toBeEnabled();
});

it("keeps native proxy limits truthful and saves acquisition-only offline settings while local actions remain available", async () => {
  const remote = catalogGate();
  const { host, commands } = fixture({
    snapshot: installed,
    catalog: (refresh) =>
      refresh ? remote.promise : Promise.resolve({ plugins: [], source: "cache" }),
    delivery: { fileInstallationAvailable: true, cachedPackages: [] },
  });
  render(<PluginsPanel host={host} />);
  await screen.findByText("Active version 2.0.0");
  const user = await settings();
  expect(
    screen.getByText(/System proxy discovery and custom proxies require the desktop app/u),
  ).toBeVisible();
  await user.click(screen.getByRole("checkbox", { name: "Offline plugin downloads" }));
  expect(screen.getByRole("button", { name: "Test connection" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Save settings" }));
  await screen.findByText(/Plugin downloads are offline/u);
  expect(commands.find((entry) => entry.command === "plugins.network.update")?.payload).toEqual({
    configuration: { mode: "system", offline: true, proxyUrl: null },
    credentials: { action: "unchanged" },
  });
  expect(screen.getByRole("button", { name: "Check for updates" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Install from file" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
  await act((): Promise<void> => {
    remote.resolve({ plugins: [], source: "unavailable" });
    return Promise.resolve();
  });
});

it("replaces proxy credentials without reading them back and requires new authorization for a changed endpoint", async () => {
  const { host, commands } = fixture({
    network: {
      ...networkSnapshot,
      configuration: { mode: "custom", offline: false, proxyUrl: "http://proxy.example:8080" },
      credentialsConfigured: true,
      credentialStorage: "encrypted",
      nativeAvailable: true,
      supportedProxyProtocols: ["http", "https"],
    },
  });
  render(<PluginsPanel host={host} />);
  const user = await settings();
  expect(screen.getByText(/Proxy credentials are stored/u)).toBeVisible();
  expect(screen.queryByLabelText("Proxy username")).not.toBeInTheDocument();
  await user.click(screen.getByRole("combobox", { name: "Proxy credentials" }));
  await user.click(screen.getByRole("option", { name: "Replace credentials" }));
  await changeField("Proxy username", "fixture-user");
  await changeField("Proxy password", "fixture-password");
  await user.click(screen.getByRole("button", { name: "Save settings" }));
  await waitFor(() => expect(screen.queryByLabelText("Proxy username")).not.toBeInTheDocument());
  expect(commands.find((entry) => entry.command === "plugins.network.update")?.payload).toEqual({
    configuration: { mode: "custom", offline: false, proxyUrl: "http://proxy.example:8080" },
    credentials: { action: "replace", username: "fixture-user", password: "fixture-password" },
  });
  await user.click(screen.getByRole("combobox", { name: "Proxy credentials" }));
  await user.click(screen.getByRole("option", { name: "Replace credentials" }));
  expect(screen.getByLabelText("Proxy username")).toHaveValue("");
  expect(screen.getByLabelText("Proxy password")).toHaveValue("");
  await changeField("Proxy URL", "https://other-proxy.example:8443");
  await user.click(screen.getByRole("combobox", { name: "Proxy credentials" }));
  await user.click(screen.getByRole("option", { name: "Keep existing credentials" }));
  expect(screen.getByText(/Choose Replace or Clear credentials/u)).toBeVisible();
  expect(screen.getByRole("button", { name: "Save settings" })).toBeDisabled();
  await user.click(screen.getByRole("combobox", { name: "Proxy credentials" }));
  await user.click(screen.getByRole("option", { name: "Clear credentials" }));
  await user.click(screen.getByRole("button", { name: "Save settings" }));
  await waitFor(() =>
    expect(commands.filter((entry) => entry.command === "plugins.network.update")).toHaveLength(2),
  );
  expect(
    commands.filter((entry) => entry.command === "plugins.network.update")[1]?.payload,
  ).toEqual({
    configuration: { mode: "custom", offline: false, proxyUrl: "https://other-proxy.example:8443" },
    credentials: { action: "clear" },
  });
});

it("tests only applied settings and suppresses a stale result after the settings revision changes", async () => {
  let complete!: (result: PluginNetworkTestResult) => void;
  const pending = new Promise<PluginNetworkTestResult>((resolve) => {
    complete = resolve;
  });
  const { host, commands } = fixture({ networkTest: () => pending });
  render(<PluginsPanel host={host} />);
  const user = await settings();
  await user.click(screen.getByRole("button", { name: "Test connection" }));
  const request = commands.find((entry) => entry.command === "plugins.network.test")!;
  await user.click(screen.getByRole("checkbox", { name: "Offline plugin downloads" }));
  await user.click(screen.getByRole("button", { name: "Save settings" }));
  await screen.findByText(/Plugin downloads are offline/u);
  expect(commands.find((entry) => entry.command === "plugins.network.cancel")?.payload).toEqual({
    requestId: request.id,
  });
  await act((): Promise<void> => {
    complete({
      settingsRevision: 0,
      checkedAt: "2026-10-06T12:00:00.000Z",
      scope: "catalog-and-assets",
    });
    return Promise.resolve();
  });
  expect(
    screen.queryByText(/Release catalog and package download are reachable/u),
  ).not.toBeInTheDocument();
  expect(screen.getByText(/Connection test:.*cancelled/u)).toBeVisible();
});

it("preserves a remembered custom origin and its credentials when changing only offline mode under system proxy settings", async () => {
  const { host, commands } = fixture({
    network: {
      ...networkSnapshot,
      configuration: { mode: "system", offline: false, proxyUrl: "http://proxy.example:8080" },
      credentialsConfigured: true,
      nativeAvailable: true,
      supportedProxyProtocols: ["http", "https"],
    },
  });
  render(<PluginsPanel host={host} />);
  const user = await settings();
  expect(screen.getByRole("button", { name: "Test connection" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Save settings" })).toBeDisabled();
  await user.click(screen.getByRole("checkbox", { name: "Offline plugin downloads" }));
  await user.click(screen.getByRole("button", { name: "Save settings" }));
  await screen.findByText(/Plugin downloads are offline/u);
  expect(commands.find((entry) => entry.command === "plugins.network.update")?.payload).toEqual({
    configuration: { mode: "system", offline: true, proxyUrl: "http://proxy.example:8080" },
    credentials: { action: "unchanged" },
  });
});

it("resets corrupt download settings explicitly without disabling installed plugin removal", async () => {
  const { host, commands } = fixture({
    snapshot: installed,
    network: {
      ...networkSnapshot,
      configuration: null,
      error: "Saved proxy settings are invalid.",
    },
  });
  render(<PluginsPanel host={host} />);
  await screen.findByText("Active version 2.0.0");
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: /^Plugin download settings/u }));
  expect(await screen.findByText(/Saved proxy settings are invalid/u)).toBeVisible();
  expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Check for updates" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Reset download settings" }));
  expect(commands.find((entry) => entry.command === "plugins.network.update")?.payload).toEqual({
    configuration: { mode: "system", offline: false, proxyUrl: null },
    credentials: { action: "clear" },
  });
  await screen.findByRole("checkbox", { name: "Offline plugin downloads" });
});

it("correlates transfer progress and cancels a stalled remote review while local file and removal actions stay available", async () => {
  let complete!: (result: PluginPackageInspection | null) => void;
  const pending = new Promise<PluginPackageInspection | null>((resolve) => {
    complete = resolve;
  });
  const previous = { ...manifest, version: "1.0.0" };
  const { host, commands, emitProgress } = fixture({
    snapshot: {
      revision: 1,
      plugins: [
        {
          id: previous.id,
          installed: previous,
          active: previous,
          pending: null,
          restartRequired: false,
        },
      ],
    },
    delivery: { fileInstallationAvailable: true, cachedPackages: [] },
    inspectDeferred: () => pending,
  });
  render(<PluginsPanel host={host} />);
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "Update to 2.0.0" }));
  await waitFor(() =>
    expect(commands.some((entry) => entry.command === "plugins.package.inspect")).toBe(true),
  );
  const request = commands.find((entry) => entry.command === "plugins.package.inspect")!;
  await act((): Promise<void> => {
    emitProgress({
      requestId: "unrelated-request",
      operation: "inspect",
      phase: "download",
      state: "running",
      receivedBytes: 999,
    });
    emitProgress({
      requestId: request.id,
      operation: "inspect",
      phase: "download",
      state: "running",
      receivedBytes: 123,
      totalBytes: 456,
    });
    return Promise.resolve();
  });
  expect(
    screen.getByText(/Package acquisition: Downloading package.*123 \/ 456 bytes/u),
  ).toBeVisible();
  expect(screen.queryByText(/999 bytes/u)).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Install from file" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
  await user.click(screen.getByRole("button", { name: "Cancel package acquisition" }));
  expect(commands.find((entry) => entry.command === "plugins.network.cancel")?.payload).toEqual({
    requestId: request.id,
  });
  await act((): Promise<void> => {
    complete({
      candidateId: "late-download",
      manifest,
      sha256: "a".repeat(64),
      source: "catalog",
      trust: "official",
      expiresAt: new Date(Date.now() + 300_000).toISOString(),
      installedVersion: previous.version,
      status: "update",
    });
    return Promise.resolve();
  });
  await waitFor(() =>
    expect(commands.find((entry) => entry.command === "plugins.package.discard")?.payload).toEqual({
      candidateId: "late-download",
    }),
  );
  expect(screen.queryByRole("dialog", { name: "Review plugin" })).not.toBeInTheDocument();
  expect(screen.getByText("Active version 1.0.0")).toBeVisible();
  expect(commands.some((entry) => entry.command === "plugins.package.install")).toBe(false);
});

it("cancels a pending acquisition on unmount and releases any late review receipt", async () => {
  let complete!: (result: PluginPackageInspection | null) => void;
  const pending = new Promise<PluginPackageInspection | null>((resolve) => {
    complete = resolve;
  });
  const { host, commands } = fixture({ inspectDeferred: () => pending });
  const view = render(<PluginsPanel host={host} />);
  await userEvent.setup().click(await screen.findByRole("button", { name: "Install" }));
  await waitFor(() =>
    expect(commands.some((entry) => entry.command === "plugins.package.inspect")).toBe(true),
  );
  const request = commands.find((entry) => entry.command === "plugins.package.inspect")!;
  view.unmount();
  expect(commands.find((entry) => entry.command === "plugins.network.cancel")?.payload).toEqual({
    requestId: request.id,
  });
  await act((): Promise<void> => {
    complete({
      candidateId: "unmounted-download",
      manifest,
      sha256: "a".repeat(64),
      source: "catalog",
      trust: "official",
      expiresAt: new Date(Date.now() + 300_000).toISOString(),
      status: "install",
    });
    return Promise.resolve();
  });
  await waitFor(() =>
    expect(commands.find((entry) => entry.command === "plugins.package.discard")?.payload).toEqual({
      candidateId: "unmounted-download",
    }),
  );
});
