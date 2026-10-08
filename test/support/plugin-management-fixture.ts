import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import type {
  PluginChangePrompt,
  PluginCatalogSnapshot,
  PluginManifest,
  PluginSnapshot,
  PluginDeliverySnapshot,
  PluginPackageInspection,
  PluginPackageInspectInput,
  PluginNetworkSnapshot,
  PluginNetworkTestResult,
  PluginAcquisitionProgress,
  PluginTransition,
} from "../../src/plugins/contracts";

import { testHostExecute } from "./host-response";

export const manifest: PluginManifest = {
  id: "sample.connection",
  name: "Sample connection",
  version: "2.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};

export function pluginTransition(overrides: Partial<PluginTransition> = {}): PluginTransition {
  const startedAt = new Date(Date.now() - 12_000).toISOString();
  return {
    operationId: "11111111-1111-4111-8111-111111111111",
    pluginId: manifest.id,
    operation: "install",
    stage: "retire-previous",
    state: "waiting",
    startedAt,
    stageStartedAt: startedAt,
    outstandingRequests: 2,
    outstandingConnections: 1,
    commit: "confirmed",
    ...overrides,
  };
}

export const networkSnapshot: PluginNetworkSnapshot = {
  revision: 0,
  configuration: { mode: "system", offline: false, proxyUrl: null },
  credentialsConfigured: false,
  credentialStorage: "session",
  nativeAvailable: false,
  supportedProxyProtocols: [],
};

export function fixture(
  options: {
    snapshot?: PluginSnapshot;
    offline?: boolean;
    failInstall?: boolean;
    prompt?: PluginChangePrompt;
    manifest?: PluginManifest;
    catalog?: (refresh: boolean) => Promise<PluginCatalogSnapshot>;
    delivery?: PluginDeliverySnapshot;
    inspection?: PluginPackageInspection | null;
    inspectDeferred?: (input: PluginPackageInspectInput) => Promise<PluginPackageInspection | null>;
    inspectFailure?: string;
    installDeferred?: () => Promise<void>;
    network?: PluginNetworkSnapshot;
    networkFailure?: string;
    networkTest?: (revision: number) => Promise<PluginNetworkTestResult>;
  } = {},
): {
  host: StreamSkopeHost;
  commands: HostCommand[];
  emitProgress: (progress: PluginAcquisitionProgress) => void;
  emitSnapshot: (snapshot: PluginSnapshot) => void;
} {
  const availableManifest = options.manifest ?? manifest;
  let snapshot = options.snapshot ?? { revision: 0, plugins: [] };
  const commands: HostCommand[] = [];
  let candidate: PluginPackageInspection | null = null;
  let network = options.network ?? networkSnapshot;
  const listeners = new Set<HostEventListener>();
  let sequence = 0;
  const host: StreamSkopeHost = {
    openExternalUrl: vi.fn(),
    subscribe: (listener): (() => void) => {
      listeners.add(listener);
      return (): void => {
        listeners.delete(listener);
      };
    },
    execute: testHostExecute(async (command) => {
      commands.push(command);
      let result: object = { correlationId: command.id };
      switch (command.command) {
        case "plugins.catalog":
          if (command.payload.refresh !== false && options.offline)
            throw new Error("Network unavailable.");
          {
            const listed: PluginCatalogSnapshot =
              options.catalog === undefined
                ? options.offline
                  ? { plugins: [], source: "unavailable" }
                  : {
                      plugins: [availableManifest],
                      source: command.payload.refresh === false ? "cache" : "live",
                      checkedAt: "2026-10-05T14:00:00.000Z",
                    }
                : await options.catalog(command.payload.refresh !== false);
            result = {
              ...result,
              pluginCatalog: {
                ...listed,
                packages:
                  listed.packages ??
                  listed.plugins.map((entry) => ({
                    pluginId: entry.id,
                    version: entry.version,
                    sha256: "a".repeat(64),
                  })),
              },
            };
          }
          break;
        case "plugins.delivery":
          result = {
            ...result,
            pluginDelivery: options.delivery ?? {
              fileInstallationAvailable: false,
              cachedPackages: [],
            },
          };
          break;
        case "plugins.package.inspect": {
          if (options.inspectFailure !== undefined) throw new Error(options.inspectFailure);
          const input = command.payload;
          const inspectedManifest =
            input.source === "cache"
              ? (options.delivery?.cachedPackages.find(
                  (entry) =>
                    entry.manifest.id === input.pluginId &&
                    entry.manifest.version === input.version,
                )?.manifest ?? availableManifest)
              : availableManifest;
          const installed = snapshot.plugins.find(
            (entry) => entry.id === inspectedManifest.id,
          )?.installed;
          candidate =
            options.inspectDeferred !== undefined
              ? await options.inspectDeferred(input)
              : options.inspection !== undefined
                ? options.inspection
                : {
                    candidateId: "review-sample",
                    manifest: inspectedManifest,
                    sha256:
                      command.payload.source === "file" ? "a".repeat(64) : command.payload.sha256,
                    source: command.payload.source,
                    trust: command.payload.source === "file" ? "publisher" : "official",
                    ...(command.payload.source === "file"
                      ? { publisher: { keyId: "fixture-publisher", name: "Fixture publisher" } }
                      : {}),
                    expiresAt: new Date(Date.now() + 300_000).toISOString(),
                    ...(installed === undefined ? {} : { installedVersion: installed.version }),
                    status:
                      installed === undefined
                        ? "install"
                        : installed.version === inspectedManifest.version &&
                            installed.apiVersion === inspectedManifest.apiVersion
                          ? "already-installed"
                          : "update",
                  };
          result = { ...result, pluginPackage: candidate };
          break;
        }
        case "plugins.package.change.prepare":
        case "plugins.change.prepare":
          result = { ...result, pluginChange: options.prompt ?? null };
          break;
        case "plugins.package.install":
        case "plugins.retry": {
          if (command.command === "plugins.package.install") await options.installDeferred?.();
          if (options.failInstall) throw new Error("The downloaded package failed verification.");
          const activated =
            command.command === "plugins.retry"
              ? snapshot.plugins.find((entry) => entry.id === command.payload.pluginId)?.installed
              : candidate?.manifest;
          if (activated === undefined) throw new Error("No installed package is available.");
          snapshot = {
            revision: snapshot.revision + 1,
            plugins: [
              {
                id: activated.id,
                installed: activated,
                active: activated,
                activationId: "new",
                pending: null,
                restartRequired: false,
              },
            ],
          };
          result = { ...result, pluginSnapshot: snapshot };
          break;
        }
        case "plugins.remove":
          snapshot = { revision: snapshot.revision + 1, plugins: [] };
          result = { ...result, pluginSnapshot: snapshot };
          break;
        case "plugins.package.discard":
          candidate = null;
          break;
        case "plugins.list":
          result = { ...result, pluginSnapshot: snapshot };
          break;
        case "plugins.network.get":
          if (options.networkFailure !== undefined) throw new Error(options.networkFailure);
          result = { ...result, pluginNetwork: network };
          break;
        case "plugins.network.update":
          network = {
            revision: network.revision + 1,
            configuration: command.payload.configuration,
            credentialsConfigured:
              command.payload.credentials.action === "replace" ||
              (command.payload.credentials.action === "unchanged" && network.credentialsConfigured),
            credentialStorage: network.credentialStorage,
            nativeAvailable: network.nativeAvailable,
            supportedProxyProtocols: network.supportedProxyProtocols,
          };
          result = { ...result, pluginNetwork: network };
          break;
        case "plugins.network.test":
          result = {
            ...result,
            pluginNetworkTest:
              options.networkTest === undefined
                ? {
                    settingsRevision: network.revision,
                    checkedAt: "2026-10-06T12:00:00.000Z",
                    scope: "catalog-only",
                    detail: "This test did not inspect a release asset.",
                  }
                : await options.networkTest(network.revision),
          };
          break;
        case "plugins.network.cancel":
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
  return {
    host,
    commands,
    emitSnapshot: (next): void => {
      snapshot = next;
      for (const listener of listeners)
        listener({
          version: HOST_PROTOCOL_VERSION,
          sequence: ++sequence,
          event: "plugins.changed",
          payload: next,
        });
    },
    emitProgress: (progress): void => {
      for (const listener of listeners)
        listener({
          version: HOST_PROTOCOL_VERSION,
          sequence: ++sequence,
          event: "plugins.network.progress",
          payload: progress,
        });
    },
  };
}

export function catalogGate(): {
  promise: Promise<PluginCatalogSnapshot>;
  resolve: (snapshot: PluginCatalogSnapshot) => void;
} {
  let resolve!: (snapshot: PluginCatalogSnapshot) => void;
  const promise = new Promise<PluginCatalogSnapshot>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

export async function approvePackage(
  action: "Install plugin" | "Update plugin" = "Install plugin",
): Promise<void> {
  const review = await screen.findByRole("dialog", { name: "Review plugin" });
  await userEvent.setup().click(within(review).getByRole("button", { name: action }));
}
