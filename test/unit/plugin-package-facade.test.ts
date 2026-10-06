import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  parseHostCommand,
  parseHostCommandResponse,
  type HostCommand,
  type HostCommandName,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceService,
} from "../../src/features/kafka/application";
import type { PluginRuntimePort } from "../../src/plugins/api";
import type {
  PluginDeliverySnapshot,
  PluginAcquisitionProgress,
  PluginNetworkSnapshot,
  PluginNetworkUpdateInput,
  PluginPackageInspection,
  PluginSnapshot,
} from "../../src/plugins/contracts";
import { createFacade, RecordingConnectionPort } from "../support/kafka-backend-facade-fixture";

const snapshot: PluginSnapshot = { revision: 1, plugins: [] };
const inspection: PluginPackageInspection = {
  candidateId: "native-candidate",
  manifest: {
    id: "example.capture",
    name: "Example capture",
    version: "1.0.0",
    apiVersion: 2,
    backend: "backend.cjs",
    renderer: "renderer.js",
  },
  sha256: "a".repeat(64),
  source: "file",
  publisher: { keyId: "streamskope-publisher", name: "StreamSkope" },
  trust: "publisher",
  expiresAt: "2026-10-06T12:05:00.000Z",
  status: "install",
};
const operations = [
  ["plugins.delivery", {}, "delivery"],
  ["plugins.package.inspect", { source: "file" }, "inspectPackage"],
  [
    "plugins.package.change.prepare",
    { candidateId: inspection.candidateId },
    "preparePackageChange",
  ],
  [
    "plugins.package.install",
    { candidateId: inspection.candidateId, confirmationToken: "consent" },
    "installPackage",
  ],
  ["plugins.package.discard", { candidateId: inspection.candidateId }, "discardPackage"],
] as const;

function request(command: HostCommandName, payload: unknown): HostCommand {
  return parseHostCommand({ command, id: command, payload, version: HOST_PROTOCOL_VERSION });
}
function pluginRuntime(): PluginRuntimePort {
  return {
    bindHost: (): void => undefined,
    start: (): Promise<void> => Promise.resolve(),
    execute: () => Promise.resolve(null),
    validateProfile: () => Promise.resolve(),
    withProfileConnection: (_source, _brokers, connect) => connect(),
    subscribe: () => (): void => undefined,
    subscribeChanges: () => (): void => undefined,
    list: () => Promise.resolve(snapshot),
    catalog: () => Promise.resolve({ plugins: [] }),
    prepareChange: () => Promise.resolve(null),
    install: () => Promise.resolve(snapshot),
    retryActivation: () => Promise.resolve(snapshot),
    remove: () => Promise.resolve(snapshot),
    rendererFailed: () => Promise.resolve(snapshot),
    restart: () => Promise.resolve(),
    prepareExit: () => Promise.resolve(null),
    resolveExit: () => Promise.resolve(true),
    close: () => Promise.resolve(),
  };
}
function facade(runtime?: PluginRuntimePort, readOnly = false): ReturnType<typeof createFacade> {
  const preferences = new KafkaOperationalPreferenceService(
    new InMemoryKafkaOperationalPreferenceStore(
      { durability: "session", state: "ready" },
      {
        ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
        protection: { readOnly, maskKey: false, maskHeaders: [], valuePaths: [] },
      },
    ),
  );
  return createFacade(
    new RecordingConnectionPort(),
    undefined,
    undefined,
    undefined,
    preferences,
    undefined,
    runtime,
  );
}

describe("plugin package host facade", () => {
  it("routes inspection and consent through host-owned candidates with typed results", async () => {
    const delivery = { fileInstallationAvailable: true, cachedPackages: [] };
    const runtime = {
      ...pluginRuntime(),
      delivery: vi.fn(function (this: PluginRuntimePort): Promise<PluginDeliverySnapshot> {
        expect(this).toBe(runtime);
        return Promise.resolve(delivery);
      }),
      inspectPackage: vi.fn(() => Promise.resolve(inspection)),
      preparePackageChange: vi.fn(() => Promise.resolve(null)),
      installPackage: vi.fn(() => Promise.resolve(snapshot)),
      discardPackage: vi.fn(() => Promise.resolve()),
    };
    const backend = facade(runtime);
    const results = [
      { pluginDelivery: delivery },
      { pluginPackage: inspection },
      { pluginChange: null },
      { pluginSnapshot: snapshot },
      {},
    ];
    for (const [index, [name, payload]] of operations.entries()) {
      const reply = await backend.execute(request(name, payload));
      expect(reply).toMatchObject({ command: name, ok: true, result: results[index] });
      expect(parseHostCommandResponse(reply)).toEqual(reply);
    }
    expect(runtime.delivery).toHaveBeenCalledOnce();
    expect(runtime.inspectPackage).toHaveBeenCalledWith(
      { source: "file" },
      "plugins.package.inspect",
    );
    expect(runtime.preparePackageChange).toHaveBeenCalledWith(inspection.candidateId);
    expect(runtime.installPackage).toHaveBeenCalledWith(inspection.candidateId, "consent");
    expect(runtime.discardPackage).toHaveBeenCalledWith(inspection.candidateId);
    await backend.shutdown();
  });

  it("keeps native picker cancellation as a successful empty inspection", async () => {
    const backend = facade({ ...pluginRuntime(), inspectPackage: () => Promise.resolve(null) });
    expect(
      await backend.execute(request("plugins.package.inspect", { source: "file" })),
    ).toMatchObject({ ok: true, result: { pluginPackage: null } });
    await backend.shutdown();
  });

  it.each(operations)(
    "reports unavailable for %s on older runtime ports and absent hosts",
    async (name, payload, method) => {
      for (const runtime of [pluginRuntime(), undefined]) {
        expect(runtime?.[method] === undefined).toBe(true);
        const backend = facade(runtime);
        expect(await backend.execute(request(name, payload))).toMatchObject({
          ok: false,
          error: { code: "BACKEND_UNAVAILABLE", retryable: false },
        });
        await backend.shutdown();
      }
    },
  );

  it("allows inspection and discard in read-only mode while refusing installation and consent", async () => {
    const runtime = {
      ...pluginRuntime(),
      delivery: vi.fn(() =>
        Promise.resolve({ fileInstallationAvailable: true, cachedPackages: [] }),
      ),
      inspectPackage: vi.fn(() => Promise.resolve(inspection)),
      discardPackage: vi.fn(() => Promise.resolve()),
      preparePackageChange: vi.fn(() => Promise.resolve(null)),
      installPackage: vi.fn(() => Promise.resolve(snapshot)),
    };
    const backend = facade(runtime, true);
    for (const [name, payload] of operations) {
      const reply = await backend.execute(request(name, payload));
      if (name === "plugins.package.install" || name === "plugins.package.change.prepare") {
        expect(reply).toMatchObject({ ok: false, error: { code: "AUTHORIZATION_DENIED" } });
      } else {
        expect(reply).toMatchObject({ ok: true });
      }
    }
    expect(runtime.discardPackage).toHaveBeenCalledOnce();
    expect(runtime.installPackage).not.toHaveBeenCalled();
    expect(runtime.preparePackageChange).not.toHaveBeenCalled();
    await backend.shutdown();
  });
});

const network: PluginNetworkSnapshot = {
  revision: 1,
  configuration: { mode: "custom", offline: false, proxyUrl: "http://proxy.example:8080" },
  credentialsConfigured: true,
  credentialStorage: "encrypted",
  nativeAvailable: true,
  supportedProxyProtocols: ["http", "https"],
};
const networkUpdate: PluginNetworkUpdateInput = {
  configuration: network.configuration!,
  credentials: {
    action: "replace",
    username: "fixture-proxy-user",
    password: "fixture-proxy-password",
  },
};
const networkOperations = [
  ["plugins.network.get", {}, "networkSettings"],
  ["plugins.network.update", networkUpdate, "updateNetwork"],
  ["plugins.network.test", {}, "testNetwork"],
  ["plugins.network.cancel", { requestId: "owned-download" }, "cancelAcquisition"],
] as const;

describe("plugin acquisition networking facade", () => {
  it("routes local settings, connectivity tests and cancellation using actual host command IDs", async () => {
    const connectivity = {
      settingsRevision: 1,
      checkedAt: "2026-10-06T12:00:00.000Z",
      scope: "catalog-and-assets" as const,
    };
    const runtime = {
      ...pluginRuntime(),
      networkSettings: vi.fn(() => Promise.resolve(network)),
      updateNetwork: vi.fn(() => Promise.resolve(network)),
      testNetwork: vi.fn(() => Promise.resolve(connectivity)),
      cancelAcquisition: vi.fn(() => Promise.resolve()),
      catalog: vi.fn(() => Promise.resolve({ plugins: [] })),
    };
    const backend = facade(runtime, true);
    for (const [name, payload] of networkOperations) {
      const reply = await backend.execute(request(name, payload));
      expect(reply.ok).toBe(true);
      expect(parseHostCommandResponse(reply)).toEqual(reply);
    }
    expect(runtime.networkSettings).toHaveBeenCalledOnce();
    expect(runtime.updateNetwork).toHaveBeenCalledWith(networkUpdate);
    expect(runtime.testNetwork).toHaveBeenCalledWith("plugins.network.test");
    expect(runtime.cancelAcquisition).toHaveBeenCalledWith("owned-download");
    await backend.execute(request("plugins.catalog", { refresh: true }));
    expect(runtime.catalog).toHaveBeenCalledWith(true, "plugins.catalog");
    await backend.shutdown();
  });

  it.each(networkOperations)(
    "reports %s unavailable for older runtime ports",
    async (name, payload, method) => {
      for (const runtime of [pluginRuntime(), undefined]) {
        expect(runtime?.[method] === undefined).toBe(true);
        const backend = facade(runtime);
        expect(await backend.execute(request(name, payload))).toMatchObject({
          ok: false,
          error: { code: "BACKEND_UNAVAILABLE" },
        });
        await backend.shutdown();
      }
    },
  );

  it("forwards correlated acquisition progress and releases its subscription on shutdown", async () => {
    let progress!: (payload: PluginAcquisitionProgress) => void;
    const unsubscribe = vi.fn();
    const runtime = {
      ...pluginRuntime(),
      subscribeAcquisition: vi.fn((listener: typeof progress): (() => void) => {
        progress = listener;
        return unsubscribe;
      }),
    };
    const backend = facade(runtime);
    const events: import("../../src/features/kafka/contracts").HostEvent[] = [];
    backend.subscribe((event) => events.push(event));
    const payload: PluginAcquisitionProgress = {
      requestId: "owned-download",
      operation: "inspect",
      phase: "download",
      state: "running",
      receivedBytes: 42,
      totalBytes: 100,
    };
    progress(payload);
    expect(events.find((event) => event.event === "plugins.network.progress")).toMatchObject({
      version: HOST_PROTOCOL_VERSION,
      payload,
    });
    await backend.shutdown();
    expect(unsubscribe).toHaveBeenCalledOnce();
  });

  it("redacts write-only proxy credentials from structured failure summaries and recovery", async () => {
    const credentials = networkUpdate.credentials;
    if (credentials.action !== "replace") throw new Error("Expected fixture credentials.");
    const sensitive = `${credentials.username} ${credentials.password}`;
    const runtime = {
      ...pluginRuntime(),
      updateNetwork: vi.fn(() =>
        Promise.reject(
          Object.assign(new Error(`Proxy authentication rejected ${sensitive}`), {
            code: "BACKEND_UNAVAILABLE",
            stage: "backend",
            retryable: false,
            recovery: `Check credentials ${sensitive}`,
          }),
        ),
      ),
    };
    const backend = facade(runtime);
    const reply = await backend.execute(request("plugins.network.update", networkUpdate));
    expect(reply).toMatchObject({ ok: false, error: { code: "BACKEND_UNAVAILABLE" } });
    expect(JSON.stringify(reply)).not.toContain(credentials.username);
    expect(JSON.stringify(reply)).not.toContain(credentials.password);
    await backend.shutdown();
  });
});
