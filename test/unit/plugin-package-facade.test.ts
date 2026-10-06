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
    expect(runtime.inspectPackage).toHaveBeenCalledWith({ source: "file" });
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
