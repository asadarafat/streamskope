import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import { activate } from "../../plugins/nsp/backend";
import { NspApiClient } from "../../plugins/nsp/backend/api-client";
import manifestJson from "../../plugins/nsp/manifest.json";
import { NSP_PLUGIN_ID } from "../../plugins/nsp/contracts";
import { HOST_PROTOCOL_VERSION, type HostCommand } from "../../src/features/kafka/contracts";
import { translateFacadeFailure } from "../../src/features/kafka/facade/facade-support";
import type { PluginBackend, PluginBackendHost, PluginBackendModule } from "../../src/plugins/api";
import type { PluginManifest } from "../../src/plugins/contracts";
import { parsePluginManifest } from "../../src/plugins/validation";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { testHostExecute } from "../support/host-response";

const runtimes: PluginRuntime[] = [];
const directories: string[] = [];

afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((runtime) => runtime.close()));
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let complete!: () => void;
  const promise = new Promise<void>((resolve) => {
    complete = resolve;
  });
  return { promise, resolve: complete };
}

function pluginVersion(patch: number): string {
  return `0.1.${patch}`;
}

function bundle(patch: number): { readonly bytes: Uint8Array; readonly sha256: string } {
  const manifest: PluginManifest = {
    ...parsePluginManifest(manifestJson),
    version: pluginVersion(patch),
  };
  const bytes = encodePluginPackage(
    manifest,
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
      ...(manifest.resources ?? []).map(({ path }): [string, Buffer] => [
        path,
        readFileSync(new URL(`../../plugins/nsp/resources/${path}`, import.meta.url)),
      ]),
    ]),
  );
  return { bytes, sha256: pluginPackageSha256(bytes) };
}

it("hot update drains the old NSP retrieval before the new backend reads recovery, and revokes old writes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-nsp-lifecycle-"));
  directories.push(directory);
  const store = new PluginStore(directory);
  const installed = bundle(1);
  await store.install(installed.bytes, installed.sha256);
  const entered = deferred();
  const release = deferred();
  const unloading = deferred();
  const executionId = randomUUID();
  const hosts: PluginBackendHost[] = [];
  vi.spyOn(NspApiClient.prototype, "authenticate").mockResolvedValue();
  vi.spyOn(NspApiClient.prototype, "readVersion").mockResolvedValue({
    raw: "NSP-CN-26.4.0-rel.200",
    product: "26.4.0",
    build: 200,
  });
  vi.spyOn(NspApiClient.prototype, "ensureWorkflow").mockResolvedValue({
    id: randomUUID(),
    name: "streamskopeNspCaptureV1",
    fingerprint: "a".repeat(64),
  });
  vi.spyOn(NspApiClient.prototype, "retrieveTrust").mockImplementation(
    async (_requestId, callbacks) => {
      await callbacks?.onExecution?.(executionId);
      entered.resolve();
      await release.promise;
      return {
        truststoreBase64: "host-only-truststore",
        truststorePassword: "host-only-password",
        sha256: "a".repeat(64),
        certificateCount: 1,
      };
    },
  );
  const cleanup = vi.spyOn(NspApiClient.prototype, "cleanupExecution").mockResolvedValue();
  vi.spyOn(NspApiClient.prototype, "close").mockResolvedValue();
  const runtime = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    catalog: {
      list: (): Promise<[]> => Promise.resolve([]),
      download: (): Promise<ReturnType<typeof bundle>> => Promise.resolve(bundle(2)),
    },
    loadModule: (): Promise<PluginBackendModule> =>
      Promise.resolve({
        activate: (host: PluginBackendHost): PluginBackend => {
          hosts.push(host);
          const backend = activate(host);
          const unload = backend.prepareUnload.bind(backend);
          backend.prepareUnload = (reason): Promise<void> => {
            unloading.resolve();
            return unload(reason);
          };
          return backend;
        },
      }),
  });
  runtimes.push(runtime);
  const disconnect = vi.fn(() => Promise.resolve());
  const hostExecute = vi.fn((command: HostCommand) =>
    Promise.resolve({
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: command.id },
    }),
  );
  runtime.bindHost({
    execute: testHostExecute(hostExecute),
    connectionActive: () => false,
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.resolve(),
    disconnectPluginConnection: disconnect,
    recordActivity: () => undefined,
    failure: (error, context) =>
      translateFacadeFailure(
        error,
        { ...context, activeStateChanged: false, connection: undefined },
        true,
      ).error,
  });
  const old = (await runtime.list()).plugins[0]!;
  const requestId = randomUUID();
  const operation = runtime.execute({
    pluginId: NSP_PLUGIN_ID,
    activationId: old.activationId!,
    method: "nspCapture.connect",
    input: {
      apiUrl: "https://nsp.example.test",
      username: "operator",
      password: "test-api-secret",
      verifyCertificate: true,
    },
    requestId,
    correlationId: randomUUID(),
  });
  await entered.promise;
  expect(await store.readRecoveryState(NSP_PLUGIN_ID)).toMatchObject({ executionId });
  const prompt = await runtime.prepareChange(NSP_PLUGIN_ID, "install");
  expect(prompt).not.toBeNull();
  const update = runtime.install(NSP_PLUGIN_ID, prompt!.token);
  try {
    await unloading.promise;
    expect(hosts).toHaveLength(2);
  } finally {
    release.resolve();
  }
  expect(await operation).toMatchObject({ ok: false, error: { code: "CANCELLED" } });
  const snapshot = await update;
  expect(cleanup).toHaveBeenCalledWith(expect.any(String), executionId);
  expect(disconnect).toHaveBeenCalledWith(NSP_PLUGIN_ID);
  expect(hostExecute).not.toHaveBeenCalled();
  expect(await store.readRecoveryState(NSP_PLUGIN_ID)).toBeNull();
  const current = snapshot.plugins[0]!;
  expect(current.active?.version).toBe(pluginVersion(2));
  expect(current.activationId).not.toBe(old.activationId);
  expect(
    await runtime.execute({
      pluginId: NSP_PLUGIN_ID,
      activationId: current.activationId!,
      method: "nspCapture.status",
      input: {},
      requestId: randomUUID(),
      correlationId: randomUUID(),
    }),
  ).toEqual({ ok: true, status: { state: "idle" } });
  await expect(hosts[0]!.recoveryState!.write({ stale: true })).rejects.toThrow(
    /no longer active/u,
  );
  await expect(hosts[0]!.recoveryState!.read()).rejects.toThrow(/no longer active/u);
  expect(await store.readRecoveryState(NSP_PLUGIN_ID)).toBeNull();
  expect(await runtime.prepareChange(NSP_PLUGIN_ID, "remove")).toBeNull();
  await runtime.remove(NSP_PLUGIN_ID);
  expect((await runtime.list()).plugins).toEqual([]);
});

it("direct host shutdown permits final NSP cleanup and journal writes while rejecting new host work", async () => {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-nsp-shutdown-"));
  directories.push(directory);
  const store = new PluginStore(directory);
  const installed = bundle(1);
  await store.install(installed.bytes, installed.sha256);
  const entered = deferred();
  const release = deferred();
  const closing = deferred();
  const executionId = randomUUID();
  let pluginHost: PluginBackendHost | undefined;
  vi.spyOn(NspApiClient.prototype, "authenticate").mockResolvedValue();
  vi.spyOn(NspApiClient.prototype, "readVersion").mockResolvedValue({
    raw: "NSP-CN-26.4.0-rel.200",
    product: "26.4.0",
    build: 200,
  });
  vi.spyOn(NspApiClient.prototype, "ensureWorkflow").mockResolvedValue({
    id: randomUUID(),
    name: "streamskopeNspCaptureV1",
    fingerprint: "a".repeat(64),
  });
  vi.spyOn(NspApiClient.prototype, "retrieveTrust").mockImplementation(
    async (_requestId, callbacks) => {
      await callbacks?.onExecution?.(executionId);
      entered.resolve();
      await release.promise;
      throw new Error("Request interrupted during shutdown");
    },
  );
  const remoteCleanup = vi
    .spyOn(NspApiClient.prototype, "cleanupExecution")
    .mockImplementation(async () => {
      expect(await pluginHost!.recoveryState!.read()).toMatchObject({ executionId });
      await expect(
        pluginHost!.execute({
          command: "profiles.list",
          id: randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).rejects.toThrow(/no longer active/u);
      await expect(pluginHost!.deleteProfile("unrelated")).rejects.toThrow(/no longer active/u);
      await expect(pluginHost!.disconnectOwnedConnection()).rejects.toThrow(/no longer active/u);
    });
  const closeClient = vi.spyOn(NspApiClient.prototype, "close").mockResolvedValue();
  const runtime = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    loadModule: (): Promise<PluginBackendModule> =>
      Promise.resolve({
        activate: (host: PluginBackendHost): PluginBackend => {
          pluginHost = host;
          const backend = activate(host);
          const close = backend.close.bind(backend);
          backend.close = (): Promise<void> => {
            closing.resolve();
            return close();
          };
          return backend;
        },
      }),
  });
  runtimes.push(runtime);
  const hostExecute = vi.fn(() => Promise.reject(new Error("Unexpected host command")));
  runtime.bindHost({
    execute: testHostExecute(hostExecute),
    connectionActive: () => false,
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.reject(new Error("Unexpected profile deletion")),
    disconnectPluginConnection: () => Promise.reject(new Error("Unexpected disconnection")),
    recordActivity: () => undefined,
    failure: (error, context) =>
      translateFacadeFailure(
        error,
        { ...context, activeStateChanged: false, connection: undefined },
        true,
      ).error,
  });
  const current = (await runtime.list()).plugins[0]!;
  const operation = runtime.execute({
    pluginId: NSP_PLUGIN_ID,
    activationId: current.activationId!,
    method: "nspCapture.connect",
    input: {
      apiUrl: "https://nsp.example.test",
      username: "operator",
      password: "test-api-secret",
      verifyCertificate: true,
    },
    requestId: randomUUID(),
    correlationId: randomUUID(),
  });
  await entered.promise;
  const shutdown = runtime.close();
  try {
    await closing.promise;
  } finally {
    release.resolve();
  }
  expect(await operation).toMatchObject({ ok: false, error: { code: "CANCELLED" } });
  await expect(shutdown).resolves.toBeUndefined();
  expect(remoteCleanup).toHaveBeenCalledWith(expect.any(String), executionId);
  expect(closeClient).toHaveBeenCalledTimes(2);
  expect(hostExecute).not.toHaveBeenCalled();
  expect(await store.readRecoveryState(NSP_PLUGIN_ID)).toBeNull();
  await expect(pluginHost!.recoveryState!.write({ stale: true })).rejects.toThrow(
    /no longer active/u,
  );
  await expect(pluginHost!.recoveryState!.read()).rejects.toThrow(/no longer active/u);
});

it("discarding a staged update cannot overwrite the active plugin's unresolved recovery", async () => {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-nsp-staged-"));
  directories.push(directory);
  const store = new PluginStore(directory);
  const installed = bundle(1);
  await store.install(installed.bytes, installed.sha256);
  const recovery = {
    version: 1,
    apiUrl: "https://nsp.example.test",
    username: "operator",
    requestId: randomUUID(),
    executionId: randomUUID(),
  };
  await store.writeRecoveryState(NSP_PLUGIN_ID, recovery);
  let activations = 0;
  const attemptedWrite = vi.fn();
  const runtime = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    catalog: {
      list: (): Promise<[]> => Promise.resolve([]),
      download: (): Promise<ReturnType<typeof bundle>> => Promise.resolve(bundle(2)),
    },
    loadModule: (): Promise<PluginBackendModule> =>
      Promise.resolve({
        activate: (host: PluginBackendHost): PluginBackend => {
          const backend = activate(host);
          if (++activations === 2) {
            const close = backend.close.bind(backend);
            backend.close = async (): Promise<void> => {
              attemptedWrite();
              await expect(host.recoveryState!.write(null)).rejects.toThrow(/no longer active/u);
              await close();
            };
          }
          return backend;
        },
      }),
  });
  runtimes.push(runtime);
  runtime.bindHost({
    execute: testHostExecute(() => Promise.reject(new Error("Unexpected host command"))),
    connectionActive: () => false,
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.resolve(),
    disconnectPluginConnection: () => Promise.resolve(),
    recordActivity: () => undefined,
    failure: (error, context) =>
      translateFacadeFailure(
        error,
        { ...context, activeStateChanged: false, connection: undefined },
        true,
      ).error,
  });
  const prompt = await runtime.prepareChange(NSP_PLUGIN_ID, "install");
  await expect(runtime.install(NSP_PLUGIN_ID, prompt!.token)).rejects.toThrow(
    /cleanup is still pending/u,
  );
  expect(attemptedWrite).toHaveBeenCalledOnce();
  expect(await store.readRecoveryState(NSP_PLUGIN_ID)).toEqual(recovery);
  expect((await runtime.list()).plugins[0]?.active?.version).toBe(pluginVersion(1));
});
