import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostCommand } from "../../src/features/kafka/contracts";
import type { PluginHostBindings } from "../../src/plugins/api";
import type { JsonValue, PluginEvent, PluginManifest } from "../../src/plugins/contracts";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { testHostExecute } from "../support/host-response";
import { formatPluginVersion } from "../../src/plugins/validation";

const manifest: PluginManifest = {
  id: "example.capture",
  name: "Example capture",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};

// A standalone installed module: it cannot resolve source files or project dependencies.
const backendCode = `exports.activate = (host) => {
  let pending = false;
  return {
    async execute(request) {
      if (request.method === "start") pending = true;
      if (request.method === "core") return host.execute({command:"connection.disconnect",id:"core",version:1,payload:{}});
      if (request.method === "delete") { await host.deleteProfile(request.input.id); return null; }
      host.publish("changed",request.input);
      return request.input;
    },
    async validateProfile(data, brokers) { if (!pending || data.broker !== brokers[0]) throw new Error("No matching session"); },
    async beforeExit() { return pending ? {title:"Pending work",message:"Keep or clean up?",detail:"Recovery is retained.",cancelAction:"cancel",actions:[{id:"cancel",label:"Cancel"},{id:"keep",label:"Keep"},{id:"cleanup",label:"Clean up"}]} : undefined; },
    async resolveExit(action) { return action !== "cancel"; },
    async beforeChange() { return pending ? {message:"Pending capture",detail:"Capture is running"} : undefined; },
    async prepareUnload() { pending = false; },
    async close() { pending = false; }
  };
};`;

function packageBytes(version = "1.0.0", code = backendCode, id = manifest.id): Uint8Array {
  return encodePluginPackage(
    { ...manifest, id, version },
    new Map([
      ["backend.cjs", Buffer.from(code)],
      ["renderer.js", Buffer.from("export default {apiVersion:2};")],
    ]),
  );
}

function bindings(overrides: Partial<PluginHostBindings> = {}): PluginHostBindings {
  return {
    execute: testHostExecute((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "fixture" },
      }),
    ),
    connectionActive: () => false,
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.resolve(),
    disconnectPluginConnection: () => Promise.resolve(),
    recordActivity: () => undefined,
    failure: (_error, context) => ({
      activeStateChanged: false,
      code: "BACKEND_UNAVAILABLE",
      correlationId: context.correlationId,
      recovery: "Retry",
      retryable: false,
      stage: "backend",
      summary: "Unavailable",
    }),
    ...overrides,
  };
}

const directories: string[] = [];
const runtimes: PluginRuntime[] = [];
async function directory(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "streamskope-plugin-runtime-"));
  directories.push(path);
  return path;
}
function runtime(store: PluginStore, host = bindings(), bytes = packageBytes()): PluginRuntime {
  const value = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    catalog: {
      list: (): Promise<{ manifest: typeof manifest; sha256: string; downloadUrl: string }[]> =>
        Promise.resolve([
          {
            manifest,
            sha256: pluginPackageSha256(bytes),
            downloadUrl: "https://api.github.com/fixture",
          },
        ]),
      download: (): Promise<{ bytes: Uint8Array; sha256: string }> =>
        Promise.resolve({ bytes, sha256: pluginPackageSha256(bytes) }),
    },
  });
  value.bindHost(host);
  runtimes.push(value);
  return value;
}
async function execute(
  host: PluginRuntime,
  method: string,
  input: JsonValue = {},
): Promise<JsonValue> {
  return host.execute({
    pluginId: manifest.id,
    activationId: (await host.list()).plugins[0]?.activationId ?? "",
    method,
    input,
    requestId: "request",
    correlationId: "correlation",
  });
}

afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((value) => value.close()));
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { force: true, recursive: true })),
  );
});

describe("optional installed plugin runtime", () => {
  it("keeps development discovery in memory without writing local build metadata into the official cache", async () => {
    const store = new PluginStore(await directory());
    const host = new PluginRuntime({
      store,
      hostRelease: "v0.2.0",
      persistCatalog: false,
      catalog: {
        list: () =>
          Promise.resolve([{ manifest, sha256: "a".repeat(64), downloadUrl: "development" }]),
        download: () => Promise.reject(new Error("Unused")),
      },
    });
    runtimes.push(host);
    expect((await host.catalog()).error).toBeUndefined();
    expect((await host.catalog(false)).plugins).toEqual([manifest]);
    expect(await store.catalogCache.read()).toBeUndefined();
  });

  it("returns the last successful catalog across reopening when GitHub is unavailable", async () => {
    const store = new PluginStore(await directory());
    const official: PluginManifest = { ...manifest, id: "streamskope.eda" };
    const entry = {
      manifest: official,
      sha256: "a".repeat(64),
      downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/42",
    };
    let online = true;
    const catalog = {
      list: vi.fn(() =>
        online
          ? Promise.resolve([entry])
          : Promise.reject(new Error("GitHub could not be reached")),
      ),
      download: vi.fn(() => Promise.reject(new Error("Unused download"))),
    };
    const initial = new PluginRuntime({ store, catalog, hostRelease: "v0.2.0" });
    runtimes.push(initial);
    expect(await initial.catalog(false)).toEqual({ plugins: [], source: "unavailable" });
    expect(catalog.list).not.toHaveBeenCalled();
    const live = await initial.catalog();
    expect(live).toMatchObject({
      plugins: [official],
      source: "live",
      checkedAt: expect.any(String) as unknown,
    });
    await initial.close();
    online = false;
    const reopened = new PluginRuntime({ store, catalog, hostRelease: "v0.2.0" });
    runtimes.push(reopened);
    expect(await reopened.catalog(false)).toEqual({
      plugins: [official],
      source: "cache",
      checkedAt: live.checkedAt,
    });
    expect(catalog.list).toHaveBeenCalledTimes(1);
    expect(await reopened.catalog()).toEqual({
      plugins: [official],
      source: "cache",
      checkedAt: live.checkedAt,
      error: "GitHub could not be reached",
    });
    expect((await reopened.catalog(false)).checkedAt).toBe(live.checkedAt);
    expect(catalog.download).not.toHaveBeenCalled();
  });

  it("does not hold the local lifecycle queue while catalog refresh waits for the network", async () => {
    const store = new PluginStore(await directory());
    const packaged = packageBytes();
    await store.install(packaged, pluginPackageSha256(packaged));
    let finish!: () => void;
    const waiting = new Promise<[]>((resolve) => {
      finish = () => resolve([]);
    });
    const list = vi.fn(() => waiting);
    const host = new PluginRuntime({
      store,
      hostRelease: "v0.2.0",
      catalog: { list, download: () => Promise.reject(new Error("No download")) },
    });
    host.bindHost(bindings());
    runtimes.push(host);
    const first = host.catalog();
    const second = host.catalog();
    expect((await host.list()).plugins[0]?.active).toEqual(manifest);
    expect(await host.catalog(false)).toEqual({ plugins: [], source: "unavailable" });
    expect((await host.remove(manifest.id)).plugins).toEqual([]);
    expect(list).toHaveBeenCalledTimes(1);
    finish();
    expect(await first).toEqual(await second);
    expect((await host.catalog(false)).source).toBe("cache");
  });

  it("filters a saved catalog against the current host compatibility and release channel", async () => {
    const store = new PluginStore(await directory());
    const official: PluginManifest = {
      ...manifest,
      id: "streamskope.eda",
      apiVersion: 4,
      version: "0.1.0-beta.1",
      compatibility: {
        streamskope: { minimum: "0.2.1-beta.1", maximumExclusive: "0.3.0" },
        target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
      },
    };
    await store.catalogCache.save({
      checkedAt: "2026-10-06T12:00:00.000Z",
      entries: [
        {
          manifest: official,
          sha256: "a".repeat(64),
          downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/42",
        },
      ],
    });
    const stable = new PluginRuntime({ store, hostRelease: "v0.2.1" });
    const older = new PluginRuntime({ store, hostRelease: "v0.1.0-beta.1" });
    const prerelease = new PluginRuntime({ store, hostRelease: "v0.2.1-beta.1" });
    runtimes.push(stable, older, prerelease);
    expect((await stable.catalog(false)).plugins).toEqual([]);
    expect((await older.catalog(false)).plugins).toEqual([]);
    expect((await prerelease.catalog(false)).plugins).toEqual([official]);
  });

  it("reports unavailable metadata when both network and saved catalog fail, without stopping installed plugins", async () => {
    const path = await directory();
    const store = new PluginStore(path);
    const packaged = packageBytes();
    await store.install(packaged, pluginPackageSha256(packaged));
    await writeFile(join(path, "catalog.json"), "corrupt saved metadata");
    const host = new PluginRuntime({
      store,
      hostRelease: "v0.2.0",
      catalog: {
        list: () => Promise.reject(new Error("Network timeout")),
        download: () => Promise.reject(new Error("Unused")),
      },
    });
    host.bindHost(bindings());
    runtimes.push(host);
    expect(await host.catalog()).toEqual({
      plugins: [],
      source: "unavailable",
      error: "Network timeout",
    });
    expect((await host.catalog(false)).error).toEqual(expect.any(String));
    expect((await host.list()).plugins[0]?.active).toEqual(manifest);
  });

  it("rejects a future host requirement before installation or loading code on startup", async () => {
    const compatibility = {
      streamskope: { minimum: "v0.1.0+build.10" },
      target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
    };
    const future: PluginManifest = {
      ...manifest,
      apiVersion: 3,
      compatibility,
      revision: 1,
      version: formatPluginVersion(compatibility, 1),
    };
    const bytes = encodePluginPackage(
      future,
      new Map([
        ["backend.cjs", Buffer.from(backendCode)],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    const store = new PluginStore(await directory());
    const loadModule = vi.fn();
    const host = new PluginRuntime({
      store,
      hostRelease: "v0.1.0+build.5",
      loadModule,
      catalog: {
        list: (): Promise<[]> => Promise.resolve([]),
        download: (): Promise<{ bytes: Uint8Array; sha256: string }> =>
          Promise.resolve({ bytes, sha256: pluginPackageSha256(bytes) }),
      },
    });
    host.bindHost(bindings());
    runtimes.push(host);
    await expect(host.install(manifest.id)).rejects.toThrow("requires StreamSkope v0.1.0+build.10");
    expect(await store.list()).toEqual([]);
    expect(loadModule).not.toHaveBeenCalled();
    await host.close();

    // Simulate opening stored data with an older desktop after a host downgrade.
    await store.install(bytes, pluginPackageSha256(bytes));
    await store.activatePending();
    const older = new PluginRuntime({ store, hostRelease: "v0.1.0+build.5", loadModule });
    older.bindHost(bindings());
    runtimes.push(older);
    await older.start();
    expect((await older.list()).plugins[0]?.error).toContain(
      "requires StreamSkope v0.1.0+build.10",
    );
    expect(loadModule).not.toHaveBeenCalled();
    expect((await store.list())[0]?.installed?.version).toBe(future.version);
    await expect(older.retryActivation(manifest.id)).rejects.toThrow(
      "requires StreamSkope v0.1.0+build.10",
    );
    expect(loadModule).not.toHaveBeenCalled();
    await older.close();
    const upgraded = new PluginRuntime({ store, hostRelease: "v0.1.0+build.10" });
    upgraded.bindHost(bindings());
    runtimes.push(upgraded);
    expect((await upgraded.list()).plugins[0]?.active?.version).toBe(future.version);
  });

  it.each([3, 4, "development"] as const)(
    "retains an active %s version when a download changes its bytes, while allowing exact-byte retry",
    async (channel) => {
      const apiVersion = channel === "development" ? 4 : channel;
      const compatibility = {
        streamskope:
          apiVersion === 3
            ? { minimum: "v0.1.0+build.5" }
            : { minimum: "0.2.0", maximumExclusive: "0.3.0" },
        target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
      };
      const current: PluginManifest = {
        ...manifest,
        apiVersion,
        compatibility,
        ...(apiVersion === 3 ? { revision: 1 } : {}),
        version:
          channel === "development"
            ? "0.0.0-dev.1790928000000"
            : apiVersion === 3
              ? formatPluginVersion(compatibility, 1)
              : "0.1.0",
      };
      const packaged = (code: string): Uint8Array =>
        encodePluginPackage(
          current,
          new Map([
            ["backend.cjs", Buffer.from(code)],
            ["renderer.js", Buffer.from("export default {};")],
          ]),
        );
      const original = packaged(backendCode);
      let available = original;
      const store = new PluginStore(await directory());
      const host = new PluginRuntime({
        store,
        hostRelease: channel === "development" ? "v0.0.0-dev" : compatibility.streamskope.minimum,
        catalog: {
          list: (): Promise<[]> => Promise.resolve([]),
          download: (): Promise<{ bytes: Uint8Array; sha256: string }> =>
            Promise.resolve({ bytes: available, sha256: pluginPackageSha256(available) }),
        },
      });
      host.bindHost(bindings());
      runtimes.push(host);
      await host.install(manifest.id);
      const before = (await host.list()).plugins[0]!;
      available = packaged(
        `${backendCode}\n// Different code cannot reuse the published identity.`,
      );
      await expect(host.install(manifest.id)).rejects.toThrow("version has different content");
      expect((await host.list()).plugins[0]?.activationId).toBe(before.activationId);
      expect((await store.list())[0]?.active).toEqual(current);
      await expect(execute(host, "echo", "still active")).resolves.toBe("still active");
      available = original;
      await expect(host.install(manifest.id)).resolves.toMatchObject({
        plugins: [{ active: current }],
      });
      await expect(execute(host, "echo", "retried")).resolves.toBe("retried");
    },
  );

  it.each([2, 3] as const)(
    "hot-migrates API %s installations to independent SemVer, retains recovery, and refuses a legacy catalog downgrade",
    async (apiVersion) => {
      const compatibility = {
        streamskope: { minimum: "v0.1.0+build.1" },
        target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
      };
      const legacy: PluginManifest =
        apiVersion === 2
          ? { ...manifest, version: "26.8.2" }
          : {
              ...manifest,
              apiVersion,
              compatibility,
              revision: 99,
              version: formatPluginVersion(compatibility, 99),
            };
      const semantic: PluginManifest = {
        ...manifest,
        apiVersion: 4,
        version: "0.1.0",
        compatibility: {
          ...compatibility,
          streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
        },
      };
      const packed = (entry: PluginManifest, code = backendCode): Uint8Array =>
        encodePluginPackage(
          entry,
          new Map([
            ["backend.cjs", Buffer.from(code)],
            ["renderer.js", Buffer.from(`export default {apiVersion:${entry.apiVersion}};`)],
          ]),
        );
      const store = new PluginStore(await directory());
      const original = packed(legacy);
      await store.install(original, pluginPackageSha256(original));
      await store.activatePending();
      const recovery = { sessionId: "retained-session", workflowId: "retained-workflow" };
      await store.writeRecoveryState(manifest.id, recovery);
      let available = packed(semantic);
      const host = new PluginRuntime({
        store,
        hostRelease: "v0.2.0",
        catalog: {
          list: (): Promise<[]> => Promise.resolve([]),
          download: (): Promise<{ bytes: Uint8Array; sha256: string }> =>
            Promise.resolve({ bytes: available, sha256: pluginPackageSha256(available) }),
        },
      });
      host.bindHost(bindings());
      runtimes.push(host);
      const before = (await host.list()).plugins[0]!;
      expect(before.active).toEqual(legacy);
      await expect(host.install(manifest.id)).resolves.toMatchObject({
        plugins: [{ active: semantic, previous: legacy, restartRequired: false }],
      });
      const activation = (await host.list()).plugins[0]!.activationId;
      expect(activation).not.toBe(before.activationId);
      await expect(execute(host, "echo", "migrated")).resolves.toBe("migrated");
      expect(await store.readRecoveryState(manifest.id)).toEqual(recovery);
      available = original;
      await expect(host.install(manifest.id)).rejects.toThrow("older than installed");
      expect((await host.list()).plugins[0]!.activationId).toBe(activation);
      // A newer candidate whose code fails activation cannot replace the working migration.
      available = packed(
        { ...semantic, version: "0.1.1" },
        'exports.activate=()=>{throw new Error("candidate failed");};',
      );
      await expect(host.install(manifest.id)).rejects.toThrow("candidate failed");
      expect((await host.list()).plugins[0]!.active).toEqual(semantic);
      expect(await store.readRecoveryState(manifest.id)).toEqual(recovery);
    },
  );

  it.each(["v0.1.0+build.1", "v0.3.0"])(
    "rejects API 4 installation and activation outside its host range (%s) while retaining the package for a compatible host",
    async (hostRelease) => {
      const current: PluginManifest = {
        ...manifest,
        version: "0.1.0",
        apiVersion: 4,
        compatibility: {
          streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
          target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
        },
      };
      const bytes = encodePluginPackage(
        current,
        new Map([
          ["backend.cjs", Buffer.from(backendCode)],
          ["renderer.js", Buffer.from("export default {};")],
        ]),
      );
      const store = new PluginStore(await directory());
      const loadModule = vi.fn();
      const host = new PluginRuntime({
        store,
        hostRelease,
        loadModule,
        catalog: {
          list: (): Promise<[]> => Promise.resolve([]),
          download: (): Promise<{ bytes: Uint8Array; sha256: string }> =>
            Promise.resolve({ bytes, sha256: pluginPackageSha256(bytes) }),
        },
      });
      host.bindHost(bindings());
      runtimes.push(host);
      await expect(host.install(manifest.id)).rejects.toThrow(
        "requires StreamSkope 0.2.0 up to, but excluding, 0.3.0",
      );
      expect(await store.list()).toEqual([]);
      expect(loadModule).not.toHaveBeenCalled();
      await host.close();
      await store.install(bytes, pluginPackageSha256(bytes));
      await store.activatePending();
      const incompatible = new PluginRuntime({ store, hostRelease, loadModule });
      incompatible.bindHost(bindings());
      runtimes.push(incompatible);
      expect((await incompatible.list()).plugins[0]?.error).toContain("requires StreamSkope 0.2.0");
      expect((await store.list())[0]?.installed).toEqual(current);
      expect(loadModule).not.toHaveBeenCalled();
      await incompatible.close();
      const compatible = new PluginRuntime({ store, hostRelease: "v0.2.5" });
      compatible.bindHost(bindings());
      runtimes.push(compatible);
      expect((await compatible.list()).plugins[0]?.active).toEqual(current);
    },
  );

  it("starts without any installed code and preserves an unavailable profile's metadata", async () => {
    const host = runtime(new PluginStore(await directory()));
    await host.start();
    expect(await host.list()).toEqual({ revision: 0, plugins: [] });
    const source = {
      kind: "plugin",
      pluginId: manifest.id,
      version: 1,
      data: { broker: "localhost:9092", session: "recoverable" },
    } as const;
    await expect(host.validateProfile(source, ["localhost:9092"])).rejects.toThrow(/not active/u);
    expect(source.data.session).toBe("recoverable");
  });

  it("loads verified standalone code, routes events, and exposes only its active renderer", async () => {
    const store = new PluginStore(await directory());
    const bytes = packageBytes();
    const digest = pluginPackageSha256(bytes);
    await store.install(bytes, digest);
    const host = runtime(store);
    const events: PluginEvent[] = [];
    host.subscribe((event) => events.push(event));
    await expect(execute(host, "echo", { hello: "world" })).resolves.toEqual({ hello: "world" });
    expect(events).toEqual([{ pluginId: manifest.id, name: "changed", data: { hello: "world" } }]);
    const rendererUrl = (await host.list()).plugins[0]!.rendererUrl!;
    const asset = await host.rendererAsset(rendererUrl);
    expect(Buffer.from(asset!.content).toString()).toContain("export default");
    for (const path of [
      `/plugins/${manifest.id}/${digest}/backend.cjs`,
      `/plugins/${manifest.id}/${"0".repeat(64)}/renderer.js`,
      `/plugins/${manifest.id}/${digest}/../backend.cjs`,
    ])
      expect(await host.rendererAsset(path)).toBeUndefined();
  });

  it("normalizes the core protocol for an independently compiled plugin", async () => {
    const store = new PluginStore(await directory());
    const bytes = packageBytes();
    await store.install(bytes, pluginPackageSha256(bytes));
    const dispatch = vi.fn((command: HostCommand) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: { correlationId: "core" },
      }),
    );
    const host = runtime(store, bindings({ execute: testHostExecute(dispatch) }));
    await execute(host, "core");
    expect(dispatch).toHaveBeenCalledWith(
      expect.objectContaining({ version: HOST_PROTOCOL_VERSION }),
    );
  });

  it("installs immediately and persists the active version without restart", async () => {
    const store = new PluginStore(await directory());
    const host = runtime(store);
    const installed = await host.install(manifest.id);
    expect(installed.plugins[0]).toMatchObject({
      installed: manifest,
      active: manifest,
      pending: null,
      restartRequired: false,
    });
    await expect(execute(host, "echo", "installed now")).resolves.toBe("installed now");
    await host.close();
    const restarted = runtime(new PluginStore(directories[0]!));
    expect((await restarted.list()).plugins[0]).toMatchObject({
      active: manifest,
      pending: null,
      restartRequired: false,
    });
  });

  it("restores the previous working version when an installed update cannot activate", async () => {
    const path = await directory();
    const store = new PluginStore(path);
    const initial = packageBytes();
    await store.install(initial, pluginPackageSha256(initial));
    const host = runtime(
      store,
      bindings(),
      packageBytes("1.1.0", 'exports.activate = () => { throw new Error("Activation failed"); };'),
    );
    await host.start();
    await expect(host.install(manifest.id)).rejects.toThrow("Activation failed");
    await expect(execute(host, "echo", "still working")).resolves.toBe("still working");
    await host.close();
    const restarted = runtime(new PluginStore(path));
    const snapshot = await restarted.list();
    expect(snapshot.plugins[0]?.active?.version).toBe("1.0.0");
    expect(snapshot.plugins[0]?.error).toBeUndefined();
    await expect(execute(restarted, "echo", "recovered")).resolves.toBe("recovered");
  });

  it("rejects an older downloaded version before changing the running backend", async () => {
    const store = new PluginStore(await directory());
    const newer = packageBytes("1.1.0");
    await store.install(newer, pluginPackageSha256(newer));
    const host = runtime(store, bindings(), packageBytes("1.0.0"));
    const before = (await host.list()).plugins[0]!;
    await expect(host.install(manifest.id)).rejects.toThrow(/older than installed version/u);
    expect((await host.list()).plugins[0]).toMatchObject({
      active: { version: "1.1.0" },
      activationId: before.activationId,
      pending: null,
    });
    await expect(execute(host, "echo", "still working")).resolves.toBe("still working");
  });

  it("requires confirmation for active work and removes the plugin immediately", async () => {
    const path = await directory();
    const store = new PluginStore(path);
    const bytes = packageBytes();
    await store.install(bytes, pluginPackageSha256(bytes));
    const host = runtime(store);
    await execute(host, "start");
    await expect(host.remove(manifest.id)).rejects.toThrow(/confirmation/u);
    expect(await host.prepareExit()).toMatchObject({
      pluginId: manifest.id,
      title: "Pending work",
    });
    expect(await host.resolveExit(manifest.id, "cancel")).toBe(false);
    expect(await host.prepareExit()).not.toBeNull();
    const prompt = await host.prepareChange(manifest.id, "remove");
    expect(prompt).toMatchObject({ pluginId: manifest.id });
    expect((await host.remove(manifest.id, prompt!.token)).plugins).toEqual([]);
    expect(await host.prepareExit()).toBeNull();
    await expect(execute(host, "echo")).rejects.toThrow(/not active/u);
    await host.close();
    expect((await runtime(new PluginStore(path)).list()).plugins).toEqual([]);
  });

  it("isolates corrupt installation state from core startup", async () => {
    const path = await directory();
    await writeFile(join(path, "state.json"), "not JSON");
    const host = runtime(new PluginStore(path));
    await expect(host.start()).resolves.toBeUndefined();
    const snapshot = await host.list();
    expect(snapshot.plugins).toEqual([]);
    expect(snapshot.error).toEqual(expect.any(String));
    await expect(execute(host, "echo")).rejects.toThrow(/not active/u);
  });

  it("rejects a mismatched downloaded identity before staging any installation", async () => {
    const store = new PluginStore(await directory());
    const host = runtime(store, bindings(), packageBytes("1.0.0", backendCode, "different.plugin"));
    await expect(host.install(manifest.id)).rejects.toThrow(/identity/u);
    expect(await store.list()).toEqual([]);
  });
});
