import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import type { PluginHostBindings } from "../../src/plugins/api";
import type {
  PluginAcquisitionProgress,
  PluginManifest,
  PluginNetworkUpdateInput,
} from "../../src/plugins/contracts";
import type {
  OfficialPluginEntry,
  PluginCatalogRequest,
  PluginCatalogSource,
} from "../../src/platform/node/plugins/catalog";
import {
  encodePluginPackage,
  pluginPackageSha256,
  signPortablePluginPackage,
} from "../../src/platform/node/plugins/package";
import { PluginRuntime, type PluginRuntimeOptions } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import type { PluginNetworkTransport } from "../../src/platform/node/plugins/network-transport";
import { testHostExecute } from "../support/host-response";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "Capture",
  version: "0.1.0",
  apiVersion: 4,
  backend: "backend.cjs",
  renderer: "renderer.js",
  compatibility: {
    streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
    target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
  },
};
const code =
  "exports.activate=()=>({async execute(){return 'active'},async validateProfile(){},async beforeExit(){},async resolveExit(){return true},async beforeChange(){},async prepareUnload(){},async close(){}});";
const primary = (version = "0.1.0"): Uint8Array =>
  encodePluginPackage(
    { ...manifest, version },
    new Map([
      ["backend.cjs", Buffer.from(code)],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
const entry = (version = "0.1.0"): OfficialPluginEntry => ({
  manifest: { ...manifest, version },
  sha256: pluginPackageSha256(primary(version)),
  downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/42",
});
const policy = (offline: boolean): PluginNetworkUpdateInput => ({
  configuration: { mode: "system", proxyUrl: null, offline },
  credentials: { action: "clear" },
});
const roots: string[] = [];
const hosts: PluginRuntime[] = [];
function bindings(): PluginHostBindings {
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
  };
}
async function setup(
  options: Omit<PluginRuntimeOptions, "store"> = {},
): Promise<{ host: PluginRuntime; store: PluginStore; root: string; signed: () => Uint8Array }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-plugin-policy-"));
  roots.push(root);
  const publisher = pluginPublisherFixture();
  const store = new PluginStore(root, { trustedPublishers: publisher.publishers });
  const host = new PluginRuntime({ store, hostRelease: "v0.2.0", ...options });
  host.bindHost(bindings());
  hosts.push(host);
  return {
    root,
    store,
    host,
    signed: (): Uint8Array =>
      signPortablePluginPackage(
        primary(),
        publisher.publishers[0]!.keyId,
        Buffer.from(publisher.encodedKey, "base64").toString(),
      ),
  };
}
afterEach(async () => {
  await Promise.all(hosts.splice(0).map((host) => host.close()));
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("plugin-only network policy", () => {
  it("blocks every remote path while keeping completed signed reviews, cache, and installed management usable", async () => {
    let chosen: Uint8Array | null = null;
    const list = vi.fn(() => Promise.resolve([entry()]));
    const download = vi.fn(() => Promise.resolve({ bytes: primary(), sha256: entry().sha256 }));
    const { host, store, signed } = await setup({
      catalog: { list, download },
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await host.catalog(true, "initial-catalog");
    chosen = signed();
    const review = (await host.inspectPackage({ source: "file" }, "signed-review"))!;
    await host.updateNetwork(policy(true));
    expect((await host.installPackage(review.candidateId)).plugins[0]?.active).toEqual(manifest);
    expect((await host.catalog(false)).plugins).toEqual([manifest]);
    expect((await host.catalog(true, "offline-refresh")).error).toContain("offline");
    await expect(host.install(manifest.id)).rejects.toThrow(/offline/u);
    await expect(
      host.inspectPackage(
        {
          source: "catalog",
          pluginId: manifest.id,
          version: manifest.version,
          sha256: entry().sha256,
        },
        "offline-download",
      ),
    ).rejects.toThrow(/offline/u);
    await expect(host.testNetwork("offline-test")).rejects.toThrow(/offline/u);
    await host.remove(manifest.id);
    const cached = (await host.inspectPackage(
      { source: "cache", pluginId: manifest.id, version: manifest.version, sha256: review.sha256 },
      "cached-review",
    ))!;
    expect((await host.installPackage(cached.candidateId)).plugins[0]?.active).toEqual(manifest);
    expect((await store.getActive(manifest.id))?.sha256).toBe(review.sha256);
    expect(list).toHaveBeenCalledOnce();
    expect(download).not.toHaveBeenCalled();
  });
  it("lets one shared catalog owner cancel without aborting or replacing the other owner's refresh", async () => {
    let resolve!: (value: readonly OfficialPluginEntry[]) => void;
    let signal!: AbortSignal;
    let entered!: () => void;
    const ready = new Promise<void>((done) => {
      entered = done;
    });
    const listing = new Promise<readonly OfficialPluginEntry[]>((done) => {
      resolve = done;
    });
    const list = vi.fn((request?: PluginCatalogRequest) => {
      signal = request!.signal!;
      entered();
      return listing;
    });
    const { host } = await setup({
      catalog: { list, download: (): Promise<never> => Promise.reject(new Error("Unused")) },
    });
    const first = host.catalog(true, "first-owner");
    const second = host.catalog(true, "second-owner");
    await ready;
    await host.cancelAcquisition("first-owner");
    expect((await first).error).toContain("cancelled");
    expect(signal.aborted).toBe(false);
    resolve([entry()]);
    expect((await second).source).toBe("live");
    expect((await host.catalog(false)).plugins).toEqual([manifest]);
    expect(list).toHaveBeenCalledOnce();
    await host.cancelAcquisition("first-owner");
    await host.cancelAcquisition("second-owner");
  });
  it("cancels settings-generation work and prevents an ignored stale completion from overwriting verified catalog metadata", async () => {
    let resolve!: (value: readonly OfficialPluginEntry[]) => void;
    let entered!: () => void;
    const ready = new Promise<void>((done) => {
      entered = done;
    });
    const stale = new Promise<readonly OfficialPluginEntry[]>((done) => {
      resolve = done;
    });
    let generation = 0;
    const list = vi.fn(() => {
      generation += 1;
      if (generation === 2) {
        entered();
        return stale;
      }
      return Promise.resolve([entry(generation === 1 ? "0.1.0" : "0.3.0")]);
    });
    const { host, store } = await setup({
      catalog: { list, download: (): Promise<never> => Promise.reject(new Error("Unused")) },
    });
    await host.catalog(true, "first");
    const refreshing = host.catalog(true, "stale");
    await ready;
    await host.updateNetwork(policy(true));
    expect((await refreshing).error).toContain("cancelled");
    resolve([entry("0.2.0")]);
    await Promise.resolve();
    await Promise.resolve();
    expect((await host.catalog(false)).plugins[0]?.version).toBe("0.1.0");
    expect((await store.catalogCache.read())?.entries[0]?.manifest.version).toBe("0.1.0");
    await host.updateNetwork(policy(false));
    expect((await host.catalog(true, "current")).plugins[0]?.version).toBe("0.3.0");
  });
  it("cancels pinned package acquisition promptly, leaving no late review or activation", async () => {
    let resolve!: (value: { bytes: Uint8Array; sha256: string }) => void;
    let entered!: () => void;
    const ready = new Promise<void>((done) => {
      entered = done;
    });
    const downloading = new Promise<{ bytes: Uint8Array; sha256: string }>((done) => {
      resolve = done;
    });
    const source: PluginCatalogSource = {
      list: (): Promise<readonly OfficialPluginEntry[]> => Promise.resolve([entry()]),
      download: (): Promise<never> => Promise.reject(new Error("Unused")),
      downloadPinned: () => {
        entered();
        return downloading;
      },
    };
    const { host, store } = await setup({ catalog: source });
    await host.catalog(true, "catalog");
    const inspecting = host.inspectPackage(
      {
        source: "catalog",
        pluginId: manifest.id,
        version: manifest.version,
        sha256: entry().sha256,
      },
      "download",
    );
    await ready;
    await host.cancelAcquisition("download");
    await expect(inspecting).rejects.toThrow(/cancelled/u);
    resolve({ bytes: primary(), sha256: entry().sha256 });
    await Promise.resolve();
    await Promise.resolve();
    expect(await store.list()).toEqual([]);
    expect((await host.delivery()).cachedPackages).toEqual([]);
  });
  it("reports truthful catalog-only and catalog-plus-bounded-asset test scope with the captured settings revision", async () => {
    let published = false;
    const probe = vi.fn(
      (_entry: OfficialPluginEntry, request?: PluginCatalogRequest): Promise<void> => {
        request?.onProgress?.("download", 10, 10);
        return Promise.resolve();
      },
    );
    const { host } = await setup({
      catalog: {
        list: (): Promise<readonly OfficialPluginEntry[]> =>
          Promise.resolve(published ? [entry()] : []),
        download: (): Promise<never> => Promise.reject(new Error("Unused")),
        probe,
      },
    });
    const progress: PluginAcquisitionProgress[] = [];
    host.subscribeAcquisition((value): void => {
      progress.push(value);
    });
    expect(await host.testNetwork("empty")).toMatchObject({
      scope: "catalog-only",
      settingsRevision: 0,
      detail: expect.stringContaining("No compatible") as unknown,
    });
    expect(probe).not.toHaveBeenCalled();
    published = true;
    expect(await host.testNetwork("assets")).toMatchObject({
      scope: "catalog-and-assets",
      settingsRevision: 0,
    });
    expect(probe).toHaveBeenCalledOnce();
    expect(progress.at(-1)).toMatchObject({
      requestId: "assets",
      state: "succeeded",
      receivedBytes: 10,
      totalBytes: 10,
    });
  });
  it("keeps a corrupt native settings file from touching the network while signed local installation still works", async () => {
    let chosen: Uint8Array | null = null;
    const list = vi.fn(() => Promise.resolve([entry()]));
    const native: PluginNetworkTransport = {
      nativeAvailable: true,
      supportedProxyProtocols: ["http", "https"],
      fetch: vi.fn<typeof fetch>(() => Promise.reject(new Error("Unused"))),
      configure: (): Promise<void> => Promise.resolve(),
      close: (): Promise<void> => Promise.resolve(),
    };
    const { host, store, signed } = await setup({
      networkTransport: native,
      catalog: { list, download: (): Promise<never> => Promise.reject(new Error("Unused")) },
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await writeFile(store.networkSettingsPath(), "corrupt configuration");
    expect((await host.catalog(true, "corrupt")).error).toContain("need attention");
    expect(list).not.toHaveBeenCalled();
    chosen = signed();
    const review = (await host.inspectPackage({ source: "file" }, "local"))!;
    expect((await host.installPackage(review.candidateId)).plugins[0]?.active).toEqual(manifest);
    await host.updateNetwork(policy(false));
    expect((await host.catalog(true, "reset")).source).toBe("live");
  });
});
