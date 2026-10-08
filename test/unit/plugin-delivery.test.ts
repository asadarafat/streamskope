import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import type { PluginBackend, PluginHostBindings } from "../../src/plugins/api";
import type { PluginManifest, PluginPackageInspection } from "../../src/plugins/contracts";
import type { OfficialPluginEntry } from "../../src/platform/node/plugins/catalog";
import {
  encodePluginPackage,
  pluginPackageSha256,
  signPortablePluginPackage,
} from "../../src/platform/node/plugins/package";
import { PluginRuntime, type PluginRuntimeOptions } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { PLUGIN_PACKAGE_REVIEW_TTL_MS } from "../../src/platform/node/plugins/package-candidates";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";
import { testHostExecute } from "../support/host-response";

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
const code = `exports.activate = () => { let busy=false; return { async execute(request){if(request.method==='start')busy=true;return 'active';}, async validateProfile(){}, async beforeExit(){}, async resolveExit(){return true;}, async beforeChange(){return busy?{message:'Capture active',detail:'Fixture session'}:undefined;}, async prepareUnload(){busy=false;}, async close(){busy=false;} }; };`;
const primary = (version = "0.1.0", descriptor = manifest): Uint8Array =>
  encodePluginPackage(
    { ...descriptor, version },
    new Map([
      ["backend.cjs", Buffer.from(code)],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
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
async function setup(options: Omit<PluginRuntimeOptions, "store"> = {}): Promise<{
  root: string;
  host: PluginRuntime;
  store: PluginStore;
  signed: (version?: string, descriptor?: PluginManifest) => Uint8Array;
  network: { list: ReturnType<typeof vi.fn>; download: ReturnType<typeof vi.fn> };
}> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-delivery-"));
  roots.push(root);
  const fixture = pluginPublisherFixture();
  const store = new PluginStore(root, { trustedPublishers: fixture.publishers });
  const network = {
    list: vi.fn(() => Promise.reject(new Error("No internet"))),
    download: vi.fn(() => Promise.reject(new Error("No internet"))),
  };
  const host = new PluginRuntime({ store, hostRelease: "v0.2.0", catalog: network, ...options });
  host.bindHost(bindings());
  hosts.push(host);
  return {
    root,
    host,
    store,
    network,
    signed: (version = "0.1.0", descriptor = manifest): Uint8Array =>
      signPortablePluginPackage(
        primary(version, descriptor),
        fixture.publishers[0]!.keyId,
        Buffer.from(fixture.encodedKey, "base64").toString(),
      ),
  };
}
async function start(host: PluginRuntime): Promise<void> {
  await host.start();
  const plugin = (await host.list()).plugins[0]!;
  await host.execute({
    pluginId: manifest.id,
    activationId: plugin.activationId!,
    method: "start",
    input: {},
    requestId: "fixture",
    correlationId: "fixture",
  });
}
function reference(value: PluginPackageInspection): {
  pluginId: string;
  version: string;
  sha256: string;
} {
  return { pluginId: value.manifest.id, version: value.manifest.version, sha256: value.sha256 };
}
afterEach(async () => {
  await Promise.all(hosts.splice(0).map((host) => host.close()));
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("offline reviewed plugin delivery", () => {
  it("pins the signed reviewed bytes, installs without GitHub and reuses its cache after removal", async () => {
    let chosen: Uint8Array | null = null;
    const { host, store, signed, network } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    chosen = signed();
    const candidate = (await host.inspectPackage({ source: "file" }))!;
    expect(candidate).toMatchObject({ status: "install", trust: "publisher", manifest });
    chosen.fill(0);
    expect(await host.preparePackageChange(candidate.candidateId)).toBeNull();
    expect((await host.installPackage(candidate.candidateId)).plugins[0]?.active).toEqual(manifest);
    await expect(host.installPackage(candidate.candidateId)).rejects.toThrow(/expired|closed/u);
    expect((await store.getActive(manifest.id))?.sha256).toBe(candidate.sha256);
    await host.remove(manifest.id);
    const cached = (await host.inspectPackage({ source: "cache", ...reference(candidate) }))!;
    expect(cached.sha256).toBe(candidate.sha256);
    expect((await host.installPackage(cached.candidateId)).plugins[0]?.active).toEqual(manifest);
    expect(network.list).not.toHaveBeenCalled();
    expect(network.download).not.toHaveBeenCalled();
  });
  it("handles chooser cancellation and refuses unsigned local files without staging code", async () => {
    let chosen: Uint8Array | null = null;
    const { host, store } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    expect(await host.inspectPackage({ source: "file" })).toBeNull();
    chosen = primary();
    await expect(host.inspectPackage({ source: "file" })).rejects.toThrow(/signed portable/u);
    expect(await store.list()).toEqual([]);
    expect((await host.delivery()).cachedPackages).toEqual([]);
    const unavailable = (await setup()).host;
    expect((await unavailable.delivery()).fileInstallationAvailable).toBe(false);
    await expect(unavailable.inspectPackage({ source: "file" })).rejects.toThrow(/desktop app/u);
  });
  it("shows verified compatibility and downgrade blocks before executing any plugin code", async () => {
    let chosen: Uint8Array | null = null;
    const { host, store, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    chosen = signed("0.1.0", {
      ...manifest,
      compatibility: {
        ...manifest.compatibility!,
        streamskope: { minimum: "0.3.0", maximumExclusive: "0.4.0" },
      },
    });
    const blocked = (await host.inspectPackage({ source: "file" }))!;
    expect(blocked.status).toBe("blocked");
    expect(blocked.reason).toContain("0.3.0");
    await expect(host.installPackage(blocked.candidateId)).rejects.toThrow(/Requires/u);
    expect(await store.list()).toEqual([]);
    await host.discardPackage(blocked.candidateId);
    const newer = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await newer.store.install(primary("0.2.0"), pluginPackageSha256(primary("0.2.0")));
    await newer.host.start();
    chosen = newer.signed();
    const older = (await newer.host.inspectPackage({ source: "file" }))!;
    expect(older.status).toBe("blocked");
    expect(older.reason).toContain("older");
    expect((await newer.host.list()).plugins[0]?.active?.version).toBe("0.2.0");
  });
  it("pins the host-known catalog version and rejects stale development-style acquisition", async () => {
    let downloaded = primary();
    const entry: OfficialPluginEntry = {
      manifest,
      sha256: pluginPackageSha256(downloaded),
      downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/42",
    };
    const downloadPinned = vi.fn(() =>
      Promise.resolve({ bytes: downloaded, sha256: pluginPackageSha256(downloaded) }),
    );
    const download = vi.fn(() => Promise.reject(new Error("Latest discovery must not run")));
    const { host } = await setup({
      catalog: {
        list: (): Promise<OfficialPluginEntry[]> => Promise.resolve([entry]),
        download,
        downloadPinned,
      },
    });
    const catalog = await host.catalog();
    expect(catalog.packages).toEqual([
      { pluginId: manifest.id, version: manifest.version, sha256: entry.sha256 },
    ]);
    const candidate = (await host.inspectPackage({ source: "catalog", ...catalog.packages![0]! }))!;
    expect(candidate.manifest.version).toBe("0.1.0");
    expect(downloadPinned).toHaveBeenCalledWith(
      entry,
      expect.any(AbortSignal),
      expect.any(Function),
    );
    await host.installPackage(candidate.candidateId);
    downloaded = primary("0.2.0");
    await expect(
      host.inspectPackage({ source: "catalog", ...catalog.packages![0]! }),
    ).rejects.toThrow(/changed/u);
    expect((await host.list()).plugins[0]?.active?.version).toBe("0.1.0");
    expect(download).not.toHaveBeenCalled();
  });
  it("binds active capture consent to exactly one candidate and retains a rejected review for renewed consent", async () => {
    let chosen: Uint8Array | null = null;
    const { host, store, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await store.install(primary(), pluginPackageSha256(primary()));
    await start(host);
    const original = (await host.list()).plugins[0]!;
    chosen = signed("0.2.0");
    const first = (await host.inspectPackage({ source: "file" }))!;
    chosen = signed("0.3.0");
    const second = (await host.inspectPackage({ source: "file" }))!;
    const firstConsent = await host.preparePackageChange(first.candidateId);
    await expect(host.installPackage(second.candidateId, firstConsent!.token)).rejects.toThrow(
      /confirmation/u,
    );
    expect((await host.list()).plugins[0]?.activationId).toBe(original.activationId);
    const secondConsent = await host.preparePackageChange(second.candidateId);
    expect(
      (await host.installPackage(second.candidateId, secondConsent!.token)).plugins[0]?.active
        ?.version,
    ).toBe("0.3.0");
    await expect(host.preparePackageChange(first.candidateId)).rejects.toThrow(/older/u);
  });
  it("does not interrupt healthy work for an already-installed signed review or upgrade unsigned provenance", async () => {
    let chosen: Uint8Array | null = null;
    const { host, store, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await store.install(primary(), pluginPackageSha256(primary()));
    await start(host);
    const before = await host.list();
    chosen = signed();
    const candidate = (await host.inspectPackage({ source: "file" }))!;
    expect(candidate.status).toBe("already-installed");
    expect(await host.preparePackageChange(candidate.candidateId)).toBeNull();
    expect((await host.installPackage(candidate.candidateId)).plugins).toEqual(before.plugins);
    expect((await store.getActive(manifest.id))?.publisher).toBeUndefined();
    expect(await host.prepareChange(manifest.id, "remove")).not.toBeNull();
  });
  it("keeps installed management responsive during a chooser and rejects late selection after host close", async () => {
    let resolve!: (value: Uint8Array | null) => void;
    const choosing = new Promise<Uint8Array | null>((done) => {
      resolve = done;
    });
    const { host, store, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => choosing,
    });
    await store.install(primary(), pluginPackageSha256(primary()));
    await host.start();
    const inspection = host.inspectPackage({ source: "file" });
    expect((await host.list()).plugins[0]?.active).toEqual(manifest);
    await host.close();
    resolve(signed());
    await expect(inspection).rejects.toThrow(/cancelled/u);
    expect(await store.packageCache.list()).toEqual([]);
  });
  it("keeps an admitted installation and its reviewed bytes valid while cleanup passes the idle expiry", async () => {
    let chosen: Uint8Array | null = null;
    let releaseCleanup!: () => void;
    let enteredCleanup!: () => void;
    const cleanup = new Promise<void>((resolve) => {
      releaseCleanup = resolve;
    });
    const entered = new Promise<void>((resolve) => {
      enteredCleanup = resolve;
    });
    const backend: PluginBackend = {
      execute: (): Promise<null> => Promise.resolve(null),
      validateProfile: (): Promise<void> => Promise.resolve(),
      beforeExit: (): Promise<undefined> => Promise.resolve(undefined),
      resolveExit: (): Promise<boolean> => Promise.resolve(true),
      beforeChange: (): ReturnType<PluginBackend["beforeChange"]> =>
        Promise.resolve({ message: "Capture active", detail: "Fixture session" }),
      prepareUnload: (): Promise<void> => {
        enteredCleanup();
        return cleanup;
      },
      close: (): Promise<void> => Promise.resolve(),
    };
    const { host, store, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
      loadModule: (): ReturnType<NonNullable<PluginRuntimeOptions["loadModule"]>> =>
        Promise.resolve({ activate: (): PluginBackend => backend }),
    });
    await store.install(primary(), pluginPackageSha256(primary()));
    await host.start();
    vi.useFakeTimers();
    try {
      chosen = signed("0.2.0");
      const candidate = (await host.inspectPackage({ source: "file" }))!;
      vi.advanceTimersByTime(PLUGIN_PACKAGE_REVIEW_TTL_MS - 10_000);
      const consent = await host.preparePackageChange(candidate.candidateId);
      const installing = host.installPackage(candidate.candidateId, consent!.token);
      await entered;
      vi.advanceTimersByTime(20_000);
      await host.discardPackage(candidate.candidateId);
      releaseCleanup();
      expect((await installing).plugins[0]?.active?.version).toBe("0.2.0");
      await expect(host.installPackage(candidate.candidateId)).rejects.toThrow(/expired|closed/u);
    } finally {
      releaseCleanup();
      vi.useRealTimers();
    }
  });
  it("repairs a corrupt cache index and installs a fresh signed file without GitHub", async () => {
    let chosen: Uint8Array | null = null;
    const { root, host, store, signed, network } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await store.packageCache.put(primary(), pluginPackageSha256(primary()), "official");
    await writeFile(join(root, ".packages", "index.json"), "truncated metadata");
    expect(await host.delivery()).toEqual({ fileInstallationAvailable: true, cachedPackages: [] });
    chosen = signed();
    const candidate = (await host.inspectPackage({ source: "file" }))!;
    expect((await host.installPackage(candidate.candidateId)).plugins[0]?.active).toEqual(manifest);
    expect(network.list).not.toHaveBeenCalled();
    expect(network.download).not.toHaveBeenCalled();
  });
  it("returns actionable expired and damaged review failures without exposing local file paths", async () => {
    let chosen: Uint8Array | null = null;
    const { root, host, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    chosen = signed();
    const candidate = (await host.inspectPackage({ source: "file" }))!;
    await rm(join(root, ".packages", `${candidate.sha256}.skope-plugin`));
    for (const operation of [
      (): Promise<unknown> => host.preparePackageChange(candidate.candidateId),
      (): Promise<unknown> => host.installPackage(candidate.candidateId),
    ]) {
      try {
        await operation();
        throw new Error("Expected unavailable reviewed bytes");
      } catch (error) {
        expect(error).toMatchObject({
          code: "BACKEND_UNAVAILABLE",
          recovery: expect.stringContaining("review") as unknown,
        });
        expect((error as Error).message).not.toContain(root);
      }
    }
    await host.discardPackage(candidate.candidateId);
    await expect(host.preparePackageChange(candidate.candidateId)).rejects.toMatchObject({
      code: "BACKEND_UNAVAILABLE",
      message: expect.stringMatching(/expired|closed/u) as unknown,
    });
    const brokenChooser = (
      await setup({
        choosePackageFile: (): Promise<never> =>
          Promise.reject(
            Object.assign(new Error(`ENOENT: ${root}/private-file`), { code: "ENOENT" }),
          ),
      })
    ).host;
    await expect(brokenChooser.inspectPackage({ source: "file" })).rejects.toMatchObject({
      code: "BACKEND_UNAVAILABLE",
      message: "The selected plugin file could not be read.",
    });
    const forgedChooser = (
      await setup({
        choosePackageFile: (): Promise<never> =>
          Promise.reject(
            Object.assign(new Error(`private-password ${root}/private-file`), {
              code: "BACKEND_UNAVAILABLE",
              recovery: "private-password",
            }),
          ),
      })
    ).host;
    await expect(forgedChooser.inspectPackage({ source: "file" })).rejects.toMatchObject({
      message: "The selected plugin file could not be read.",
      recovery: "Choose a readable signed portable package and retry.",
    });
  });
  it("offers repair when a healthy running plugin has lost its retained archive", async () => {
    let chosen: Uint8Array | null = null;
    const { host, store, signed } = await setup({
      choosePackageFile: (): Promise<Uint8Array | null> => Promise.resolve(chosen),
    });
    await store.install(primary(), pluginPackageSha256(primary()));
    await host.start();
    const original = (await store.getActive(manifest.id))!;
    await writeFile(join(original.directory, "package.skope-plugin"), "damaged retained archive");
    chosen = signed();
    const candidate = (await host.inspectPackage({ source: "file" }))!;
    expect(candidate.status).toBe("update");
    expect((await host.installPackage(candidate.candidateId)).plugins[0]?.active).toEqual(manifest);
    expect((await store.getActive(manifest.id))?.sha256).toBe(candidate.sha256);
  });
});
