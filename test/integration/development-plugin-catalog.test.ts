import { mkdtemp, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { DevelopmentPluginCatalog } from "../../tools/dev/plugin-catalog";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";

const directories: string[] = [];
const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "EDA Capture",
  version: "0.0.0-dev.1790928000000",
  apiVersion: 4,
  backend: "backend.cjs",
  renderer: "renderer.js",
  compatibility: {
    streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
    target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
  },
};

afterEach(async () => {
  vi.unstubAllGlobals();
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function directory(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-development-catalog-"));
  directories.push(root);
  return root;
}

async function writePackage(
  root: string,
  descriptor: PluginManifest = manifest,
): Promise<{ path: string; bytes: Uint8Array }> {
  const bytes = encodePluginPackage(
    descriptor,
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
  const path = join(
    root,
    officialPluginAssets(OFFICIAL_PLUGINS[0], descriptor.version).packageAsset,
  );
  await writeFile(path, bytes);
  return { path, bytes };
}

it("allows startup without build output and explains how to build a missing plugin", async () => {
  const catalog = new DevelopmentPluginCatalog(join(await directory(), "missing"));
  await expect(catalog.list()).resolves.toEqual([]);
  await expect(catalog.download(manifest.id)).rejects.toThrow("npm run package -- plugin");
});

it("keeps local development discovery separate from the persisted official catalog", async () => {
  const root = await directory();
  await writePackage(root);
  const store = new PluginStore(await directory());
  const runtime = new PluginRuntime({
    store,
    catalog: new DevelopmentPluginCatalog(root),
    persistCatalog: false,
  });
  try {
    const refreshed = await runtime.catalog();
    expect(refreshed).toMatchObject({ source: "live", plugins: [manifest] });
    expect(refreshed.error).toBeUndefined();
    expect(await store.catalogCache.read()).toBeUndefined();
    expect(await runtime.catalog(false)).toEqual({ ...refreshed, source: "cache" });
  } finally {
    await runtime.close();
  }
});

it("refreshes verified package bytes and hashes from local output without network access", async () => {
  const root = await directory();
  const network = vi.fn<typeof fetch>(() => Promise.reject(new Error("Unexpected network")));
  vi.stubGlobal("fetch", network);
  const first = await writePackage(root);
  const catalog = new DevelopmentPluginCatalog(root);
  const [original] = await catalog.list();
  expect(original?.manifest).toEqual(manifest);
  expect(original?.sha256).toBe(pluginPackageSha256(first.bytes));
  expect((await catalog.download(manifest.id)).bytes).toEqual(Buffer.from(first.bytes));
  await rm(first.path);
  const next = { ...manifest, version: "0.0.0-dev.1790928000001" };
  const second = await writePackage(root, next);
  const [refreshed] = await catalog.list();
  expect(refreshed?.manifest).toEqual(next);
  expect(refreshed?.sha256).not.toBe(original?.sha256);
  expect(await catalog.download(manifest.id)).toEqual({
    bytes: Buffer.from(second.bytes),
    sha256: pluginPackageSha256(second.bytes),
  });
  expect(network).not.toHaveBeenCalled();
});

it.each([
  { ...manifest, id: "unofficial.capture" },
  { ...manifest, version: "0.1.0" },
  { ...manifest, version: "0.0.0-dev.local" },
  {
    id: manifest.id,
    name: manifest.name,
    version: manifest.version,
    apiVersion: 2 as const,
    backend: manifest.backend,
    renderer: manifest.renderer,
  },
  {
    ...manifest,
    compatibility: {
      ...manifest.compatibility!,
      target: { system: "nsp", minimum: "26.4.0", maximum: "26.4.0" },
    },
  },
])("refuses a local package outside the development contract: %j", async (descriptor) => {
  const root = await directory();
  await writePackage(root, descriptor);
  const catalog = new DevelopmentPluginCatalog(root);
  await expect(catalog.list()).rejects.toThrow("official identity");
  await expect(catalog.download(descriptor.id)).rejects.toThrow("official identity");
});

it("fails closed for duplicate identities instead of guessing which version to install", async () => {
  const root = await directory();
  await writePackage(root);
  await writePackage(root, { ...manifest, version: "0.0.0-dev.1790928000001" });
  const catalog = new DevelopmentPluginCatalog(root);
  await expect(catalog.list()).rejects.toThrow("Multiple local packages");
  await expect(catalog.download(manifest.id)).rejects.toThrow("Multiple local packages");
});

it("rejects malformed packages, mismatched filenames and symlinked package files", async () => {
  const root = await directory();
  const { path } = await writePackage(root);
  const catalog = new DevelopmentPluginCatalog(root);
  await writeFile(path, "not a plugin package");
  await expect(catalog.list()).rejects.toThrow();
  await writePackage(root);
  const renamed = join(root, "unrelated.skope-plugin");
  await rename(path, renamed);
  await expect(catalog.list()).rejects.toThrow("filename does not match");
  await rm(renamed);
  const external = await writePackage(await directory());
  await symlink(external.path, path);
  await expect(catalog.list()).rejects.toThrow("regular file");
});

it("checks declared workflow resource hashes before offering a local package", async () => {
  const root = await directory();
  const resource = Buffer.from("verified workflow");
  const bytes = encodePluginPackage(
    { ...manifest, resources: [{ path: "fixture.yaml", sha256: pluginPackageSha256(resource) }] },
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
      ["fixture.yaml", resource],
    ]),
  );
  const path = join(root, officialPluginAssets(OFFICIAL_PLUGINS[0], manifest.version).packageAsset);
  await writeFile(
    path,
    Buffer.from(bytes)
      .toString("utf8")
      .replace(resource.toString("base64"), Buffer.from("altered workflow").toString("base64")),
  );
  await expect(new DevelopmentPluginCatalog(root).list()).rejects.toThrow("resource SHA256");
});
