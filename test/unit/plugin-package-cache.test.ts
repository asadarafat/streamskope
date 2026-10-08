import { lstat, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import {
  encodePluginPackage,
  pluginPackageSha256,
  signPortablePluginPackage,
} from "../../src/platform/node/plugins/package";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

const roots: string[] = [];
const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "Capture",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
function bytes(version = "1.0.0"): Uint8Array {
  return encodePluginPackage(
    { ...manifest, version },
    new Map([
      ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
}
function reference(version = "1.0.0"): { pluginId: string; version: string; sha256: string } {
  return { pluginId: manifest.id, version, sha256: pluginPackageSha256(bytes(version)) };
}
async function setup(): Promise<{ root: string; store: PluginStore }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-package-cache-"));
  roots.push(root);
  return { root, store: new PluginStore(root) };
}
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("verified plugin delivery cache", () => {
  it("inspects retained unindexed bytes without repairing provenance or touching metadata", async () => {
    const { root, store } = await setup();
    await store.packageCache.put(bytes(), reference().sha256, "official");
    const index = join(root, ".packages", "index.json");
    const original = await readFile(index);
    expect((await store.packageCache.inspect()).indexed[0]?.trust).toBe("official");
    expect(await readFile(index)).toEqual(original);
    await rm(index);
    expect(await store.packageCache.inspect()).toEqual({
      indexed: [],
      retainedDigests: [reference().sha256],
    });
    expect(await readdir(join(root, ".packages"))).toEqual([`${reference().sha256}.skope-plugin`]);
  });
  it("refuses damaged cache metadata without invoking the runtime repair path", async () => {
    const { root, store } = await setup();
    await store.packageCache.put(bytes(), reference().sha256, "official");
    const index = join(root, ".packages", "index.json");
    const corrupt = '{"formatVersion":99,"packages":[]}';
    await writeFile(index, corrupt);
    await expect(store.packageCache.inspect()).rejects.toThrow();
    expect(await readFile(index, "utf8")).toBe(corrupt);
    expect(await readdir(join(root, ".packages"))).toHaveLength(2);
  });
  it("persists bounded private original release copies independently of installed packages", async () => {
    const { root, store } = await setup();
    expect(await store.packageCache.list()).toEqual([]);
    await store.packageCache.put(bytes(), reference().sha256, "official");
    const reopened = new PluginStore(root);
    const cached = await reopened.packageCache.read(reference());
    expect(cached.bytes).toEqual(bytes());
    expect(cached.record.trust).toBe("official");
    expect(cached.record.publisher).toBeUndefined();
    expect(await reopened.list()).toEqual([]);
    expect((await lstat(join(root, ".packages"))).mode & 0o777).toBe(0o700);
    expect(
      (await lstat(join(root, ".packages", `${reference().sha256}.skope-plugin`))).mode & 0o777,
    ).toBe(0o600);
  });
  it("evicts an old unpinned copy while retaining exact reviewed bytes", async () => {
    const { root, store } = await setup();
    const release = store.packageCache.pin(reference().sha256);
    for (let version = 1; version <= 5; version += 1) {
      const item = bytes(`${version}.0.0`);
      await store.packageCache.put(item, pluginPackageSha256(item), "official");
    }
    expect((await store.packageCache.list()).map((entry) => entry.manifest.version)).toEqual([
      "1.0.0",
      "3.0.0",
      "4.0.0",
      "5.0.0",
    ]);
    expect((await store.packageCache.read(reference())).bytes).toEqual(bytes());
    expect(
      (await readdir(join(root, ".packages"))).filter((name) => name.endsWith(".skope-plugin")),
    ).toHaveLength(4);
    release();
    release();
  });
  it("rejects a full pinned cache and does not relabel development bytes as an official source", async () => {
    const { store } = await setup();
    const releases: (() => void)[] = [];
    for (let version = 1; version <= 4; version += 1) {
      const item = bytes(`${version}.0.0`);
      const digest = pluginPackageSha256(item);
      releases.push(store.packageCache.pin(digest));
      await store.packageCache.put(item, digest, "development");
    }
    expect(await store.packageCache.list()).toEqual([]);
    expect(await store.packageCache.list(true)).toHaveLength(4);
    await expect(store.packageCache.read(reference())).rejects.toThrow(/Development/u);
    const extra = bytes("5.0.0");
    await expect(
      store.packageCache.put(extra, pluginPackageSha256(extra), "official"),
    ).rejects.toThrow(/full/u);
    releases.forEach((release) => release());
  });
  it("rechecks signed publisher provenance and refuses damaged or symlinked archive copies", async () => {
    const { root } = await setup();
    const fixture = pluginPublisherFixture();
    const store = new PluginStore(root, { trustedPublishers: fixture.publishers });
    const portable = signPortablePluginPackage(
      bytes(),
      fixture.publishers[0]!.keyId,
      Buffer.from(fixture.encodedKey, "base64").toString(),
    );
    const digest = pluginPackageSha256(portable);
    await store.packageCache.put(portable, digest, "publisher");
    const ref = { pluginId: manifest.id, version: manifest.version, sha256: digest };
    expect((await store.packageCache.read(ref)).record.publisher?.keyId).toBe(
      fixture.publishers[0]!.keyId,
    );
    const path = join(root, ".packages", `${digest}.skope-plugin`);
    await writeFile(path, "damaged archive");
    expect(await store.packageCache.list()).toEqual([]);
    await expect(store.packageCache.read(ref)).rejects.toThrow(/SHA256/u);
    await rm(path);
    const target = join(root, "outside");
    await writeFile(target, "untouched");
    await symlink(target, path);
    await expect(store.packageCache.put(portable, digest, "publisher")).rejects.toThrow(/regular/u);
    expect(await readFile(target, "utf8")).toBe("untouched");
  });
  it("resets malformed metadata without promoting unindexed bytes and accepts a new verified package", async () => {
    const { root, store } = await setup();
    await store.packageCache.put(bytes(), reference().sha256, "official");
    await writeFile(join(root, ".packages", "index.json"), '{"formatVersion":1,"packages":[');
    expect(await store.packageCache.list()).toEqual([]);
    await expect(store.packageCache.read(reference())).rejects.toThrow(/not cached/u);
    const replacement = bytes("2.0.0");
    await store.packageCache.put(replacement, pluginPackageSha256(replacement), "official");
    expect((await store.packageCache.list()).map((value) => value.manifest.version)).toEqual([
      "2.0.0",
    ]);
    expect(
      (await readdir(join(root, ".packages"))).filter((name) => name.endsWith(".skope-plugin")),
    ).toHaveLength(1);
  });
  it("refuses a symlinked metadata index without changing its target", async () => {
    const { root, store } = await setup();
    await store.packageCache.put(bytes(), reference().sha256, "official");
    const index = join(root, ".packages", "index.json");
    const target = join(root, "outside-index");
    await writeFile(target, "untouched");
    await rm(index);
    await symlink(target, index);
    await expect(store.packageCache.list()).rejects.toThrow(/regular/u);
    await expect(
      store.packageCache.put(bytes("2.0.0"), reference("2.0.0").sha256, "official"),
    ).rejects.toThrow(/regular/u);
    expect(await readFile(target, "utf8")).toBe("untouched");
  });
});
