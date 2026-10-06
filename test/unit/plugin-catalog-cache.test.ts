import { lstat, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import {
  PluginCatalogCache,
  type StoredPluginCatalog,
} from "../../src/platform/node/plugins/catalog-cache";

const directories: string[] = [];
const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "EDA Connector",
  version: "0.1.0",
  apiVersion: 4,
  compatibility: {
    streamskope: { minimum: "0.2.0", maximumExclusive: "1.0.0" },
    target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
  },
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const saved: StoredPluginCatalog = {
  checkedAt: "2026-10-06T12:00:00.000Z",
  entries: [
    {
      manifest,
      sha256: "a".repeat(64),
      downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/42",
    },
  ],
};

async function setup(): Promise<{ root: string; cache: PluginCatalogCache }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-plugin-catalog-"));
  directories.push(root);
  return { root, cache: new PluginCatalogCache(root) };
}

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});

describe("last successful official plugin catalog", () => {
  it("persists exact checked metadata with private file and directory permissions", async () => {
    const { root, cache } = await setup();
    expect(await cache.read()).toBeUndefined();
    await cache.save(saved);
    expect(await new PluginCatalogCache(root).read()).toEqual(saved);
    expect((await lstat(root)).mode & 0o777).toBe(0o700);
    expect((await lstat(join(root, "catalog.json"))).mode & 0o777).toBe(0o600);
    await cache.save({ checkedAt: "2026-10-06T13:00:00.000Z", entries: [] });
    expect(await cache.read()).toEqual({ checkedAt: "2026-10-06T13:00:00.000Z", entries: [] });
  });

  it.each([
    { label: "invalid digest", entries: [{ ...saved.entries[0]!, sha256: "wrong" }] },
    {
      label: "foreign source",
      entries: [{ ...saved.entries[0]!, downloadUrl: "https://example.com/plugin" }],
    },
    {
      label: "unknown plugin",
      entries: [{ ...saved.entries[0]!, manifest: { ...manifest, id: "unknown.capture" } }],
    },
    { label: "duplicate plugin", entries: [...saved.entries, ...saved.entries] },
    {
      label: "unexpected field",
      entries: [{ ...saved.entries[0]!, privateToken: "must-not-save" }],
    },
  ])("rejects $label without replacing useful metadata", async ({ entries }) => {
    const { root, cache } = await setup();
    await cache.save(saved);
    await expect(cache.save({ ...saved, entries })).rejects.toThrow(/saved plugin catalog/u);
    expect(await cache.read()).toEqual(saved);
    expect(await readFile(join(root, "catalog.json"), "utf8")).not.toContain("must-not-save");
  });

  it("rejects noncanonical timestamps, corrupt contents and oversized disk metadata", async () => {
    const { root, cache } = await setup();
    await expect(cache.save({ ...saved, checkedAt: "2026-10-06" })).rejects.toThrow(
      /saved plugin catalog/u,
    );
    await writeFile(join(root, "catalog.json"), "not JSON");
    await expect(cache.read()).rejects.toThrow();
    await writeFile(join(root, "catalog.json"), "x".repeat(128 * 1024 + 1));
    await expect(cache.read()).rejects.toThrow(/bound/u);
  });

  it("refuses symlinked catalog files and roots without altering their targets", async () => {
    const { root, cache } = await setup();
    const target = join(root, "target.json");
    await writeFile(target, "unchanged");
    await symlink(target, join(root, "catalog.json"));
    await expect(cache.read()).rejects.toThrow();
    await expect(cache.save(saved)).rejects.toThrow(/regular file/u);
    expect(await readFile(target, "utf8")).toBe("unchanged");
    const link = join(root, "linked-root");
    await symlink(root, link);
    await expect(new PluginCatalogCache(link).read()).rejects.toThrow(/regular directory/u);
    await expect(new PluginCatalogCache(link).save(saved)).rejects.toThrow(/regular directory/u);
  });
});
