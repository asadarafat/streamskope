import { mkdtemp, readFile, readdir, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import type { PluginManifest } from "../../src/plugins/contracts";
import {
  encodePluginPackage,
  pluginPackageSha256,
  signPortablePluginPackage,
} from "../../src/platform/node/plugins/package";
import { PluginStore, type PluginStoreOptions } from "../../src/platform/node/plugins/store";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

vi.mock("node:fs/promises", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:fs/promises")>();
  return { ...original, rename: vi.fn(original.rename), rm: vi.fn(original.rm) };
});

const roots: string[] = [];
const manifest: PluginManifest = {
  id: "streamskope.eda",
  name: "EDA Capture",
  version: "26.8.2",
  targetEdaVersion: "26.8.2",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};

afterEach(async () => {
  await Promise.all(roots.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

async function setup(
  options: PluginStoreOptions = {},
): Promise<{ root: string; store: PluginStore }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-plugins-"));
  roots.push(root);
  return { root, store: new PluginStore(root, options) };
}

function bundle(version = "26.8.2", id = manifest.id): Uint8Array {
  return encodePluginPackage(
    { ...manifest, id, version },
    new Map([
      ["backend.cjs", Buffer.from(`exports.version = '${version}';`)],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
}

async function install(store: PluginStore, bytes = bundle()): Promise<void> {
  await store.install(bytes, pluginPackageSha256(bytes));
}

describe("plugin installation storage", () => {
  it.each(["unsigned", "signed"] as const)(
    "retains %s archive authority and publisher provenance across equivalent portable delivery",
    async (first) => {
      const fixture = pluginPublisherFixture();
      const { root, store } = await setup({ trustedPublishers: fixture.publishers });
      const primary = bundle();
      const portable = signPortablePluginPackage(
        primary,
        fixture.publishers[0]!.keyId,
        Buffer.from(fixture.encodedKey, "base64").toString(),
      );
      const original = first === "unsigned" ? primary : portable;
      const other = first === "unsigned" ? portable : primary;
      await install(store, original);
      const [active] = await store.activatePending();
      const before = await readFile(join(root, "state.json"), "utf8");
      const prepared = await store.prepareInstall(other, pluginPackageSha256(other));
      expect(prepared.sha256).toBe(pluginPackageSha256(original));
      expect(prepared.contentSha256).toBe(pluginPackageSha256(primary));
      expect(prepared.publisher).toEqual(
        first === "unsigned"
          ? undefined
          : { keyId: fixture.publishers[0]!.keyId, name: fixture.publishers[0]!.name },
      );
      expect((await store.install(other, pluginPackageSha256(other))).pending).toBeNull();
      expect(await readFile(join(root, "state.json"), "utf8")).toBe(before);
      expect(await readdir(join(root, manifest.id))).toEqual([active!.sha256]);
      const reopened = new PluginStore(root, { trustedPublishers: fixture.publishers });
      expect((await reopened.activatePending())[0]?.sha256).toBe(active!.sha256);
      expect((await reopened.getActive(manifest.id))?.publisher).toEqual(prepared.publisher);
    },
  );

  it("re-verifies retained signatures even when an altered envelope has a matching archive digest and state pointer", async () => {
    const fixture = pluginPublisherFixture();
    const { root, store } = await setup({ trustedPublishers: fixture.publishers });
    const portable = signPortablePluginPackage(
      bundle(),
      fixture.publishers[0]!.keyId,
      Buffer.from(fixture.encodedKey, "base64").toString(),
    );
    await install(store, portable);
    const [active] = await store.activatePending();
    const outer = JSON.parse(Buffer.from(portable).toString()) as Record<string, unknown>;
    const altered = Buffer.from(
      JSON.stringify({ ...outer, signature: Buffer.alloc(64).toString("base64") }),
    );
    const changedDigest = pluginPackageSha256(altered);
    await rename(active!.directory, join(root, manifest.id, changedDigest));
    await writeFile(join(root, manifest.id, changedDigest, "package.skope-plugin"), altered);
    await writeFile(
      join(root, "state.json"),
      JSON.stringify({ formatVersion: 1, plugins: { [manifest.id]: { active: changedDigest } } }),
    );
    await expect(
      new PluginStore(root, { trustedPublishers: fixture.publishers }).getActive(manifest.id),
    ).rejects.toThrow(/signature/u);
  });

  it("does not stage untrusted portable code and protects an API4 version against changed exact primary bytes", async () => {
    const fixture = pluginPublisherFixture();
    const { root, store } = await setup({ trustedPublishers: fixture.publishers });
    const modern: PluginManifest = {
      id: manifest.id,
      name: manifest.name,
      backend: "backend.cjs",
      renderer: "renderer.js",
      apiVersion: 4,
      version: "0.1.0",
      compatibility: {
        streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
        target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
      },
    };
    const primary = encodePluginPackage(
      modern,
      new Map([
        ["backend.cjs", Buffer.from("original code")],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    const key = Buffer.from(fixture.encodedKey, "base64").toString();
    const portable = signPortablePluginPackage(primary, fixture.publishers[0]!.keyId, key);
    await expect(
      new PluginStore(root).install(portable, pluginPackageSha256(portable)),
    ).rejects.toThrow(/not trusted/u);
    expect(await readdir(root)).toEqual([]);
    await install(store, primary);
    await store.activatePending();
    const differentSerialization = Buffer.from(
      JSON.stringify(JSON.parse(Buffer.from(primary).toString()), null, 2),
    );
    const changed = signPortablePluginPackage(
      differentSerialization,
      fixture.publishers[0]!.keyId,
      key,
    );
    await expect(store.prepareInstall(changed, pluginPackageSha256(changed))).rejects.toThrow(
      /different content/u,
    );
    expect((await store.getActive(manifest.id))?.sha256).toBe(pluginPackageSha256(primary));
  });

  it("protects inactive retained API 4 versions from changed content, including when extracted code is damaged", async () => {
    const { store, root } = await setup();
    const descriptor: PluginManifest = {
      id: manifest.id,
      name: manifest.name,
      version: "0.1.0",
      apiVersion: 4,
      backend: "backend.cjs",
      renderer: "renderer.js",
      compatibility: {
        streamskope: { minimum: "0.2.0", maximumExclusive: "0.3.0" },
        target: { system: "eda", minimum: "26.8.2", maximum: "26.8.2" },
      },
    };
    const packaged = (version: string, code: string): Uint8Array =>
      encodePluginPackage(
        { ...descriptor, version },
        new Map([
          ["backend.cjs", Buffer.from(code)],
          ["renderer.js", Buffer.from("export default {};")],
        ]),
      );
    const original = packaged("0.1.0", "original code");
    await install(store, original);
    const [active] = await store.activatePending();
    await writeFile(active!.backendPath, "corrupt extracted code");
    const changed = packaged("0.1.0", "changed published code");
    await expect(store.prepareInstall(changed, pluginPackageSha256(changed))).rejects.toThrow(
      "different content",
    );
    await expect(install(store, changed)).rejects.toThrow("different content");
    await expect(
      store.prepareInstall(original, pluginPackageSha256(original)),
    ).resolves.toMatchObject({ manifest: descriptor });
    expect(await readFile(active!.backendPath, "utf8")).toBe("original code");
    // A corrupted retained envelope cannot prevent installing a verified newer version.
    await writeFile(
      join(root, manifest.id, pluginPackageSha256(original), "package.skope-plugin"),
      "corrupt package envelope",
    );
    const update = packaged("0.1.1", "new code");
    const prepared = await store.prepareInstall(update, pluginPackageSha256(update));
    await store.commitInstall(manifest.id, prepared.sha256);
    expect((await store.getActive(manifest.id))?.manifest.version).toBe("0.1.1");
    const restored = (await store.list())[0]!;
    expect(restored.installed?.version).toBe("0.1.1");
    expect(restored.error).toBeUndefined();
    expect(restored.previous).toBeUndefined();
    await store.prepareInstall(update, pluginPackageSha256(update));
    await store.commitInstall(manifest.id, prepared.sha256);
    expect((await store.list())[0]).toEqual(restored);
  });

  it("repairs corrupt extracted files by explicitly reinstalling the same verified package", async () => {
    const { store, root } = await setup();
    await install(store);
    const [active] = await store.activatePending();
    await writeFile(active!.rendererPath, "corrupt renderer");
    expect(await new PluginStore(root).activatePending()).toEqual([]);
    await install(store);
    expect(await new PluginStore(root).activatePending()).toMatchObject([{ manifest }]);
    expect(await readFile(active!.rendererPath, "utf8")).toBe("export default {};");
    expect((await readdir(join(root, manifest.id))).filter((name) => name.startsWith("."))).toEqual(
      [],
    );
  });

  it("restores existing files and retains the working version when repair publication fails", async () => {
    const { store, root } = await setup();
    await install(store);
    await store.activatePending();
    const update = bundle("26.8.3");
    await install(store, update);
    const damagedDirectory = join(root, manifest.id, pluginPackageSha256(update));
    const damagedRenderer = join(damagedDirectory, "renderer.js");
    await writeFile(damagedRenderer, "corrupt pending renderer");
    const state = await readFile(join(root, "state.json"));
    const original = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
    vi.mocked(rename).mockImplementation(async (from, to) => {
      if (String(from).includes(".install-") && String(to) === damagedDirectory)
        throw new Error("Injected publication failure");
      await original.rename(from, to);
    });
    await expect(install(store, update)).rejects.toThrow(/publication failure/u);
    expect(await readFile(join(root, "state.json"))).toEqual(state);
    expect(await readFile(damagedRenderer, "utf8")).toBe("corrupt pending renderer");
    expect((await store.getActive(manifest.id))?.manifest.version).toBe("26.8.2");
    expect((await readdir(join(root, manifest.id))).filter((name) => name.startsWith("."))).toEqual(
      [],
    );
  });

  it("refuses to repair a package directory replaced by a symlink", async () => {
    const { store, root } = await setup();
    await install(store);
    const destination = join(root, manifest.id, pluginPackageSha256(bundle()));
    const outside = await mkdtemp(join(tmpdir(), "streamskope-outside-plugin-"));
    roots.push(outside);
    await writeFile(join(outside, "sentinel"), "preserve");
    await rm(destination, { recursive: true });
    await symlink(outside, destination, "dir");
    await expect(install(store)).rejects.toThrow(/regular directories/u);
    expect(await readFile(join(outside, "sentinel"), "utf8")).toBe("preserve");
  });

  it("rejects an installation above capacity without corrupting the existing state", async () => {
    const { store, root } = await setup();
    const state = JSON.stringify({
      formatVersion: 1,
      plugins: Object.fromEntries(
        Array.from({ length: 32 }, (_, index) => [`example.plugin${index}`, {}]),
      ),
    });
    await writeFile(join(root, "state.json"), state);
    await expect(install(store)).rejects.toThrow(/32 plugin installations/u);
    expect(await readFile(join(root, "state.json"), "utf8")).toBe(state);
    expect(await store.list()).toHaveLength(32);
    expect(await store.activatePending()).toEqual([]);
  });

  it("keeps a clean desktop empty and stages installation and removal until restart", async () => {
    const { store, root } = await setup();
    expect(await store.activatePending()).toEqual([]);
    await install(store);
    expect(await store.getActive(manifest.id)).toBeUndefined();
    expect(await store.list()).toMatchObject([
      { installed: manifest, pending: "install", restartRequired: true },
    ]);
    const restarted = new PluginStore(root);
    expect(await restarted.activatePending()).toMatchObject([{ manifest }]);
    await restarted.remove(manifest.id);
    expect(await restarted.getActive(manifest.id)).toBeDefined();
    expect(await restarted.list()).toMatchObject([{ active: manifest, pending: "remove" }]);
    expect(await new PluginStore(root).activatePending()).toEqual([]);
  });

  it("leaves the working version intact after failed verification and rolls back failed activation", async () => {
    const { store } = await setup();
    await install(store);
    await store.activatePending();
    const update = bundle("26.8.3");
    await expect(store.install(update, "0".repeat(64))).rejects.toThrow("SHA256");
    expect((await store.getActive(manifest.id))?.manifest.version).toBe("26.8.2");
    await install(store, update);
    expect((await store.getActive(manifest.id))?.manifest.version).toBe("26.8.2");
    expect(await store.activatePending()).toMatchObject([{ manifest: { version: "26.8.3" } }]);
    expect(await store.rollback(manifest.id)).toMatchObject({ manifest: { version: "26.8.2" } });
    expect(await store.list()).toMatchObject([
      {
        active: { version: "26.8.2" },
        error: expect.stringContaining("failed to start") as unknown,
      },
    ]);
  });

  it("rechecks downloaded and extracted bytes on restart, isolates corruption and restores a previous package", async () => {
    const { store, root } = await setup();
    await install(store);
    await store.activatePending();
    const update = bundle("26.8.3");
    await install(store, update);
    const digest = pluginPackageSha256(update);
    await writeFile(join(root, manifest.id, digest, "backend.cjs"), "tampered");
    expect(await store.activatePending()).toMatchObject([{ manifest: { version: "26.8.2" } }]);
    expect(await store.list()).toMatchObject([
      { error: expect.stringContaining("verified package") as unknown },
    ]);
    const active = await store.getActive(manifest.id);
    await writeFile(join(active!.directory, "package.skope-plugin"), "tampered");
    expect(await new PluginStore(root).activatePending()).toEqual([]);
    expect(await store.list()).toMatchObject([
      { error: expect.stringContaining("SHA256") as unknown },
    ]);
  });

  it("starts healthy plugins when another installed plugin is corrupt", async () => {
    const { store, root } = await setup();
    await install(store);
    const second = bundle("1.0.0", "streamskope.second");
    await install(store, second);
    await writeFile(join(root, manifest.id, pluginPackageSha256(bundle()), "renderer.js"), "wrong");
    expect(await store.activatePending()).toMatchObject([
      { manifest: { id: "streamskope.second" } },
    ]);
  });

  it("rejects package files and storage directories replaced by symlinks", async () => {
    const { store, root } = await setup();
    await install(store);
    const target = join(root, "outside.cjs");
    await writeFile(target, "outside");
    const backend = join(root, manifest.id, pluginPackageSha256(bundle()), "backend.cjs");
    await rm(backend);
    await symlink(target, backend);
    expect(await store.activatePending()).toEqual([]);
    expect(await readFile(target, "utf8")).toBe("outside");
    await expect(store.remove("../outside")).rejects.toThrow("identifier");
  });

  it("rejects traversal in persisted state before reading any package path", async () => {
    const { store, root } = await setup();
    await writeFile(
      join(root, "state.json"),
      JSON.stringify({ formatVersion: 1, plugins: { "../escape": { active: "a".repeat(64) } } }),
    );
    await expect(store.activatePending()).rejects.toThrow("identifier");
  });
});

describe("hot installation transactions", () => {
  it("never activates an uncommitted candidate, including after a restart", async () => {
    const { store, root } = await setup();
    await install(store);
    await store.activatePending();
    const bytes = bundle("26.8.3");
    const candidate = await store.prepareInstall(bytes, pluginPackageSha256(bytes));
    expect(candidate.manifest.version).toBe("26.8.3");
    expect(await new PluginStore(root).activatePending()).toMatchObject([{ manifest }]);
    expect((await store.list())[0]).toMatchObject({
      pending: null,
      restartRequired: false,
      installed: manifest,
    });
    await store.discardInstall(manifest.id, candidate.sha256);
    expect(await readdir(join(root, manifest.id))).not.toContain(candidate.sha256);
  });

  it("commits one plugin without applying another pending installation", async () => {
    const { store } = await setup();
    await install(store);
    await store.activatePending();
    const second = bundle("1.0.0", "example.other");
    await install(store, second);
    const update = bundle("26.8.3");
    const candidate = await store.prepareInstall(update, pluginPackageSha256(update));
    await store.commitInstall(manifest.id, candidate.sha256);
    expect((await store.getActive(manifest.id))?.manifest.version).toBe("26.8.3");
    expect(await store.getActive("example.other")).toBeUndefined();
    expect((await store.list()).find((entry) => entry.id === "example.other")?.pending).toBe(
      "install",
    );
  });

  it("restores all stored versions if publishing removal state fails", async () => {
    const { store, root } = await setup();
    await install(store);
    await store.activatePending();
    const state = await readFile(join(root, "state.json"));
    const original = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
    vi.mocked(rename).mockImplementation(async (from, to) => {
      if (String(to) === join(root, "state.json")) throw new Error("Removal state write failed");
      await original.rename(from, to);
    });
    await expect(store.uninstall(manifest.id)).rejects.toThrow("Removal state write failed");
    expect(await readFile(join(root, "state.json"))).toEqual(state);
    expect((await store.getActive(manifest.id))?.manifest).toEqual(manifest);
    expect((await readdir(root)).filter((name) => name.startsWith(".remove-"))).toEqual([]);
  });

  it("deletes every package and installation record so repeated removal and reinstall remain safe", async () => {
    const { store, root } = await setup();
    await install(store);
    await store.activatePending();
    const update = bundle("26.8.3");
    const candidate = await store.prepareInstall(update, pluginPackageSha256(update));
    await store.commitInstall(manifest.id, candidate.sha256);
    await store.uninstall(manifest.id);
    expect(await store.list()).toEqual([]);
    expect(await readdir(root)).toEqual(["state.json"]);
    await expect(store.uninstall(manifest.id)).resolves.toBeUndefined();
    await install(store);
    expect(await store.activatePending()).toMatchObject([{ manifest }]);
  });

  it("keeps a failed byte deletion inactive and retries it on the next removal", async () => {
    const { store, root } = await setup();
    await install(store);
    await store.activatePending();
    const original = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
    vi.mocked(rm).mockImplementationOnce(() =>
      Promise.reject(new Error("Package files are locked")),
    );
    await expect(store.uninstall(manifest.id)).resolves.toContain("retained package files");
    expect(await store.list()).toMatchObject([
      { id: manifest.id, error: expect.stringContaining("retained package files") as unknown },
    ]);
    expect(await store.getActive(manifest.id)).toBeUndefined();
    expect((await readdir(root)).some((name) => name.startsWith(".remove-"))).toBe(true);
    vi.mocked(rm).mockImplementation(original.rm);
    await expect(store.uninstall(manifest.id)).resolves.toBeUndefined();
    expect(await readdir(root)).toEqual(["state.json"]);
  });

  it("retains only the active and immediately previous versions across repeated updates", async () => {
    const { store, root } = await setup();
    for (const version of ["26.8.2", "26.8.3", "26.8.4"]) {
      const bytes = bundle(version);
      const candidate = await store.prepareInstall(bytes, pluginPackageSha256(bytes));
      await store.commitInstall(manifest.id, candidate.sha256);
    }
    expect(await readdir(join(root, manifest.id))).toHaveLength(2);
    expect((await store.list())[0]).toMatchObject({
      active: { version: "26.8.4" },
      previous: { version: "26.8.3" },
    });
    expect((await store.rollback(manifest.id))?.manifest.version).toBe("26.8.3");
  });
});
