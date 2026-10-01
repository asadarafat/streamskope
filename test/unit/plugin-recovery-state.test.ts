import { mkdtemp, readFile, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it } from "vitest";

import { PluginStore } from "../../src/platform/node/plugins/store";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
async function fixture(): Promise<{ root: string; store: PluginStore }> {
  const root = await mkdtemp(join(tmpdir(), "plugin-recovery-"));
  roots.push(root);
  return { root, store: new PluginStore(root) };
}

it("persists isolated recovery across host restarts and explicit repeated cleanup", async () => {
  const { root, store } = await fixture();
  expect(await store.readRecoveryState("streamskope.nsp")).toBeNull();
  const recovery = { requestId: "owned-request", apiUrl: "https://nsp.example.test" };
  await store.writeRecoveryState("streamskope.nsp", recovery);
  await store.writeRecoveryState("streamskope.eda", { sessionId: "other" });
  const restarted = new PluginStore(root);
  expect(await restarted.readRecoveryState("streamskope.nsp")).toEqual(recovery);
  if (process.platform !== "win32") {
    expect((await stat(join(root, ".recovery/streamskope.nsp.json"))).mode & 0o777).toBe(0o600);
  }
  await restarted.writeRecoveryState("streamskope.nsp", null);
  await restarted.writeRecoveryState("streamskope.nsp", null);
  expect(await restarted.readRecoveryState("streamskope.nsp")).toBeNull();
  expect(await restarted.readRecoveryState("streamskope.eda")).toEqual({ sessionId: "other" });
});

it("rejects traversal, oversized state, corruption and symlinked recovery files", async () => {
  const { root, store } = await fixture();
  expect(() => store.writeRecoveryState("../outside", {})).toThrow();
  expect(() =>
    store.writeRecoveryState("streamskope.nsp", { value: "x".repeat(70_000) }),
  ).toThrow();
  await store.writeRecoveryState("streamskope.nsp", {});
  const path = join(root, ".recovery/streamskope.nsp.json");
  await writeFile(path, "invalid");
  await expect(store.readRecoveryState("streamskope.nsp")).rejects.toThrow();
  if (process.platform !== "win32") {
    const outside = join(root, "outside.json");
    await writeFile(outside, "{}");
    await rm(path);
    await symlink(outside, path);
    await expect(store.readRecoveryState("streamskope.nsp")).rejects.toThrow();
    await expect(store.writeRecoveryState("streamskope.nsp", { modified: true })).rejects.toThrow();
    expect(await readFile(outside, "utf8")).toBe("{}");
  }
});
