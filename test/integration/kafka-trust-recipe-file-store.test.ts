import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { AtomicKafkaTrustRecipeFileStore } from "../../src/platform/electron/main/kafka-trust-recipe-file-store";
import { trustRecipeInput } from "../support/trust-recipe";

const directories: string[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function storePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-recipe-test-"));
  directories.push(directory);
  return join(directory, "recipes.json");
}

describe("Atomic trust recipe file storage", () => {
  it("round-trips a private versioned document independently of legacy data", async () => {
    const path = await storePath();
    const store = new AtomicKafkaTrustRecipeFileStore(path);
    expect(await store.load()).toBeUndefined();
    const document = {
      version: 1 as const,
      recipes: [{ ...trustRecipeInput(), id: "recipe-1", revision: 1 }],
    };
    await store.commit(document);
    expect(await new AtomicKafkaTrustRecipeFileStore(path).load()).toEqual(document);
    expect((await stat(path)).mode & 0o077).toBe(0);
    expect(store.capability()).toEqual({ durability: "durable", state: "ready" });
  });

  it.each([
    '{"version":9,"recipes":[]}',
    '{"version":1,"recipes":[{}]}',
    "not-json",
    "x".repeat(4 * 1048576 + 1),
  ])("preserves corrupt/unsupported/oversized bytes %#", async (contents) => {
    const path = await storePath();
    await writeFile(path, contents);
    const store = new AtomicKafkaTrustRecipeFileStore(path);
    await expect(store.load()).rejects.toMatchObject({ code: "TEMPLATE_CORRUPT" });
    expect(await readFile(path, "utf8")).toBe(contents);
    expect(store.capability().state).toBe("unavailable");
    await expect(store.commit({ version: 1, recipes: [] })).rejects.toMatchObject({
      code: "TEMPLATE_STORE_UNAVAILABLE",
    });
  });

  it("preserves the previous committed bytes when replacement is aborted", async () => {
    const path = await storePath();
    const store = new AtomicKafkaTrustRecipeFileStore(path);
    await store.commit({ version: 1, recipes: [] });
    const before = await readFile(path, "utf8");
    const controller = new AbortController();
    controller.abort();
    await expect(
      store.commit(
        { version: 1, recipes: [{ ...trustRecipeInput(), id: "new", revision: 1 }] },
        controller.signal,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(await readFile(path, "utf8")).toBe(before);
  });
});
