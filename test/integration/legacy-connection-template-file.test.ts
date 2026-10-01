import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { LegacyKafkaConnectionTemplateFile } from "../../src/platform/node/legacy-connection-template-file";
import { DEFAULT_CONNECTION_TEMPLATE_DOCUMENT } from "../support/legacy-template-document";

const directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { force: true, recursive: true })),
  );
});

async function legacyPath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-legacy-source-"));
  directories.push(directory);
  return join(directory, "catalog.json");
}

describe("read-only legacy catalog source", () => {
  it("does not create missing files and rereads externally edited catalog bytes", async () => {
    const path = await legacyPath();
    const source = new LegacyKafkaConnectionTemplateFile(path);
    expect(await source.load()).toBeUndefined();
    await expect(readFile(path)).rejects.toMatchObject({ code: "ENOENT" });
    const bytes = JSON.stringify({ version: 1, ...DEFAULT_CONNECTION_TEMPLATE_DOCUMENT });
    await writeFile(path, bytes);
    expect(await source.load()).toEqual(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    expect(await readFile(path, "utf8")).toBe(bytes);
    const edited = {
      version: 1,
      catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs.map((catalog) => ({
        ...catalog,
        selectedName: null,
      })),
    };
    await writeFile(path, JSON.stringify(edited));
    expect((await source.load())?.catalogs.every((catalog) => catalog.selectedName === null)).toBe(
      true,
    );
  });

  it.each([
    Buffer.from("not-json"),
    Buffer.from(JSON.stringify({ version: 2, ...DEFAULT_CONNECTION_TEMPLATE_DOCUMENT })),
    Buffer.from('{"version":1,"catalogs":[]}'),
    Buffer.from([0xff]),
    Buffer.alloc(4 * 1_048_576 + 1, 120),
  ])("preserves corrupt, unsupported, invalid UTF-8 and oversized data %#", async (bytes) => {
    const path = await legacyPath();
    await writeFile(path, bytes);
    await expect(new LegacyKafkaConnectionTemplateFile(path).load()).rejects.toMatchObject({
      code: "TEMPLATE_CORRUPT",
    });
    expect((await readFile(path)).equals(bytes)).toBe(true);
  });

  it("propagates cancellation without rewriting the source", async () => {
    const path = await legacyPath();
    await writeFile(path, "preserved");
    await expect(
      new LegacyKafkaConnectionTemplateFile(path).load(AbortSignal.abort()),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(await readFile(path, "utf8")).toBe("preserved");
  });
});
