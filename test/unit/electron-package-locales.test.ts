import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { pruneElectronLocales } from "../../tools/electron-package-locales";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-locales-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Electron package locale pruning", () => {
  it("retains supported English locales and unrelated Linux runtime files", async () => {
    const buildPath = await temporaryDirectory();
    const localePath = join(buildPath, "locales");
    await mkdir(localePath);
    await writeFile(join(localePath, "en-US.pak"), "english-us", "utf8");
    await writeFile(join(localePath, "en-GB.pak"), "english-gb", "utf8");
    await writeFile(join(localePath, "de.pak"), "german", "utf8");
    await writeFile(join(localePath, "fr.pak"), "french", "utf8");
    await writeFile(join(buildPath, "resources.pak"), "runtime-resource", "utf8");

    const evidence = await pruneElectronLocales(buildPath, "linux");

    expect(evidence).toEqual({
      localeFiles: ["en-GB.pak", "en-US.pak"],
      removedLocaleBytes: Buffer.byteLength("germanfrench"),
    });
    await expect(readFile(join(localePath, "en-US.pak"), "utf8")).resolves.toBe("english-us");
    await expect(readFile(join(localePath, "en-GB.pak"), "utf8")).resolves.toBe("english-gb");
    await expect(readFile(join(buildPath, "resources.pak"), "utf8")).resolves.toBe(
      "runtime-resource",
    );
    await expect(readFile(join(localePath, "de.pak"), "utf8")).rejects.toThrow();
    await expect(readFile(join(localePath, "fr.pak"), "utf8")).rejects.toThrow();
  });

  it("fails closed when the extracted Electron locale layout is absent", async () => {
    const buildPath = await temporaryDirectory();

    await expect(pruneElectronLocales(buildPath, "linux")).rejects.toThrow(
      /locale directory.*unavailable/iu,
    );
  });
});
