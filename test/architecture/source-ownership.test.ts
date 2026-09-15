import { readFileSync, readdirSync } from "node:fs";
import { relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { expect, it } from "vitest";

const root = fileURLToPath(new URL("../../", import.meta.url));

it("exposes Kafka layers directly without a redundant feature container", () => {
  expect(readdirSync(resolve(root, "src"))).toContain("kafka");
  expect(readdirSync(resolve(root, "src/kafka")).sort()).toEqual([
    "application",
    "contracts",
    "engine",
    "facade",
    "ui",
  ]);
});

it("keeps source modules imported or explicitly launched by a runtime", () => {
  const files = ["src", "test", "tools"].flatMap((directory) =>
    ts.sys.readDirectory(
      resolve(root, directory),
      [".ts", ".tsx", ".js", ".mjs"],
      ["**/node_modules/**", "**/dist/**", "**/generated/**"],
    ),
  );
  const imported = new Set<string>();
  for (const file of files) {
    for (const entry of ts.preProcessFile(readFileSync(file, "utf8"), true, true).importedFiles) {
      const module = ts.resolveModuleName(
        entry.fileName,
        file,
        { moduleResolution: ts.ModuleResolutionKind.Bundler, allowJs: true },
        ts.sys,
      ).resolvedModule;
      if (module !== undefined) imported.add(resolve(module.resolvedFileName));
    }
  }

  // These process/worker entries are launched by the Electron build, not imported.
  const entries = [
    "src/main/electron-entry.ts",
    "src/preload/index.ts",
    "src/kafka/engine/trust-material-worker.ts",
  ];
  const build = readFileSync(resolve(root, "tools/build-electron.mjs"), "utf8");
  for (const entry of entries) expect(build).toContain(entry);

  expect(
    files
      .filter((file) => !file.endsWith(".d.ts") && !imported.has(resolve(file)))
      .map((file) => relative(root, file).replaceAll("\\", "/"))
      .filter((file) => file.startsWith("src/"))
      .filter((file) => !entries.includes(file))
      .sort(),
  ).toEqual([]);
});
