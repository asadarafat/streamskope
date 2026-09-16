import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { expect, it } from "vitest";

const root = fileURLToPath(new URL("../../", import.meta.url));

function project(path: string): ts.ParsedCommandLine {
  const loaded = ts.readConfigFile(resolve(root, path), (file) => ts.sys.readFile(file));
  expect(loaded.error).toBeUndefined();
  const parsed = ts.parseJsonConfigFileContent(loaded.config, ts.sys, resolve(root, path, ".."));
  expect(parsed.errors).toEqual([]);
  return parsed;
}

it("keeps root editor discovery pointing at the three consolidated compiler projects", () => {
  const solution = project("tsconfig.json");
  expect(solution.projectReferences?.map((entry) => entry.path).sort()).toEqual(
    ["host", "renderer", "test"].map((name) => resolve(root, `config/typescript/${name}.json`)),
  );
  for (const path of [
    "vite.config.ts",
    "vitest.config.ts",
    "playwright.config.ts",
    "tsconfig.base.json",
    "tsconfig.host.json",
    "tsconfig.renderer.json",
    "tsconfig.test.json",
  ]) {
    expect(existsSync(resolve(root, path)), path).toBe(false);
  }
});

it("resolves compiler source coverage and caches against the repository, not config", () => {
  const host = project("config/typescript/host.json");
  const renderer = project("config/typescript/renderer.json");
  const tests = project("config/typescript/test.json");
  expect(host.fileNames).toContain(resolve(root, "src/platform/electron/main/electron-entry.ts"));
  expect(host.fileNames).not.toContain(resolve(root, "src/platform/electron/renderer/main.tsx"));
  expect(host.fileNames).not.toContain(resolve(root, "tools/capture-docs.ts"));
  expect(host.options.lib).not.toContain("lib.dom.d.ts");
  expect(renderer.fileNames).toContain(resolve(root, "src/platform/electron/renderer/main.tsx"));
  expect(renderer.fileNames).not.toContain(
    resolve(root, "src/platform/electron/main/electron-entry.ts"),
  );
  expect(tests.fileNames).toContain(resolve(root, "config/playwright.config.ts"));
  expect(tests.fileNames).toContain(resolve(root, "test/unit/tool-configuration.test.ts"));
  expect(tests.fileNames).toContain(resolve(root, "tools/capture-docs.ts"));
  expect(tests.options.lib).toContain("lib.dom.d.ts");
  for (const [name, config] of [
    ["host", host],
    ["renderer", renderer],
    ["test", tests],
  ] as const) {
    expect(config.options.noEmit, `${name} must never emit beside source files`).toBe(true);
    expect(config.options.strict).toBe(true);
    expect(config.options.rootDir).toBe(root.replace(/\/$/u, ""));
    expect(config.options.tsBuildInfoFile).toBe(
      resolve(root, `.cache/typescript/${name}.tsbuildinfo`),
    );
  }
});

it("keeps generated JavaScript and declarations out of TypeScript source directories", () => {
  const sources = new Set(
    ["host", "renderer", "test"].flatMap(
      (name) => project(`config/typescript/${name}.json`).fileNames,
    ),
  );
  const generated = [...sources]
    .filter((path) => /\.tsx?$/u.test(path) && !path.endsWith(".d.ts"))
    .flatMap((path) => [path.replace(/\.tsx?$/u, ".js"), path.replace(/\.tsx?$/u, ".d.ts")])
    .filter((path) => existsSync(path));
  expect(generated, "Build output belongs in dist/, not beside TypeScript sources").toEqual([]);
});
