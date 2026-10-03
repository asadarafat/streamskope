import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { expect, it } from "vitest";

import { OFFICIAL_PLUGINS } from "../../src/platform/node/plugins/official";

const root = fileURLToPath(new URL("../../", import.meta.url));

it("exposes feature, platform and plugin API layers as the source roots", () => {
  expect(readdirSync(resolve(root, "src")).sort()).toEqual(["features", "platform", "plugins"]);
  expect(readdirSync(resolve(root, "src/features/kafka")).sort()).toEqual([
    "application",
    "contracts",
    "engine",
    "facade",
    "ui",
  ]);
});

it("keeps direct Kubernetes access out of the desktop and browser hosts", () => {
  const imports = ts.sys
    .readDirectory(resolve(root, "src"), [".ts", ".tsx"])
    .filter((file) => readFileSync(file, "utf8").includes('"@kubernetes/client-node"'))
    .map((file) => relative(root, file).replaceAll("\\", "/"));

  expect(imports).toEqual([]);
});

function unreachableSourceFiles(
  directory: string,
  entrypoints: readonly string[],
  sourceRoots: readonly string[] = ["src"],
): string[] {
  const files = sourceRoots.flatMap((sourceRoot) =>
    ts.sys.readDirectory(
      resolve(directory, sourceRoot),
      [".ts", ".tsx", ".js", ".mjs"],
      ["**/node_modules/**", "**/dist/**", "**/generated/**"],
    ),
  );
  const visited = new Set<string>();
  const pending = entrypoints.map((entry) => resolve(directory, entry));
  while (pending.length > 0) {
    const file = pending.pop()!;
    if (visited.has(file) || file.replaceAll("\\", "/").includes("/node_modules/")) continue;
    visited.add(file);
    const source = readFileSync(file, "utf8");
    for (const entry of ts.preProcessFile(source, true, true).importedFiles) {
      const resolved = ts.resolveModuleName(
        entry.fileName,
        file,
        {
          moduleResolution: ts.ModuleResolutionKind.Bundler,
          allowJs: true,
        },
        ts.sys,
      ).resolvedModule;
      if (resolved !== undefined) pending.push(resolve(resolved.resolvedFileName));
    }
  }
  return files
    .filter((file) => !file.endsWith(".d.ts") && !visited.has(resolve(file)))
    .map((file) => relative(directory, file).replaceAll("\\", "/"))
    .sort();
}

it("keeps source modules reachable from production and development entrypoints", () => {
  // These process/worker entries are launched by the Electron build, not imported.
  const entries = [
    "src/platform/electron/main/electron-entry.ts",
    "src/platform/electron/preload/index.ts",
    "src/features/kafka/engine/trust-material-worker.ts",
    "src/features/kafka/engine/record-codec-worker.ts",
  ];
  const build = readFileSync(resolve(root, "tools/build.mjs"), "utf8");
  for (const entry of entries) expect(build).toContain(entry);

  const renderer = "src/platform/electron/renderer/main.tsx";
  const development = "tools/dev/start.ts";
  const cli = "tools/cli.ts";
  expect(readFileSync(resolve(root, "tools/dev.mjs"), "utf8")).toContain(cli);
  const pluginEntries = OFFICIAL_PLUGINS.flatMap((plugin) => [
    `plugins/${plugin.directory}/backend/index.ts`,
    `plugins/${plugin.directory}/ui/renderer.tsx`,
  ]);
  const pluginBuild = readFileSync(resolve(root, "tools/build/plugin.ts"), "utf8");
  expect(build).toContain("for (const plugin of OFFICIAL_PLUGINS) await buildPlugin(plugin)");
  expect(pluginBuild).toContain('resolve(root, "plugins", plugin.directory)');
  expect(pluginBuild).toContain('resolve(source, "backend/index.ts")');
  expect(pluginBuild).toContain('resolve(source, "ui/renderer.tsx")');
  expect(readFileSync(resolve(root, "index.html"), "utf8")).toContain(renderer);
  expect(readFileSync(resolve(root, "tools/dev.mjs"), "utf8")).toContain(development);
  expect(
    unreachableSourceFiles(
      root,
      [...entries, renderer, development, cli, ...pluginEntries],
      ["src", "plugins"],
    ),
  ).toEqual([]);
});

it("keeps core production imports independent of separately installed plugin code", () => {
  const violations: string[] = [];
  for (const file of ts.sys.readDirectory(resolve(root, "src"), [".ts", ".tsx"])) {
    const source = readFileSync(file, "utf8");
    for (const entry of ts.preProcessFile(source, true, true).importedFiles) {
      const imported = ts.resolveModuleName(
        entry.fileName,
        file,
        { moduleResolution: ts.ModuleResolutionKind.Bundler },
        ts.sys,
      ).resolvedModule;
      if (imported?.resolvedFileName.startsWith(resolve(root, "plugins") + "/")) {
        violations.push(`${relative(root, file)} -> ${entry.fileName}`);
      }
    }
  }
  expect(violations).toEqual([]);
});

it("rejects test-only imports and disconnected cycles while following runtime imports and workers", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "streamskope-source-ownership-"));
  try {
    const fixtures = {
      "src/main.ts":
        'export const main = true; export { active } from "./active"; import("./lazy");',
      "src/active.ts": 'export { main } from "./main"; export const active = true;',
      "src/lazy.ts": "export const lazy = true;",
      "src/worker.ts": "export const worker = true;",
      "src/orphan-a.ts": 'import "./orphan-b";',
      "src/orphan-b.ts": 'import "./orphan-a";',
      "src/test-only.ts": "export const unusedAdapter = true;",
      "test/old-adapter.test.ts": 'import "../src/test-only";',
    };
    for (const [file, source] of Object.entries(fixtures)) {
      await mkdir(dirname(resolve(directory, file)), { recursive: true });
      await writeFile(resolve(directory, file), source);
    }
    expect(unreachableSourceFiles(directory, ["src/main.ts", "src/worker.ts"])).toEqual([
      "src/orphan-a.ts",
      "src/orphan-b.ts",
      "src/test-only.ts",
    ]);
    expect(unreachableSourceFiles(directory, ["src/main.ts"])).toContain("src/worker.ts");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
