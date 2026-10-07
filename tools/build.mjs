import { spawnSync } from "node:child_process";
import process from "node:process";
import { resolve } from "node:path";

import { build } from "vite";

import { OFFICIAL_PLUGINS } from "../src/platform/node/plugins/official.ts";
import { buildPlugin } from "./build/plugin.ts";
import { applyForgePatch } from "./check/forge-patch.ts";
import {
  applyBuildDependencyPatches,
  applyRuntimeDependencyPatches,
} from "./check/build-dependency-patches.ts";

const [target = "desktop", ...extraArguments] = process.argv.slice(2);
if (!["desktop", "web"].includes(target) || extraArguments.length > 0) {
  throw new Error("Usage: npm run build [-- desktop|web]");
}

await applyForgePatch(process.cwd());
await applyBuildDependencyPatches(process.cwd());
await applyRuntimeDependencyPatches(process.cwd());
// Compile all projects once before producing renderer and Electron bundles.
const compilation = spawnSync(
  process.execPath,
  [
    "node_modules/typescript/bin/tsc",
    "-b",
    "config/typescript/host.json",
    "config/typescript/renderer.json",
    "config/typescript/test.json",
    "config/typescript/packaging.json",
    "--pretty",
    "false",
  ],
  { stdio: "inherit" },
);
if (compilation.error) throw compilation.error;
if (compilation.status !== 0) process.exit(compilation.status ?? 1);
await build({ configFile: "config/vite.config.ts" });

const repositoryRoot = process.cwd();
const outputDirectory = resolve(repositoryRoot, target === "web" ? "dist/web" : "dist/electron");
const entries = [
  ...(target === "web"
    ? [
        {
          emptyOutDir: true,
          input: resolve(repositoryRoot, "src/platform/node/browser-entry.ts"),
          name: "server",
        },
      ]
    : [
        {
          emptyOutDir: true,
          input: resolve(repositoryRoot, "src/platform/electron/main/electron-entry.ts"),
          name: "main",
        },
        {
          emptyOutDir: false,
          input: resolve(repositoryRoot, "src/platform/electron/preload/index.ts"),
          name: "preload",
        },
      ]),
  {
    emptyOutDir: false,
    input: resolve(repositoryRoot, "src/features/kafka/engine/trust-material-worker.ts"),
    name: "trust-material-worker",
  },
  {
    emptyOutDir: false,
    input: resolve(repositoryRoot, "src/features/kafka/engine/record-codec-worker.ts"),
    name: "record-codec-worker",
  },
];

for (const entry of entries) {
  await build({
    build: {
      emptyOutDir: entry.emptyOutDir,
      minify: entry.name === "main" ? "esbuild" : false,
      outDir: outputDirectory,
      rollupOptions: {
        external: ["electron"],
        input: entry.input,
        output: {
          codeSplitting: false,
          entryFileNames: `${entry.name}.cjs`,
          format: "cjs",
        },
      },
      sourcemap: true,
      ssr: true,
    },
    configFile: false,
    esbuild: { keepNames: true },
    logLevel: "silent",
    root: repositoryRoot,
    ssr: {
      external: ["@platformatic/kafka", "jks-js", "node-forge", "ssh2"],
      noExternal: true,
    },
  });
}

if (target === "desktop") {
  for (const plugin of OFFICIAL_PLUGINS) await buildPlugin(plugin);
}
