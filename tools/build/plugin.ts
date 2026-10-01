import { builtinModules } from "node:module";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import react from "@vitejs/plugin-react";
import { build } from "vite";

import type { OfficialPlugin } from "../../src/platform/node/plugins/official";
import {
  MAX_PLUGIN_RESOURCE_BYTES,
  pluginPackageSha256,
} from "../../src/platform/node/plugins/package";
import { parsePluginManifest } from "../../src/plugins/validation";

/** Build an optional plugin separately; none of these files belong in the desktop ASAR. */
export async function buildPlugin(plugin: OfficialPlugin): Promise<void> {
  const root = process.cwd();
  const source = resolve(root, "plugins", plugin.directory);
  const outDir = resolve(root, "dist/plugins", plugin.directory);
  const manifest = parsePluginManifest(
    JSON.parse(await readFile(resolve(source, "manifest.json"), "utf8")),
  );
  const resources = new Map<string, Uint8Array>();
  for (const resource of manifest.resources ?? []) {
    const bytes = await readFile(resolve(source, "resources", resource.path));
    if (
      bytes.byteLength > MAX_PLUGIN_RESOURCE_BYTES ||
      pluginPackageSha256(bytes) !== resource.sha256
    ) {
      throw new Error(
        `Plugin resource ${resource.path} does not match its declared size limit or SHA256.`,
      );
    }
    resources.set(resource.path, bytes);
  }
  await build({
    configFile: false,
    root,
    logLevel: "warn",
    build: {
      emptyOutDir: true,
      outDir,
      minify: "esbuild",
      sourcemap: false,
      ssr: true,
      target: "node24",
      rollupOptions: {
        input: resolve(source, "backend/index.ts"),
        external: [
          ...builtinModules,
          ...builtinModules.map((name) => `node:${name}`),
          "bufferutil",
          "utf-8-validate",
        ],
        output: { codeSplitting: false, entryFileNames: "backend.cjs", format: "cjs" },
      },
    },
    ssr: { noExternal: true },
    define: {
      "import.meta.url": "__filename",
      __STREAMSKOPE_PLUGIN_RESOURCES__: JSON.stringify(
        Object.fromEntries(
          [...resources].map(([path, bytes]) => [
            path,
            new TextDecoder("utf-8", { fatal: true }).decode(bytes),
          ]),
        ),
      ),
    },
    esbuild: { keepNames: true },
  });
  await build({
    configFile: false,
    root,
    logLevel: "warn",
    plugins: [react({})],
    define: { "process.env.NODE_ENV": JSON.stringify("production") },
    build: {
      emptyOutDir: false,
      outDir,
      minify: "esbuild",
      sourcemap: false,
      assetsInlineLimit: Infinity,
      lib: {
        entry: resolve(source, "ui/renderer.tsx"),
        formats: ["es"],
        fileName: () => "renderer.js",
        cssFileName: "renderer",
      },
      rollupOptions: { output: { codeSplitting: false } },
    },
  });
  for (const [path, bytes] of resources) await writeFile(resolve(outDir, path), bytes);
  const names = await readdir(outDir);
  if (
    !names.includes("backend.cjs") ||
    !names.includes("renderer.js") ||
    names.some(
      (name) => !["backend.cjs", "renderer.js", "renderer.css", ...resources.keys()].includes(name),
    )
  ) {
    throw new Error(
      "Plugin build must contain only its standalone backend, renderer, optional stylesheet and declared resources.",
    );
  }
}
