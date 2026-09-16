import { resolve } from "node:path";

import { build } from "vite";

const repositoryRoot = process.cwd();
const outputDirectory = resolve(repositoryRoot, "dist/electron");
const entries = [
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
  {
    emptyOutDir: false,
    input: resolve(repositoryRoot, "src/features/kafka/engine/trust-material-worker.ts"),
    name: "trust-material-worker",
  },
];

for (const entry of entries) {
  await build({
    build: {
      emptyOutDir: entry.emptyOutDir,
      minify: false,
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
    logLevel: "silent",
    root: repositoryRoot,
  });
}
