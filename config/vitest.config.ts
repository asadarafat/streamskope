import { availableParallelism } from "node:os";
import { fileURLToPath } from "node:url";

import { defineConfig } from "vitest/config";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  test: {
    clearMocks: true,
    coverage: {
      enabled: false,
      provider: "v8",
      reporter: ["text", "json-summary"],
    },
    include: ["test/**/*.test.ts", "test/**/*.test.tsx"],
    // Leave a core for the desktop; bound memory use on larger workstations.
    maxWorkers: Math.min(4, Math.max(1, availableParallelism() - 1)),
    mockReset: true,
    restoreMocks: true,
    reporters: ["default", "json"],
    outputFile: { json: ".artifacts/ci/vitest.json" },
    testTimeout: 30_000,
  },
});
