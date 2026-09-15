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
    maxWorkers: 1,
    mockReset: true,
    restoreMocks: true,
    testTimeout: 10_000,
  },
});
