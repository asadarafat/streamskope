import { fileURLToPath } from "node:url";

import { afterEach, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

it.each(["web", "electron", "boundary", "package", "all"])(
  "keeps %s reports inside the scanned project evidence directory",
  async (project) => {
    vi.stubEnv("STREAMSKOPE_TEST_PROJECT", project);
    vi.resetModules();
    const { default: config } = await import("../../config/playwright.config");
    const directory = fileURLToPath(new URL(`../../test-results/${project}`, import.meta.url));
    expect(config.outputDir).toBe(directory);
    expect(config.reporter).toEqual([
      ["list"],
      ["json", { outputFile: `${directory}/playwright-results.json` }],
    ]);
  },
);

it("rejects an evidence namespace outside the owned projects", async () => {
  vi.stubEnv("STREAMSKOPE_TEST_PROJECT", "../escape");
  vi.resetModules();
  await expect(import("../../config/playwright.config")).rejects.toThrow(
    "Unknown Playwright evidence project",
  );
});
