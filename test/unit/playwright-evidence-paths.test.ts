import { fileURLToPath } from "node:url";

import { afterEach, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

it.each([
  "production-startup",
  "workbench",
  "nats-workspace",
  "structured-records",
  "plugin-lifecycle",
])("retains the %s browser suite report alongside other invocations", async (suite) => {
  vi.stubEnv("STREAMSKOPE_TEST_PROJECT", "web");
  vi.stubEnv("STREAMSKOPE_TEST_SUITE", suite);
  const { default: config } = await import("../../config/playwright.config");
  const directory = fileURLToPath(new URL(`../../test-results/web/${suite}`, import.meta.url));
  expect(config.outputDir).toBe(directory);
  expect(config.reporter).toContainEqual([
    "json",
    { outputFile: `${directory}/playwright-results.json` },
  ]);
});

it.each(["", "../escape", "/absolute", "nested/path", "..", "a".repeat(65)])(
  "rejects invalid suite namespace %j",
  async (suite) => {
    vi.stubEnv("STREAMSKOPE_TEST_PROJECT", "web");
    vi.stubEnv("STREAMSKOPE_TEST_SUITE", suite);
    await expect(import("../../config/playwright.config")).rejects.toThrow(
      "Invalid Playwright evidence suite",
    );
  },
);

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
