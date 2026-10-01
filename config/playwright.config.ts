import { resolve } from "node:path";

import { chromium, defineConfig } from "@playwright/test";

const reviewVideo = process.env.STREAMSKOPE_REVIEW_VIDEO === "1" ? ("on" as const) : undefined;
const project = process.env.STREAMSKOPE_TEST_PROJECT ?? "all";
if (!["web", "electron", "package", "boundary", "all"].includes(project)) {
  throw new Error("Unknown Playwright evidence project.");
}
const evidenceDirectory = resolve("test-results", project);

export default defineConfig({
  expect: {
    timeout: 5_000,
  },
  fullyParallel: false,
  outputDir: evidenceDirectory,
  projects: [
    {
      name: "web",
      testMatch: /web-.*\.spec\.ts/,
      use: {
        browserName: "chromium",
        launchOptions: {
          executablePath: chromium.executablePath(),
        },
      },
    },
    {
      name: "electron",
      testMatch: /electron-.*\.spec\.ts/,
    },
    {
      name: "package",
      testMatch: /package-.*\.spec\.ts/,
    },
  ],
  reporter: [["list"], ["json", { outputFile: `${evidenceDirectory}/playwright-results.json` }]],
  testDir: "../test/e2e",
  timeout: 20_000,
  use: {
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    ...(reviewVideo === undefined ? {} : { video: reviewVideo }),
  },
  workers: 1,
});
