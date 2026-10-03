import { execFile } from "node:child_process";
import { createRequire } from "node:module";
import { promisify } from "node:util";

import { expect, it } from "vitest";

const execute = promisify(execFile);
const playwrightCli = createRequire(import.meta.url).resolve("@playwright/test/cli");

it("loads all Playwright projects without launching browsers or package builds", async () => {
  // Discovery compiles all browser/native test modules; it is not a performance probe.
  const { stdout } = await execute(
    process.execPath,
    [playwrightCli, "test", "--config", "config/playwright.config.ts", "--list"],
    { timeout: 60_000 },
  );
  for (const project of ["web", "electron", "package"]) expect(stdout).toContain(`[${project}]`);
  expect(stdout).toContain("narrow working preload");
  expect(stdout).toContain("launches and measures the inspected production package");
  expect(stdout).not.toContain("Test evidence and packaged application payloads");
}, 90_000);
