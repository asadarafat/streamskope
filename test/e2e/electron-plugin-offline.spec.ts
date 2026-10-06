import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { expect, test } from "@playwright/test";

import { signPortablePluginPackage } from "../../src/platform/node/plugins/package";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";
import {
  electronPluginStorageAvailable,
  startElectronPluginFixture,
} from "../support/electron-plugin";

const run = promisify(execFile);

test("installs a reviewed signed file and cached copy in the native host while GitHub is unavailable", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(180_000);
  test.skip(!electronPluginStorageAvailable, "A native protected credential service is required.");
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1")
    await run(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin", "eda"], {
      timeout: 90_000,
      maxBuffer: 2 * 1024 * 1024,
    });
  const fixture = await pluginPackageFixtures("eda");
  const primary = fixture.current.bytes;
  const publisher = pluginPublisherFixture();
  const portable = signPortablePluginPackage(
    primary,
    publisher.publishers[0]!.keyId,
    Buffer.from(publisher.encodedKey, "base64").toString(),
  );
  const root = await mkdtemp(join(tmpdir(), "streamskope-native-portable-"));
  const path = join(root, "portable.skope-plugin");
  await writeFile(path, portable);
  let running: Awaited<ReturnType<typeof startElectronPluginFixture>> | undefined;
  try {
    running = await startElectronPluginFixture(primary, info, [], {
      STREAMSKOPE_PLUGIN_TEST_PUBLISHERS: JSON.stringify(publisher.publishers),
      STREAMSKOPE_PLUGIN_TEST_FILE: path,
      STREAMSKOPE_PLUGIN_TEST_OFFLINE: "1",
    });
    const { page } = running;
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    await page.getByRole("tab", { name: "Plugins", exact: true }).click();
    await page.getByRole("button", { name: "Install from file", exact: true }).click();
    const review = page.getByRole("dialog", { name: "Review plugin", exact: true });
    await expect(review).toBeVisible();
    await expect(review).toContainText(publisher.publishers[0]!.name);
    await review.getByRole("button", { name: "Install plugin", exact: true }).click();
    await expect(review).toBeHidden();
    await expect(
      page.getByText(`Active version ${fixture.current.manifest.version}`, { exact: true }),
    ).toBeVisible();
    await page.screenshot({ path: info.outputPath("signed-native-install.png") });

    await page.getByRole("button", { name: "Install from file", exact: true }).click();
    await expect(review).toContainText(/already installed/iu);
    await review.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Remove", exact: true }).click();
    await page
      .getByRole("dialog", { name: /^Remove/u })
      .getByRole("button", { name: "Remove plugin", exact: true })
      .click();
    await expect(
      page.getByText(`Active version ${fixture.current.manifest.version}`, { exact: true }),
    ).toHaveCount(0);
    await page.getByRole("button", { name: "Use cached package", exact: true }).click();
    await expect(review).toBeVisible();
    await review.getByRole("button", { name: "Install plugin", exact: true }).click();
    await expect(
      page.getByText(`Active version ${fixture.current.manifest.version}`, { exact: true }),
    ).toBeVisible();
    expect(await page.evaluate(() => performance.timeOrigin)).toBe(running.origin);
    expect(running.application.process().pid).toBe(running.processId);
    expect(running.errors).toEqual([]);
    expect(running.assetFailures).toEqual([]);
  } finally {
    await running?.close();
    await rm(root, { recursive: true, force: true });
  }
});
