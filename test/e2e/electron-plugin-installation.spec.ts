import { execFile } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { promisify } from "node:util";

import { expect, test, type Page } from "@playwright/test";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../../src/features/kafka/contracts";
import {
  electronPluginStorageAvailable,
  startElectronPluginFixture,
} from "../support/electron-plugin";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";

const run = promisify(execFile);

async function openPlugins(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Preferences", exact: true }).click();
  await page.getByRole("tab", { name: "Plugins", exact: true }).click();
  await expect(page.getByRole("region", { name: "EDA Capture", exact: true })).toBeVisible();
}

async function approvePlugin(
  page: Page,
  action: "Install plugin" | "Update plugin",
): Promise<void> {
  const review = page.getByRole("dialog", { name: "Review plugin", exact: true });
  await expect(review).toBeVisible();
  await review.getByRole("button", { name: action, exact: true }).click();
  await expect(review).toHaveCount(0);
}

async function closePreferences(page: Page): Promise<void> {
  await page
    .getByRole("dialog", { name: "Workbench Preferences" })
    .getByRole("button", { name: "Close", exact: true })
    .click();
}

async function pluginInstallation(page: Page): Promise<
  | {
      activationId: string | undefined;
      rendererUrl: string | undefined;
      version: string | undefined;
    }
  | undefined
> {
  return page.evaluate(async (version) => {
    const host = (window as unknown as { streamSkopeHost: StreamSkopeHost }).streamSkopeHost;
    const response = await host.execute({
      command: "plugins.list",
      id: crypto.randomUUID(),
      payload: {},
      version,
    });
    if (!response.ok) throw new Error(response.error.summary);
    const plugin = response.result.pluginSnapshot.plugins.find(
      (entry) => entry.id === "streamskope.eda",
    );
    return plugin === undefined
      ? undefined
      : {
          activationId: plugin.activationId,
          rendererUrl: plugin.rendererUrl,
          version: plugin.active?.version,
        };
  }, HOST_PROTOCOL_VERSION);
}

async function openCapture(page: Page, screenshot?: string): Promise<void> {
  await page.getByRole("button", { name: "Add connection", exact: true }).click();
  await page.getByRole("menuitem", { name: "Capture from EDA", exact: true }).click();
  const capture = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
  await expect(capture.getByLabel("EDA API URL")).toBeVisible();
  await expect(capture.getByLabel("EDA username")).toBeVisible();
  await expect(capture.getByRole("button", { name: "Discover sources" })).toBeVisible();
  if (screenshot !== undefined) await page.screenshot({ path: screenshot, animations: "disabled" });
  await page.keyboard.press("Escape");
  await expect(capture).toHaveCount(0);
}

test("installs, updates, removes and reinstalls EDA in one production Electron window", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(180_000);
  test.skip(
    !electronPluginStorageAvailable,
    "Real protected-storage acceptance requires D-Bus and GNOME Keyring on Linux.",
  );
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1")
    await run(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin"], {
      maxBuffer: 4 * 1_048_576,
    });
  const { current: original, update } = await pluginPackageFixtures();
  const updateVersion = update.manifest.version;
  const fixture = await startElectronPluginFixture(original.bytes, info);
  const { application, page, catalogPath, processId, origin, security, errors, assetFailures } =
    fixture;
  try {
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Capture from EDA", exact: true })).toHaveCount(
      0,
    );
    await page.keyboard.press("Escape");
    await openPlugins(page);
    const card = page.getByRole("region", { name: "EDA Capture", exact: true });
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await approvePlugin(page, "Install plugin");
    await expect(card).toContainText(`Active version ${original.manifest.version}`);
    const installed = await pluginInstallation(page);
    expect(installed?.activationId).toBeTruthy();
    await closePreferences(page);
    await openCapture(page);

    await writeFile(catalogPath, update.bytes);
    await openPlugins(page);
    await card.getByRole("button", { name: `Update to ${updateVersion}`, exact: true }).click();
    await approvePlugin(page, "Update plugin");
    await expect(card).toContainText(`Active version ${updateVersion}`);
    const replacement = await pluginInstallation(page);
    expect(replacement?.activationId).not.toBe(installed?.activationId);
    expect(replacement?.rendererUrl).not.toBe(installed?.rendererUrl);
    await closePreferences(page);
    await openCapture(page);

    await openPlugins(page);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
    await expect(card).toContainText("Not installed");
    await closePreferences(page);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Capture from EDA", exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByRole("menuitem", { name: "Existing Kafka cluster", exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");

    await openPlugins(page);
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await approvePlugin(page, "Install plugin");
    await expect(card).toContainText(`Active version ${updateVersion}`);
    const reinstalled = await pluginInstallation(page);
    expect(reinstalled?.activationId).not.toBe(replacement?.activationId);
    expect(reinstalled?.rendererUrl).not.toBe(replacement?.rendererUrl);
    await closePreferences(page);
    await openCapture(page, info.outputPath("electron-plugin-hot-lifecycle.png"));
    expect(await page.evaluate(() => performance.timeOrigin)).toBe(origin);
    expect(application.process().pid).toBe(processId);
    expect(application.windows()).toEqual([page]);
    expect(errors).toEqual([]);
    expect(assetFailures).toEqual([]);
    await info.attach("plugin-lifecycle-evidence", {
      body: JSON.stringify(
        {
          security,
          installed,
          replacement,
          reinstalled,
          sameProcess: true,
          sameWindow: true,
          rendererReloaded: false,
        },
        null,
        2,
      ),
      contentType: "application/json",
    });
  } finally {
    await fixture.close();
  }
});
