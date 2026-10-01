import { mkdtemp, rm } from "node:fs/promises";
import { execFile } from "node:child_process";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { expect, test, type Page } from "@playwright/test";

import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import {
  createBrowserKafkaProfileStore,
  createKafkaBackend,
} from "../../src/platform/node/kafka-backend";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import type { OfficialPluginEntry } from "../../src/platform/node/plugins/catalog";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { expectWorkbenchReady } from "../support/workbench-browser";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";

async function port(): Promise<number> {
  const server = createServer();
  await new Promise<void>((done, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", done);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("No browser test port.");
  await new Promise<void>((done, reject) =>
    server.close((error) => (error ? reject(error) : done())),
  );
  return address.port;
}

async function openPlugins(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Preferences", exact: true }).click();
  await page.getByRole("tab", { name: "Plugins", exact: true }).click();
  await expect(page.getByRole("region", { name: "EDA Capture", exact: true })).toBeVisible();
}

test("installs, updates, rolls back, removes and reinstalls EDA in the same workbench", async ({
  page,
}, info) => {
  test.setTimeout(120_000);
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1") {
    await promisify(execFile)(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin"], {
      maxBuffer: 4 * 1_048_576,
    });
  }
  const fixtures = await pluginPackageFixtures();
  let available = fixtures.current;
  const root = await mkdtemp(join(tmpdir(), "streamskope-ui-plugin-"));
  const profiles = createBrowserKafkaProfileStore();
  let launch: RunningWebDevelopment | undefined;
  let restarts = 0;
  let downloads = 0;
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));

  async function start(): Promise<void> {
    const plugins = new PluginRuntime({
      store: new PluginStore(root),
      catalog: {
        list: (): Promise<readonly OfficialPluginEntry[]> =>
          Promise.resolve([
            {
              manifest: available.manifest,
              sha256: available.sha256,
              downloadUrl: "https://api.github.com/test-release-asset",
            },
          ]),
        download: (): Promise<{ bytes: Uint8Array; sha256: string }> => {
          downloads += 1;
          return Promise.resolve(available);
        },
      },
      // A lifecycle operation must never request host restart.
      restart: (): void => {
        restarts += 1;
      },
    });
    const backend = createKafkaBackend(
      profiles,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      plugins,
    );
    await plugins.start();
    launch = await launchWebDevelopment({
      backend: Object.assign(backend, { pluginAsset: plugins.rendererAsset.bind(plugins) }),
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    await expectWorkbenchReady(page);
  }

  try {
    await start();
    const documentId = await page.evaluate(() => {
      const id = crypto.randomUUID();
      (globalThis as typeof globalThis & { lifecycleDocument: string }).lifecycleDocument = id;
      return id;
    });
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Capture from EDA", exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByRole("menuitem", { name: "Existing Kafka cluster", exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    await openPlugins(page);
    const card = page.getByRole("region", { name: "EDA Capture", exact: true });
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${fixtures.current.manifest.version}`);
    expect(downloads).toBe(1);
    await page.screenshot({ path: info.outputPath("plugin-installed.png") });
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Capture from EDA", exact: true }).click();
    const capture = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
    await expect(capture.getByLabel("EDA API URL")).toBeVisible();
    await expect(capture.getByLabel("EDA username")).toBeVisible();
    await page.screenshot({ path: info.outputPath("installed-eda-capture.png") });
    await page.keyboard.press("Escape");
    await expect(capture).toHaveCount(0);
    await openPlugins(page);
    available = fixtures.update;
    await page.getByRole("button", { name: "Refresh plugins", exact: true }).click();
    await card
      .getByRole("button", { name: `Update to ${available.manifest.version}`, exact: true })
      .click();
    await expect(card).toContainText(`Active version ${available.manifest.version}`);
    available = fixtures.broken;
    await page.getByRole("button", { name: "Refresh plugins", exact: true }).click();
    await card
      .getByRole("button", { name: `Update to ${available.manifest.version}`, exact: true })
      .click();
    await expect(card).toContainText("previous version was restored");
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await page.screenshot({ path: info.outputPath("plugin-update-recovered.png") });
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
    await expect(card).toContainText("Not installed");
    // The card updates before the confirmation dialog finishes its exit transition.
    await expect(page.getByRole("button", { name: "Remove plugin", exact: true })).toHaveCount(0);
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog", { name: "Workbench Preferences" })).toHaveCount(0);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Capture from EDA", exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByRole("menuitem", { name: "Existing Kafka cluster", exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    available = fixtures.update;
    await openPlugins(page);
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Capture from EDA", exact: true }).click();
    await expect(capture.getByLabel("EDA API URL")).toBeVisible();
    await page.keyboard.press("Escape");
    expect(
      await page.evaluate(
        () => (globalThis as typeof globalThis & { lifecycleDocument: string }).lifecycleDocument,
      ),
    ).toBe(documentId);
    expect(restarts).toBe(0);
    expect(downloads).toBe(4);
    expect(errors).toEqual([]);
  } finally {
    await page.goto("about:blank");
    await launch?.close();
    await rm(root, { recursive: true, force: true });
  }
});
