import { mkdtemp, rm } from "node:fs/promises";
import { execFile } from "node:child_process";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { expect, test, type Locator, type Page } from "@playwright/test";

import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import {
  createBrowserKafkaProfileStore,
  createKafkaBackend,
} from "../../src/platform/node/kafka-backend";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import type { OfficialPluginEntry } from "../../src/platform/node/plugins/catalog";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { signPortablePluginPackage } from "../../src/platform/node/plugins/package";
import { expectWorkbenchReady } from "../support/workbench-browser";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";
import { pluginPublisherFixture } from "../support/plugin-publisher-fixture";

// Exercise transactional controls through the supported reduced-motion UI.
// Accordion scrolling must not move a pointer target during a settings action.
test.use({ contextOptions: { reducedMotion: "reduce" } });

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
  await expect(page.getByRole("region", { name: "EDA Connector", exact: true })).toBeVisible();
}

async function approvePlugin(
  page: Page,
  action: "Install plugin" | "Update plugin",
  screenshot?: string,
): Promise<void> {
  const review = page.getByRole("dialog", { name: "Review plugin", exact: true });
  await expect(review).toBeVisible();
  if (screenshot !== undefined) await page.screenshot({ path: screenshot, animations: "disabled" });
  await review.getByRole("button", { name: action, exact: true }).click();
  await expect(review).toHaveCount(0);
}

async function expectDialogControlOwnership(dialog: Locator): Promise<void> {
  await expect(dialog).toBeVisible();
  const ownership = await dialog.evaluate((element) =>
    Array.from(element.querySelectorAll<HTMLLabelElement>("label[for]"), (label) => ({
      label: label.textContent,
      targetId: label.htmlFor,
      matches: Array.from(element.ownerDocument.querySelectorAll("[id]")).filter(
        (candidate) => candidate.id === label.htmlFor,
      ).length,
      belongsToDialog: label.control !== null && element.contains(label.control),
    })),
  );
  expect(ownership.length).toBeGreaterThan(0);
  for (const control of ownership) {
    expect(control).toMatchObject({ matches: 1, belongsToDialog: true });
  }
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
  let catalogUnavailable = false;
  let pendingCatalog: Promise<void> | undefined;
  let completeCatalog: (() => void) | undefined;
  let pendingDownload: Promise<void> | undefined;
  let completeDownload: (() => void) | undefined;
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));

  async function start(): Promise<void> {
    const plugins = new PluginRuntime({
      store: new PluginStore(root),
      catalog: {
        list: async (): Promise<readonly OfficialPluginEntry[]> => {
          await pendingCatalog;
          if (catalogUnavailable) throw new Error("The plugin catalog could not be reached.");
          return [
            {
              manifest: available.manifest,
              sha256: available.sha256,
              downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/42",
            },
          ];
        },
        download: async (): Promise<{ bytes: Uint8Array; sha256: string }> => {
          downloads += 1;
          await pendingDownload;
          return available;
        },
      },
      // A lifecycle operation must never request host restart.
      restart: (): void => {
        restarts += 1;
      },
    });
    const backend = createKafkaBackend({ profileStore: profiles, plugins });
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
    await expect(page.getByRole("menuitem", { name: "Connect via EDA", exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByRole("menuitem", { name: "Existing Kafka cluster", exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    await openPlugins(page);
    const card = page.getByRole("region", { name: "EDA Connector", exact: true });
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await approvePlugin(page, "Install plugin", info.outputPath("plugin-package-review.png"));
    await expect(card).toContainText(`Active version ${fixtures.current.manifest.version}`);
    expect(downloads).toBe(1);
    await page.screenshot({ path: info.outputPath("plugin-installed.png") });
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Connect via EDA", exact: true }).click();
    const capture = page.getByRole("dialog", { name: "Connect via EDA" });
    await expectDialogControlOwnership(capture);
    await expect(capture.getByLabel("EDA API URL")).toBeVisible();
    await expect(capture.getByLabel("EDA username")).toBeVisible();
    await page.screenshot({ path: info.outputPath("installed-eda-capture.png") });
    await page.keyboard.press("Escape");
    await expect(capture).toHaveCount(0);
    await openPlugins(page);
    available = fixtures.update;
    await page.getByRole("button", { name: "Check for updates", exact: true }).click();
    await card
      .getByRole("button", { name: `Update to ${available.manifest.version}`, exact: true })
      .click();
    await approvePlugin(page, "Update plugin");
    await expect(card).toContainText(`Active version ${available.manifest.version}`);
    available = fixtures.broken;
    await page.getByRole("button", { name: "Check for updates", exact: true }).click();
    pendingDownload = new Promise<void>((complete) => {
      completeDownload = complete;
    });
    await card
      .getByRole("button", { name: `Update to ${available.manifest.version}`, exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "Cancel package acquisition", exact: true }),
    ).toBeVisible();
    await expect.poll(() => downloads).toBe(3);
    await expect(card.getByRole("button", { name: "Remove", exact: true })).toBeEnabled();
    await page.getByRole("button", { name: "Cancel package acquisition", exact: true }).click();
    await expect(page.getByText(/Package acquisition:.*cancelled/u)).toBeVisible();
    completeDownload?.();
    pendingDownload = undefined;
    await expect(page.getByRole("dialog", { name: "Review plugin", exact: true })).toHaveCount(0);
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await card
      .getByRole("button", { name: `Update to ${available.manifest.version}`, exact: true })
      .click();
    await approvePlugin(page, "Update plugin");
    await expect(card).toContainText("previous version was restored");
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await page.screenshot({ path: info.outputPath("plugin-update-recovered.png") });

    // An optional lookup must not hold local cleanup hostage. Exercise the real
    // runtime and typed host while its catalog source cannot finish its request.
    pendingCatalog = new Promise<void>((complete) => {
      completeCatalog = complete;
    });
    await page.getByRole("button", { name: "Check for updates", exact: true }).click();
    await expect(page.getByText("Checking for plugin updates…", { exact: true })).toBeVisible();
    await expect(card.getByRole("button", { name: "Remove", exact: true })).toBeEnabled();
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
    await expect(card).toContainText("Not installed");
    await expect(page.getByText("Checking for plugin updates…", { exact: true })).toBeVisible();
    expect(downloads).toBe(4);
    catalogUnavailable = true;
    completeCatalog?.();
    await expect(page.getByText(/The plugin catalog is unavailable/u)).toBeVisible();
    await expect(page.getByText(/^Cached catalog · Last checked/u)).toBeVisible();
    await expect(card.getByRole("button", { name: "Install", exact: true })).toBeEnabled();
    catalogUnavailable = false;
    pendingCatalog = undefined;
    // Wait for the modal itself: its controls disappear before its exit transition
    // releases the keyboard to Preferences.
    await expect(
      page.locator('[role="dialog"][aria-labelledby="change-plugin-title"]'),
    ).toHaveCount(0);
    // Removal deletes the button that opened the confirmation. Give the underlying
    // dialog keyboard focus before Escape rather than racing its focus restoration.
    const preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.press("Escape");
    await expect(preferences).toHaveCount(0);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Connect via EDA", exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByRole("menuitem", { name: "Existing Kafka cluster", exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    available = fixtures.update;
    await openPlugins(page);
    await expect(
      page.getByRole("button", { name: "Check for updates", exact: true }),
    ).toBeEnabled();
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await approvePlugin(page, "Install plugin");
    await expect(card).toContainText(`Active version ${fixtures.update.manifest.version}`);
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Connect via EDA", exact: true }).click();
    await expectDialogControlOwnership(capture);
    await expect(capture.getByLabel("EDA API URL")).toBeVisible();
    await page.keyboard.press("Escape");
    expect(
      await page.evaluate(
        () => (globalThis as typeof globalThis & { lifecycleDocument: string }).lifecycleDocument,
      ),
    ).toBe(documentId);
    expect(restarts).toBe(0);
    expect(downloads).toBe(5);
    expect(errors).toEqual([]);
  } finally {
    completeCatalog?.();
    completeDownload?.();
    await page.goto("about:blank");
    await launch?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("reviews signed files, preserves a working plugin after a failed update, and reinstalls from cache offline", async ({
  page,
}, info) => {
  test.setTimeout(120_000);
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1") {
    await promisify(execFile)(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin"], {
      maxBuffer: 4 * 1_048_576,
    });
  }
  const fixtures = await pluginPackageFixtures();
  const publisher = pluginPublisherFixture();
  const portable = (bytes: Uint8Array): Uint8Array =>
    signPortablePluginPackage(
      bytes,
      publisher.publishers[0]!.keyId,
      Buffer.from(publisher.encodedKey, "base64").toString(),
    );
  let selected = portable(fixtures.current.bytes);
  const root = await mkdtemp(join(tmpdir(), "streamskope-offline-plugin-"));
  let launch: RunningWebDevelopment | undefined;
  let catalogLookups = 0;
  let downloads = 0;
  let restarts = 0;
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  // A test-only chooser and public registry exercise the actual typed host and
  // signature verifier. This does not claim to exercise an operating-system dialog.
  const plugins = new PluginRuntime({
    store: new PluginStore(root, { trustedPublishers: publisher.publishers }),
    choosePackageFile: (): Promise<Uint8Array> => Promise.resolve(selected),
    catalog: {
      list: (): Promise<readonly OfficialPluginEntry[]> => {
        catalogLookups++;
        return Promise.reject(new Error("GitHub is unavailable in this fixture."));
      },
      download: (): Promise<{ bytes: Uint8Array; sha256: string }> => {
        downloads++;
        return Promise.reject(new Error("No remote package download is allowed."));
      },
    },
    restart: (): void => {
      restarts++;
    },
  });
  try {
    await plugins.start();
    const backend = createKafkaBackend({ profileStore: createBrowserKafkaProfileStore(), plugins });
    launch = await launchWebDevelopment({
      backend: Object.assign(backend, { pluginAsset: plugins.rendererAsset.bind(plugins) }),
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    await expectWorkbenchReady(page);
    const documentId = await page.evaluate(() => {
      const id = crypto.randomUUID();
      (globalThis as typeof globalThis & { lifecycleDocument: string }).lifecycleDocument = id;
      return id;
    });
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    await page.getByRole("tab", { name: "Plugins", exact: true }).click();
    await expect(page.getByText(/The plugin catalog is unavailable/u)).toBeVisible();
    const lookups = catalogLookups;
    const file = page.getByRole("button", { name: "Install from file", exact: true });
    await expect(file).toBeEnabled();
    await file.click();
    const review = page.getByRole("dialog", { name: "Review plugin", exact: true });
    await expect(review).toContainText("Signed local file");
    await expect(review).toContainText(publisher.publishers[0]!.name);
    await review.getByRole("button", { name: "Cancel", exact: true }).click();
    expect((await plugins.list()).plugins).toEqual([]);
    await file.click();
    await approvePlugin(page, "Install plugin", info.outputPath("plugin-file-review.png"));
    const card = page.getByRole("region", { name: "EDA Connector", exact: true });
    await expect(card).toContainText(`Active version ${fixtures.current.manifest.version}`);
    const installed = (await plugins.list()).plugins[0]!.activationId;
    await page.getByRole("button", { name: /^Plugin download settings/u }).click();
    await expect(
      page.getByText(/System proxy discovery and custom proxies require the desktop app/u),
    ).toBeVisible();
    await page.getByRole("checkbox", { name: "Offline plugin downloads", exact: true }).click();
    await page.getByRole("button", { name: "Save settings", exact: true }).click();
    await expect(page.getByText(/Plugin downloads are offline/u)).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Check for updates", exact: true }),
    ).toBeDisabled();
    await expect(file).toBeEnabled();
    await page.screenshot({
      path: info.outputPath("plugin-offline-settings.png"),
      animations: "disabled",
    });
    await page.getByRole("button", { name: /^Plugin download settings/u }).click();

    await file.click();
    await expect(review).toContainText("version and content are already installed");
    await expect(review.getByRole("button", { name: "Install plugin", exact: true })).toHaveCount(
      0,
    );
    await review.getByRole("button", { name: "Close", exact: true }).click();
    expect((await plugins.list()).plugins[0]!.activationId).toBe(installed);

    selected = portable(fixtures.broken.bytes);
    await file.click();
    await approvePlugin(page, "Update plugin");
    await expect(card).toContainText("previous version was restored");
    await expect(card).toContainText(`Active version ${fixtures.current.manifest.version}`);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
    await expect(card).toHaveCount(0);
    const cached = page.getByRole("region", {
      name: `Cached EDA Connector ${fixtures.current.manifest.version}`,
      exact: true,
    });
    await expect(cached).toBeVisible();
    await cached.scrollIntoViewIfNeeded();
    await page.screenshot({
      path: info.outputPath("plugin-offline-cache.png"),
      animations: "disabled",
    });
    await cached.getByRole("button", { name: "Use cached package", exact: true }).click();
    await expect(review).toContainText("Verified local cache");
    await expect(review).toContainText(fixtures.current.manifest.version);
    await approvePlugin(page, "Install plugin");
    await expect(card).toContainText(`Active version ${fixtures.current.manifest.version}`);
    expect(catalogLookups).toBe(lookups);
    expect(downloads).toBe(0);
    await expect(
      page.locator('[role="dialog"][aria-labelledby="change-plugin-title"]'),
    ).toHaveCount(0);
    const preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.press("Escape");
    await expect(preferences).toHaveCount(0);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Connect via EDA", exact: true }).click();
    await expectDialogControlOwnership(page.getByRole("dialog", { name: "Connect via EDA" }));
    expect(
      await page.evaluate(
        () => (globalThis as typeof globalThis & { lifecycleDocument: string }).lifecycleDocument,
      ),
    ).toBe(documentId);
    expect(restarts).toBe(0);
    expect(errors).toEqual([]);
  } finally {
    await page.goto("about:blank");
    await launch?.close();
    await rm(root, { recursive: true, force: true });
  }
});
