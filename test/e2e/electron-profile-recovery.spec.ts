import { cp, mkdtemp, readFile, rename, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { _electron as electron, expect, test, type ElectronApplication } from "@playwright/test";

import {
  chooseNextElectronSavePath,
  connectElectronToFixture,
} from "../support/electron-application";
import {
  loadFixtureConfig,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
  type SeededFixtureTopic,
} from "../support/kafka-fixture";
import { openProfileAction, openTopicDetail } from "../support/workbench-browser";

// Real credentials are entered in the UI; never retain DOM traces.
test.use({ trace: "off" });

// Explicit operator rehearsal: requires a real, unlocked OS credential service
// and the disposable AIO fixture. Never substitute deterministic safeStorage.
test("restores a full backup with native credential protection and reconnects", async () => {
  test.skip(
    process.env.STREAMSKOPE_NATIVE_RECOVERY !== "1",
    "Opt in with an unlocked OS credential service and disposable Kafka fixture.",
  );
  test.setTimeout(90_000);
  expect(process.env.STREAMSKOPE_RENDERER_URL).toBeUndefined();
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  const root = await mkdtemp(join(tmpdir(), "streamskope-native-recovery-"));
  const active = join(root, "active");
  const backup = join(root, "backup");
  const require = createRequire(resolve("package.json"));
  let application: ElectronApplication | undefined;
  let seeded: SeededFixtureTopic | undefined;
  const launch = (): Promise<ElectronApplication> =>
    electron.launch({
      executablePath: require("electron") as string,
      args: [
        ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
        `--user-data-dir=${active}`,
        resolve("dist/electron/main.cjs"),
      ],
    });
  try {
    seeded = await provisionSeededFixtureTopic();
    application = await launch();
    expect(await application.evaluate(({ app }) => app.getPath("userData"))).toBe(active);
    const protection = await application.evaluate(async ({ safeStorage }) => ({
      available: await safeStorage.isAsyncEncryptionAvailable(),
      backend:
        process.platform === "linux" ? safeStorage.getSelectedStorageBackend() : process.platform,
    }));
    expect(protection.available).toBe(true);
    expect(["basic_text", "unknown"]).not.toContain(protection.backend);
    let page = await application.firstWindow();
    await connectElectronToFixture(page, config, fixture);
    await application.close();
    application = undefined;

    // Follow the guide: quit, copy the entire directory, keep the credential context.
    await cp(active, backup, { recursive: true, preserveTimestamps: true });
    const saved = await readFile(join(backup, "profiles/kafka-profiles.json"), "utf8");
    expect(saved).not.toContain(config.oauthClientSecret);
    expect(saved).not.toContain("BEGIN CERTIFICATE");

    application = await launch();
    page = await application.firstWindow();
    await openProfileAction(page, "Electron local aio", "Delete");
    await page
      .getByRole("dialog", { name: "Delete Kafka profile Electron local aio" })
      .getByRole("button", { name: "Delete profile" })
      .click();
    await expect(
      page.getByRole("button", { name: "Connect profile Electron local aio" }),
    ).toHaveCount(0);
    await application.close();
    application = undefined;
    await rename(active, join(root, "preserved-after-change"));
    await cp(backup, active, { recursive: true, preserveTimestamps: true });

    application = await launch();
    page = await application.firstWindow();
    await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openTopicDetail(page, seeded.config.topic);
    await page
      .getByRole("button", { name: `Stop tail ${seeded.config.topic}`, exact: true })
      .click();
    const readMode = page.getByRole("combobox", { name: "Read mode" });
    await expect(readMode).toBeEnabled();
    await readMode.click();
    await page.getByRole("option", { name: "Time window", exact: true }).click();
    await page
      .getByRole("button", { name: `Load messages ${seeded.config.topic}`, exact: true })
      .click();
    await expect(readMode).toBeEnabled({ timeout: 20_000 });
    await expect(page.getByLabel("Active fetch request")).toContainText("→");
    await page.getByRole("button", { name: "Show message filters" }).click();
    await page.getByRole("textbox", { name: "Key contains" }).fill("streamskope-seed");
    const exportPath = join(root, "incident.json");
    await chooseNextElectronSavePath(application, exportPath);
    await page.getByRole("button", { name: "Export filtered JSON" }).click();
    await expect.poll(() => readFile(exportPath, "utf8").catch(() => null)).not.toBeNull();
    const exported: unknown = JSON.parse(await readFile(exportPath, "utf8"));
    expect(exported).toMatchObject({
      topic: seeded.config.topic,
      exportedMessageCount: 1,
      filters: { key: "streamskope-seed" },
      messages: [{ key: "streamskope-seed", payload: config.seedPayload }],
    });
    await test.info().attach("native-recovery", {
      body: JSON.stringify({
        platform: process.platform,
        architecture: process.arch,
        protection: protection.backend,
        restoredProfileReconnected: true,
        credentialsReentered: false,
        recentWindowFilteredExportVerified: true,
      }),
      contentType: "application/json",
    });
  } finally {
    await application?.close();
    await seeded?.dispose();
    await rm(root, { recursive: true, force: true });
  }
});
