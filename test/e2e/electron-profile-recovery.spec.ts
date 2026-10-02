import { cp, mkdtemp, readFile, realpath, rename, rm } from "node:fs/promises";
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
  test.setTimeout(180_000);
  expect(process.env.STREAMSKOPE_RENDERER_URL).toBeUndefined();
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  const root = await realpath(await mkdtemp(join(tmpdir(), "streamskope-native-recovery-")));
  const active = join(root, "active");
  const backup = join(root, "backup");
  const require = createRequire(resolve("package.json"));
  const previousExecutable = process.env.STREAMSKOPE_UPGRADE_FROM_EXECUTABLE;
  let application: ElectronApplication | undefined;
  let seeded: SeededFixtureTopic | undefined;
  const launch = (previous = false): Promise<ElectronApplication> =>
    electron.launch({
      executablePath:
        previous && previousExecutable ? previousExecutable : (require("electron") as string),
      args: [
        ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
        `--user-data-dir=${active}`,
        // Load package.json as Electron would in a package, preserving the app's credential identity.
        ...(previous && previousExecutable ? [] : [resolve(".")]),
      ],
    });
  try {
    seeded = await provisionSeededFixtureTopic();
    application = await launch(previousExecutable !== undefined);
    expect(await application.evaluate(({ app }) => app.getPath("userData"))).toBe(active);
    const baseline = await application.evaluate(({ app }) => ({
      name: app.getName(),
      version: app.getVersion(),
      electron: process.versions.electron,
    }));
    const protection = await application.evaluate(async ({ safeStorage }) => ({
      available: await safeStorage.isAsyncEncryptionAvailable(),
      backend:
        process.platform === "linux" ? safeStorage.getSelectedStorageBackend() : process.platform,
    }));
    expect(protection.available).toBe(true);
    expect(["basic_text", "unknown"]).not.toContain(protection.backend);
    let page = await application.firstWindow();
    await connectElectronToFixture(page, config, fixture);
    if (previousExecutable !== undefined) {
      await application.close();
      application = undefined;
      await cp(active, join(root, "pre-upgrade"), { recursive: true, preserveTimestamps: true });
      application = await launch();
      expect(await application.evaluate(({ app }) => app.getName())).toBe(baseline.name);
      expect(await application.evaluate(({ app }) => app.getPath("userData"))).toBe(active);
      page = await application.firstWindow();
      await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
      await expect(page.getByLabel("Connection status")).toContainText("Connected");
    }
    const candidateRuntime = await application.evaluate(({ app }) => ({
      version: app.getVersion(),
      electron: process.versions.electron,
    }));
    await openTopicDetail(page, seeded.config.topic);
    await page
      .getByRole("button", { name: `Stop tail ${seeded.config.topic}`, exact: true })
      .click();
    const planningMode = page.getByRole("combobox", { name: "Read mode" });
    await expect(planningMode).toBeEnabled();
    await planningMode.click();
    await page.getByRole("option", { name: "Time window", exact: true }).click();
    await page.getByRole("combobox", { name: "Time interval" }).click();
    await page.getByRole("option", { name: "Custom interval" }).click();
    const startTime = new Date(Date.now() - 600_000).toISOString();
    const endTime = new Date(Date.now() + 60_000).toISOString();
    await page.getByRole("textbox", { name: "Start time (inclusive)" }).fill(startTime);
    await page.getByRole("textbox", { name: "End time (exclusive)" }).fill(endTime);
    await page.getByRole("button", { name: "Show message filters" }).click();
    await page.getByRole("textbox", { name: "Key contains" }).fill("streamskope-seed");
    await page.getByRole("button", { name: "Saved queries" }).click();
    let queries = page.getByRole("dialog", { name: "Saved queries" });
    await queries.getByRole("textbox", { name: "Query name" }).fill("Native incident");
    await queries.getByRole("combobox", { name: "Local connection profile" }).click();
    await page.getByRole("option", { name: "Electron local aio" }).click();
    await queries.getByRole("button", { name: "Save current as new" }).click();
    await expect(queries).toContainText("Query saved.");
    await queries.getByRole("button", { name: "Close", exact: true }).click();
    await application.close();
    application = undefined;

    // Follow the guide: quit, copy the entire directory, keep the credential context.
    await cp(active, backup, { recursive: true, preserveTimestamps: true });
    const saved = await readFile(join(backup, "profiles/kafka-profiles.json"), "utf8");
    expect(saved).not.toContain(config.oauthClientSecret);
    expect(saved).not.toContain("BEGIN CERTIFICATE");
    const queryBackup = await readFile(join(backup, "queries/kafka-queries.json"), "utf8");
    expect(queryBackup).not.toContain(config.oauthClientSecret);
    expect(JSON.parse(queryBackup)).toMatchObject({
      schemaVersion: 1,
      queries: [
        {
          name: "Native incident",
          configuration: {
            request: {
              topic: seeded.config.topic,
              mode: "time-window",
              startTimeMs: Date.parse(startTime),
              endTimeMs: Date.parse(endTime),
            },
            filters: { key: "streamskope-seed" },
          },
        },
      ],
    });

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
    await page.getByRole("button", { name: "Saved queries" }).click();
    queries = page.getByRole("dialog", { name: "Saved queries" });
    await queries.getByRole("combobox", { name: "Saved query" }).click();
    await page.getByRole("option", { name: "Native incident" }).click();
    await queries.getByRole("button", { name: "Delete selected" }).click();
    await queries.getByRole("button", { name: "Delete query", exact: true }).click();
    await expect(queries).toContainText("Query deleted.");
    await application.close();
    application = undefined;
    await rename(active, join(root, "preserved-after-change"));
    await cp(backup, active, { recursive: true, preserveTimestamps: true });

    application = await launch();
    page = await application.firstWindow();
    await page.getByRole("button", { name: "Saved queries" }).click();
    queries = page.getByRole("dialog", { name: "Saved queries" });
    await queries.getByRole("combobox", { name: "Saved query" }).click();
    await page.getByRole("option", { name: "Native incident" }).click();
    await queries.getByRole("button", { name: "Open query" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const readMode = page.getByRole("combobox", { name: "Read mode" });
    await expect(readMode).toBeEnabled();
    await expect(readMode).toContainText("Time window");
    await expect(page.getByRole("textbox", { name: "Start time (inclusive)" })).toHaveValue(
      startTime,
    );
    await expect(page.getByRole("textbox", { name: "End time (exclusive)" })).toHaveValue(endTime);
    await page
      .getByRole("button", { name: `Load messages ${seeded.config.topic}`, exact: true })
      .click();
    await expect(readMode).toBeEnabled({ timeout: 20_000 });
    await expect(page.getByLabel("Active fetch request")).toContainText("→");
    await page.getByRole("button", { name: "Show message filters" }).click();
    await expect(page.getByRole("textbox", { name: "Key contains" })).toHaveValue(
      "streamskope-seed",
    );
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
        baseline,
        candidateVersion: candidateRuntime.version,
        electronVersion: candidateRuntime.electron,
        previousExecutableUsed: previousExecutable !== undefined,
        upgradedProfileReconnected: previousExecutable === undefined ? "not-run" : true,
        savedQueryRestored: true,
        explicitAbsoluteBoundsRestored: true,
        restoredProfileReconnected: true,
        credentialsReentered: false,
        historicalFilteredExportVerified: true,
      }),
      contentType: "application/json",
    });
  } finally {
    await application?.close();
    await seeded?.dispose();
    await rm(root, { recursive: true, force: true });
  }
});
