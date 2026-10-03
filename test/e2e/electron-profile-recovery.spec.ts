import { cp, mkdtemp, readFile, realpath, rename, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { release, tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
  _electron as electron,
  expect,
  test,
  type ElectronApplication,
  type Page,
} from "@playwright/test";

import { installNativeRelease, readRecoveryPlan } from "../../tools/check/native-installers";
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
import { startProtectedStorageSession } from "../support/protected-storage-session";
import { openProfileAction, openTopicDetail } from "../support/workbench-browser";

// Real credentials are entered in the UI; never retain DOM traces.
test.use({ trace: "off", screenshot: "off", video: "off" });

async function reconnectSavedProfile(
  page: Page,
  phase: string,
  sensitiveValues: readonly string[],
): Promise<{ readonly phase: string; readonly elapsedMs: number }> {
  const started = performance.now();
  await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
  try {
    // OAuth, native credential access and broker discovery run sequentially.
    // This is an operation completion bound, not a latency qualification budget.
    await expect(page.getByLabel("Connection status")).toContainText("Connected", {
      timeout: 20_000,
    });
    return { phase, elapsedMs: Math.round(performance.now() - started) };
  } catch (error) {
    const alerts = await page.getByRole("alert").allTextContents();
    const redact = (value: string): string =>
      sensitiveValues
        .reduce(
          (text, secret) => (secret.length === 0 ? text : text.replaceAll(secret, "[redacted]")),
          value,
        )
        .replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/gu, "[redacted token]")
        .slice(0, 512);
    await test.info().attach("native-connection-failure", {
      body: JSON.stringify({
        phase,
        elapsedMs: Math.round(performance.now() - started),
        alerts: alerts.slice(0, 4).map(redact),
      }),
      contentType: "application/json",
    });
    throw error;
  }
}

// Explicit operator rehearsal: requires a real, unlocked OS credential service
// and the disposable AIO fixture. Never substitute deterministic safeStorage.
test("restores a full backup with native credential protection and reconnects", async () => {
  test.skip(
    process.env.STREAMSKOPE_NATIVE_RECOVERY !== "1",
    "Opt in with an unlocked OS credential service and disposable Kafka fixture.",
  );
  test.setTimeout(360_000);
  expect(process.env.STREAMSKOPE_RENDERER_URL).toBeUndefined();
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  const root = await realpath(await mkdtemp(join(tmpdir(), "streamskope-native-recovery-")));
  const active = join(root, "active");
  const backup = join(root, "backup");
  const require = createRequire(resolve("package.json"));
  const previousExecutable = process.env.STREAMSKOPE_UPGRADE_FROM_EXECUTABLE;
  const planPath = process.env.STREAMSKOPE_NATIVE_RECOVERY_PLAN;
  const plan = planPath === undefined ? undefined : await readRecoveryPlan(planPath);
  let installed: Awaited<ReturnType<typeof installNativeRelease>> | undefined;
  let initialInstall: typeof installed;
  let protectedStorage: Awaited<ReturnType<typeof startProtectedStorageSession>> | undefined;
  let application: ElectronApplication | undefined;
  let seeded: SeededFixtureTopic | undefined;
  const reconnects: { readonly phase: string; readonly elapsedMs: number }[] = [];
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  delete environment.ELECTRON_RUN_AS_NODE;
  delete environment.STREAMSKOPE_RENDERER_URL;
  environment.NODE_ENV = "production";
  const launch = (previous = false): Promise<ElectronApplication> =>
    electron.launch({
      env: environment,
      executablePath:
        installed?.executablePath ??
        (previous && previousExecutable ? previousExecutable : (require("electron") as string)),
      args: [
        ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
        ...(protectedStorage?.electronArguments ?? []),
        `--user-data-dir=${active}`,
        // Load package.json as Electron would in a package, preserving the app's credential identity.
        ...(installed !== undefined || (previous && previousExecutable) ? [] : [resolve(".")]),
      ],
    });
  try {
    protectedStorage = await startProtectedStorageSession(join(root, "protected-storage"));
    Object.assign(environment, protectedStorage.environment);
    installed = plan === undefined ? undefined : await installNativeRelease(plan, plan.from);
    initialInstall = installed;
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
    if (plan !== undefined) expect(baseline.version).toBe(plan.from.version);
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

    // Copy the previous release's full backup before the installer replacement.
    await cp(active, join(root, "pre-upgrade"), { recursive: true, preserveTimestamps: true });
    application = await launch(previousExecutable !== undefined);
    page = await application.firstWindow();
    reconnects.push(
      await reconnectSavedProfile(page, "baseline restart", [
        config.oauthClientSecret,
        config.seedPayload,
      ]),
    );
    await application.close();
    application = undefined;
    if (plan !== undefined) installed = await installNativeRelease(plan, plan.to);
    application = await launch();
    expect(await application.evaluate(({ app }) => app.getName())).toBe(baseline.name);
    expect(await application.evaluate(({ app }) => app.getPath("userData"))).toBe(active);
    const candidateRuntime = await application.evaluate(({ app, safeStorage }) => ({
      version: app.getVersion(),
      electron: process.versions.electron,
      protection:
        process.platform === "linux" ? safeStorage.getSelectedStorageBackend() : process.platform,
    }));
    if (plan !== undefined) expect(candidateRuntime.version).toBe(plan.to.version);
    expect(candidateRuntime.protection).toBe(protection.backend);
    if (initialInstall !== undefined) {
      expect(installed?.executablePath).toBe(initialInstall.executablePath);
      expect(installed?.archiveSha256).not.toBe(initialInstall.archiveSha256);
    }
    page = await application.firstWindow();
    await page.getByRole("button", { name: "Saved queries" }).click();
    queries = page.getByRole("dialog", { name: "Saved queries" });
    await queries.getByRole("combobox", { name: "Saved query" }).click();
    await page.getByRole("option", { name: "Native incident" }).click();
    await queries.getByRole("button", { name: "Open query" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    reconnects.push(
      await reconnectSavedProfile(page, "after installer replacement", [
        config.oauthClientSecret,
        config.seedPayload,
      ]),
    );
    await expect(page.getByRole("textbox", { name: "Start time (inclusive)" })).toHaveValue(
      startTime,
    );
    await expect(page.getByRole("textbox", { name: "End time (exclusive)" })).toHaveValue(endTime);
    await application.close();
    application = undefined;

    // Follow the guide: keep the complete older-release backup and credential context.
    await cp(join(root, "pre-upgrade"), backup, { recursive: true, preserveTimestamps: true });
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
    reconnects.push(
      await reconnectSavedProfile(page, "after backup restoration", [
        config.oauthClientSecret,
        config.seedPayload,
      ]),
    );
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
    const evidence = {
      platform: process.platform,
      architecture: process.arch,
      osRelease: release(),
      credentialService:
        process.platform === "darwin"
          ? "macOS Keychain"
          : process.platform === "win32"
            ? "Windows DPAPI"
            : protection.backend,
      protection: protection.backend,
      baseline,
      candidateVersion: candidateRuntime.version,
      electronVersion: candidateRuntime.electron,
      previousExecutableUsed: plan !== undefined || previousExecutable !== undefined,
      installerReplacement:
        plan === undefined
          ? "not-run"
          : {
              method: installed!.method,
              baselineArchiveSha256: initialInstall!.archiveSha256,
              candidateArchiveSha256: installed!.archiveSha256,
              installationDirectoryReused: true,
            },
      baselineRestartReconnected: true,
      upgradedProfileReconnected:
        plan === undefined && previousExecutable === undefined ? "not-run" : true,
      upgradedSavedQueryRetained: true,
      candidateRestarted: true,
      savedQueryRestored: true,
      backupCreatedWithBaselineRelease: true,
      explicitAbsoluteBoundsRestored: true,
      restoredProfileReconnected: true,
      credentialsReentered: false,
      reconnects,
      historicalFilteredExportVerified: true,
      scope:
        "Same OS user and credential service; complete backup restored after installer replacement. Does not transfer credentials to another account or replace a lost OS keyring.",
    };
    await writeFile(
      resolve("test-results/electron/native-recovery.json"),
      `${JSON.stringify(evidence, null, 2)}\n`,
    );
    await test.info().attach("native-recovery", {
      body: JSON.stringify(evidence),
      contentType: "application/json",
    });
  } finally {
    try {
      await application?.close();
    } finally {
      try {
        await seeded?.dispose();
      } finally {
        try {
          await protectedStorage?.dispose();
        } finally {
          await rm(root, { recursive: true, force: true, maxRetries: 3 });
        }
      }
    }
  }
});
