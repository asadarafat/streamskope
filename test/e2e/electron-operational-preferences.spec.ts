import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type ElectronApplication, type Page } from "@playwright/test";

import { KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS } from "../../src/features/kafka/contracts";
import {
  findSensitiveArtifactPaths,
  PRIVATE_RUNBOOK_ARTIFACT_SENTINEL,
  RULE_EXPRESSION_ARTIFACT_SENTINEL,
} from "../../tools/sensitive-artifact-policy";
import {
  buildElectronSmoke,
  buildRenderer,
  connectElectronToFixture,
  launchProfileApplication,
} from "../support/electron-application";
import { loadFixtureConnection, provisionSeededFixtureTopic } from "../support/kafka-fixture";
import { fetchTopicMessages, openTopicTask } from "../support/workbench-browser";

const repositoryRoot = process.cwd();
const runbookUrl = "https://runbooks.example.test/kafka/latency";

async function createIsolationRule(page: Page, topic: string): Promise<void> {
  await openTopicTask(page, "Rules");
  await page.getByRole("button", { name: "Create rule" }).click();
  await page.getByRole("textbox", { name: "Rule name" }).fill("Reset isolation warning");
  await page
    .getByRole("textbox", { name: "JSONPath expression" })
    .fill(RULE_EXPRESSION_ARTIFACT_SENTINEL);
  await page.getByRole("textbox", { name: "Topic filter" }).fill(topic);
  await page.getByRole("combobox", { name: "Severity" }).click();
  await page.getByRole("option", { exact: true, name: "warn" }).click();
  await page.getByRole("button", { name: "Save rule" }).click();
  await expect(page.getByRole("heading", { name: "Reset isolation warning" })).toBeVisible();
}

async function saveDurablePreferences(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Preferences" }).click();
  const preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
  await expect(preferences.getByText("Durable storage")).toBeVisible();
  await preferences.getByRole("combobox", { name: "Default fetch mode" }).click();
  await page.getByRole("option", { name: "First N" }).click();
  await preferences.getByLabel("Default maximum results").fill("25");
  await preferences.getByLabel("Stream queue depth").fill("120");
  await preferences.getByLabel("Stream batch size").fill("10");
  await preferences.getByLabel("Stream delivery interval").fill("5");
  await preferences.getByLabel("Monitor history samples").fill("10");
  await preferences.getByLabel("Default probe records").fill("5");
  await preferences.getByLabel("Latency runbook URL").fill(PRIVATE_RUNBOOK_ARTIFACT_SENTINEL);
  await preferences.getByRole("combobox", { name: "Rule Activity threshold" }).click();
  await page.getByRole("option", { name: "warn" }).click();
  await preferences.getByRole("button", { name: "Save preferences" }).click();
  await expect(preferences.getByRole("status")).toContainText(
    "Saved durable workbench preferences.",
  );
  await preferences.getByLabel("Latency runbook URL").fill(runbookUrl);
  await preferences.getByRole("button", { name: "Save preferences" }).click();
  await expect(preferences.getByRole("status")).toContainText(
    "Saved durable workbench preferences.",
  );
  await preferences.getByRole("button", { name: "Close" }).click();
}

test("restores, applies and exactly resets durable operational preferences in Electron", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(120_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-operational-preferences-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  const preferencePath = join(userDataPath, "preferences", "kafka-operational-preferences.json");
  const rulePath = join(userDataPath, "rules", "kafka-rules.json");
  const hostLogDirectory = join(outputDirectory, "host-logs");
  const externalUrlLogPath = join(hostLogDirectory, "external-urls.log");
  const seededFixtureTopic = await provisionSeededFixtureTopic();
  const config = seededFixtureTopic.config;
  const fixture = await loadFixtureConnection();
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    await mkdir(hostLogDirectory, { recursive: true });
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(
      outputDirectory,
      rendererUrl,
      userDataPath,
      "available",
      externalUrlLogPath,
    );
    let page = await application.firstWindow();
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await connectElectronToFixture(page, config, fixture);
    await page.getByRole("button", { name: config.topic, exact: true }).click();
    await createIsolationRule(page, config.topic);
    await saveDurablePreferences(page);
    const ruleBeforeReset = await readFile(rulePath, "utf8");
    expect(JSON.parse(await readFile(preferencePath, "utf8"))).toMatchObject({
      preferences: {
        fetch: { maxMessages: 25, mode: "earliest" },
        latency: { messageCount: 5, runbookUrl },
        rules: { logLevel: "warn" },
        stream: { batchSize: 10, historySamples: 10, intervalMs: 5, queueDepth: 120 },
      },
    });

    await application.close();
    application = await launchProfileApplication(
      outputDirectory,
      rendererUrl,
      userDataPath,
      "available",
      externalUrlLogPath,
    );
    page = await application.firstWindow();
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });

    await page.getByRole("button", { name: "Preferences" }).click();
    let preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await expect(preferences.getByText("Durable storage")).toBeVisible();
    await expect(preferences.getByRole("combobox", { name: "Default fetch mode" })).toContainText(
      "First N",
    );
    await expect(preferences.getByLabel("Default maximum results")).toHaveValue("25");
    await expect(preferences.getByLabel("Latency runbook URL")).toHaveValue(runbookUrl);
    expect((await new AxeBuilder({ page }).setLegacyMode().analyze()).violations).toEqual([]);
    await preferences.getByRole("heading", { name: "Fetch and stream" }).scrollIntoViewIfNeeded();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-operational-preferences-restored-light.png"),
    });
    await preferences.getByRole("button", { name: "Close" }).click();

    await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const fixtureTopic = page.getByRole("button", { name: config.topic, exact: true });
    await fixtureTopic.click();
    await openTopicTask(page, "Messages");
    await expect(page.getByRole("combobox", { name: "Read mode" })).toContainText("First N");
    await expect(page.getByRole("combobox", { name: "Record limit" })).toContainText("25");
    await expect(page.getByLabel("Consumption status")).toContainText("Snapshot complete", {
      timeout: 15_000,
    });
    await page.getByRole("combobox", { name: "Read mode" }).click();
    await page.getByRole("option", { name: "Tail" }).click();
    await fetchTopicMessages(page, config.topic);
    await expect(page.getByLabel("Consumption status")).toContainText("Streaming", {
      timeout: 15_000,
    });
    const messageGrid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(messageGrid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();

    await openTopicTask(page, "Latency");
    const latency = page.getByRole("region", { name: "Latency workspace" });
    await expect(latency.getByText(runbookUrl, { exact: true })).toBeVisible();
    const rendererUrlBeforeRunbook = page.url();
    const windowCountBeforeRunbook = application.windows().length;
    await latency.getByRole("button", { name: "Open runbook" }).click();
    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      "Runbook request accepted by the platform.",
    );
    await expect
      .poll(async () => readFile(externalUrlLogPath, "utf8").catch(() => ""))
      .toBe(`${runbookUrl}\n`);
    expect(page.url()).toBe(rendererUrlBeforeRunbook);
    expect(application.windows()).toHaveLength(windowCountBeforeRunbook);

    await page.getByRole("button", { name: "Preferences" }).click();
    preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.getByRole("button", { name: "Reset workbench preferences" }).click();
    const confirmation = page.getByRole("dialog", {
      name: "Reset Workbench Preferences?",
    });
    await expect(confirmation).toContainText(
      "Profiles, rules, templates, topic history, and Kafka data are unaffected.",
    );
    await confirmation.getByRole("button", { name: "Reset workbench preferences" }).click();
    await expect(preferences.getByLabel("Default maximum results")).toHaveValue("1000");
    await expect(preferences.getByLabel("Latency runbook URL")).toHaveValue("");
    await expect(preferences.getByRole("status")).toContainText(
      "Factory workbench preferences confirmed.",
    );
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).setLegacyMode().analyze()).violations).toEqual([]);
    await preferences.getByRole("heading", { name: "Fetch and stream" }).scrollIntoViewIfNeeded();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-operational-preferences-reset-dark.png"),
    });
    await preferences.getByRole("button", { name: "Close" }).click();

    await openTopicTask(page, "Messages");
    await expect(page.getByLabel("Consumption status")).toContainText("Streaming");
    const maximumResults = page.getByRole("combobox", { name: "Record limit" });
    await expect(maximumResults).toBeVisible();
    await expect(maximumResults).toContainText("25");
    await expect(messageGrid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await page.getByRole("button", { name: "Stop tail" }).click();
    await expect(page.getByLabel("Consumption status")).toContainText("Stopped");
    await expect(maximumResults).toContainText("1,000");
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    expect(await readFile(rulePath, "utf8")).toBe(ruleBeforeReset);
    expect(JSON.parse(await readFile(preferencePath, "utf8"))).toMatchObject({
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
    });
    await openTopicTask(page, "Rules");
    await expect(page.getByRole("heading", { name: "Reset isolation warning" })).toBeVisible();
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await expect(
      findSensitiveArtifactPaths(
        [join(userDataPath, "preferences"), hostLogDirectory, testInfo.outputDir],
        [
          config.oauthClientSecret,
          `${config.oauthClientSecret}-invalid`,
          config.seedPayload,
          RULE_EXPRESSION_ARTIFACT_SENTINEL,
          PRIVATE_RUNBOOK_ARTIFACT_SENTINEL,
        ],
      ),
    ).resolves.toEqual([]);
  } finally {
    await application?.close();
    await seededFixtureTopic.dispose();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});
