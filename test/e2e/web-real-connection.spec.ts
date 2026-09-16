import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/electron/main";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import {
  PRIVATE_RUNBOOK_ARTIFACT_SENTINEL,
  RULE_EXPRESSION_ARTIFACT_SENTINEL,
} from "../../tools/sensitive-artifact-policy";
import {
  loadFixtureConfig,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
  type SeededFixtureTopic,
} from "../support/kafka-fixture";
import {
  assertPersistentWorkbenchBars,
  collapseActivity,
  expectRawLogEvidence,
  fetchTopicMessages,
  observeBrowserDiagnostics,
  openActivity,
  openProfileAction,
  openProfileActions,
  openTopicTask,
  openWorkbenchResource,
} from "../support/workbench-browser";
import { configureLocalConnection, connectLocalProfile } from "../support/web-profile-workflow";

let launch: RunningWebDevelopment | undefined;
let kafkaBackend: ReturnType<typeof createKafkaBackend> | undefined;
let seededFixtureTopic: SeededFixtureTopic | undefined;

test.use({ trace: "off" });

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolveListen();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected a temporary TCP endpoint.");
  }
  return address.port;
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) {
    return;
  }
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolveClose();
      } else {
        reject(error);
      }
    });
  });
}

async function reservePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await closeServer(server);
  return port;
}

function activeKafkaBackend(): ReturnType<typeof createKafkaBackend> {
  if (kafkaBackend === undefined) {
    throw new Error("Real Kafka backend is unavailable.");
  }
  return kafkaBackend;
}

test.describe("real StreamSkope browser connection", () => {
  test.beforeEach(async () => {
    kafkaBackend = createKafkaBackend();
    launch = await launchWebDevelopment({
      backend: kafkaBackend,
      hostPort: await reservePort(),
      rendererPort: await reservePort(),
      rendererRoot: resolve(process.cwd()),
    });
  });

  test.afterEach(async () => {
    try {
      await launch?.close();
    } finally {
      launch = undefined;
      kafkaBackend = undefined;
      await seededFixtureTopic?.dispose();
      seededFixtureTopic = undefined;
    }
  });

  test("connects, discovers, consumes, and inspects the aio-kafka topic through the browser workflow", async ({
    page,
  }, testInfo) => {
    test.setTimeout(90_000);
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    seededFixtureTopic = await provisionSeededFixtureTopic();
    const config = seededFixtureTopic.config;
    const diagnostics = observeBrowserDiagnostics(page);

    await page.goto(launch.browserUrl);
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    await configureLocalConnection(page);

    const secret = page.getByRole("textbox", {
      name: "OAuth client secret",
      exact: true,
    });
    await expect(secret).toHaveAttribute("type", "password");
    await page.getByRole("button", { name: "Show OAuth client secret" }).click();
    await expect(secret).toHaveAttribute("type", "text");
    expect(
      await secret.evaluate(
        (element, expected) => element instanceof HTMLInputElement && element.value === expected,
        config.oauthClientSecret,
      ),
    ).toBe(true);
    await page.getByRole("button", { name: "Hide OAuth client secret" }).click();
    await expect(secret).toHaveAttribute("type", "password");

    await page.getByRole("button", { name: "Test connection" }).click();
    await expect(
      page.getByText(
        "Connection test passed. No profile was saved and the active connection was unchanged.",
      ),
    ).toBeVisible();

    await page.getByRole("button", { name: "Save profile" }).click();
    await page.getByRole("button", { name: "Connect profile Local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected · Local aio");
    await expect(page.getByRole("navigation", { name: "StreamSkope resources" })).toHaveAttribute(
      "data-active-resource",
      "topics",
    );
    const fixtureTopic = page.getByRole("button", { name: config.topic, exact: true });
    await expect(fixtureTopic).toBeVisible();
    const listedTopicCount =
      (await page.getByRole("grid", { name: "Kafka topics" }).getByRole("row").count()) - 1;
    await fixtureTopic.click();
    await expect(
      page.getByRole("tablist", { name: "Topic sections" }).getByRole("tab", { name: "Messages" }),
    ).toHaveAttribute("aria-selected", "true");
    await expect(page.getByRole("contentinfo")).toContainText(`${listedTopicCount} topics`);

    const workspaceTasks = page.getByRole("tablist", {
      name: "Topic sections",
    });
    await workspaceTasks.getByRole("tab", { name: "Configuration" }).click();
    const configurationGrid = page.getByRole("grid", {
      name: "Topic configuration entries",
    });
    await expect(configurationGrid).toBeVisible({ timeout: 10_000 });
    await page
      .getByRole("searchbox", { name: "Search configuration" })
      .fill("compression.gzip.level");
    await configurationGrid
      .getByRole("gridcell", { name: "compression.gzip.level", exact: true })
      .click();
    const selectedConfiguration = page
      .getByRole("heading", { name: "Selected entry" })
      .locator("..");
    await expect(selectedConfiguration.locator("code")).toHaveText("gzip");
    await expect(selectedConfiguration).not.toContainText("<code>gzip</code>");
    await expect(selectedConfiguration.locator("a")).toHaveCount(0);
    await page.getByRole("searchbox", { name: "Search configuration" }).fill("retention.ms");
    await configurationGrid.getByRole("gridcell", { name: "retention.ms", exact: true }).click();
    const proposedValue = page.getByRole("textbox", { name: "Proposed value" });
    const retainedValue = await proposedValue.inputValue();
    expect(retainedValue.length).toBeGreaterThan(0);
    await page.getByRole("button", { name: "Queue change" }).click();
    await page.getByRole("button", { name: "Dry-run changes" }).click();
    await expect(page.getByRole("button", { name: "Dry-run changes" })).toBeEnabled({
      timeout: 10_000,
    });
    await page.getByRole("button", { name: "Configuration history" }).click();
    const configurationHistory = page.getByRole("dialog", {
      name: "Configuration history",
    });
    await expect(configurationHistory).toContainText("Session-only history");
    await expect(configurationHistory).toContainText("Validate · Succeeded");
    await expect(configurationHistory).toContainText("retention.ms");
    await configurationHistory.getByRole("button", { name: "Close" }).click();
    await page.getByRole("button", { name: "Clear" }).click();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-topic-configuration.png"),
    });
    await workspaceTasks.getByRole("tab", { name: "Messages" }).click();

    await expect(page.getByLabel("Consumption status")).toContainText("Streaming");
    const messageGrid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(messageGrid).toBeVisible();
    const seedPayload = messageGrid.getByText(config.seedPayload, { exact: true }).first();
    await expect(seedPayload).toBeVisible();
    await seedPayload.click();
    const inspector = page.getByRole("complementary", { name: "Message inspector" });
    await expect(inspector.getByRole("tab", { name: "Metadata" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    await inspector.getByRole("tab", { name: "Key" }).click();
    await expect(inspector).toContainText("streamskope-seed");
    await inspector.getByRole("tab", { name: "Value" }).click();
    await expect(inspector).toContainText('"source": "streamskope-fixture"');
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-message-inspector.png"),
    });
    const streamControl = page.getByRole("button", { name: `Stop tail ${config.topic}` });
    await expect(streamControl).toHaveText("Stop tail");
    await streamControl.click();
    await expect(page.getByLabel("Consumption status")).toContainText("Stopped");

    await page.getByRole("combobox", { name: "Read mode" }).click();
    await page.getByRole("option", { name: "First N" }).click();
    await page.getByRole("combobox", { name: "Record limit" }).click();
    await page.getByRole("option", { name: "10", exact: true }).click();
    await page.getByRole("button", { name: `Load messages ${config.topic}` }).click();
    await expect(page.getByLabel("Consumption status")).toContainText("Snapshot complete", {
      timeout: 10_000,
    });
    await expect(inspector).not.toBeVisible();
    await expect(page.getByText("The selected message is no longer retained.")).toHaveCount(0);
    await expect(messageGrid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    expect((await messageGrid.getByRole("row").count()) - 1).toBeLessThanOrEqual(10);

    const activity = await openActivity(page);
    await expect(activity).toContainText("Connect");
    await expectRawLogEvidence(
      activity,
      "Test profile connection",
      "Confirmed checks",
      "Local aio",
    );
    await expectRawLogEvidence(
      activity,
      "Refresh topics",
      `Loaded ${listedTopicCount} authorized Kafka topic${listedTopicCount === 1 ? "" : "s"}.`,
      "Local aio",
    );
    await expect(activity).toContainText("Consume messages");
    await expect(activity).toContainText("Stop consumption");
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await testInfo.attach("redacted-host-activity", {
      body: await activity.innerText(),
      contentType: "text/plain",
    });
    await page.context().tracing.start({
      screenshots: true,
      snapshots: true,
      sources: false,
    });
    await collapseActivity(page);
    await openActivity(page);
    await page.context().tracing.stop({
      path: testInfo.outputPath("real-aio-kafka-sanitized-trace.zip"),
    });
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-connected.png"),
    });
    expect(diagnostics.problems).toEqual([]);
  });

  test("inspects, registers, and permanently removes a schema through the live Registry", async ({
    page,
  }) => {
    test.setTimeout(90_000);
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    const fixture = await loadFixtureConnection();
    if (fixture.schemaRegistryEndpoint === undefined) {
      throw new Error("The owned fixture has no current Schema Registry endpoint evidence.");
    }
    const config = await loadFixtureConfig();
    const subject = `streamskope-e2e-${randomUUID()}`;
    const schema = JSON.stringify({
      fields: [
        { name: "id", type: "string" },
        { default: null, name: "note", type: ["null", "string"] },
      ],
      name: "StreamSkopeE2EEvent",
      namespace: "dev.streamskope.e2e",
      type: "record",
    });

    await page.goto(launch.browserUrl);
    await connectLocalProfile(page);
    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    await navigation.getByRole("button", { name: "Connection Profiles" }).click();
    const profileWorkspace = page.getByRole("region", { name: "Connection profile workspace" });
    await expect(profileWorkspace).toContainText(fixture.schemaRegistryEndpoint);
    await expect(profileWorkspace).toContainText("Profile OAuth bearer token");
    await navigation.getByRole("button", { name: "Schema Registry" }).click();

    await expect(page.getByRole("button", { name: config.schemaSubject })).toBeVisible();
    await page.getByRole("button", { name: config.schemaSubject }).click();
    await expect(page.getByRole("heading", { name: config.schemaSubject })).toBeVisible();
    await expect(page.getByText("StreamSkopeFixtureEvent", { exact: false })).toBeVisible();

    await page.getByRole("button", { name: "Register schema" }).click();
    const registration = page.getByRole("dialog", { name: "Register schema version" });
    await registration.getByRole("textbox", { name: "Subject" }).fill(subject);
    await registration.getByRole("textbox", { name: "Schema" }).fill(schema);
    await registration.getByRole("button", { name: "Check compatibility" }).click();
    await expect(
      registration.getByText("Compatible with the latest registered version."),
    ).toBeVisible();
    await registration.getByRole("button", { name: "Register", exact: true }).click();

    await expect(page.getByRole("heading", { name: subject })).toBeVisible();
    await expect(page.getByText("StreamSkopeE2EEvent", { exact: false })).toBeVisible();
    await page.getByRole("button", { name: "Delete subject" }).click();
    const deletion = page.getByRole("dialog", { name: "Delete schema subject" });
    await deletion.getByRole("combobox", { name: "Deletion mode" }).click();
    await page.getByRole("option", { name: "Permanent delete" }).click();
    await deletion.getByRole("textbox", { name: `Type ${subject} to confirm` }).fill(subject);
    await deletion.getByRole("button", { name: "Delete permanently" }).click();
    await expect(page.getByRole("button", { name: subject })).toHaveCount(0);
  });

  test("evaluates a saved rule against the deterministic aio-kafka seed record", async ({
    page,
  }, testInfo) => {
    test.setTimeout(60_000);
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    seededFixtureTopic = await provisionSeededFixtureTopic();
    const config = seededFixtureTopic.config;
    const diagnostics = observeBrowserDiagnostics(page);
    const expression = RULE_EXPRESSION_ARTIFACT_SENTINEL;

    await page.goto(launch.browserUrl);
    await connectLocalProfile(page);
    const fixtureTopic = page.getByRole("button", { name: config.topic, exact: true });
    await expect(fixtureTopic).toBeVisible();
    await fixtureTopic.click();
    await page.getByRole("button", { name: `Stop tail ${config.topic}` }).click();
    await expect(page.getByLabel("Consumption status")).toContainText("Stopped");
    await openTopicTask(page, "Rules");
    await expect(page.getByRole("status", { name: "Rule storage status" })).toContainText(
      "Session-only rules",
    );
    await page.getByRole("button", { name: "Create rule" }).click();
    await page.getByRole("textbox", { name: "Rule name" }).fill("Fixture source");
    await page.getByRole("textbox", { name: "JSONPath expression" }).fill(expression);
    await page.getByRole("textbox", { name: "Topic filter" }).fill(config.topic);
    await page.getByRole("button", { name: "Save rule" }).click();
    await expect(page.getByRole("heading", { name: "Fixture source" })).toBeVisible();

    await openTopicTask(page, "Messages");
    await page.getByRole("button", { name: `Start tail ${config.topic}` }).click();

    const grid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(grid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await page.getByRole("button", { name: "Show message filters" }).click();
    const activeOnly = page.getByRole("checkbox", { name: "Rule matches only" });
    await activeOnly.check();
    await expect(grid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();

    await grid.getByText(config.seedPayload, { exact: true }).first().click();
    const inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { exact: true, name: "Rules" }).click();
    await expect(inspector).toContainText("Fixture source · Info");
    await inspector.getByRole("tab", { name: "Value" }).click();
    await expect(inspector).toContainText('"source": "streamskope-fixture"');

    const streamControl = page.getByRole("button", { name: `Stop tail ${config.topic}` });
    await expect(streamControl).toHaveText("Stop tail");
    await streamControl.click();
    await expect(page.getByLabel("Consumption status")).toContainText("Stopped");
    const activity = await openActivity(page);
    await expect(activity).toContainText("Create rule");
    await expect(activity).toContainText("Consume messages");
    await expect(activity).not.toContainText(expression);
    await expect(activity).not.toContainText(config.seedPayload);
    await testInfo.attach("live-rule-redacted-host-activity", {
      body: await activity.innerText(),
      contentType: "text/plain",
    });
    expect(diagnostics.problems).toEqual([]);
  });

  test("monitors live aio-kafka delivery through current, terminal and stale browser states", async ({
    page,
  }, testInfo) => {
    test.setTimeout(60_000);
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    seededFixtureTopic = await provisionSeededFixtureTopic();
    const config = seededFixtureTopic.config;
    const diagnostics = observeBrowserDiagnostics(page);

    await page.goto(launch.browserUrl);
    await connectLocalProfile(page);

    await fetchTopicMessages(page, config.topic);
    const messageGrid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(messageGrid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();

    await openTopicTask(page, "Monitor");
    const monitor = page.getByRole("region", { name: "Stream monitor", exact: true });
    await expect(monitor.getByRole("heading", { name: "Stream Monitor" })).toBeVisible();
    const currentStatus = monitor.getByRole("status", { name: "Stream monitor status" });
    await expect(currentStatus).toHaveText(/^(?:Nominal|Backpressure)$/u);
    if ((await currentStatus.innerText()) === "Backpressure") {
      await expect(monitor).toContainText(
        /(?:record loss|observation bound|exceeded \d+ ms|renderer evictions)/iu,
      );
    }
    const streamContext = monitor.getByLabel("Stream context");
    await expect(streamContext).toContainText(config.topic);
    await expect(monitor.getByLabel("Host delivery metrics")).toContainText("Delivered");
    await expect(monitor.getByLabel("Host delivery metrics")).toContainText("1");
    const recentHostSamples = monitor.getByRole("table", { name: "Recent host samples" });
    await expect(recentHostSamples).toBeVisible();
    const recentHostRows = await recentHostSamples.getByRole("row").count();
    expect(recentHostRows).toBeGreaterThan(1);
    expect(recentHostRows).toBeLessThanOrEqual(13);
    await expect(messageGrid).toHaveCount(0);
    const currentMonitorText = await monitor.innerText();
    expect(currentMonitorText).not.toContain(config.seedPayload);
    expect(currentMonitorText).not.toContain(config.oauthClientSecret);
    expect(currentMonitorText).not.toContain("streamskope-seed");
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-stream-monitor-current.png"),
    });

    await expect(page.getByRole("button", { name: `Stop tail ${config.topic}` })).toHaveCount(0);
    await openTopicTask(page, "Messages");
    const streamControl = page.getByRole("button", { name: `Stop tail ${config.topic}` });
    await expect(streamControl).toHaveText("Stop tail");
    await streamControl.click();
    await expect(page.getByRole("contentinfo")).toContainText("Stopped");
    await expect(page.getByRole("button", { name: `Start tail ${config.topic}` })).toHaveText(
      "Start tail",
    );
    await openTopicTask(page, "Monitor");
    await expect(monitor.getByRole("status", { name: "Stream monitor status" })).toContainText(
      "Stopped",
    );
    await expect(messageGrid).toHaveCount(0);
    const lastSample = await streamContext
      .locator('[data-property-label="Last sampled"] dd')
      .innerText();
    expect(lastSample).toMatch(/^20\d{2}-/u);

    const disconnected = await activeKafkaBackend().execute({
      command: "connection.disconnect",
      id: globalThis.crypto.randomUUID(),
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    expect(disconnected.ok).toBe(true);
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    await expect(monitor.getByRole("status", { name: "Stream monitor status" })).toContainText(
      "Stale",
    );
    await expect(streamContext.getByText(lastSample, { exact: true })).toBeVisible();
    expect(await monitor.innerText()).not.toContain(config.seedPayload);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-stream-monitor-stale.png"),
    });
    await openTopicTask(page, "Messages");
    await expect(messageGrid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await expect(page.getByRole("alert").filter({ hasText: "messages are stale" })).toBeVisible();
    expect(diagnostics.problems).toEqual([]);
  });

  test("runs and exports one bounded latency probe through the browser host", async ({
    page,
  }, testInfo) => {
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    const config = await loadFixtureConfig();
    const diagnostics = observeBrowserDiagnostics(page);

    await page.goto(launch.browserUrl);
    await connectLocalProfile(page);

    const fixtureTopic = page.getByRole("button", { name: config.topic, exact: true });
    await expect(fixtureTopic).toBeVisible();
    await fixtureTopic.click();
    await openTopicTask(page, "Latency");
    const latency = page.getByRole("region", { name: "Latency workspace" });
    await expect(latency).toBeVisible();
    await latency.getByRole("combobox", { name: "Probe records" }).click();
    await page.getByRole("option", { exact: true, name: "5" }).click();
    await latency.getByRole("combobox", { name: "Kafka acknowledgements" }).click();
    await page.getByRole("option", { exact: true, name: "Leader" }).click();
    await latency.getByRole("button", { name: "Run latency probe" }).click();

    const confirmation = page.getByRole("dialog", { name: "Run latency probe?" });
    await expect(confirmation).toContainText("Local aio");
    await expect(confirmation).toContainText(config.topic);
    await expect(confirmation).toContainText("5 synthetic records");
    await confirmation.getByRole("button", { name: "Run latency probe" }).click();

    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      "Current latency evidence: 5 of 5 records observed.",
      { timeout: 20_000 },
    );
    await expect(latency.getByRole("table", { name: "Latency metrics" })).toContainText(
      "Produce acknowledgement",
    );
    await expect(latency.getByRole("table", { name: "Fetch latency by broker" })).toBeVisible();

    const downloadPromise = page.waitForEvent("download");
    await latency.getByRole("button", { name: "Export latency JSON" }).click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toMatch(/^streamskope-latency-.+\.json$/u);
    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      "Latency JSON download started.",
    );
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-latency.png"),
    });

    await latency.getByRole("button", { name: "Open activity" }).click();
    const activity = page.getByRole("complementary", { name: "Activity log" });
    await expect(activity).toContainText("Run latency probe");
    await expect(activity).toContainText("Export latency evidence");
    await expect(activity).not.toContainText(config.oauthClientSecret);
    expect(diagnostics.problems).toEqual([]);
  });

  test("applies operational preferences across one real browser workflow", async ({
    page,
  }, testInfo) => {
    test.setTimeout(150_000);
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    seededFixtureTopic = await provisionSeededFixtureTopic();
    const config = seededFixtureTopic.config;
    const diagnostics = observeBrowserDiagnostics(page);
    const runbookUrl = "https://runbooks.example.test/kafka/latency";

    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await page.goto(launch.browserUrl);
    await connectLocalProfile(page);
    const fixtureTopic = page.getByRole("button", { name: config.topic, exact: true });
    await expect(fixtureTopic).toBeVisible();
    await fixtureTopic.click();
    await page.getByRole("button", { name: `Stop tail ${config.topic}` }).click();
    await expect(page.getByLabel("Consumption status")).toContainText("Stopped");
    await openTopicTask(page, "Rules");
    await page.getByRole("button", { name: "Create rule" }).click();
    await page.getByRole("textbox", { name: "Rule name" }).fill("Operational fixture warning");
    await page
      .getByRole("textbox", { name: "JSONPath expression" })
      .fill(RULE_EXPRESSION_ARTIFACT_SENTINEL);
    await page.getByRole("textbox", { name: "Topic filter" }).fill(config.topic);
    await page.getByRole("combobox", { name: "Severity" }).click();
    await page.getByRole("option", { exact: true, name: "warn" }).click();
    await page.getByRole("button", { name: "Save rule" }).click();
    await expect(page.getByRole("heading", { name: "Operational fixture warning" })).toBeVisible();

    const preferenceTrigger = page.getByRole("button", { name: "Preferences" });
    await preferenceTrigger.click();
    let preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await expect(preferences.getByText("Session-only storage")).toBeVisible();
    await preferences.getByRole("combobox", { name: "Default fetch mode" }).click();
    await page.getByRole("option", { name: "First N" }).click();
    await preferences.getByLabel("Default maximum results").fill("25");
    await preferences.getByLabel("Stream queue depth").fill("120");
    await preferences.getByLabel("Stream batch size").fill("10");
    await preferences.getByLabel("Stream delivery interval").fill("5");
    await preferences.getByLabel("Monitor history samples").fill("10");
    await preferences.getByLabel("Default probe records").fill("5");
    await preferences.getByLabel("Default probe timeout").fill("10000");
    await preferences.getByLabel("Latency runbook URL").fill(PRIVATE_RUNBOOK_ARTIFACT_SENTINEL);
    await preferences.getByRole("combobox", { name: "Rule Activity threshold" }).click();
    await page.getByRole("option", { name: "warn" }).click();
    await preferences.getByRole("switch", { name: "Show rule match notifications" }).uncheck();
    await preferences
      .getByRole("switch", { name: "Record successful rule matches in Activity" })
      .uncheck();
    await preferences.getByRole("button", { name: "Save preferences" }).click();
    await expect(preferences.getByRole("status")).toContainText(
      "Confirmed workbench preferences for this browser-development session.",
    );
    await preferences.getByLabel("Latency runbook URL").fill(runbookUrl);
    await preferences.getByRole("button", { name: "Save preferences" }).click();
    await expect(preferences.getByRole("status")).toContainText(
      "Confirmed workbench preferences for this browser-development session.",
    );
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await preferences.getByRole("heading", { name: "Fetch and stream" }).scrollIntoViewIfNeeded();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-preferences-fetch-light.png"),
    });
    await preferences.getByRole("heading", { name: "Rules" }).scrollIntoViewIfNeeded();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-preferences-rules-light.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-preferences-rules-dark.png"),
    });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await preferences.getByRole("button", { name: "Close" }).click();

    await openTopicTask(page, "Messages");
    await expect(page.getByRole("combobox", { name: "Read mode" })).toContainText("First N");
    await expect(page.getByRole("combobox", { name: "Record limit" })).toContainText("25");
    await page.getByRole("button", { name: `Load messages ${config.topic}` }).click();
    await expect(page.getByLabel("Consumption status")).toContainText("Snapshot complete", {
      timeout: 15_000,
    });
    const messageGrid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(messageGrid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await expect(page.getByRole("alert", { name: "Rule match notification" })).toHaveCount(0);

    let activity = await openActivity(page);
    await expect(activity).not.toContainText("Match live rules");
    await collapseActivity(page);

    await openTopicTask(page, "Monitor");
    const monitor = page.getByRole("region", { name: "Stream monitor", exact: true });
    const delivery = monitor.getByLabel("Host delivery metrics");
    const queue = monitor.getByLabel("Host queue metrics");
    await expect(delivery.locator('[data-property-label="Tuning source"] dd')).toHaveText(
      "Confirmed preferences",
    );
    await expect(delivery.locator('[data-property-label="Effective batch"] dd')).toHaveText(
      "10 messages",
    );
    await expect(delivery.locator('[data-property-label="Shaping interval"] dd')).toHaveText(
      "5 ms",
    );
    await expect(delivery.locator('[data-property-label="History limit"] dd')).toHaveText(
      "10 samples",
    );
    await expect(queue.locator('[data-property-label="Current queue"] dd')).toHaveText(
      "0 / 120 messages",
    );

    await preferenceTrigger.click();
    preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.getByRole("switch", { name: "Show rule match notifications" }).check();
    await preferences
      .getByRole("switch", { name: "Record successful rule matches in Activity" })
      .check();
    await preferences.getByRole("button", { name: "Save preferences" }).click();
    await expect(preferences.getByRole("status")).toContainText(
      "Confirmed workbench preferences for this browser-development session.",
    );
    await preferences.getByRole("button", { name: "Close" }).click();

    await openTopicTask(page, "Messages");
    await page.getByRole("button", { name: `Load messages ${config.topic}` }).click();
    const notification = page.getByRole("alert", { name: "Rule match notification" });
    await expect(notification).toContainText("Operational fixture warning · warn", {
      timeout: 15_000,
    });
    const [notificationBounds, navigationBounds] = await Promise.all([
      notification.boundingBox(),
      page.getByRole("navigation", { name: "StreamSkope resources" }).boundingBox(),
    ]);
    expect(notificationBounds).not.toBeNull();
    expect(navigationBounds).not.toBeNull();
    expect(notificationBounds!.x).toBeGreaterThanOrEqual(
      navigationBounds!.x + navigationBounds!.width,
    );
    await expect(page.getByLabel("Consumption status")).toContainText("Snapshot complete", {
      timeout: 15_000,
    });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-rule-notice-light.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-rule-notice-dark.png"),
    });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await notification.getByRole("button", { name: "Dismiss rule notification" }).click();
    activity = await openActivity(page);
    await expect(activity).toContainText("Match live rules");
    await expect(activity).not.toContainText(RULE_EXPRESSION_ARTIFACT_SENTINEL);
    await expect(activity).not.toContainText(config.seedPayload);
    await collapseActivity(page);

    await openTopicTask(page, "Latency");
    const latency = page.getByRole("region", { name: "Latency workspace" });
    await expect(latency.getByText(runbookUrl, { exact: true })).toBeVisible();
    await expect(latency.getByRole("combobox", { name: "Probe records" })).toHaveText("5");
    for (let run = 0; run < 2; run += 1) {
      if (run > 0) {
        await latency.getByRole("button", { name: "Run again" }).click();
      }
      await latency.getByRole("button", { name: "Run latency probe" }).click();
      await page
        .getByRole("dialog", { name: "Run latency probe?" })
        .getByRole("button", { name: "Run latency probe" })
        .click();
      await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
        "Current latency evidence: 5 of 5 records observed.",
        { timeout: 20_000 },
      );
    }
    const history = latency.getByRole("table", { name: "Latency probe history" });
    await expect(history).toBeVisible();
    expect(await history.getByRole("row").count()).toBe(3);

    await page.context().route(`${runbookUrl}*`, async (route) => {
      await route.fulfill({ body: "Runbook fixture", contentType: "text/plain", status: 200 });
    });
    await latency.getByRole("button", { name: "Run again" }).click();
    const originalUrl = page.url();
    const externalPagePromise = page.context().waitForEvent("page");
    await latency.getByRole("button", { name: "Open runbook" }).click();
    const externalPage = await externalPagePromise;
    await externalPage.waitForLoadState();
    expect(externalPage.url()).toBe(runbookUrl);
    expect(page.url()).toBe(originalUrl);
    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      "Runbook request accepted by the platform.",
    );
    await externalPage.close();

    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await history.scrollIntoViewIfNeeded();
    await assertPersistentWorkbenchBars(page);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-latency-history-light.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await history.scrollIntoViewIfNeeded();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-operational-latency-history-dark.png"),
    });
    expect(diagnostics.problems).toEqual([]);
  });

  test("creates, edits, connects, disconnects and deletes a session-only profile", async ({
    page,
  }, testInfo) => {
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const diagnostics = observeBrowserDiagnostics(page);

    await page.goto(launch.browserUrl);
    await expect(page.getByRole("status", { name: "Profile storage status" })).toContainText(
      "Session-only",
    );
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Local stored aio");
    await editor.getByRole("textbox", { name: "Bootstrap brokers" }).fill(fixture.kafkaEndpoint);
    await editor.getByRole("combobox", { name: "Trust material format" }).click();
    await page.getByRole("option", { name: "PEM certificate" }).click();
    await editor.getByLabel("Trust material file").setInputFiles(fixture.caPath);
    await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
    await editor.getByRole("textbox", { name: "OAuth token endpoint" }).fill(fixture.oauthEndpoint);
    await editor.getByRole("textbox", { name: "OAuth client ID" }).fill(config.oauthClientId);
    await editor
      .getByRole("textbox", { name: "OAuth client secret", exact: true })
      .fill(config.oauthClientSecret);
    await editor.getByRole("textbox", { name: "OAuth scope" }).fill(config.oauthScope);
    await editor.getByRole("button", { name: "Save profile" }).click();

    const profileList = page.getByRole("list", { name: "Kafka connection profiles" });
    await expect(profileList.getByText("Local stored aio")).toBeVisible();
    await page.getByRole("searchbox", { name: "Search profiles" }).fill(fixture.kafkaEndpoint);
    await expect(profileList.getByText("Local stored aio")).toBeVisible();
    await page.getByRole("searchbox", { name: "Search profiles" }).fill("");

    await openProfileAction(page, "Local stored aio", "Edit");
    const editDialog = page.getByRole("dialog", {
      name: "Edit Kafka profile Local stored aio",
    });
    await expect(editDialog.getByText("Saved trust material will be retained.")).toBeVisible();
    await expect(editDialog.getByText("Saved client secret will be retained.")).toBeVisible();
    await expect(
      editDialog.getByRole("textbox", { name: "OAuth client secret", exact: true }),
    ).toHaveValue("");
    await editDialog.getByRole("textbox", { name: "Profile name" }).fill("Local fixture");
    await editDialog.getByRole("button", { name: "Update profile" }).click();
    await expect(profileList.getByText("Local fixture")).toBeVisible();

    await page.getByRole("button", { name: "Connect profile Local fixture" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await expect(page.getByRole("navigation", { name: "StreamSkope resources" })).toHaveAttribute(
      "data-active-resource",
      "topics",
    );
    await expect(page.getByRole("button", { name: config.topic, exact: true })).toBeVisible();

    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Select profile Local fixture" }).click();
    const connectedActions = await openProfileActions(page, "Local fixture");
    await expect(connectedActions.getByRole("menuitem", { name: "Edit" })).toBeDisabled();
    await expect(connectedActions.getByRole("menuitem", { name: "Delete" })).toBeDisabled();
    await connectedActions.getByRole("menuitem", { name: "Cluster detail" }).click();
    const clusterDialog = page.getByRole("dialog", {
      name: "Cluster details — Local fixture",
    });
    await expect(clusterDialog.getByText("Cluster ID")).toBeVisible();
    await expect(clusterDialog.getByText("Broker 1", { exact: true })).toBeVisible({
      timeout: 10_000,
    });
    await expect(
      clusterDialog.getByRole("grid", { name: "Broker configuration entries" }),
    ).toBeVisible();
    await clusterDialog
      .getByRole("searchbox", { name: "Filter broker configuration" })
      .fill("num.partitions");
    await expect(
      clusterDialog.getByRole("gridcell", { name: "num.partitions", exact: true }),
    ).toBeVisible();
    const downloadStarted = page.waitForEvent("download");
    await clusterDialog.getByRole("button", { name: "Download cluster details JSON" }).click();
    const download = await downloadStarted;
    expect(download.suggestedFilename()).toMatch(/^streamskope-cluster-.*\.json$/u);
    const downloadPath = await download.path();
    if (downloadPath === null) {
      throw new Error("The browser did not retain the real cluster-details export.");
    }
    const clusterExport = await readFile(downloadPath, "utf8");
    expect(clusterExport.endsWith("\n")).toBe(true);
    expect(clusterExport).not.toContain(config.oauthClientSecret);
    expect(JSON.parse(clusterExport)).toMatchObject({
      cluster: {
        brokers: [{ nodeId: 1 }],
        configurationSourceBrokerId: 1,
        controllerId: 1,
      },
      profile: {
        id: expect.any(String),
        name: "Local fixture",
      },
    });
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-cluster-details.png"),
    });
    await clusterDialog.getByRole("button", { name: "Close" }).click();
    await page.getByRole("button", { name: "Disconnect profile Local fixture" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");

    await openProfileAction(page, "Local fixture", "Delete");
    const deletion = page.getByRole("dialog", {
      name: "Delete Kafka profile Local fixture",
    });
    await expect(deletion).toContainText("Local fixture");
    await expect(deletion).toContainText(fixture.kafkaEndpoint);
    await expect(deletion).toContainText("stored credentials");
    await deletion.getByRole("button", { name: "Delete profile" }).click();
    await expect(page.getByText("Add a connection profile to connect to Kafka.")).toBeVisible();

    const activity = await openActivity(page);
    await expect(activity).toContainText("Create profile");
    await expect(activity).toContainText("Update profile");
    await expect(activity).toContainText("Connect profile");
    await expect(activity).toContainText("Refresh cluster details");
    await expect(activity).toContainText("Export cluster details");
    await expect(activity).toContainText("Disconnect");
    await expect(activity).toContainText("Delete profile");
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-profile-lifecycle.png"),
    });
    expect(diagnostics.problems).toEqual([]);
  });
});
