import { createRequire } from "node:module";
import { createServer, type Server } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
  _electron as electron,
  expect,
  test,
  type ElectronApplication,
  type Locator,
} from "@playwright/test";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { parseTrustMaterial } from "../../src/features/kafka/engine";
import {
  ELECTRON_RUNTIME_EFFICIENCY_POLICY,
  assertElectronProcessEvidence,
} from "../../tools/electron-runtime-efficiency-policy";
import { summarizeElectronProcessEvidence } from "../../tools/electron-runtime-evidence";
import { createPerformanceEvidence } from "../../tools/performance-evidence";
import { RULE_EXPRESSION_ARTIFACT_SENTINEL } from "../../tools/sensitive-artifact-policy";
import {
  buildElectronSmoke,
  buildRenderer,
  chooseNextElectronSavePath,
  connectElectronToFixture,
  launchProfileApplication,
} from "../support/electron-application";
import { sampleElectronProcesses } from "../support/electron-runtime";
import {
  loadFixtureConfig,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
} from "../support/kafka-fixture";
import {
  collapseActivity,
  expectRawLogEvidence,
  fetchTopicMessages,
  openActivity,
  openProfileAction,
  openProfileActions,
  openTopicTask,
  openWorkbenchResource,
} from "../support/workbench-browser";

async function openRetrievalLibrary(editor: Locator): Promise<void> {
  await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
  await editor.getByRole("button", { name: "Manage retrieval profiles", exact: true }).click();
}

const repositoryRoot = process.cwd();
const require = createRequire(join(repositoryRoot, "package.json"));
const electronExecutable = require("electron") as string;

// These real-host flows enter credentials; retain redacted screenshots/video, not DOM traces.
test.use({ trace: "off" });

const rendererDocument = `<!doctype html>
<html lang="en">
  <head><meta charset="UTF-8"><title>StreamSkope Electron smoke</title></head>
  <body data-command="pending" data-desktop="pending" data-event="pending" data-host="pending">
    <script>
      const host = window.streamSkopeHost;
      const desktop = window.streamSkopeDesktop;
      document.body.dataset.host =
        host && Object.keys(host).sort().join(",") === "execute,openExternalUrl,subscribe"
          ? "narrow"
          : "invalid";
      document.body.dataset.desktop =
        desktop && Object.keys(desktop).sort().join(",") === "saveTextDocument,subscribeActions"
          ? "narrow"
          : "invalid";
      host.subscribe((event) => {
        if (event.event === "backend.availability" && event.payload.state === "ready") {
          document.body.dataset.event = "ready";
        }
      });
      host.execute({
        command: "connection.disconnect",
        id: "electron-smoke",
        payload: {},
        version: ${String(HOST_PROTOCOL_VERSION)}
      }).then((response) => {
        document.body.dataset.command =
          response.ok && response.id === "electron-smoke" ? "accepted" : "invalid";
      });
    </script>
  </body>
</html>`;

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
    throw new Error("Expected a renderer TCP endpoint.");
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

test("launches the production Electron boundary with a narrow working preload", async () => {
  const outputDirectory = await mkdtemp(join(tmpdir(), "streamskope-electron-"));
  const rendererServer = createServer((_request, response) => {
    response.setHeader("Content-Type", "text/html; charset=utf-8");
    response.end(rendererDocument);
  });
  let electronApplication: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererPort = await listen(rendererServer);
    const launchArguments = [
      ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
      join(outputDirectory, "main.cjs"),
    ];
    const electronEnvironment = Object.fromEntries(
      Object.entries(process.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      ),
    );
    electronEnvironment.STREAMSKOPE_SMOKE_RENDERER_URL = `http://127.0.0.1:${rendererPort}/`;
    electronApplication = await electron.launch({
      args: launchArguments,
      env: electronEnvironment,
      executablePath: electronExecutable,
    });
    const window = await electronApplication.firstWindow();

    await expect(window.locator("body")).toHaveAttribute("data-host", "narrow");
    await expect(window.locator("body")).toHaveAttribute("data-desktop", "narrow");
    await expect(window.locator("body")).toHaveAttribute("data-command", "accepted");
    await expect(window.locator("body")).toHaveAttribute("data-event", "ready");
    await window.reload();
    await expect(window.locator("body")).toHaveAttribute("data-event", "ready");
    await expect(window.locator("body")).toHaveAttribute("data-command", "accepted");
  } finally {
    await electronApplication?.close();
    await closeServer(rendererServer);
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("restores a protected profile without returning secrets and confirms exact deletion", async () => {
  test.setTimeout(45_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-profile-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    const jks = await readFile(
      join(repositoryRoot, "node_modules/jks-js/examples/assets/truststore.jks"),
    );
    const caPath = join(outputDirectory, "profile-ca.pem");
    await writeFile(
      caPath,
      parseTrustMaterial({
        kind: "jks",
        material: jks.toString("base64"),
        password: "password",
      }).caPem,
      "utf8",
    );

    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    let page = await application.firstWindow();
    await expect(page.getByLabel("Backend status")).toContainText("Host ready");
    await expect(page.getByRole("status", { name: "Profile storage status" })).toContainText(
      "OS-protected profiles",
    );
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Durable local");
    await editor
      .getByRole("textbox", { name: "Bootstrap brokers" })
      .fill("broker.example.test:9093");
    await editor.getByRole("combobox", { name: "Trust material format" }).click();
    await page.getByRole("option", { name: "PEM certificate" }).click();
    await editor.getByLabel("Trust material file").setInputFiles(caPath);
    await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
    await editor
      .getByRole("textbox", { name: "OAuth token endpoint" })
      .fill("https://identity.example.test/token");
    await editor.getByRole("textbox", { name: "OAuth client ID" }).fill("streamskope");
    await editor
      .getByRole("textbox", { name: "OAuth client secret", exact: true })
      .fill("electron-secret-sentinel");
    await editor.getByRole("textbox", { name: "OAuth scope" }).fill("events");
    await editor.getByRole("button", { name: "Save profile" }).click();
    await expect(page.getByRole("button", { name: "Select profile Durable local" })).toBeVisible();

    await application.close();
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await expect(page.getByRole("button", { name: "Select profile Durable local" })).toBeVisible();
    await expect(page.locator("body")).not.toContainText("electron-secret-sentinel");
    await openProfileAction(page, "Durable local", "Edit");
    const restoredEditor = page.getByRole("dialog", {
      name: "Edit Kafka profile Durable local",
    });
    await expect(
      restoredEditor.getByRole("textbox", { name: "OAuth client secret", exact: true }),
    ).toHaveValue("");
    await expect(restoredEditor.getByText("Saved client secret will be retained.")).toBeVisible();
    await restoredEditor.getByRole("button", { name: "Cancel" }).click();

    await openProfileAction(page, "Durable local", "Delete");
    const deletion = page.getByRole("dialog", {
      name: "Delete Kafka profile Durable local",
    });
    await expect(deletion).toContainText("Durable local");
    await expect(deletion).toContainText("broker.example.test:9093");
    await expect(deletion).toContainText("stored credentials and trust material");
    await deletion.getByRole("button", { name: "Cancel" }).click();
    await expect(page.getByRole("button", { name: "Select profile Durable local" })).toBeVisible();
    await openProfileAction(page, "Durable local", "Delete");
    await page
      .getByRole("dialog", { name: "Delete Kafka profile Durable local" })
      .getByRole("button", { name: "Delete profile" })
      .click();
    await expect(page.getByText("Add a connection profile to connect to Kafka.")).toBeVisible();
  } finally {
    await application?.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("loads and exports real cluster details through a restored protected Electron profile", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(90_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-cluster-details-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    let page = await application.firstWindow();
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Electron cluster");
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
    await page.getByRole("button", { name: "Connect profile Electron cluster" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const connectedSamples = await sampleElectronProcesses(application, page);
    assertElectronProcessEvidence("connected", connectedSamples);
    await writeFile(
      testInfo.outputPath("electron-connected-runtime-evidence.json"),
      `${JSON.stringify(
        createPerformanceEvidence({
          capturedAt: new Date().toISOString(),
          check: "connected-electron-runtime",
          command:
            'npm run test:e2e:electron -- test/e2e/electron-host.spec.ts --grep "loads and exports real cluster details"',
          evidence: {
            connected: summarizeElectronProcessEvidence(connectedSamples),
            connectedPolicyCpuMedianPercent:
              ELECTRON_RUNTIME_EFFICIENCY_POLICY.cpuMedianPercent.connected,
            processWorkingSetPolicyBytes: ELECTRON_RUNTIME_EFFICIENCY_POLICY.processWorkingSetBytes,
            totalWorkingSetPolicyBytes: ELECTRON_RUNTIME_EFFICIENCY_POLICY.totalWorkingSetBytes,
          },
          outcome: "passed",
          runtime: {
            arch: process.arch,
            node: process.version,
            platform: process.platform,
          },
          sampleMethod:
            "Three Electron app.getAppMetrics samples at 500 millisecond intervals after a confirmed protected-profile connection to the controlled aio-kafka fixture.",
        }),
        null,
        2,
      )}\n`,
      "utf8",
    );
    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Select profile Electron cluster" }).click();
    await openProfileAction(page, "Electron cluster", "Cluster detail");
    let clusterDialog = page.getByRole("dialog", {
      name: "Cluster details — Electron cluster",
    });
    await expect(clusterDialog.getByText("Broker 1", { exact: true })).toBeVisible({
      timeout: 10_000,
    });
    await clusterDialog
      .getByRole("searchbox", { name: "Filter broker configuration" })
      .fill("num.partitions");
    await expect(
      clusterDialog.getByRole("gridcell", { name: "num.partitions", exact: true }),
    ).toBeVisible();
    await clusterDialog.getByRole("button", { name: "Copy cluster details JSON" }).click();
    await expect(clusterDialog.getByRole("status")).toContainText("Cluster JSON copied.");
    const exported = await page.evaluate(() => navigator.clipboard.readText());
    const clusterExportPath = join(outputDirectory, "cluster-details.json");
    await chooseNextElectronSavePath(application, clusterExportPath);
    await clusterDialog.getByRole("button", { name: "Download cluster details JSON" }).click();
    await expect(clusterDialog.getByRole("status")).toContainText("Cluster JSON saved.");
    await expect(readFile(clusterExportPath, "utf8")).resolves.toBe(exported);
    expect(exported.endsWith("\n")).toBe(true);
    expect(exported).not.toContain(config.oauthClientSecret);
    expect(JSON.parse(exported)).toMatchObject({
      cluster: {
        brokers: [{ nodeId: 1 }],
        configurationSourceBrokerId: 1,
        controllerId: 1,
      },
      profile: {
        id: expect.any(String),
        name: "Electron cluster",
      },
    });
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-cluster-details.png"),
    });
    await clusterDialog.getByRole("button", { name: "Close" }).click();
    let activity = await openActivity(page);
    await expect(activity).toContainText("Refresh cluster details");
    await expect(activity).toContainText("Export cluster details");
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await collapseActivity(page);
    await page.getByRole("button", { name: "Disconnect profile Electron cluster" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    const disconnectedActions = await openProfileActions(page, "Electron cluster");
    await expect(
      disconnectedActions.getByRole("menuitem", { name: "Cluster detail" }),
    ).toHaveAttribute("aria-disabled", "true");
    await page.keyboard.press("Escape");

    await application.close();
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await expect(
      page.getByRole("button", { name: "Select profile Electron cluster" }),
    ).toBeVisible();
    const restoredActions = await openProfileActions(page, "Electron cluster");
    await expect(restoredActions.getByRole("menuitem", { name: "Cluster detail" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Connect profile Electron cluster" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Select profile Electron cluster" }).click();
    await openProfileAction(page, "Electron cluster", "Cluster detail");
    clusterDialog = page.getByRole("dialog", {
      name: "Cluster details — Electron cluster",
    });
    await expect(clusterDialog.getByText("Broker 1", { exact: true })).toBeVisible({
      timeout: 10_000,
    });
    await clusterDialog.getByRole("button", { name: "Close" }).click();
    activity = await openActivity(page);
    await expect(activity).toContainText("Refresh cluster details");
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await expect(page.locator("body")).not.toContainText(config.oauthClientSecret);
  } finally {
    await application?.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("restores durable templates and isolates a corrupt template file from profiles", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(60_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-template-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    const jks = await readFile(
      join(repositoryRoot, "node_modules/jks-js/examples/assets/truststore.jks"),
    );
    const caPath = join(outputDirectory, "template-profile-ca.pem");
    await writeFile(
      caPath,
      parseTrustMaterial({
        kind: "jks",
        material: jks.toString("base64"),
        password: "password",
      }).caPem,
      "utf8",
    );

    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    let page = await application.firstWindow();
    await page.getByRole("button", { name: "Add profile" }).click();
    let profileEditor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await openRetrievalLibrary(profileEditor);
    let manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
    await manager.getByRole("button", { name: "New retrieval profile" }).click();
    await manager.getByLabel("Retrieval profile name", { exact: true }).fill("Local gateway");
    await manager.getByLabel("Remote file", { exact: true }).fill("/etc/kafka/ca.pem");
    await manager.getByRole("button", { name: "Save retrieval profile" }).click();
    await expect(manager.getByRole("status")).toHaveText("Retrieval profile saved.");
    await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
    await profileEditor.getByRole("textbox", { name: "Profile name" }).fill("Template isolation");
    await profileEditor
      .getByRole("textbox", { name: "Bootstrap brokers" })
      .fill("broker.example.test:9093");
    await profileEditor.getByRole("combobox", { name: "Trust material format" }).click();
    await page.getByRole("option", { name: "PEM certificate" }).click();
    await profileEditor.getByLabel("Trust material file").setInputFiles(caPath);
    await profileEditor.getByRole("button", { name: "Save profile" }).click();
    await expect(
      page.getByRole("button", { name: "Select profile Template isolation" }),
    ).toBeVisible();

    await application.close();
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await expect(
      page.getByRole("button", { name: "Select profile Template isolation" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Add profile" }).click();
    profileEditor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await openRetrievalLibrary(profileEditor);
    manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
    await expect(manager.getByText("Local gateway", { exact: true })).toBeVisible();
    await manager.getByRole("button", { name: /Local gateway/ }).click();
    await expect(manager.getByLabel("Remote file", { exact: true })).toHaveValue(
      "/etc/kafka/ca.pem",
    );
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-template-restored.png"),
    });
    await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
    await profileEditor.getByRole("button", { name: "Cancel" }).click();

    await application.close();
    const templatePath = join(userDataPath, "templates", "trust-acquisition-recipes.json");
    const corruptTemplateDocument = '{"version":1,"catalogs":"corrupt"}';
    await writeFile(templatePath, corruptTemplateDocument, "utf8");
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await expect(
      page.getByRole("button", { name: "Select profile Template isolation" }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: "Add profile" })).toBeEnabled();
    await page.getByRole("button", { name: "Add profile" }).click();
    profileEditor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await expect(profileEditor.getByRole("button", { name: "Test connection" })).toBeEnabled();
    await openRetrievalLibrary(profileEditor);
    manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
    await expect(manager.getByRole("alert")).toBeVisible();
    await expect(manager.getByRole("button", { name: "New retrieval profile" })).toBeDisabled();
    await expect(readFile(templatePath, "utf8")).resolves.toBe(corruptTemplateDocument);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-template-unavailable.png"),
    });
  } finally {
    await application?.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("keeps the header status-only when protected profile storage is unavailable", async () => {
  test.setTimeout(30_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-profile-unavailable-e2e-"),
  );
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(
      outputDirectory,
      rendererUrl,
      join(outputDirectory, "user-data"),
      "unavailable",
    );
    const page = await application.firstWindow();

    await expect(page.getByText("Profile storage unavailable", { exact: true })).toBeVisible();
    await expect(page.getByText("Existing connection state was not changed.")).toBeVisible();
    await expect(page.getByRole("button", { name: "Add profile" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Configure ad hoc connection" })).toHaveCount(0);
  } finally {
    await application?.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("persists redacted topic configuration history across Electron restarts", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(90_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-topic-configuration-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  const historyPath = join(userDataPath, "history", "kafka-topic-configuration-history.json");
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    let page = await application.firstWindow();
    await connectElectronToFixture(page, config, fixture);
    await page.getByRole("button", { name: config.topic, exact: true }).click();
    await openTopicTask(page, "Configuration");
    let configurationGrid = page.getByRole("grid", {
      name: "Topic configuration entries",
    });
    await expect(configurationGrid).toBeVisible({ timeout: 10_000 });
    await page.getByRole("searchbox", { name: "Search configuration" }).fill("retention.ms");
    await configurationGrid.getByRole("gridcell", { name: "retention.ms", exact: true }).click();
    const proposedValue = page.getByRole("textbox", { name: "Proposed value" });
    expect((await proposedValue.inputValue()).length).toBeGreaterThan(0);
    await page.getByRole("button", { name: "Queue change" }).click();
    await page.getByRole("button", { name: "Dry-run changes" }).click();
    await expect(page.getByRole("button", { name: "Dry-run changes" })).toBeEnabled({
      timeout: 10_000,
    });
    await page.getByRole("button", { name: "Configuration history" }).click();
    let history = page.getByRole("dialog", { name: "Configuration history" });
    await expect(history).toContainText("Durable history");
    await expect(history).toContainText("Validate · Succeeded");
    await history.getByRole("button", { name: "Close" }).click();

    await application.close();
    const storedHistory = await readFile(historyPath, "utf8");
    expect(storedHistory).toContain('"action":"validate"');
    expect(storedHistory).toContain(config.topic);
    expect(storedHistory).not.toContain(config.oauthClientSecret);

    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await page.getByRole("button", { name: config.topic, exact: true }).click();
    await openTopicTask(page, "Configuration");
    configurationGrid = page.getByRole("grid", {
      name: "Topic configuration entries",
    });
    await expect(configurationGrid).toBeVisible({ timeout: 10_000 });
    await page.getByRole("button", { name: "Configuration history" }).click();
    history = page.getByRole("dialog", { name: "Configuration history" });
    await expect(history).toContainText("Durable history");
    await expect(history).toContainText("Validate · Succeeded");
    await expect(history).toContainText("retention.ms");
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-topic-configuration-history.png"),
    });
  } finally {
    await application?.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("runs and exports one bounded real latency probe through Electron", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(90_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-latency-e2e-"),
  );
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(
      outputDirectory,
      rendererUrl,
      join(outputDirectory, "user-data"),
    );
    const page = await application.firstWindow();
    await connectElectronToFixture(page, config, fixture);
    await page.getByRole("button", { name: config.topic, exact: true }).click();
    await openTopicTask(page, "Latency");

    const latency = page.getByRole("region", { name: "Latency workspace" });
    await latency.getByRole("combobox", { name: "Probe records" }).click();
    await page.getByRole("option", { exact: true, name: "5" }).click();
    await latency.getByRole("button", { name: "Run latency probe" }).click();
    const confirmation = page.getByRole("dialog", { name: "Run latency probe?" });
    await expect(confirmation).toContainText("Electron local aio");
    await expect(confirmation).toContainText(config.topic);
    await expect(confirmation).toContainText("5 synthetic records");
    await confirmation.getByRole("button", { name: "Run latency probe" }).click();

    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      "Current latency evidence: 5 of 5 records observed.",
      { timeout: 20_000 },
    );
    await expect(latency.getByRole("table", { name: "Latency metrics" })).toContainText(
      "Publish to observe",
    );

    const latencyExportPath = join(outputDirectory, "latency.json");
    await chooseNextElectronSavePath(application, latencyExportPath);
    await latency.getByRole("button", { name: "Export latency JSON" }).click();
    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      "Latency JSON saved.",
    );
    const exported = await readFile(latencyExportPath, "utf8");
    expect(JSON.parse(exported)).toMatchObject({
      observedMessages: 5,
      requestedMessages: 5,
      topic: config.topic,
    });
    expect(exported).not.toContain(config.oauthClientSecret);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-real-aio-kafka-latency.png"),
    });

    await latency.getByRole("button", { name: "Run again" }).click();
    await latency.getByRole("combobox", { name: "Probe records" }).click();
    await page.getByRole("option", { exact: true, name: "200" }).click();
    await latency.getByRole("button", { name: "Run latency probe" }).click();
    const cancellationConfirmation = page.getByRole("dialog", {
      name: "Run latency probe?",
    });
    await cancellationConfirmation.getByRole("button", { name: "Run latency probe" }).click();
    await expect(latency.getByRole("status", { name: "Latency operation status" })).toContainText(
      `Running 200-record probe on ${config.topic}.`,
    );
    await latency.getByRole("button", { name: "Stop latency probe" }).click();
    const terminal = latency.getByRole("status", { name: "Latency operation status" });
    // A real 200-record probe may finish before the stop request reaches the host.
    // Deterministic in-flight cancellation is covered at the controlled adapter boundary.
    await expect(terminal).toHaveText(
      /Latency probe cancelled\.|Current latency evidence: 200 of 200 records observed\./u,
      { timeout: 20_000 },
    );
    const cancelled = (await terminal.innerText()).includes("cancelled");
    await latency.getByRole("button", { name: "Open activity" }).click();
    const activity = page.getByRole("complementary", { name: "Activity log" });
    await expect(activity).toContainText("Run latency probe");
    await expectRawLogEvidence(
      activity,
      "Run latency probe",
      cancelled ? "cancelled" : "succeeded",
      "test",
    );
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-real-aio-kafka-latency-terminal.png"),
    });
  } finally {
    await application?.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("restores a durable live rule match and keeps raw exploration when rules are corrupt", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(90_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-rule-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  const rulePath = join(userDataPath, "rules", "kafka-rules.json");
  const seededFixtureTopic = await provisionSeededFixtureTopic();
  const config = seededFixtureTopic.config;
  const fixture = await loadFixtureConnection();
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    let page = await application.firstWindow();
    await connectElectronToFixture(page, config, fixture);
    await page.getByRole("button", { name: config.topic, exact: true }).click();
    await openTopicTask(page, "Rules");
    await expect(page.getByRole("status", { name: "Rule storage status" })).toContainText(
      "Durable rules",
    );
    await page.getByRole("button", { name: "Create rule" }).click();
    await page.getByRole("textbox", { name: "Rule name" }).fill("Durable fixture source");
    await page
      .getByRole("textbox", { name: "JSONPath expression" })
      .fill(RULE_EXPRESSION_ARTIFACT_SENTINEL);
    await page.getByRole("textbox", { name: "Topic filter" }).fill(config.topic);
    await page.getByRole("spinbutton", { name: "Cooldown (milliseconds)" }).fill("5000");
    await page.getByRole("button", { name: "Save rule" }).click();
    await expect(page.getByRole("heading", { name: "Durable fixture source" })).toBeVisible();
    await expect(readFile(rulePath, "utf8")).resolves.toContain('"name":"Durable fixture source"');

    await application.close();
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const restoredTopic = page.getByRole("button", { name: config.topic, exact: true });
    await expect(restoredTopic).toBeVisible();
    await restoredTopic.click();
    await openTopicTask(page, "Rules");
    await expect(page.getByRole("heading", { name: "Durable fixture source" })).toBeVisible();
    await expect(page.getByRole("status", { name: "Rule storage status" })).toContainText(
      "Durable rules",
    );
    await openTopicTask(page, "Messages");
    await fetchTopicMessages(page, config.topic);
    let grid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(grid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await expect(grid.getByText("1 match · Info", { exact: true }).first()).toBeVisible();
    await grid.getByText(config.seedPayload, { exact: true }).first().click();
    let inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { exact: true, name: "Rules" }).click();
    await expect(inspector).toContainText("Durable fixture source · Info");
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-rule-restored-match.png"),
    });
    await page.getByRole("button", { name: "Stop tail" }).click();

    await application.close();
    const corruptRuleDocument = '{"version":99,"rules":"corrupt"}';
    await writeFile(rulePath, corruptRuleDocument, "utf8");
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const corruptStoreTopic = page.getByRole("button", { name: config.topic, exact: true });
    await expect(corruptStoreTopic).toBeVisible();
    await corruptStoreTopic.click();
    await openTopicTask(page, "Rules");
    const unavailable = page.getByRole("alert");
    await expect(unavailable).toContainText("Rule storage unavailable");
    await expect(unavailable).toContainText(
      "Preserve the rule file, restore a known-good copy, or move it aside after confirming a backup.",
    );
    await expect(page.getByRole("button", { name: "Create rule" })).toBeDisabled();
    await expect(page.getByText("No rules configured.")).toBeHidden();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-rule-unavailable.png"),
    });

    await openTopicTask(page, "Messages");
    await openWorkbenchResource(page, "Connection Profiles");
    await expect(page.getByRole("status", { name: "Profile storage status" })).toContainText(
      "OS-protected profiles",
    );
    await expect(page.getByRole("button", { name: "Add profile" })).toBeEnabled();
    await page.getByRole("button", { name: "Add profile" }).click();
    const profileEditor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await openRetrievalLibrary(profileEditor);
    const templateManager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
    await expect(
      templateManager.getByRole("button", { name: "New retrieval profile" }),
    ).toBeEnabled();
    await templateManager.getByRole("button", { name: "Close retrieval profiles" }).click();
    await profileEditor.getByRole("button", { name: "Cancel" }).click();
    await openWorkbenchResource(page, "Topics");
    await expect(page.getByRole("main", { name: "Topics page" })).toBeVisible();
    await fetchTopicMessages(page, config.topic);
    await expect(
      page.getByRole("alert").filter({ hasText: "Live rule evaluation is unavailable." }),
    ).toBeVisible();
    grid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(grid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await expect(grid.getByText("Unavailable", { exact: true }).first()).toBeVisible();
    await grid.getByText(config.seedPayload, { exact: true }).first().click();
    inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Rules" }).click();
    await expect(inspector).toContainText("Rule catalog unavailable");
    await inspector.getByRole("tab", { name: "Value" }).click();
    await inspector.getByRole("tab", { name: "Raw" }).click();
    await expect(inspector).toContainText(config.seedPayload);
    await page.getByRole("button", { name: "Stop tail" }).click();
    await expect(readFile(rulePath, "utf8")).resolves.toBe(corruptRuleDocument);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-rule-corrupt-raw-message.png"),
    });
  } finally {
    await application?.close();
    await seededFixtureTopic.dispose();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});

test("operates on filtered real messages and exports stale evidence in Electron", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(90_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-message-operations-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  const seededFixtureTopic = await provisionSeededFixtureTopic();
  const config = seededFixtureTopic.config;
  const fixture = await loadFixtureConnection();
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    let page = await application.firstWindow();

    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Electron messages");
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
    await page.getByRole("button", { name: "Connect profile Electron messages" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");

    await fetchTopicMessages(page, config.topic);
    const grid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(grid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible({
      timeout: 10_000,
    });
    const openMonitor = page
      .getByRole("tablist", { name: "Topic sections" })
      .getByRole("tab", { name: "Monitor" });
    await openMonitor.focus();
    await page.keyboard.press("Enter");
    const monitor = page.getByRole("region", { name: "Stream monitor", exact: true });
    const currentMonitorStatus = monitor.getByRole("status", {
      name: "Stream monitor status",
    });
    await expect(currentMonitorStatus).toHaveText(/^(?:Nominal|Backpressure)$/u);
    if ((await currentMonitorStatus.innerText()) === "Backpressure") {
      await expect(monitor).toContainText(
        /(?:record loss|observation bound|exceeded \d+ ms|renderer evictions)/iu,
      );
    }
    await expect(page.getByRole("main", { name: "Message workspace" })).toHaveCount(0);
    await expect(grid).toHaveCount(0);
    expect(await monitor.innerText()).not.toContain(config.seedPayload);
    expect(await monitor.innerText()).not.toContain(config.oauthClientSecret);
    expect(
      await page.evaluate(() => {
        const scope = globalThis as typeof globalThis & {
          readonly process?: unknown;
          readonly require?: unknown;
          readonly streamSkopeHost?: unknown;
        };
        const host = scope.streamSkopeHost;
        return {
          host:
            (typeof host === "object" && host !== null) || typeof host === "function"
              ? Object.keys(host).sort()
              : [],
          process: typeof scope.process,
          require: typeof scope.require,
        };
      }),
    ).toEqual({
      host: ["execute", "openExternalUrl", "subscribe"],
      process: "undefined",
      require: "undefined",
    });
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-stream-monitor-current.png"),
    });
    await page
      .getByRole("tablist", { name: "Topic sections" })
      .getByRole("tab", { name: "Messages" })
      .focus();
    await page.keyboard.press("Enter");
    await expect(grid.getByText(config.seedPayload, { exact: true }).first()).toBeVisible();
    await page.getByRole("button", { name: "Show message filters" }).click();
    await page.getByRole("textbox", { name: "Key contains" }).fill("streamskope-seed");
    await page
      .getByRole("textbox", { name: "Value or retained preview contains" })
      .fill("streamskope-fixture");
    await expect(page.getByText("2 active filters", { exact: true })).toBeVisible();
    await grid.getByText(config.seedPayload, { exact: true }).first().click();

    let inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Value" }).click();
    await inspector.getByRole("button", { name: "Copy value" }).click();
    await expect(inspector.getByRole("status")).toContainText("Value copied to clipboard.");
    await expect
      .poll(async () => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(config.seedPayload);
    const openEditor = inspector.getByRole("button", { name: "Open in editor" });
    await openEditor.click();
    const scratchValue = page.getByRole("textbox", { name: "Scratch message value" });
    await expect(scratchValue).toHaveValue(config.seedPayload);
    await scratchValue.fill("discarded Electron scratch content");
    await page.keyboard.press("Escape");
    await expect(openEditor).toBeFocused();

    await openMonitor.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("button", { name: `Stop tail ${config.topic}` })).toHaveCount(0);
    await openTopicTask(page, "Messages");
    const stopConsumption = page.getByRole("button", {
      name: `Stop tail ${config.topic}`,
    });
    await stopConsumption.focus();
    await page.keyboard.press("Enter");
    await openTopicTask(page, "Monitor");
    await expect(monitor.getByRole("status", { name: "Stream monitor status" })).toContainText(
      "Stopped",
    );
    await openTopicTask(page, "Messages");
    await page.evaluate(async (version) => {
      const host = (
        globalThis as typeof globalThis & {
          readonly streamSkopeHost?: {
            execute(command: {
              readonly command: "connection.disconnect";
              readonly id: string;
              readonly payload: Record<string, never>;
              readonly version: number;
            }): Promise<{ readonly ok: boolean }>;
          };
        }
      ).streamSkopeHost;
      if (host === undefined) throw new Error("StreamSkope host bridge is unavailable.");
      const response = await host.execute({
        command: "connection.disconnect",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version,
      });
      if (!response.ok) throw new Error("The host did not disconnect the active connection.");
    }, HOST_PROTOCOL_VERSION);
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    await expect(
      page
        .getByRole("navigation", { name: "StreamSkope resources" })
        .getByRole("button", { name: "Topics" }),
    ).toBeDisabled();
    await openTopicTask(page, "Monitor");
    await expect(monitor.getByRole("status", { name: "Stream monitor status" })).toContainText(
      "Stale",
    );
    expect(await monitor.innerText()).not.toContain(config.seedPayload);
    expect(await monitor.innerText()).not.toContain(config.oauthClientSecret);
    await openTopicTask(page, "Messages");

    const messageExportPath = join(outputDirectory, "filtered-messages.json");
    await chooseNextElectronSavePath(application, messageExportPath);
    await page.getByRole("button", { name: "Export filtered JSON" }).click();
    await expect(page.getByRole("status").filter({ hasText: "saved" })).toContainText(
      "Filtered message JSON saved.",
    );
    const exportContent = await readFile(messageExportPath, "utf8");
    const exported = JSON.parse(exportContent) as {
      readonly messages: readonly { readonly payload: string | null }[];
      readonly stale: boolean;
    };
    expect(exported.stale).toBe(true);
    expect(exported.messages.length).toBeGreaterThan(0);
    expect(exported.messages.every((message) => message.payload === config.seedPayload)).toBe(true);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-message-operations-stale.png"),
    });

    await application.close();
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    page = await application.firstWindow();
    await expect(
      page.getByRole("button", { name: "Select profile Electron messages" }),
    ).toBeVisible();
    inspector = page.getByRole("complementary", { name: "Message inspector" });
    await expect(inspector).toHaveCount(0);
    await expect(page.locator("body")).not.toContainText(config.seedPayload);
    await expect(page.locator("body")).not.toContainText(config.oauthClientSecret);
  } finally {
    await application?.close();
    await seededFixtureTopic.dispose();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});
