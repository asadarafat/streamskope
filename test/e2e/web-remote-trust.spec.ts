import { readFile } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { createKafkaBackend } from "../../src/main";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { loadFixtureConfig, loadFixtureConnection } from "../support/kafka-fixture";
import {
  acquireRemoteJksTrust,
  prepareRemoteJksTrust,
  reviewSavedRemoteTrust,
} from "../support/remote-trust-workflow";
import { startControlledSshServer } from "../support/ssh-fixture";
import {
  collapseActivity,
  expectRawLogEvidence,
  observeBrowserDiagnostics,
  openActivity,
  openWorkbenchResource,
} from "../support/workbench-browser";

test.use({ trace: "off" });
let launch: RunningWebDevelopment | undefined;

async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("No test port.");
  await new Promise<void>((done, reject) =>
    server.close((error) => (error ? reject(error) : done())),
  );
  return address.port;
}

test.beforeEach(async () => {
  launch = await launchWebDevelopment({
    backend: createKafkaBackend(),
    hostPort: await reservePort(),
    rendererPort: await reservePort(),
    rendererRoot: resolve(process.cwd()),
  });
});
test.afterEach(async () => {
  await launch?.close();
  launch = undefined;
});

test("acquires selected remote JKS trust and connects a profile to aio-kafka", async ({
  page,
}, testInfo) => {
  test.setTimeout(60_000);
  if (launch === undefined) {
    throw new Error("Real browser development launch is unavailable.");
  }
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  const truststore = await readFile(join(dirname(fixture.caPath), "kafka.truststore.jks"));
  const ssh = await startControlledSshServer({
    commandOutput: "password\n",
    materialBytes: truststore,
  });
  ssh.putFile("/etc/kafka/truststore.jks", truststore);
  const diagnostics = observeBrowserDiagnostics(page);

  try {
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.goto(launch.browserUrl);
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Remote acquired aio");
    await editor.getByRole("textbox", { name: "Bootstrap brokers" }).fill(fixture.kafkaEndpoint);
    await acquireRemoteJksTrust(page, ssh);
    await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
    await editor.getByRole("textbox", { name: "OAuth token endpoint" }).fill(fixture.oauthEndpoint);
    await editor.getByRole("textbox", { name: "OAuth client ID" }).fill(config.oauthClientId);
    await editor
      .getByRole("textbox", { name: "OAuth client secret", exact: true })
      .fill(config.oauthClientSecret);
    await editor.getByRole("textbox", { name: "OAuth scope" }).fill(config.oauthScope);

    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("remote-trust-profile-ready.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("remote-trust-profile-ready-dark.png"),
    });
    await editor.getByRole("button", { name: "Save profile" }).click();
    await expect(
      page
        .getByRole("main", { name: "Connection profiles page" })
        .getByRole("heading", { name: "Remote acquired aio" }),
    ).toBeVisible();
    const connectedResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        response.request().postData()?.includes('"command":"profiles.connect"') === true,
    );
    await page.getByRole("button", { name: "Connect profile Remote acquired aio" }).click();
    const response = await connectedResponse;
    expect(await response.json()).toMatchObject({ ok: true });
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await expect(page.getByRole("button", { name: config.topic, exact: true })).toBeVisible();

    expect(ssh.commands).toHaveLength(0);
    expect(ssh.events.filter((event) => event === "AUTHENTICATION:password")).toHaveLength(1);
    expect(ssh.removedPaths).toHaveLength(0);
    const activity = await openActivity(page);
    await expect(activity).toContainText("Discover SSH host identity");
    await expectRawLogEvidence(
      activity,
      "Fetch remote trust material",
      "SSH truststore",
      `${ssh.host}:${String(ssh.port)}`,
    );
    await expect(activity).not.toContainText("Fetch remote trust password");
    await expect(activity).not.toContainText("ssh-password");
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await collapseActivity(page);
    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Select profile Remote acquired aio" }).click();
    await page.getByRole("button", { name: "Disconnect profile Remote acquired aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    await reviewSavedRemoteTrust(page, ssh, testInfo);
    expect(diagnostics.problems).toEqual([]);
  } finally {
    await ssh.close();
  }
});

test("opens structured Activity from an invalid remote JKS acquisition", async ({
  page,
}, testInfo) => {
  test.setTimeout(60_000);
  if (launch === undefined) {
    throw new Error("Real browser development launch is unavailable.");
  }
  const ssh = await startControlledSshServer({
    commandOutput: "password\n",
    materialBytes: Buffer.from("not-a-jks-truststore", "utf8"),
  });
  ssh.putFile("/etc/kafka/truststore.jks", Buffer.from("not-a-jks-truststore", "utf8"));
  const diagnostics = observeBrowserDiagnostics(page);

  try {
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.goto(launch.browserUrl);
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Invalid remote JKS");
    await prepareRemoteJksTrust(page, ssh);
    await editor.getByRole("button", { name: "Retrieve" }).click();
    await editor.getByRole("button", { name: "Accept identity and acquire" }).click();

    await expect(editor.getByRole("button", { name: "Retrieve" })).toBeEnabled({
      timeout: 30_000,
    });
    const error = editor.getByRole("alert");
    await expect(error).toContainText(
      "Kafka trust material is malformed, empty, unsupported, or contains a private key.",
    );
    await expect(error).toContainText(
      "Select a certificate-only PEM CA or a valid [REDACTED]-protected JKS/PKCS12 truststore.",
    );
    await error.getByRole("button", { name: "Open activity log" }).click();
    const activity = page.getByRole("complementary", { name: "Activity log" });
    const rawLog = activity.getByRole("log", { name: "Raw activity log" });
    await expect(rawLog).toContainText(
      new RegExp(
        `level=error.+operation="Fetch remote trust material".+object="${ssh.host}:${String(ssh.port)}"`,
        "u",
      ),
    );
    await expect(rawLog).toContainText("TRUST_MATERIAL");
    await expect(rawLog).not.toContainText("ssh-password");
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("remote-trust-invalid-material-activity.png"),
    });
    await collapseActivity(page);
    await expect(editor).toBeVisible();
    await expect(editor.getByRole("textbox", { name: "Profile name" })).toHaveValue(
      "Invalid remote JKS",
    );
    await expect(editor.getByRole("textbox", { name: "SSH password", exact: true })).toHaveValue(
      "ssh-password",
    );
    await expect(
      editor.getByRole("status", { name: "Remote trust acquisition status" }),
    ).toContainText("No remote trust acquired");
    expect(ssh.removedPaths).toHaveLength(0);
    expect(diagnostics.problems).toEqual([]);
  } finally {
    await ssh.close();
  }
});
