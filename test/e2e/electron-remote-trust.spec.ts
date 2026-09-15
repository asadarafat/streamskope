import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { expect, test, type ElectronApplication } from "@playwright/test";

import {
  buildElectronSmoke,
  buildRenderer,
  launchProfileApplication,
} from "../support/electron-application";
import { loadFixtureConfig, loadFixtureConnection } from "../support/kafka-fixture";
import { acquireRemoteJksTrust } from "../support/remote-trust-workflow";
import { startControlledSshServer } from "../support/ssh-fixture";
import {
  collapseActivity,
  expectRawLogEvidence,
  openActivity,
  openWorkbenchResource,
} from "../support/workbench-browser";

const repositoryRoot = process.cwd();

test.use({ trace: "off" });

test("acquires selected remote JKS trust and connects through the Electron host", async ({
  browserName: _browserName,
}, testInfo) => {
  test.setTimeout(90_000);
  await mkdir(resolve(repositoryRoot, "dist"), { recursive: true });
  const outputDirectory = await mkdtemp(
    join(resolve(repositoryRoot, "dist"), "electron-remote-trust-e2e-"),
  );
  const userDataPath = join(outputDirectory, "user-data");
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  const truststore = await readFile(join(dirname(fixture.caPath), "kafka.truststore.jks"));
  const ssh = await startControlledSshServer({
    commandOutput: "password\n",
    materialBytes: truststore,
  });
  ssh.putFile("/etc/kafka/truststore.jks", truststore);
  let application: ElectronApplication | undefined;

  try {
    await buildElectronSmoke(outputDirectory);
    const rendererUrl = await buildRenderer(outputDirectory);
    application = await launchProfileApplication(outputDirectory, rendererUrl, userDataPath);
    const page = await application.firstWindow();
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Electron remote aio");
    await editor.getByRole("textbox", { name: "Bootstrap brokers" }).fill(fixture.kafkaEndpoint);
    await acquireRemoteJksTrust(page, ssh);
    await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
    await editor.getByRole("textbox", { name: "OAuth token endpoint" }).fill(fixture.oauthEndpoint);
    await editor.getByRole("textbox", { name: "OAuth client ID" }).fill(config.oauthClientId);
    await editor
      .getByRole("textbox", { name: "OAuth client secret", exact: true })
      .fill(config.oauthClientSecret);
    await editor.getByRole("textbox", { name: "OAuth scope" }).fill(config.oauthScope);

    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("electron-remote-trust-profile-ready.png"),
    });
    await editor.getByRole("button", { name: "Save profile" }).click();
    await expect(
      page.getByRole("button", { name: "Select profile Electron remote aio" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Connect profile Electron remote aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected", {
      timeout: 10_000,
    });
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
    await page.getByRole("button", { name: "Select profile Electron remote aio" }).click();
    await page.getByRole("button", { name: "Disconnect profile Electron remote aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
  } finally {
    await application?.close();
    await ssh.close();
    await rm(outputDirectory, { force: true, recursive: true });
  }
});
