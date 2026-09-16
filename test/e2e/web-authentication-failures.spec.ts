import { createServer } from "node:http";
import { resolve } from "node:path";

import { expect, test } from "@playwright/test";

import { createKafkaBackend } from "../../src/platform/electron/main";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { loadFixtureConfig } from "../support/kafka-fixture";
import {
  collapseActivity,
  expectRawLogFields,
  observeBrowserDiagnostics,
} from "../support/workbench-browser";
import { configureLocalConnection } from "../support/web-profile-workflow";

let launch: RunningWebDevelopment | undefined;

async function reservePort(): Promise<number> {
  const server = createServer();
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
  const port = address.port;
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => (error === undefined ? resolveClose() : reject(error)));
  });
  return port;
}

test.describe("StreamSkope browser authentication failures", () => {
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

  test("rejects invalid OAuth credentials with actionable redacted diagnostics", async ({
    page,
  }, testInfo) => {
    if (launch === undefined) {
      throw new Error("Real browser development launch is unavailable.");
    }
    const config = await loadFixtureConfig();
    const invalidSecret = `${config.oauthClientSecret}-invalid`;
    const diagnostics = observeBrowserDiagnostics(page);

    await page.goto(launch.browserUrl);
    await configureLocalConnection(page, invalidSecret);
    await page.getByRole("button", { name: "Test connection" }).click();

    const error = page.getByRole("alert");
    await expect(error).toContainText("OAuth credentials were rejected.");
    await expect(error).toContainText(
      "Check the token endpoint, client identifier, client secret and required scope.",
    );
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");

    await error.getByRole("button", { name: "Open activity log" }).click();
    const activity = page.getByRole("complementary", { name: "Activity log" });
    await expectRawLogFields(activity, [
      ["Stage", "OAuth"],
      ["Category", "OAUTH_REJECTED"],
      ["Active connection changed", "No"],
    ]);
    await expect(activity).not.toContainText(config.oauthClientSecret);
    await expect(activity).not.toContainText(invalidSecret);
    await expect(page.locator("body")).not.toContainText(invalidSecret);
    await expect
      .poll(async () =>
        page.evaluate(() =>
          JSON.stringify({
            local: globalThis.localStorage,
            session: globalThis.sessionStorage,
            url: globalThis.location.href,
          }),
        ),
      )
      .not.toContain(invalidSecret);
    await testInfo.attach("redacted-host-activity", {
      body: await activity.innerText(),
      contentType: "text/plain",
    });
    await collapseActivity(page);
    const profileEditor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await expect(profileEditor).toBeVisible();
    await expect(profileEditor.getByRole("textbox", { name: "Profile name" })).toHaveValue(
      "Local aio",
    );
    await error.getByRole("button", { name: "Open activity log" }).click();
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("real-aio-kafka-oauth-rejected.png"),
    });
    expect(diagnostics.problems).toEqual([]);
  });
});
