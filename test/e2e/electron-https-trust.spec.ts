import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type ElectronApplication } from "@playwright/test";

import { AtomicKafkaTrustRecipeFileStore } from "../../src/main/kafka-trust-recipe-file-store";
import {
  buildElectronSmoke,
  buildRenderer,
  launchProfileApplication,
} from "../support/electron-application";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";
import { loadFixtureConnection } from "../support/kafka-fixture";
import { configureLocalConnection } from "../support/web-profile-workflow";
import { openWorkbenchResource, openProfileAction } from "../support/workbench-browser";

test.use({ trace: "off" });
test("protects HTTPS API CA across Electron restart while requiring new API credentials", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(120000);
  const kafka = await loadFixtureConnection();
  const kafkaCa = await readFile(kafka.caPath);
  const auth = `Basic ${Buffer.from("operator:fixture-api-password").toString("base64")}`;
  const fixture = await createHttpsTrustFixture((request, response) => {
    if (request.headers.authorization !== auth) {
      response.writeHead(401);
      response.end("not-authorized");
    } else response.end(JSON.stringify({ trust: kafkaCa.toString("base64") }));
  });
  await mkdir(resolve("dist"), { recursive: true });
  const directory = await mkdtemp(join(resolve("dist"), "electron-https-trust-"));
  const userData = join(directory, "user-data");
  let application: ElectronApplication | undefined;
  try {
    await buildElectronSmoke(directory);
    const renderer = await buildRenderer(directory);
    await new AtomicKafkaTrustRecipeFileStore(
      join(userData, "templates", "trust-acquisition-recipes.json"),
    ).commit({
      version: 1,
      recipes: [
        {
          id: "fixture-api",
          revision: 1,
          name: "Fixture HTTPS",
          method: "https",
          syntax: "named-v1",
          kind: "pem",
          parameters: [],
          timeoutSeconds: 30,
          https: {
            authentication: "basic",
            material: {
              url: `${fixture.origin}/certificates`,
              headers: [],
              query: [],
              extraction: { mode: "json-base64", pointer: "/trust" },
            },
            password: { source: "none" },
          },
        },
      ],
    });
    application = await launchProfileApplication(
      directory,
      renderer,
      userData,
      "available",
      undefined,
      info.outputPath("video"),
    );
    let page = await application.firstWindow();
    await page.setViewportSize({ width: 1440, height: 1000 });
    await configureLocalConnection(page);
    let editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    await editor.getByRole("combobox", { name: "Use profile", exact: true }).click();
    await page.getByRole("option", { name: "Fixture HTTPS", exact: true }).click();
    await editor.getByLabel("API username", { exact: true }).fill("operator");
    await editor.getByLabel("API password", { exact: false }).fill("fixture-api-password");
    await editor.getByLabel("API TLS trust", { exact: true }).click();
    await page.getByRole("option", { name: "Separate API CA (PEM)", exact: true }).click();
    await editor.getByLabel("API CA file", { exact: true }).setInputFiles({
      name: "fixture-api-ca.pem",
      mimeType: "application/x-pem-file",
      buffer: Buffer.from(fixture.caPem),
    });
    await editor.getByRole("button", { name: "Retrieve", exact: true }).click();
    await editor.getByRole("button", { name: "Apply to connection" }).click();
    await editor.getByRole("button", { name: "Test connection", exact: true }).click();
    await expect(
      editor.getByText(
        "Connection test passed. No profile was saved and the active connection was unchanged.",
      ),
    ).toBeVisible();
    await editor.getByRole("button", { name: "Save profile", exact: true }).click();
    await expect(editor).toBeHidden();
    const disk = await readFile(join(userData, "profiles", "kafka-profiles.json"), "utf8");
    expect(disk).not.toContain("fixture-api-password");
    expect(disk).not.toContain(fixture.caPem);
    await application.close();
    application = await launchProfileApplication(
      directory,
      renderer,
      userData,
      "available",
      undefined,
      info.outputPath("restart-video"),
    );
    page = await application.firstWindow();
    await page.setViewportSize({ width: 1440, height: 1000 });
    const requests = fixture.requests.length;
    await page.getByRole("button", { name: "Connect profile Local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected", {
      timeout: 15000,
    });
    expect(fixture.requests).toHaveLength(requests);
    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Select profile Local aio" }).click();
    await page.getByRole("button", { name: "Disconnect profile Local aio" }).click();
    await openProfileAction(page, "Local aio", "Edit");
    editor = page.getByRole("dialog", { name: "Edit Kafka profile Local aio" });
    await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    await expect(editor.getByLabel("API username", { exact: true })).toHaveValue("operator");
    await expect(editor.getByLabel("API password", { exact: false })).toHaveValue("");
    await expect(
      editor.getByText("Protected API CA retained. Select a file to replace it."),
    ).toBeVisible();
    await expect(editor.getByRole("button", { name: "Retrieve", exact: true })).toBeDisabled();
    await page.screenshot({ path: info.outputPath("protected-api-ca-ephemeral-credential.png") });
    await editor.getByLabel("API password", { exact: false }).fill("fixture-api-password");
    await editor.getByRole("button", { name: "Retrieve", exact: true }).click();
    await expect(editor.getByRole("button", { name: "Apply to connection" })).toBeVisible();
    expect(
      (await new AxeBuilder({ page }).setLegacyMode().include('[role="dialog"]').analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({ path: info.outputPath("retained-api-ca-reacquisition.png") });
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
  } finally {
    await application?.close();
    await fixture.close();
    await rm(directory, { recursive: true, force: true });
  }
});
