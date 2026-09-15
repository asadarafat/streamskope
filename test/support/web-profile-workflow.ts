import { expect, type Page } from "@playwright/test";

import { loadFixtureConfig, loadFixtureConnection } from "./kafka-fixture";

export async function configureLocalConnection(page: Page, clientSecret?: string): Promise<void> {
  const config = await loadFixtureConfig();
  const fixture = await loadFixtureConnection();
  await page.getByRole("button", { name: "Add profile" }).click();
  const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
  await editor.getByRole("textbox", { name: "Profile name" }).fill("Local aio");
  await editor.getByRole("textbox", { name: "Bootstrap brokers" }).fill(fixture.kafkaEndpoint);
  await editor.getByRole("combobox", { name: "Trust material format" }).click();
  await page.getByRole("option", { name: "PEM certificate" }).click();
  await editor.getByLabel("Trust material file").setInputFiles(fixture.caPath);
  await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
  await editor.getByRole("textbox", { name: "OAuth token endpoint" }).fill(fixture.oauthEndpoint);
  await editor.getByRole("textbox", { name: "OAuth client ID" }).fill(config.oauthClientId);
  await editor
    .getByRole("textbox", { name: "OAuth client secret", exact: true })
    .fill(clientSecret ?? config.oauthClientSecret);
  await editor.getByRole("textbox", { name: "OAuth scope" }).fill(config.oauthScope);
  if (fixture.schemaRegistryEndpoint !== undefined) {
    await editor
      .getByRole("textbox", { name: "Schema Registry URL" })
      .fill(fixture.schemaRegistryEndpoint);
    await editor.getByRole("combobox", { name: "Schema Registry authentication" }).click();
    await page.getByRole("option", { name: "Profile OAuth bearer token" }).click();
  }
}

export async function connectLocalProfile(page: Page): Promise<void> {
  await configureLocalConnection(page);
  const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
  await editor.getByRole("button", { name: "Save profile" }).click();
  await page.getByRole("button", { name: "Connect profile Local aio" }).click();
  await expect(page.getByLabel("Connection status")).toContainText("Connected");
}
