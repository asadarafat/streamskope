import { createServer } from "node:net";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { createKafkaBackend } from "../../src/main";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";

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

let launch: RunningWebDevelopment;
test.beforeAll(async () => {
  launch = await launchWebDevelopment({
    backend: createKafkaBackend(),
    hostPort: await reservePort(),
    rendererPort: await reservePort(),
    rendererRoot: resolve(process.cwd()),
  });
});
test.afterAll(async () => {
  await launch?.close();
});

test("explains command output and flags legacy redirection before retrieval", async ({
  page,
}, info) => {
  await page.goto(launch.browserUrl);
  await page.getByRole("button", { name: "Add profile", exact: true }).click();
  await page.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
  await page
    .getByRole("dialog", { name: "Add Kafka profile" })
    .getByRole("button", { name: "Manage retrieval profiles", exact: true })
    .click();
  const manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
  await manager.getByRole("button", { name: /^SSH certificate file/u }).click();
  await manager.getByLabel("Material source", { exact: true }).click();
  await page.getByRole("option", { name: "Command output", exact: true }).click();
  const command = manager.getByLabel("Material command", { exact: true });
  await command.fill("cat /certificates/truststore.jks > {truststorePath}");
  await expect(command).toHaveAccessibleDescription(/raw certificate or truststore bytes/u);
  const warning = manager.getByRole("alert").filter({ hasText: "legacy output-file placeholder" });
  await expect(warning).toBeVisible();
  await page.screenshot({
    path: info.outputPath("command-output-guidance.png"),
    animations: "disabled",
  });
  await command.fill("cat /certificates/truststore.jks");
  await expect(warning).toHaveCount(0);
  await expect(command).toHaveValue("cat /certificates/truststore.jks");
});

test("manages generic retrieval profiles and exchanges definitions without conversion", async ({
  page,
}, info) => {
  await page.goto(launch.browserUrl);
  await page.getByRole("button", { name: "Add profile", exact: true }).click();
  const profile = page.getByRole("dialog", { name: "Add Kafka profile" });
  await profile.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
  await profile.getByRole("button", { name: "Manage retrieval profiles", exact: true }).click();
  const manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
  await expect(manager.getByRole("button", { name: "Convert legacy templates" })).toHaveCount(0);
  await manager.getByRole("button", { name: /^SSH certificate file/u }).click();
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("retrieval-profile-editor.png"),
  });
  await manager.getByRole("button", { name: "Duplicate", exact: true }).click();
  await expect(manager.getByLabel("Retrieval profile name", { exact: true })).toHaveValue(
    "SSH certificate file copy",
  );
  await manager.getByLabel("Search retrieval profiles", { exact: true }).fill("no such template");
  await expect(manager.getByText("No matching templates.")).toBeVisible();
  await expect(manager.getByLabel("Retrieval profile name", { exact: true })).toHaveValue(
    "SSH certificate file copy",
  );
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("empty-search-retains-draft.png"),
  });
  await manager.getByLabel("Search retrieval profiles", { exact: true }).fill("");
  await manager.getByRole("button", { name: "Save retrieval profile", exact: true }).click();
  await expect(manager.getByRole("status")).toHaveText("Retrieval profile saved.");
  const downloadEvent = page.waitForEvent("download");
  await manager.getByRole("button", { name: "Export", exact: true }).click();
  const download = await downloadEvent;
  const downloadedPath = await download.path();
  if (downloadedPath === null) throw new Error("Template download did not produce a file.");
  const contents = await readFile(downloadedPath);
  await manager
    .getByLabel("Import template file", { exact: true })
    .setInputFiles({ name: "reviewed.json", mimeType: "application/json", buffer: contents });
  await expect(manager.getByText(/Review imported commands/u)).toBeVisible();
  await manager.getByLabel("Retrieval profile name", { exact: true }).fill("Imported review");
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("template-import-review.png"),
  });
  await manager.getByRole("button", { name: "Save retrieval profile", exact: true }).click();
  await expect(manager.getByRole("status")).toHaveText("Retrieval profile saved.");
  await manager.getByRole("button", { name: "Delete retrieval profile", exact: true }).click();
  const deletion = page.getByRole("dialog", { name: "Delete retrieval profile Imported review?" });
  await expect(deletion).toContainText("No profiles use this template.");
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("template-delete-confirmation.png"),
  });
  await deletion.getByRole("button", { name: "Confirm deletion", exact: true }).click();
  await expect(manager.getByRole("status")).toHaveText(
    "Template deleted. Stored Kafka trust was not changed.",
  );
  await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
  await profile.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
});

for (const [colorScheme, width] of [
  ["light", 1440],
  ["light", 800],
  ["dark", 640],
] as const) {
  test(`edits trust recipes and preserves profile context in ${colorScheme} at ${width}`, async ({
    page,
  }, info) => {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    await page.setViewportSize({ width, height: 1000 });
    await page.goto(launch.browserUrl);
    const add = page.getByRole("button", { name: "Add profile", exact: true });
    await expect(add).toBeEnabled();
    await add.click();
    const profile = page.getByRole("dialog", { name: "Add Kafka profile" });
    await profile.getByRole("textbox", { name: "Profile name" }).fill("Unsaved profile");
    await page.screenshot({
      animations: "disabled",
      path: info.outputPath(`manual-connection-${colorScheme}-${width}.png`),
    });
    await profile.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    await expect(profile.getByRole("button", { name: "Select trust material" })).toBeVisible();
    await expect(profile.getByRole("textbox", { name: "SSH username" })).toHaveCount(0);
    await profile.getByRole("combobox", { name: "Use profile", exact: true }).click();
    await page.getByRole("option", { name: "SSH certificate file", exact: true }).click();
    await expect(profile.getByRole("textbox", { name: "SSH username" })).toBeVisible();
    await expect(profile).toContainText("not your Kafka login");
    const selectionBounds = await profile
      .getByRole("combobox", { name: "Use profile" })
      .boundingBox();
    const management = profile.getByRole("button", { name: "Manage retrieval profiles" });
    await expect(management).toHaveText("Manage…");
    const managementBounds = await management.boundingBox();
    if (selectionBounds === null || managementBounds === null)
      throw new Error("Missing retrieval controls");
    expect(
      Math.abs(
        selectionBounds.y +
          selectionBounds.height / 2 -
          managementBounds.y -
          managementBounds.height / 2,
      ),
    ).toBeLessThan(3);
    expect(managementBounds.x).toBeGreaterThan(selectionBounds.x + selectionBounds.width);
    await page.screenshot({
      animations: "disabled",
      path: info.outputPath(`optional-retrieval-${colorScheme}-${width}.png`),
    });
    await profile.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    await expect(profile.getByRole("textbox", { name: "Profile name" })).toHaveValue(
      "Unsaved profile",
    );
    const profileAccessibility = await new AxeBuilder({ page })
      .include('[role="dialog"]')
      .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
      .analyze();
    expect(profileAccessibility.violations).toEqual([]);
    await profile.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    const trigger = profile.getByRole("button", {
      name: "Manage retrieval profiles",
      exact: true,
    });
    await trigger.focus();
    await page.keyboard.press("Enter");
    const manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
    await manager.getByRole("button", { name: "New retrieval profile" }).click();
    await manager
      .getByLabel("Retrieval profile name", { exact: true })
      .fill(`Browser CA ${colorScheme} ${width}`);
    await manager.getByLabel("Remote file", { exact: true }).fill("/etc/kafka/ca.pem");
    await manager.getByRole("button", { name: "Save retrieval profile" }).click();
    await expect(manager.getByRole("status")).toHaveText("Retrieval profile saved.");
    await expect(manager).toBeVisible();
    const overflow = await manager.evaluate(
      (element) => element.scrollWidth > element.clientWidth + 1,
    );
    expect(overflow).toBe(false);
    const accessibility = await new AxeBuilder({ page })
      .include('[aria-labelledby="trust-recipes-title"]')
      .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
      .analyze();
    expect(accessibility.violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: info.outputPath(`trust-recipes-${colorScheme}-${width}.png`),
      fullPage: true,
    });
    await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
    await expect(manager).not.toBeVisible();
    await expect(trigger).toBeFocused();
    await expect(profile.getByRole("textbox", { name: "Profile name" })).toHaveValue(
      "Unsaved profile",
    );
  });
}
