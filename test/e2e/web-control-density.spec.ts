import { createServer } from "node:net";
import { resolve } from "node:path";

import { expect, test, type Locator } from "@playwright/test";

import { createKafkaBackend } from "../../src/platform/electron/main";
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

async function height(locator: Locator): Promise<number> {
  return locator.evaluate((element) => element.getBoundingClientRect().height);
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

for (const colorScheme of ["light", "dark"] as const) {
  test(`aligns compact controls with natural buttons in ${colorScheme}`, async ({ page }, info) => {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    await page.goto(launch.browserUrl);
    const add = page.getByRole("button", { name: "Add profile", exact: true });
    await expect(add).toBeEnabled();
    const buttonHeight = await height(add);
    expect(buttonHeight).toBeGreaterThanOrEqual(24);
    expect(buttonHeight).toBeLessThan(30);
    await add.click();
    const dialog = page.getByRole("dialog", { name: "Add Kafka profile" });
    const name = dialog.getByRole("textbox", { name: "Profile name" });
    const inputBox = name.locator("..");
    const select = dialog.getByRole("combobox", { name: "Trust material format" });
    await expect
      .poll(async () => Math.abs((await height(inputBox)) - buttonHeight))
      .toBeLessThan(0.5);
    expect(Math.abs((await height(select.locator(".."))) - buttonHeight)).toBeLessThan(0.5);
    await name.fill("Compact profile");
    await expect(name).toHaveValue("Compact profile");
    await select.focus();
    await page.keyboard.press("ArrowDown");
    await expect(page.getByRole("listbox")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(select).toBeFocused();
    const brokerLabel = dialog.locator("label").filter({ hasText: "Bootstrap brokers" });
    const labelBounds = await brokerLabel.boundingBox();
    const brokerBounds = await dialog
      .getByRole("textbox", { name: "Bootstrap brokers" })
      .locator("..")
      .boundingBox();
    expect(labelBounds).not.toBeNull();
    expect(brokerBounds).not.toBeNull();
    if (labelBounds !== null && brokerBounds !== null) {
      expect(
        Math.abs(labelBounds.y + labelBounds.height / 2 - brokerBounds.y - brokerBounds.height / 2),
      ).toBeLessThan(0.5);
    }
    await page.screenshot({ path: info.outputPath(`compact-controls-${colorScheme}.png`) });
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
  });
}
