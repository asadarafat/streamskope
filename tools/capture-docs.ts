import { mkdir, copyFile, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";

import { chromium, expect } from "@playwright/test";

import { createKafkaBackend } from "../src/platform/electron/main";
import { launchWebDevelopment } from "../src/platform/dev-host";
import { connectLocalProfile } from "../test/support/web-profile-workflow";
import { openWorkbenchResource } from "../test/support/workbench-browser";

async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  const address = server.address();
  if (address === null || typeof address === "string")
    throw new Error("No capture port available.");
  await new Promise<void>((done, reject) =>
    server.close((error) => (error ? reject(error) : done())),
  );
  return address.port;
}

async function main(): Promise<void> {
  const output = resolve(".artifacts/website/aio-captures");
  await mkdir(output, { recursive: true });
  const launch = await launchWebDevelopment({
    backend: createKafkaBackend(),
    hostPort: await availablePort(),
    rendererPort: await availablePort(),
    rendererRoot: process.cwd(),
  });
  try {
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage({
        viewport: { width: 1440, height: 900 },
        deviceScaleFactor: 2,
        colorScheme: "light",
        reducedMotion: "reduce",
      });
      const captures: string[] = [];
      async function capture(subject: string): Promise<void> {
        for (const theme of ["light", "dark"] as const) {
          await page.emulateMedia({ colorScheme: theme });
          await expect(page.locator("html")).toHaveAttribute("data-mui-color-scheme", theme);
          await expect(page.getByRole("banner", { name: "StreamSkope application bar" })).toHaveCSS(
            "background-color",
            theme === "dark" ? "rgb(21, 21, 24)" : "rgb(244, 244, 245)",
          );
          await page.mouse.move(0, 0);
          const name = `${subject}${theme === "dark" ? "-dark" : ""}.png`;
          await page.screenshot({ path: resolve(output, name), animations: "disabled" });
          captures.push(name);
        }
      }
      await page.goto(launch.browserUrl);
      await connectLocalProfile(page);
      await expect(page.getByRole("button", { name: "orders.events", exact: true })).toBeVisible();
      await capture("topics");
      await page.getByRole("button", { name: "orders.events", exact: true }).click();
      const grid = page.getByRole("grid", { name: "Kafka messages" });
      await expect(grid.getByText("ord-1042", { exact: true })).toBeVisible({ timeout: 15_000 });
      await grid.getByText("ord-1042", { exact: true }).click();
      const inspector = page.getByRole("complementary", { name: "Message inspector" });
      await inspector.getByRole("tab", { name: "Metadata", exact: true }).click();
      await expect(inspector.getByLabel("Message headers")).toContainText("application/json");
      await capture("messages");
      await inspector.getByRole("tab", { name: "Value", exact: true }).click();
      await expect(inspector).toContainText("accepted");
      await capture("message-value");
      await openWorkbenchResource(page, "Consumer Groups");
      await page.getByRole("searchbox", { name: "Search consumer groups" }).fill("orders-workers");
      await page.getByRole("button", { name: "orders-workers", exact: true }).click();
      const offsets = page.getByRole("grid", { name: "Consumer group offsets" });
      await expect(offsets).toContainText("42");
      await expect(offsets).toContainText("49");
      await expect(offsets).toContainText("7");
      await capture("consumers");
      await openWorkbenchResource(page, "Connection Profiles");
      await capture("profiles");
      // Publish only after every real-broker view was successfully captured.
      for (const name of captures)
        await copyFile(resolve(output, name), resolve("website/docs/assets", name));
      await writeFile(
        resolve(output, "provenance.json"),
        JSON.stringify(
          {
            capturedAt: new Date().toISOString(),
            backend: "createKafkaBackend connected to repository AIO Kafka",
            topic: "orders.events",
            key: "ord-1042",
            group: "orders-workers",
            viewport: [1440, 900],
            deviceScaleFactor: 2,
            captures,
          },
          null,
          2,
        ) + "\n",
      );
      process.stdout.write(`Captured ${captures.length} real AIO-backed documentation images.\n`);
    } finally {
      await browser.close();
    }
  } finally {
    await launch.close();
  }
}

main().catch(() => {
  process.stderr.write(
    "AIO documentation capture failed. Verify the local fixture and required record/group; images were not substituted.\n",
  );
  process.exitCode = 1;
});
