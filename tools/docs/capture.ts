import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, copyFile, readFile, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { promisify } from "node:util";

import { chromium, expect } from "@playwright/test";

import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { createKafkaProviderEndpoint } from "../../src/platform/node/kafka-provider";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import { launchWebDevelopment } from "../../src/platform/dev-host";
import { connectLocalProfile } from "../../test/support/web-profile-workflow";
import { openWorkbenchResource } from "../../test/support/workbench-browser";

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
  const arguments_ = process.argv.slice(2);
  if (arguments_.length > 1 || arguments_.some((argument) => argument !== "--profiles-only"))
    throw new Error("Usage: node --import tsx tools/docs/capture.ts [--profiles-only]");
  const profilesOnly = arguments_.includes("--profiles-only");
  const output = resolve(".artifacts/website/aio-captures");
  await mkdir(output, { recursive: true });
  const launch = await launchWebDevelopment({
    providers: new ProviderHostRegistry([
      createKafkaProviderEndpoint(createKafkaBackend()),
      createNatsProviderEndpoint(createNatsBackend()),
    ]),
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
      if (!profilesOnly) {
        await expect(
          page.getByRole("button", { name: "orders.events", exact: true }),
        ).toBeVisible();
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
        await page
          .getByRole("searchbox", { name: "Search consumer groups" })
          .fill("orders-workers");
        await page.getByRole("button", { name: "orders-workers", exact: true }).click();
        const offsets = page.getByRole("grid", { name: "Consumer group offsets" });
        await expect(offsets).toContainText("42");
        await expect(offsets).toContainText("49");
        await expect(offsets).toContainText("7");
        await capture("consumers");
      }
      await openWorkbenchResource(page, "Connection Profiles");
      const profiles = page.getByTestId("connection-profiles-grid");
      await expect(
        profiles.getByRole("columnheader", { name: "System", exact: true }),
      ).toBeVisible();
      await expect(profiles.locator('[role="gridcell"][data-field="providerLabel"]')).toHaveText(
        "Kafka",
      );
      await expect(page.getByRole("combobox", { name: "Messaging provider" })).toHaveCount(0);
      await profiles.getByRole("button", { name: "Select profile Local aio", exact: true }).click();
      const profileDetails = page.getByRole("region", { name: "Connection profile workspace" });
      await expect(profileDetails).toContainText("Local aio");
      // The inspector shares a narrow pane; reject a collapsed value column before publishing images.
      await expect
        .poll(async () =>
          profileDetails
            .locator('[data-property-label="Brokers"] dd')
            .evaluate((element) => element.getBoundingClientRect().width),
        )
        .toBeGreaterThanOrEqual(100);
      await capture("profiles");
      // Publish only after every real-broker view was successfully captured.
      for (const name of captures)
        await copyFile(resolve(output, name), resolve("website/docs/assets", name));
      const sourcePatch = profilesOnly
        ? (
            await promisify(execFile)(
              "git",
              [
                "diff",
                "HEAD",
                "--",
                "src",
                "config",
                "package.json",
                "package-lock.json",
                "test/support",
                "tools/docs/capture.ts",
              ],
              { encoding: "utf8" },
            )
          ).stdout
        : "";
      await writeFile(
        resolve(output, profilesOnly ? "profiles-provenance.json" : "provenance.json"),
        JSON.stringify(
          {
            capturedAt: new Date().toISOString(),
            backend: "createKafkaBackend connected to repository AIO Kafka",
            ...(profilesOnly
              ? {
                  sourceRevision: (
                    await promisify(execFile)("git", ["rev-parse", "HEAD"], { encoding: "utf8" })
                  ).stdout.trim(),
                  sourceDirty: sourcePatch.length > 0,
                  sourcePatchSha256: createHash("sha256").update(sourcePatch).digest("hex"),
                  view: "Shared Connection Profiles with selected, live-connected Kafka profile",
                  sha256: Object.fromEntries(
                    await Promise.all(
                      captures.map(
                        async (name) =>
                          [
                            name,
                            createHash("sha256")
                              .update(await readFile(resolve(output, name)))
                              .digest("hex"),
                          ] as const,
                      ),
                    ),
                  ),
                }
              : { topic: "orders.events", key: "ord-1042", group: "orders-workers" }),
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
