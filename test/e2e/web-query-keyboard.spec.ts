import { createServer } from "node:net";
import { resolve } from "node:path";

import { expect, test, type Locator, type Page } from "@playwright/test";

import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import {
  launchProductWebFixture,
  type RunningWebDevelopment,
} from "../support/product-web-fixture";
import {
  loadFixtureConnection,
  provisionSeededFixtureTopic,
  type SeededFixtureTopic,
} from "../support/kafka-fixture";
import { configureLocalConnection } from "../support/web-profile-workflow";

test.use({ trace: "off" });
let launch: RunningWebDevelopment | undefined;
let seededFixtureTopic: SeededFixtureTopic | undefined;

async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  await new Promise<void>((resolveClose, reject) =>
    server.close((error) => (error ? reject(error) : resolveClose())),
  );
  if (address === null || typeof address === "string") throw new Error("No fixture port reserved.");
  return address.port;
}

test.beforeEach(async () => {
  launch = await launchProductWebFixture({
    backend: createKafkaBackend(),
    hostPort: await reservePort(),
    rendererPort: await reservePort(),
    rendererRoot: resolve(process.cwd()),
  });
});
test.afterEach(async () => {
  try {
    await launch?.close();
  } finally {
    launch = undefined;
    await seededFixtureTopic?.dispose();
    seededFixtureTopic = undefined;
  }
});

async function tabTo(page: Page, target: Locator): Promise<void> {
  for (let index = 0; index < 100; index += 1) {
    if (await target.evaluate((element) => element === document.activeElement)) return;
    await page.keyboard.press("Tab");
  }
  await expect(target).toBeFocused();
}

async function keyboardCommand(page: Page, query: string, label: string): Promise<void> {
  await page.keyboard.press("ControlOrMeta+k");
  const palette = page.getByRole("dialog", { name: "Search and commands" });
  await expect(palette.getByRole("searchbox")).toBeFocused();
  await page.keyboard.type(query);
  await page.keyboard.press("ArrowDown");
  await expect(palette.getByRole("button", { name: label, exact: true })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(palette).toBeHidden();
}

test("investigates and reopens a saved query using only the keyboard after profile setup", async ({
  page,
}) => {
  test.setTimeout(90_000);
  if (launch === undefined) throw new Error("Browser fixture is unavailable.");
  seededFixtureTopic = await provisionSeededFixtureTopic();
  const config = seededFixtureTopic.config;
  const fixture = await loadFixtureConnection();
  await page.goto(launch.browserUrl);
  // Profile provisioning is setup. All connection and investigation actions below use keys.
  await configureLocalConnection(page);
  await page.getByRole("button", { name: "Save profile" }).click();
  await expect(page.getByRole("dialog", { name: "Add Kafka profile" })).toBeHidden();
  await keyboardCommand(page, fixture.kafkaEndpoint, "Select profile Local aio");
  await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
  await keyboardCommand(page, "Connect profile Local aio", "Connect profile Local aio");
  await expect(page.getByLabel("Connection status")).toContainText("Connected");
  await expect(page.getByRole("button", { name: config.topic, exact: true })).toBeVisible();
  await keyboardCommand(page, config.topic, `Open and read topic ${config.topic}`);
  await expect(page.getByLabel("Consumption status")).toContainText("Streaming");
  await keyboardCommand(page, "Stop current read", "Stop current read");

  const mode = page.getByRole("combobox", { name: "Read mode" });
  await expect(mode).toBeEnabled();
  await tabTo(page, mode);
  await page.keyboard.press("Enter");
  await page.keyboard.press("Home");
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Enter");
  await expect(mode).toContainText("First N");
  await tabTo(page, page.getByRole("button", { name: "Show message filters" }));
  await page.keyboard.press("Enter");
  await tabTo(page, page.getByRole("textbox", { name: "Key contains" }));
  await page.keyboard.type("streamskope-seed");
  await keyboardCommand(page, "Search broker", `Search broker ${config.topic}`);
  await expect(page.getByRole("region", { name: "Read coverage" })).toContainText(
    "1 match returned",
    { timeout: 20_000 },
  );
  const grid = page.getByRole("grid", { name: "Kafka messages" });
  await expect(grid.getByText(config.seedPayload, { exact: true })).toBeVisible();
  await tabTo(page, grid.getByRole("columnheader").first());
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Shift+Space");
  const inspector = page.getByRole("complementary", { name: "Message inspector" });
  await expect(inspector).toBeVisible();
  await tabTo(page, page.getByRole("button", { name: "Close inspector" }));
  await page.keyboard.press("Enter");
  await expect(inspector).toBeHidden();

  await keyboardCommand(page, "Saved queries", "Saved queries");
  let queries = page.getByRole("dialog", { name: "Saved queries" });
  await tabTo(page, queries.getByRole("textbox", { name: "Query name" }));
  await page.keyboard.type("Keyboard incident");
  await tabTo(page, queries.getByRole("button", { name: "Save current as new" }));
  await page.keyboard.press("Enter");
  await expect(queries).toContainText("Query saved.");
  await page.keyboard.press("Escape");
  await expect(queries).toBeHidden();
  await keyboardCommand(page, "Saved queries", "Saved queries");
  queries = page.getByRole("dialog", { name: "Saved queries" });
  await tabTo(page, queries.getByRole("combobox", { name: "Saved query" }));
  await page.keyboard.press("Enter");
  await page.keyboard.press("End");
  await page.keyboard.press("Enter");
  await tabTo(page, queries.getByRole("button", { name: "Open query" }));
  await page.keyboard.press("Enter");
  await expect(queries).toBeHidden();
  await expect(page.getByRole("region", { name: "Read coverage" })).toHaveCount(0);
  await expect(mode).toContainText("First N");
  await expect(page.getByRole("textbox", { name: "Key contains" })).toHaveValue("streamskope-seed");
  await expect(page.getByRole("button", { name: `Load messages ${config.topic}` })).toBeEnabled();
  await keyboardCommand(page, "Search broker", `Search broker ${config.topic}`);
  await expect(page.getByRole("region", { name: "Read coverage" })).toContainText(
    "1 match returned",
    { timeout: 20_000 },
  );
});
