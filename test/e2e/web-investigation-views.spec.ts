import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test, type Page } from "@playwright/test";
import { Admin } from "@platformatic/kafka";
import { build } from "vite";

import { inspectKafkaQueryLibraryDocument } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import {
  openTopicDetail,
  openTopicTask,
  openWorkbenchResource,
} from "../support/workbench-browser";

async function saveView(page: Page, name: string): Promise<void> {
  await page.getByRole("button", { name: "Saved views", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Saved views", exact: true });
  await dialog.getByRole("textbox", { name: "View name", exact: true }).fill(name);
  await dialog.getByRole("combobox", { name: "Local connection profile", exact: true }).click();
  await page.getByRole("option", { name: "Local aio", exact: true }).click();
  await dialog.getByRole("button", { name: "Save current view", exact: true }).click();
  await expect(dialog.getByRole("status")).toHaveText("View saved.");
  await dialog.getByRole("button", { name: "Close", exact: true }).click();
}
async function openView(page: Page, name: string): Promise<void> {
  const trigger = page.getByRole("button", { name: "Saved views", exact: true });
  await trigger.focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Saved views", exact: true });
  await dialog.getByRole("combobox", { name: "Saved view", exact: true }).click();
  await page.getByRole("option", { name, exact: true }).click();
  await dialog.getByRole("button", { name: "Open view", exact: true }).click();
  await expect(dialog).toHaveCount(0);
}

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("restores durable topic and group views after a production vault host restart without starting work", async ({
  page,
}, info) => {
  test.setTimeout(300_000);
  page.setDefaultTimeout(30_000);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const commands: string[] = [];
  // Passive observation of real browser traffic; never replace runtime dispatch.
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  const activeCommands = (start: number): string[] =>
    commands
      .slice(start)
      .filter(
        (name) =>
          [
            "profiles.connect",
            "connection.connect",
            "messages.start",
            "messages.continue",
            "consumerGroups.load",
            "latency.start",
            "records.locator.load",
            "records.analysis.start",
            "records.export.start",
          ].includes(name) || /\.(?:apply|register|create|delete)$/u.test(name),
      );
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-views-browser-"));
  await mkdir(resolve("dist"), { recursive: true });
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-views-"));
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let admin: Admin | undefined;
  let groupCreated = false;
  const groupId = `saved-view-${randomUUID()}`;
  const passphrase = `test-vault-${randomUUID()}`;
  const startGateway = (): ReturnType<typeof startWebGateway> =>
    startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: (value, mode) => openBrowserRuntime(dataRoot, value, mode),
    });
  try {
    await build({
      configFile: resolve("config/vite.config.ts"),
      logLevel: "silent",
      build: { outDir: rendererRoot },
    });
    fixture = await startStructuredBrowserFixture();
    admin = new Admin(
      await fixtureClientOptions(
        fixture.connection,
        fixture.config,
        `view-fixture-${randomUUID()}`,
      ),
    );
    await admin.alterConsumerGroupOffsets({
      groupId,
      topics: [{ name: fixture.config.topic, partitionOffsets: [{ partition: 0, offset: 0n }] }],
    });
    groupCreated = true;
    gateway = await startGateway();
    if (gateway.setupCodePath === undefined) throw new Error("Fresh vault omitted setup code.");
    await page.goto(gateway.origin);
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await connectLocalProfile(page, fixture.connection);
    await openTopicDetail(page, fixture.config.topic);
    await page
      .getByRole("button", { name: `Stop tail ${fixture.config.topic}`, exact: true })
      .click();
    await expect(page.getByRole("combobox", { name: "Read mode", exact: true })).toBeEnabled();
    await page.getByRole("combobox", { name: "Read mode", exact: true }).click();
    await page.getByRole("option", { name: "First N", exact: true }).click();
    await page.getByRole("button", { name: "Show message filters", exact: true }).click();
    await page.getByRole("textbox", { name: "Key contains", exact: true }).fill("streamskope-seed");
    const read = page.getByRole("button", {
      name: `Load messages ${fixture.config.topic}`,
      exact: true,
    });
    await read.click();
    const grid = page.getByRole("grid", { name: "Kafka messages", exact: true });
    await grid.getByText(fixture.config.seedPayload, { exact: true }).click();
    await expect(read).toBeEnabled();
    const separator = page.getByRole("separator", {
      name: "Resize messages and inspector",
      exact: true,
    });
    await separator.focus();
    await page.keyboard.press("ArrowLeft");
    const inspectorWidth = Number(await separator.getAttribute("aria-valuenow"));
    expect(inspectorWidth).toBeGreaterThan(320);
    await page.getByRole("button", { name: "Close inspector", exact: true }).click();
    // Use the shipped grid's column menu, not persisted-file injection.
    const partition = grid.getByRole("columnheader", { name: /^Partition/u });
    await partition.focus();
    await page.keyboard.press("Control+Enter");
    await page.getByRole("menuitem", { name: /Hide column/u }).click();
    await expect(grid.getByRole("columnheader", { name: /^Partition/u })).toHaveCount(0);
    await saveView(page, "Order investigation");
    await openTopicTask(page, "Monitor");
    await saveView(page, "Order monitor");
    await openWorkbenchResource(page, "Consumer Groups");
    await page.getByRole("button", { name: groupId, exact: true }).click();
    await expect(page.getByRole("main", { name: "Consumer group detail page" })).toContainText(
      groupId,
    );
    await saveView(page, "Order workers");
    await openWorkbenchResource(page, "Topics");
    await expect(
      page.getByRole("heading", { name: fixture.config.topic, exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("tab", { name: "Monitor", exact: true })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    const file = join(dataRoot, "queries", "kafka-queries.json");
    const beforeRestart = await readFile(file, "utf8");
    const stored = inspectKafkaQueryLibraryDocument(JSON.parse(beforeRestart));
    expect(stored.schemaVersion).toBe(4);
    expect(stored.queries).toHaveLength(3);
    expect(
      stored.queries.find((view) => view.name === "Order investigation")?.view.messages,
    ).toMatchObject({
      visibleColumns: ["timestamp", "key", "preview", "offset", "rules"],
      inspectorWidth,
      filtersOpen: true,
    });
    expect(beforeRestart).not.toContain(fixture.config.seedPayload);
    expect(beforeRestart).not.toContain(passphrase);
    expect(beforeRestart).not.toContain(fixture.connection.oauthEndpoint);
    await page.goto("about:blank");
    await gateway.close();
    gateway = undefined;
    gateway = await startGateway();
    await page.goto(gateway.origin);
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    const disconnectedStart = commands.length;
    await openView(page, "Order investigation");
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    expect(activeCommands(disconnectedStart)).toEqual([]);
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(
      page.getByRole("heading", { name: fixture.config.topic, exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("combobox", { name: "Read mode", exact: true })).toHaveText(
      "First N",
    );
    await expect(page.getByRole("textbox", { name: "Key contains", exact: true })).toHaveValue(
      "streamskope-seed",
    );
    await expect(grid.getByRole("columnheader", { name: /^Partition/u })).toHaveCount(0);
    expect(activeCommands(disconnectedStart)).toEqual(["profiles.connect"]);
    expect(await readFile(file, "utf8")).toBe(beforeRestart);
    await expect(grid.getByText(fixture.config.seedPayload, { exact: true })).toHaveCount(0);
    await read.click();
    await grid.getByText(fixture.config.seedPayload, { exact: true }).click();
    await expect(read).toBeEnabled();
    await expect(separator).toHaveAttribute("aria-valuenow", String(inspectorWidth));
    await page.getByRole("button", { name: "Close inspector", exact: true }).click();
    const groupStart = commands.length;
    await openView(page, "Order workers");
    const group = page.getByRole("main", { name: "Consumer group detail page" });
    await expect(group).toContainText(groupId);
    await expect(group).toContainText(/not loaded/iu);
    expect(activeCommands(groupStart)).toEqual([]);
    await group
      .getByRole("button", { name: `Refresh consumer group ${groupId}`, exact: true })
      .click();
    await expect(group).toContainText(fixture.config.topic);
    expect(activeCommands(groupStart)).toEqual(["consumerGroups.load"]);
    const monitorStart = commands.length;
    await openView(page, "Order monitor");
    await expect(page.getByRole("tab", { name: "Monitor", exact: true })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(activeCommands(monitorStart)).toEqual([]);
    await page.setViewportSize({ width: 1000, height: 900 });
    await openView(page, "Order investigation");
    await expect(page.getByRole("textbox", { name: "Key contains", exact: true })).toHaveValue(
      "streamskope-seed",
    );
    const bounds = await page.getByRole("main", { name: "Topic detail page" }).boundingBox();
    expect(bounds).not.toBeNull();
    expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(1000);
    const filterRegion = page.getByRole("region", { name: "Message filters", exact: true });
    for (const field of [filterRegion, ...(await filterRegion.getByRole("textbox").all())]) {
      const box = await field.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.x).toBeGreaterThanOrEqual(bounds!.x);
      expect(box!.x + box!.width).toBeLessThanOrEqual(bounds!.x + bounds!.width);
    }
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      path: info.outputPath("restored-view-1000px.png"),
      animations: "disabled",
    });
    expect(errors).toEqual([]);
    expect(commands.filter((name) => name === "messages.start")).toHaveLength(3);
    expect(
      commands.filter((name) =>
        ["latency.start", "records.analysis.start", "records.export.start"].includes(name),
      ),
    ).toEqual([]);
    await info.attach("view-restoration", {
      body: Buffer.from(
        JSON.stringify({
          savedViews: 3,
          schemaVersion: 4,
          restart: true,
          explicitReads: commands.filter((name) => name === "messages.start").length,
          inspectorWidth,
        }),
      ),
      contentType: "application/json",
    });
  } finally {
    if (!page.isClosed() && info.status !== info.expectedStatus)
      await page.screenshot({ path: info.outputPath("view-failure.png") }).catch(() => undefined);
    await page.close();
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      async (): Promise<void> => {
        if (admin !== undefined && groupCreated) await admin.deleteGroups({ groups: [groupId] });
      },
      (): Promise<void> => admin?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
