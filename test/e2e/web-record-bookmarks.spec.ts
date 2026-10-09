import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test, type Page } from "@playwright/test";
import { Admin, Producer } from "@platformatic/kafka";
import { build } from "vite";

import { inspectKafkaQueryLibraryDocument } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { openTopicDetail, openWorkbenchResource } from "../support/workbench-browser";

async function chooseView(page: Page, name: string): Promise<ReturnType<Page["getByRole"]>> {
  await page.getByRole("button", { name: "Saved views", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Saved views", exact: true });
  await dialog.getByRole("combobox", { name: "Saved view", exact: true }).click();
  await page.getByRole("option", { name, exact: true }).click();
  return dialog;
}
async function openView(page: Page, name: string): Promise<void> {
  const dialog = await chooseView(page, name);
  await dialog.getByRole("button", { name: "Open view", exact: true }).click();
  await expect(dialog).toHaveCount(0);
}
async function profileForView(page: Page): Promise<void> {
  await page
    .getByRole("dialog", { name: "Saved views", exact: true })
    .getByRole("combobox", { name: "Local connection profile", exact: true })
    .click();
  await page.getByRole("option", { name: "Local aio", exact: true }).click();
}

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("saves record positions through the UI and reloads current protected evidence after a vault host restart", async ({
  page,
}, info) => {
  test.setTimeout(300_000);
  page.setDefaultTimeout(30_000);
  const errors: string[] = [],
    commands: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  const activeCommands = (start: number): string[] =>
    commands
      .slice(start)
      .filter((name) =>
        [
          "profiles.connect",
          "connection.connect",
          "messages.start",
          "messages.continue",
          "consumerGroups.load",
          "records.locator.load",
          "latency.start",
          "records.analysis.start",
          "records.export.start",
        ].includes(name),
      );
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-bookmarks-browser-"));
  await mkdir(resolve("dist"), { recursive: true });
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-bookmarks-"));
  const groupId = `bookmark-group-${randomUUID()}`;
  const passphrase = `test-vault-${randomUUID()}`;
  const privateMarker = `unpersisted-record-${randomUUID()}`;
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let admin: Admin | undefined, producer: Producer<Buffer, Buffer, Buffer, Buffer> | undefined;
  let groupCreated = false;
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
    const options = await fixtureClientOptions(
      fixture.connection,
      fixture.config,
      `bookmark-browser-${randomUUID()}`,
    );
    admin = new Admin(options);
    producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      ...options,
      autocreateTopics: false,
    });
    await producer.send({
      messages: ["baseline", "selected"].map((kind) => ({
        topic: fixture!.config.topic,
        partition: 0,
        key: Buffer.from(`bookmark-${kind}`),
        value: Buffer.from(JSON.stringify({ kind, privateMarker })),
      })),
    });
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
    const read = page.getByRole("button", {
      name: `Load messages ${fixture.config.topic}`,
      exact: true,
    });
    await read.click();
    const grid = page.getByRole("grid", { name: "Kafka messages", exact: true });
    await grid.getByText("bookmark-baseline", { exact: true }).click();
    await expect(read).toBeEnabled();
    const inspector = page.getByRole("complementary", { name: "Message inspector", exact: true });
    await inspector.getByRole("tab", { name: "Compare", exact: true }).click();
    await inspector.getByRole("button", { name: "Pin as baseline", exact: true }).click();
    await grid.getByText("bookmark-selected", { exact: true }).click();
    await expect(
      inspector.getByRole("button", { name: "Bookmark record", exact: true }),
    ).toBeEnabled();
    await inspector.getByRole("button", { name: "Bookmark record", exact: true }).click();
    let dialog = page.getByRole("dialog", { name: "Saved views", exact: true });
    await dialog
      .getByRole("textbox", { name: "View name", exact: true })
      .fill("Record investigation");
    await profileForView(page);
    await dialog
      .getByRole("textbox", { name: "New bookmark name", exact: true })
      .fill("Selected fault");
    await dialog.getByRole("button", { name: "Save bookmark", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText(
      "Bookmark saved. Open the view to use its positions.",
    );
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await openView(page, "Record investigation");
    await openWorkbenchResource(page, "Consumer Groups");
    await page.getByRole("button", { name: groupId, exact: true }).click();
    await page.getByRole("button", { name: "Saved views", exact: true }).click();
    dialog = page.getByRole("dialog", { name: "Saved views", exact: true });
    await dialog.getByRole("textbox", { name: "View name", exact: true }).fill("Record group");
    await profileForView(page);
    await dialog.getByRole("button", { name: "Save current view", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText("View saved.");
    await dialog.getByRole("button", { name: "Close", exact: true }).click();

    const file = join(dataRoot, "queries", "kafka-queries.json");
    const beforeRestart = await readFile(file, "utf8");
    const stored = inspectKafkaQueryLibraryDocument(JSON.parse(beforeRestart));
    expect(stored.schemaVersion).toBe(3);
    expect(stored.queries).toHaveLength(2);
    const records = stored.queries.find((entry) => entry.name === "Record investigation")!.records;
    expect(records.selected).not.toBeNull();
    expect(records.comparison).not.toBeNull();
    expect(records.selected!.offset).not.toBe(records.comparison!.offset);
    expect(records.bookmarks).toHaveLength(1);
    expect(records.bookmarks[0]!.locator).toEqual(records.selected);
    expect(beforeRestart).not.toContain(privateMarker);
    expect(beforeRestart).not.toContain(Buffer.from(privateMarker).toString("base64"));
    expect(beforeRestart).not.toContain(passphrase);
    expect(beforeRestart).not.toContain(fixture.config.seedPayload);
    expect(JSON.stringify(records)).not.toMatch(
      /payload|original|structured|requestId|continuation/,
    );
    await page.goto("about:blank");
    await gateway.close();
    gateway = undefined;
    gateway = await startGateway();
    await page.goto(gateway.origin);
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    const passiveStart = commands.length;
    await openView(page, "Record group");
    expect(activeCommands(passiveStart)).toEqual([]);
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(page.getByRole("main", { name: "Consumer group detail page" })).toContainText(
      /not loaded/iu,
    );
    expect(activeCommands(passiveStart)).toEqual(["profiles.connect"]);
    dialog = await chooseView(page, "Record group");
    await dialog.getByRole("combobox", { name: "Saved bookmark", exact: true }).click();
    await page.getByRole("option", { name: "Selected fault", exact: true }).click();
    await dialog.getByRole("button", { name: "Open bookmarked topic", exact: true }).click();
    const positions = page.getByRole("region", { name: "Saved record positions", exact: true });
    await expect(positions.getByText("Not loaded", { exact: true })).toHaveCount(2);
    expect(activeCommands(passiveStart)).toEqual(["profiles.connect"]);
    await expect(inspector).toHaveCount(0);
    await expect(grid.getByText("bookmark-selected", { exact: true })).toHaveCount(0);
    await positions.getByRole("button", { name: "Load baseline", exact: true }).click();
    await expect(
      positions.getByText("Reloaded with current protection", { exact: true }),
    ).toHaveCount(1);
    await positions.getByRole("button", { name: "Reload selected", exact: true }).click();
    await inspector.getByRole("tab", { name: "Decoded", exact: true }).click();
    await expect(inspector.getByLabel("Decoded JSON", { exact: true })).toContainText(
      privateMarker,
    );

    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Disconnect profile Local aio", exact: true }).click();
    await expect(
      page.getByRole("button", { name: "Connect profile Local aio", exact: true }),
    ).toBeEnabled();
    await expect(inspector).toHaveCount(0);
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    const preferences = page.getByRole("dialog", { name: "Workbench Preferences", exact: true });
    await preferences.getByRole("tab", { name: "Protection", exact: true }).click();
    await preferences
      .getByRole("textbox", { name: "Decoded JSON value paths to mask", exact: true })
      .fill("/privateMarker");
    await preferences.getByRole("button", { name: "Save protection", exact: true }).click();
    await expect(preferences.getByRole("status")).toContainText("Protection saved");
    await preferences.getByRole("button", { name: "Close", exact: true }).click();
    const policyRestoreStart = commands.length;
    await openView(page, "Record investigation");
    expect(activeCommands(policyRestoreStart)).toEqual([]);
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(inspector).toHaveCount(0);
    await expect(positions.getByText("Not loaded", { exact: true })).toHaveCount(2);
    expect(activeCommands(policyRestoreStart)).toEqual(["profiles.connect"]);
    await positions.getByRole("button", { name: "Load baseline", exact: true }).click();
    await expect(
      positions.getByText("Reloaded with current protection", { exact: true }),
    ).toHaveCount(1);
    await positions.getByRole("button", { name: "Reload selected", exact: true }).click();
    await inspector.getByRole("tab", { name: "Decoded", exact: true }).click();
    await expect(inspector.getByLabel("Decoded JSON", { exact: true })).toContainText("[MASKED]");
    await expect(inspector).not.toContainText(privateMarker);
    await inspector.getByRole("tab", { name: "Compare", exact: true }).click();
    await inspector.getByRole("button", { name: "Compare records", exact: true }).click();
    await expect(inspector.getByRole("table", { name: "Differences", exact: true })).toContainText(
      "baseline",
    );
    await expect(inspector.getByRole("table", { name: "Differences", exact: true })).toContainText(
      "selected",
    );
    await expect(inspector).not.toContainText(privateMarker);
    await inspector.getByRole("tab", { name: "Rules", exact: true }).click();
    await expect(inspector).toContainText("Rules not evaluated");
    await page.getByRole("button", { name: "Close inspector", exact: true }).click();
    await page.setViewportSize({ width: 1000, height: 900 });
    await positions.getByRole("combobox", { name: "Record bookmark", exact: true }).click();
    await page.getByRole("option", { name: "Selected fault", exact: true }).click();
    const beforeChoice = commands.length;
    await positions.getByRole("button", { name: "Use as baseline", exact: true }).focus();
    await page.keyboard.press("Enter");
    expect(activeCommands(beforeChoice)).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      path: info.outputPath("saved-record-positions-1000px.png"),
      animations: "disabled",
    });
    expect(await readFile(file, "utf8")).toBe(beforeRestart);
    expect(commands.filter((name) => name === "records.locator.load")).toHaveLength(4);
    expect(commands.filter((name) => name === "messages.start")).toHaveLength(2);
    expect(errors).toEqual([]);
    await info.attach("record-bookmark-evidence", {
      body: Buffer.from(
        JSON.stringify({
          schemaVersion: 3,
          restart: true,
          selectedAndBaselineRestored: true,
          bookmarkedGroupNavigation: true,
          currentProtectionReapplied: true,
          explicitReloads: 4,
          implicitReloads: 0,
          savedRecordPayloads: 0,
          unchangedLibraryOnOpenReload: true,
        }),
      ),
      contentType: "application/json",
    });
  } finally {
    if (!page.isClosed() && info.status !== info.expectedStatus)
      await page
        .screenshot({ path: info.outputPath("bookmark-failure.png") })
        .catch(() => undefined);
    await page.close();
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      async (): Promise<void> => {
        if (admin !== undefined && groupCreated) await admin.deleteGroups({ groups: [groupId] });
      },
      (): Promise<void> => producer?.close() ?? Promise.resolve(),
      (): Promise<void> => admin?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
