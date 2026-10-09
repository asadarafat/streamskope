import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test, type Page } from "@playwright/test";
import { Producer } from "@platformatic/kafka";
import { build } from "vite";

import { inspectKafkaQueryLibraryDocument } from "../../src/features/kafka/contracts";
import { parseKafkaPortableView } from "../../src/features/kafka/contracts/view-transfer";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { openTopicDetail } from "../support/workbench-browser";

async function views(page: Page): Promise<ReturnType<Page["getByRole"]>> {
  await page.getByRole("button", { name: "Saved views", exact: true }).click();
  return page.getByRole("dialog", { name: "Saved views", exact: true });
}
async function useProfile(page: Page): Promise<void> {
  await page
    .getByRole("dialog", { name: "Saved views", exact: true })
    .getByRole("combobox", { name: "Local connection profile", exact: true })
    .click();
  await page.getByRole("option", { name: "Local aio", exact: true }).click();
}

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("exports and reviews portable views, persists only on Save and reloads protected positions after restart", async ({
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
  const effects = (start: number): string[] =>
    commands
      .slice(start)
      .filter((command) =>
        [
          "queries.put",
          "queries.delete",
          "catalog.put",
          "catalog.delete",
          "profiles.connect",
          "connection.connect",
          "messages.start",
          "messages.continue",
          "consumerGroups.load",
          "records.locator.load",
          "latency.start",
          "records.analysis.start",
          "records.export.start",
        ].includes(command),
      );
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-portable-browser-"));
  await mkdir(resolve("dist"), { recursive: true });
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-portable-"));
  const passphrase = `portable-vault-${randomUUID()}`;
  const privateMarker = `unshared-record-${randomUUID()}`;
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let producer: Producer<Buffer, Buffer, Buffer, Buffer> | undefined;
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
    producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      ...(await fixtureClientOptions(
        fixture.connection,
        fixture.config,
        `portable-${randomUUID()}`,
      )),
      autocreateTopics: false,
    });
    await producer.send({
      messages: [
        {
          topic: fixture.config.topic,
          partition: 0,
          key: Buffer.from("portable-protected"),
          value: Buffer.from(JSON.stringify({ kind: "portable", privateMarker })),
        },
      ],
    });
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
    await grid.getByText("portable-protected", { exact: true }).click();
    await expect(read).toBeEnabled();
    const inspector = page.getByRole("complementary", { name: "Message inspector", exact: true });
    await inspector.getByRole("tab", { name: "Compare", exact: true }).click();
    await inspector.getByRole("button", { name: "Pin as baseline", exact: true }).click();
    await inspector.getByRole("button", { name: "Bookmark record", exact: true }).click();
    let dialog = page.getByRole("dialog", { name: "Saved views", exact: true });
    await dialog
      .getByRole("textbox", { name: "View name", exact: true })
      .fill("Source investigation");
    await useProfile(page);
    await dialog
      .getByRole("textbox", { name: "New bookmark name", exact: true })
      .fill("First failure");
    await dialog.getByRole("button", { name: "Save bookmark", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText(
      "Bookmark saved. Open the view to use its positions.",
    );
    const file = join(dataRoot, "queries", "kafka-queries.json");
    const sourceBytes = await readFile(file, "utf8");
    const source = inspectKafkaQueryLibraryDocument(JSON.parse(sourceBytes)).queries[0]!;
    await dialog.getByText("Import/share investigations", { exact: true }).click();
    const downloaded = page.waitForEvent("download");
    await dialog.getByRole("button", { name: "Export view JSON", exact: true }).click();
    const download = await downloaded;
    expect(download.suggestedFilename()).toBe("streamskope-view.json");
    const portablePath = info.outputPath("portable-view.json");
    await download.saveAs(portablePath);
    const portableBytes = await readFile(portablePath, "utf8");
    const portable = parseKafkaPortableView(JSON.parse(portableBytes));
    expect(portable.records.selected).toEqual(source.records.selected);
    expect(portable.records.comparison).toEqual(source.records.comparison);
    expect(portable.records.bookmarks[0]?.locator).toEqual(source.records.bookmarks[0]?.locator);
    for (const excluded of [
      source.id,
      source.profileId!,
      source.records.bookmarks[0]!.id,
      privateMarker,
      passphrase,
      fixture.connection.oauthEndpoint,
    ])
      expect(portableBytes).not.toContain(excluded);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();

    dialog = await views(page);
    await dialog.getByText("Import/share investigations", { exact: true }).click();
    const passiveImport = commands.length;
    await dialog.getByLabel("View or query file", { exact: true }).setInputFiles({
      name: "bad-view.json",
      mimeType: "application/json",
      buffer: Buffer.from(JSON.stringify({ ...portable, profileId: "forbidden-source-profile" })),
    });
    await expect(dialog.getByRole("alert")).toContainText("invalid or unsupported import");
    expect(await readFile(file, "utf8")).toBe(sourceBytes);
    await dialog.getByLabel("View or query file", { exact: true }).setInputFiles(portablePath);
    await expect(dialog.getByLabel("Imported view preview", { exact: true })).toContainText(
      "First failure",
    );
    expect(effects(passiveImport)).toEqual([]);
    await dialog.getByRole("button", { name: "Open imported view", exact: true }).click();
    await expect(dialog).toHaveCount(0);
    const positions = page.getByRole("region", { name: "Saved record positions", exact: true });
    await expect(positions.getByText("Not loaded", { exact: true })).toHaveCount(2);
    await expect(inspector).toHaveCount(0);
    expect(effects(passiveImport)).toEqual([]);
    expect(await readFile(file, "utf8")).toBe(sourceBytes);
    dialog = await views(page);
    await dialog
      .getByRole("textbox", { name: "View name", exact: true })
      .fill("Imported investigation");
    await useProfile(page);
    await dialog.getByRole("button", { name: "Save current view", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText("View saved.");
    expect(effects(passiveImport)).toEqual(["queries.put"]);
    const afterSave = inspectKafkaQueryLibraryDocument(JSON.parse(await readFile(file, "utf8")));
    const imported = afterSave.queries.find((entry) => entry.name === "Imported investigation")!;
    expect(imported.id).not.toBe(source.id);
    expect(imported.records.bookmarks[0]?.id).not.toBe(source.records.bookmarks[0]?.id);
    expect(imported.records.selected).toEqual(source.records.selected);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();

    // A group-only portable file keeps the same verified positions without inventing a topic query.
    const group = {
      ...portable,
      suggestedName: "Portable worker context",
      configuration: null,
      view: {
        ...portable.view,
        destination: { kind: "consumer-group", groupId: "portable-workers" },
      },
    };
    dialog = await views(page);
    await dialog.getByText("Import/share investigations", { exact: true }).click();
    const groupStart = commands.length;
    await dialog.getByLabel("View or query file", { exact: true }).setInputFiles({
      name: "group-view.json",
      mimeType: "application/json",
      buffer: Buffer.from(JSON.stringify(group)),
    });
    await expect(dialog.getByLabel("Imported view preview", { exact: true })).toContainText(
      "portable-workers",
    );
    await dialog.getByRole("button", { name: "Open imported view", exact: true }).click();
    await expect(page.getByRole("main", { name: "Consumer group detail page" })).toContainText(
      /not loaded/iu,
    );
    expect(effects(groupStart)).toEqual([]);
    dialog = await views(page);
    await dialog
      .getByRole("textbox", { name: "View name", exact: true })
      .fill("Imported worker context");
    await useProfile(page);
    await dialog.getByRole("button", { name: "Save current view", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText("View saved.");
    const savedBytes = await readFile(file, "utf8");
    expect(savedBytes).not.toContain(privateMarker);
    expect(savedBytes).not.toContain(passphrase);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.goto("about:blank");
    await gateway.close();
    gateway = undefined;
    gateway = await startGateway();
    await page.goto(gateway.origin);
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    const preferences = page.getByRole("dialog", { name: "Workbench Preferences", exact: true });
    await preferences.getByRole("tab", { name: "Protection", exact: true }).click();
    await preferences
      .getByRole("textbox", { name: "Decoded JSON value paths to mask", exact: true })
      .fill("/privateMarker");
    await preferences.getByRole("button", { name: "Save protection", exact: true }).click();
    await expect(preferences.getByRole("status")).toContainText("Protection saved");
    await preferences.getByRole("button", { name: "Close", exact: true }).click();
    dialog = await views(page);
    await dialog.getByRole("combobox", { name: "Saved view", exact: true }).click();
    await page.getByRole("option", { name: "Imported worker context", exact: true }).click();
    const restartStart = commands.length;
    await dialog.getByRole("button", { name: "Open view", exact: true }).click();
    expect(effects(restartStart)).toEqual([]);
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(page.getByRole("main", { name: "Consumer group detail page" })).toContainText(
      /not loaded/iu,
    );
    dialog = await views(page);
    await dialog.getByRole("combobox", { name: "Saved view", exact: true }).click();
    await page.getByRole("option", { name: "Imported worker context", exact: true }).click();
    await dialog.getByRole("combobox", { name: "Saved bookmark", exact: true }).click();
    await page.getByRole("option", { name: "First failure", exact: true }).click();
    await dialog.getByRole("button", { name: "Open bookmarked topic", exact: true }).click();
    await expect(positions.getByText("Not loaded", { exact: true })).toHaveCount(2);
    expect(effects(restartStart)).toEqual(["profiles.connect"]);
    await positions.getByRole("button", { name: "Reload selected", exact: true }).click();
    await inspector.getByRole("tab", { name: "Decoded", exact: true }).click();
    await expect(inspector.getByLabel("Decoded JSON", { exact: true })).toContainText("[MASKED]");
    await expect(inspector).not.toContainText(privateMarker);
    expect(effects(restartStart)).toEqual(["profiles.connect", "records.locator.load"]);
    expect(await readFile(file, "utf8")).toBe(savedBytes);
    expect(errors).toEqual([]);
    await info.attach("portable-view-evidence", {
      body: Buffer.from(
        JSON.stringify({
          sourceIdsExcluded: true,
          reviewAndOpenPassive: true,
          explicitSave: true,
          groupOnlyPortable: true,
          hostRestart: true,
          currentProtectionOnExplicitReload: true,
        }),
      ),
      contentType: "application/json",
    });
  } finally {
    if (!page.isClosed() && info.status !== info.expectedStatus)
      await page
        .screenshot({ path: info.outputPath("portable-view-failure.png") })
        .catch(() => undefined);
    await page.close();
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      (): Promise<void> => producer?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
