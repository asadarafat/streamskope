import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Admin } from "@platformatic/kafka";
import { expect, test, type Locator, type Page } from "@playwright/test";
import { build } from "vite";

import { inspectKafkaQueryLibraryDocument } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { openTopicDetail } from "../support/workbench-browser";

async function openSavedNotes(page: Page): Promise<Locator> {
  await page.getByRole("button", { name: "Saved views", exact: true }).click();
  await page
    .getByRole("dialog", { name: "Saved views", exact: true })
    .getByRole("button", { name: "Local topic notes", exact: true })
    .click();
  return page.getByRole("dialog", { name: "Local topic notes", exact: true });
}
async function chooseSavedNotes(page: Page, dialog: Locator, prefix: string): Promise<void> {
  await dialog.getByRole("combobox", { name: "Saved topic notes", exact: true }).click();
  await page.getByRole("option").filter({ hasText: prefix }).click();
}
async function stopTopicTail(page: Page, topic: string): Promise<void> {
  const stop = page.getByRole("button", { name: `Stop tail ${topic}`, exact: true });
  if (await stop.isVisible()) await stop.click();
  await expect(page.getByRole("combobox", { name: "Read mode", exact: true })).toBeEnabled();
}

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("keeps verified local topic notes through vault lock and restart without inheriting them on topic replacement", async ({
  page,
}, info) => {
  test.setTimeout(300_000);
  page.setDefaultTimeout(30_000);
  const errors: string[] = [],
    commands: string[] = [],
    runbookRequests: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.hostname === "example.com") runbookRequests.push(url.pathname);
    if (request.method() !== "POST" || !url.pathname.endsWith("/commands")) return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  const activeCommands = (start: number): string[] =>
    commands
      .slice(start)
      .filter((name) =>
        [
          "connection.connect",
          "profiles.connect",
          "messages.start",
          "messages.continue",
          "catalog.load",
          "records.locator.load",
        ].includes(name),
      );
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-topic-notes-browser-"));
  await mkdir(resolve("dist"), { recursive: true });
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-topic-notes-"));
  const passphrase = `test-vault-${randomUUID()}`;
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let admin: Admin | undefined;
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
  const unlock = async (): Promise<void> => {
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
  };
  try {
    await build({
      configFile: resolve("config/vite.config.ts"),
      logLevel: "silent",
      build: { outDir: rendererRoot },
    });
    fixture = await startStructuredBrowserFixture();
    admin = new Admin(
      await fixtureClientOptions(fixture.connection, fixture.config, `topic-notes-${randomUUID()}`),
    );
    const topic = fixture.config.topic;
    const metadata = await admin.metadata({
      topics: [topic],
      forceUpdate: true,
      autocreateTopics: false,
    });
    const originalId = metadata.topics.get(topic)?.id;
    expect(originalId).toBeTruthy();
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
    await openTopicDetail(page, topic);
    await stopTopicTail(page, topic);
    const notesStart = commands.length;
    await page.getByRole("button", { name: "Local topic notes", exact: true }).click();
    let dialog = page.getByRole("dialog", { name: "Local topic notes", exact: true });
    await expect(dialog.getByRole("status")).toHaveText(
      "No notes are saved for this topic identity.",
    );
    await dialog
      .getByRole("textbox", { name: "Description", exact: true })
      .fill("Investigate delayed orders using the recorded runbook.");
    await dialog.getByRole("textbox", { name: "Owner", exact: true }).fill("Operations");
    await dialog.getByRole("textbox", { name: "Labels", exact: true }).fill("critical, orders");
    await dialog.getByRole("button", { name: "Add runbook link", exact: true }).click();
    await dialog
      .getByRole("textbox", { name: "Runbook 1 title", exact: true })
      .fill("Order recovery");
    await dialog
      .getByRole("textbox", { name: "Runbook 1 HTTPS URL", exact: true })
      .fill("https://example.com/order-recovery");
    await dialog.getByRole("button", { name: "Save topic notes", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText("Topic notes saved.");
    expect(activeCommands(notesStart)).toEqual(["catalog.load"]);
    const file = join(dataRoot, "queries", "kafka-queries.json");
    const savedBytes = await readFile(file, "utf8");
    const stored = inspectKafkaQueryLibraryDocument(JSON.parse(savedBytes));
    expect(stored.schemaVersion).toBe(4);
    expect(stored.topics).toHaveLength(1);
    expect(stored.topics[0]?.identity).toEqual({
      clusterId: metadata.id,
      topicId: originalId,
      topic,
    });
    expect(savedBytes).not.toContain(passphrase);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Lock vault and disconnect", exact: true }).click();
    await expect(page.getByLabel("Vault passphrase", { exact: true })).toBeVisible();
    await unlock();
    const lockedStart = commands.length;
    dialog = await openSavedNotes(page);
    await chooseSavedNotes(page, dialog, originalId!.slice(0, 8));
    await expect(dialog.getByRole("textbox", { name: "Owner", exact: true })).toHaveValue(
      "Operations",
    );
    await expect(
      dialog.getByRole("button", { name: "Save topic notes", exact: true }),
    ).toBeDisabled();
    expect(activeCommands(lockedStart)).toEqual([]);
    await page.goto("about:blank");
    await gateway.close();
    gateway = undefined;
    await admin.deleteTopics({ topics: [topic] });
    await expect
      .poll(() => admin!.listTopics(), { timeout: 15_000, intervals: [100, 250, 500] })
      .not.toContain(topic);
    const recreated = await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    const replacementId = recreated.find((entry) => entry.name === topic)?.id;
    expect(replacementId).toBeTruthy();
    expect(replacementId).not.toBe(originalId);
    await expect
      .poll(
        async () =>
          (
            await admin!.metadata({ topics: [topic], forceUpdate: true, autocreateTopics: false })
          ).topics.get(topic)?.id,
        { timeout: 15_000, intervals: [100, 250, 500] },
      )
      .toBe(replacementId);
    gateway = await startGateway();
    await page.goto(gateway.origin);
    await unlock();
    const restartStart = commands.length;
    dialog = await openSavedNotes(page);
    await chooseSavedNotes(page, dialog, originalId!.slice(0, 8));
    await expect(dialog.getByRole("textbox", { name: "Description", exact: true })).toHaveValue(
      stored.topics[0]!.description,
    );
    expect(activeCommands(restartStart)).toEqual([]);
    expect(await readFile(file, "utf8")).toBe(savedBytes);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openTopicDetail(page, topic);
    await stopTopicTail(page, topic);
    await page.getByRole("button", { name: "Local topic notes", exact: true }).click();
    dialog = page.getByRole("dialog", { name: "Local topic notes", exact: true });
    await expect(dialog.getByRole("status")).toHaveText(
      "No notes are saved for this topic identity.",
    );
    await expect(dialog.getByRole("textbox", { name: "Description", exact: true })).toHaveValue("");
    await expect(
      dialog.getByRole("region", { name: "Topic note identity", exact: true }),
    ).toContainText(replacementId!);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Lock vault and disconnect", exact: true }).click();
    await unlock();
    const orphanStart = commands.length;
    dialog = await openSavedNotes(page);
    await chooseSavedNotes(page, dialog, originalId!.slice(0, 8));
    await page.setViewportSize({ width: 1000, height: 900 });
    await page.screenshot({
      path: info.outputPath("local-topic-notes-offline-1000px.png"),
      animations: "disabled",
    });
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await dialog.getByRole("button", { name: "Remove saved notes", exact: true }).click();
    await dialog.getByRole("button", { name: "Confirm remove notes", exact: true }).click();
    await expect(dialog.getByRole("status")).toHaveText(
      "Local topic notes removed. Kafka was not changed.",
    );
    expect(activeCommands(orphanStart)).toEqual([]);
    expect(
      inspectKafkaQueryLibraryDocument(JSON.parse(await readFile(file, "utf8"))).topics,
    ).toEqual([]);
    expect(
      (
        await admin.metadata({ topics: [topic], forceUpdate: true, autocreateTopics: false })
      ).topics.get(topic)?.id,
    ).toBe(replacementId);
    expect(runbookRequests).toEqual([]);
    expect(errors).toEqual([]);
    await info.attach("local-topic-notes-evidence", {
      body: Buffer.from(
        JSON.stringify({
          schemaVersion: 4,
          realMetadataIdentity: true,
          vaultLock: true,
          hostRestart: true,
          topicReplacementIsolation: true,
          disconnectedOrphanRemoval: true,
          implicitRunbookRequests: 0,
        }),
      ),
      contentType: "application/json",
    });
  } finally {
    if (!page.isClosed() && info.status !== info.expectedStatus)
      await page
        .screenshot({ path: info.outputPath("topic-notes-failure.png") })
        .catch(() => undefined);
    await page.close();
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      (): Promise<void> => admin?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
