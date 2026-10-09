import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test, type Page } from "@playwright/test";
import { Producer } from "@platformatic/kafka";
import { build } from "vite";

import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { openTopicDetail } from "../support/workbench-browser";

/** Independent RFC 4180 reader, deliberately separate from the production encoder. */
function csvRows(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [],
    field = "",
    quoted = false;
  for (let index = 0; index < text.length; index++) {
    const character = text[index];
    if (character === '"') {
      if (quoted && text[index + 1] === '"') {
        field += '"';
        index++;
      } else quoted = !quoted;
    } else if (!quoted && character === ",") {
      row.push(field);
      field = "";
    } else if (!quoted && character === "\n") {
      row.push(field.endsWith("\r") ? field.slice(0, -1) : field);
      rows.push(row);
      row = [];
      field = "";
    } else field += character;
  }
  if (quoted || field.length > 0 || row.length > 0)
    throw new Error("CSV output ended in a partial row.");
  return rows;
}
async function download(page: Page, name: string): Promise<{ bytes: Buffer; url: string }> {
  const event = page.waitForEvent("download");
  await page
    .getByRole("region", { name: "Range export" })
    .getByRole("button", { name, exact: true })
    .click();
  const file = await event;
  const path = await file.path();
  if (path === null) throw new Error("The browser did not retain its download.");
  expect(await file.failure()).toBeNull();
  return { bytes: await readFile(path), url: file.url() };
}

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("streams a real range beyond grid capacity through the production vault gateway and revokes downloads on lock", async ({
  page,
  request,
}, info) => {
  test.setTimeout(300_000);
  page.setDefaultTimeout(30_000);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-export-browser-"));
  await mkdir(resolve("dist"), { recursive: true });
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-range-export-"));
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let producer: Producer<Buffer, Buffer, Buffer, Buffer> | undefined;
  const count = 1_200;
  const passphrase = `test-vault-${randomUUID()}`;
  try {
    // This proves the minified production renderer and the real encrypted-vault runtime together.
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
        `range-export-${randomUUID()}`,
      )),
      autocreateTopics: false,
    });
    for (let start = 0; start < count; start += 100) {
      await producer.send({
        messages: Array.from({ length: 100 }, (_, position) => {
          const index = start + position;
          return {
            topic: fixture!.config.topic,
            partition: 0,
            key: Buffer.from(`export-${String(index).padStart(4, "0")}`),
            value: Buffer.from(
              JSON.stringify({
                sequence: index,
                text: '=SUM(1,2)\n"quoted"',
                large: "9007199254740993",
                padding: "x".repeat(8_192),
              }),
            ),
          };
        }),
      });
    }
    gateway = await startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: (value, mode) => openBrowserRuntime(dataRoot, value, mode),
    });
    if (gateway.setupCodePath === undefined)
      throw new Error("Fresh gateway did not expose its owned setup code.");
    await page.goto(gateway.origin);
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await connectLocalProfile(page, fixture.connection);
    await openTopicDetail(page, fixture.config.topic);
    await page.getByRole("button", { name: "Show message filters" }).click();
    await page.getByRole("textbox", { name: "Key contains" }).fill("export-");
    const openDialog = async (): Promise<ReturnType<Page["getByRole"]>> => {
      await page.getByRole("button", { name: "Export records" }).click();
      await page.getByRole("menuitem", { name: "Read range…" }).click();
      return page.getByRole("dialog", { name: "Export a topic range" });
    };
    let dialog = await openDialog();
    await dialog.getByRole("button", { name: "Start export", exact: true }).click();
    await expect(dialog).toHaveCount(0);
    const status = page.getByRole("region", { name: "Range export" });
    await expect(status).toContainText("Range export complete", { timeout: 90_000 });
    await expect(status).toContainText("1,200 records written");
    const jsonl = await download(page, "Download export");
    const receiptDownload = await download(page, "Download receipt");
    const receipt = JSON.parse(receiptDownload.bytes.toString("utf8")) as {
      schema: string;
      output: { sha256: string; bytes: number };
      outcome: string;
      counts: { writtenRecords: number };
      coverage: { partitions: { startOffset: string; endOffset: string; nextOffset: string }[] };
    };
    expect(receipt.schema).toBe("streamskope.record-export/v1");
    expect(receipt.outcome).toBe("complete");
    expect(receipt.output.sha256).toBe(createHash("sha256").update(jsonl.bytes).digest("hex"));
    expect(receipt.output.bytes).toBe(jsonl.bytes.byteLength);
    expect(jsonl.bytes.byteLength).toBeGreaterThan(8 * 1_048_576);
    expect(receipt.counts.writtenRecords).toBe(count);
    expect(
      receipt.coverage.partitions.every(
        (partition) => partition.endOffset === partition.nextOffset,
      ),
    ).toBe(true);
    const rows = jsonl.bytes
      .toString("utf8")
      .trimEnd()
      .split("\n")
      .map(
        (line) =>
          JSON.parse(line) as {
            partition: number;
            offset: string;
            structured: { key: { text: string }; value: { json: string } };
          },
      );
    expect(rows).toHaveLength(count);
    expect(new Set(rows.map((row) => `${row.partition}:${row.offset}`)).size).toBe(count);
    expect(rows.map((row) => row.structured.key.text).sort()).toEqual(
      Array.from({ length: count }, (_, index) => `export-${String(index).padStart(4, "0")}`),
    );
    expect(
      rows
        .map((row) => (JSON.parse(row.structured.value.json) as { sequence: number }).sequence)
        .sort((a, b) => a - b),
    ).toEqual(Array.from({ length: count }, (_, index) => index));
    expect((await page.request.head(jsonl.url)).status()).toBe(200);
    expect((await request.get(jsonl.url)).status()).toBe(401);
    expect(
      (
        await page.request.head(jsonl.url, { headers: { "sec-fetch-site": "cross-site" } })
      ).status(),
    ).toBe(403);

    dialog = await openDialog();
    await expect(dialog.getByRole("button", { name: "Start export", exact: true })).toBeDisabled();
    await dialog.getByRole("checkbox", { name: "Replace the previous prepared download" }).check();
    await dialog.getByRole("combobox", { name: "Export format" }).click();
    await page.getByRole("option", { name: "CSV", exact: true }).click();
    await dialog.getByRole("button", { name: "Start export", exact: true }).click();
    await expect(dialog).toHaveCount(0);
    await expect(status).toContainText("Range export complete", { timeout: 90_000 });
    const csv = await download(page, "Download export");
    const csvReceipt = JSON.parse(
      (await download(page, "Download receipt")).bytes.toString("utf8"),
    ) as typeof receipt;
    expect(csvReceipt.output.sha256).toBe(createHash("sha256").update(csv.bytes).digest("hex"));
    expect(csvReceipt.output.bytes).toBe(csv.bytes.byteLength);
    const table = csvRows(csv.bytes.toString("utf8"));
    expect(table).toHaveLength(count + 1);
    const keys = table[0]!;
    expect(keys).toContain("value_json");
    expect(
      table
        .slice(1)
        .map((row) => JSON.parse(row[keys.indexOf("key_json")]!) as { text: string })
        .map((field) => field.text)
        .sort(),
    ).toEqual(rows.map((row) => row.structured.key.text).sort());
    expect(JSON.parse(table[1]![keys.indexOf("value_json")]!)).toMatchObject({
      state: "decoded",
      json: expect.stringContaining("=SUM(1,2)"),
    });
    expect((await page.request.get(jsonl.url)).status()).toBeGreaterThanOrEqual(400);
    await expect(status).toContainText("download started");
    await page.screenshot({ path: info.outputPath("range-export.png"), animations: "disabled" });

    const locked = await page.request.post(`${gateway.origin}/__streamskope_session/lock`, {
      headers: { origin: gateway.origin },
      data: {},
    });
    expect(locked.status()).toBe(200);
    expect((await page.request.get(csv.url)).status()).toBe(401);
    await page.goto(gateway.origin);
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    expect((await page.request.get(csv.url)).status()).toBeGreaterThanOrEqual(400);
    expect(errors).toEqual([]);
    await info.attach("range-export-receipt", {
      body: receiptDownload.bytes,
      contentType: "application/json",
    });
  } finally {
    await page.close();
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      (): Promise<void> => producer?.close(true) ?? Promise.resolve(),
      (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
