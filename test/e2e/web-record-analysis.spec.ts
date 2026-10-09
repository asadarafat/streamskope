import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test, type Locator } from "@playwright/test";
import { Consumer, Producer } from "@platformatic/kafka";
import { build } from "vite";

import { parseHostEvent } from "../../src/features/kafka/contracts";
import type { RecordAnalysisSnapshot } from "../../src/features/kafka/contracts/record-analysis";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { waitForKafkaTopicOffsets } from "../support/kafka-topic-readiness";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { openTopicDetail, openWorkbenchResource } from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1_000, height: 900 } });
test("counts a protected real range beyond its preview and clears analysis on vault lock", async ({
  page,
}, info) => {
  test.setTimeout(300_000);
  page.setDefaultTimeout(30_000);
  const problems: string[] = [];
  page.on("pageerror", (error) => problems.push(error.message));
  const snapshots: RecordAnalysisSnapshot[] = [];
  const unsubscribe: (() => void)[] = [];
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-analysis-browser-"));
  await mkdir(resolve("dist"), { recursive: true });
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-record-analysis-"));
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let producer: Producer<Buffer, Buffer | null, Buffer, Buffer> | undefined;
  let readiness: Consumer | undefined;
  const count = 800;
  const secret = `protected-analysis-${randomUUID()}`;
  const passphrase = `test-vault-${randomUUID()}`;
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
      `analysis-browser-${randomUUID()}`,
    );
    producer = new Producer<Buffer, Buffer | null, Buffer, Buffer>({
      ...options,
      autocreateTopics: false,
      repeatOnStaleMetadata: false,
    });
    readiness = new Consumer({ ...options, groupId: randomUUID(), autocreateTopics: false });
    const topicId = (
      await readiness.metadata({ topics: [fixture.config.topic], forceUpdate: true })
    ).topics.get(fixture.config.topic)?.id;
    if (topicId === undefined) throw new Error("The seeded fixture topic has no Kafka identity.");
    const initialOffsets = await waitForKafkaTopicOffsets(
      readiness,
      fixture.config.topic,
      topicId,
      1,
    );
    for (let start = 0; start < count; start += 100) {
      await producer.send({
        messages: Array.from({ length: 100 }, (_, position) => {
          const index = start + position;
          const category = [1, "1", null, undefined, { nested: true }, true][index % 8];
          return {
            topic: fixture!.config.topic,
            partition: 0,
            key: Buffer.from(`analysis-${String(index).padStart(4, "0")}`),
            value:
              index % 8 === 6
                ? null
                : Buffer.from(
                    index % 8 === 7
                      ? '{"malformed"'
                      : JSON.stringify({ category, secret, sequence: index }),
                  ),
          };
        }),
      });
    }
    expect(await waitForKafkaTopicOffsets(readiness, fixture.config.topic, topicId, 1)).toEqual(
      initialOffsets.map((offset) => offset + BigInt(count)),
    );
    gateway = await startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: async (value, mode) => {
        const runtime = await openBrowserRuntime(dataRoot, value, mode);
        const kafka = runtime.providers.endpoints().find((provider) => provider.id === "kafka");
        if (kafka === undefined) throw new Error("Production runtime omitted Kafka.");
        // Observe the real endpoint without replacing its commands, readers or transport.
        unsubscribe.push(
          kafka.subscribe((wire) => {
            try {
              const event = parseHostEvent(wire);
              if (event.event === "records.analysis.changed") snapshots.push(event.payload);
            } catch (error) {
              problems.push(error instanceof Error ? error.message : "Invalid host event.");
            }
          }),
        );
        return runtime;
      },
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
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    const preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.getByRole("combobox", { name: "Default fetch mode" }).click();
    await page.getByRole("option", { name: "Time window", exact: true }).click();
    await preferences.getByLabel("Default maximum results").fill("10");
    await preferences.getByRole("button", { name: "Save preferences", exact: true }).click();
    await expect(preferences.getByRole("status")).toBeVisible();
    await preferences.getByRole("tab", { name: "Protection", exact: true }).click();
    await preferences
      .getByRole("textbox", { name: "Decoded JSON value paths to mask" })
      .fill("/secret");
    await preferences.getByRole("button", { name: "Save protection", exact: true }).click();
    await expect(preferences.getByRole("status")).toContainText("Protection saved");
    await preferences.getByRole("button", { name: "Close", exact: true }).click();
    await connectLocalProfile(page, fixture.connection);
    await openTopicDetail(page, fixture.config.topic);
    await page.getByRole("button", { name: "Show message filters" }).click();
    await page.getByRole("textbox", { name: "Key contains" }).fill("analysis-");
    const trigger = page.getByRole("button", { name: "Analyze range…", exact: true });
    await trigger.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", { name: "Analyze a topic range" });
    await expect(dialog).toBeVisible();
    const field = async (index: number, path: string, label: string): Promise<void> => {
      await dialog.getByRole("button", { name: "Add field", exact: true }).click();
      await dialog.getByRole("textbox", { name: `Field ${index} path` }).fill(path);
      await dialog.getByRole("textbox", { name: `Field ${index} label` }).fill(label);
    };
    await field(1, "$.category", "Category");
    await field(2, "$.secret", "Secret");
    await field(3, "$", "Key");
    await dialog.getByRole("combobox", { name: "Field 3 source" }).click();
    await page.getByRole("option", { name: "Key", exact: true }).click();
    await dialog.getByRole("combobox", { name: "Count by", exact: true }).click();
    await page.getByRole("option", { name: "Category · value $.category", exact: true }).click();
    await dialog.getByRole("button", { name: "Start analysis", exact: true }).click();
    await expect(dialog.getByRole("status", { name: "Analysis count" })).toContainText(
      "Count for captured range complete: 800 matching records.",
      { timeout: 90_000 },
    );
    await expect(dialog.getByRole("region", { name: "Count by results" })).toContainText(
      "600 grouped of 800 counted records.",
    );
    await expect(dialog.getByRole("region", { name: "Count by results" })).toContainText(
      "100 masked; 100 unavailable or unsupported.",
    );
    const groups = dialog.getByRole("table", { name: "Analysis groups" });
    const group = (type: string, value: string): Locator =>
      groups
        .getByRole("row")
        .filter({ has: page.getByRole("cell", { name: type, exact: true }) })
        .filter({ has: page.getByRole("cell", { name: value, exact: true }) });
    for (const [type, value] of [
      ["number", "1"],
      ["string", '"1"'],
      ["JSON null", "null"],
      ["Missing", "Missing path"],
      ["Tombstone", "Tombstone"],
      ["boolean", "true"],
    ] as const)
      await expect(group(type, value)).toContainText("100");
    await expect(dialog.getByRole("region", { name: "Projection preview" })).toContainText(
      "First 200 of 800 counted records shown; 600 omitted from the preview.",
    );
    await expect(
      dialog.getByRole("table", { name: "Analysis preview" }).getByRole("row"),
    ).toHaveCount(201);
    await expect(dialog).not.toContainText(secret);
    await expect(
      dialog.getByRole("cell", { name: 'string: "analysis-0000"', exact: true }),
    ).toBeVisible();
    await expect(
      dialog.getByRole("cell", { name: "Masked: Masked", exact: true }).first(),
    ).toBeVisible();
    const terminal = [...snapshots]
      .reverse()
      .find((snapshot) => snapshot.operation?.state === "completed")?.operation;
    expect(terminal?.counts.countedRecords).toBe(count);
    expect(terminal?.result?.preview).toHaveLength(200);
    expect(terminal?.result?.columns[1]).toMatchObject({ masked: 700, tombstone: 100 });
    expect(terminal?.result?.columns[2]).toMatchObject({ scalar: 800 });
    expect(
      terminal?.coverage?.partitions.every(
        (partition) => partition.nextOffset === partition.endOffset,
      ),
    ).toBe(true);
    expect(JSON.stringify(snapshots)).not.toContain(secret);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    const box = await dialog.boundingBox();
    expect(box).not.toBeNull();
    expect(box!.x).toBeGreaterThanOrEqual(0);
    expect(box!.x + box!.width).toBeLessThanOrEqual(1_000);
    await page.screenshot({ path: info.outputPath("analysis-1000px.png"), animations: "disabled" });

    await page.keyboard.press("Escape");
    await expect(dialog).toHaveCount(0);
    await openWorkbenchResource(page, "Overview");
    await openTopicDetail(page, fixture.config.topic);
    const summary = page.getByRole("region", { name: "Range analysis", exact: true });
    await expect(summary).toContainText("800 matching records");
    await summary.getByRole("button", { name: "View analysis" }).click();
    await expect(dialog.getByRole("tab", { name: "Results", exact: true })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    await dialog.getByRole("tab", { name: "Setup", exact: true }).click();
    await dialog.getByRole("textbox", { name: "Maximum counted records" }).fill("250");
    await expect(
      dialog.getByRole("button", { name: "Start analysis", exact: true }),
    ).toBeDisabled();
    await dialog.getByRole("checkbox", { name: "Replace the previous analysis result" }).check();
    await dialog.getByRole("button", { name: "Start analysis", exact: true }).click();
    await expect(dialog.getByRole("status", { name: "Analysis count" })).toContainText(
      "Partial count: 250 matching records counted.",
      { timeout: 90_000 },
    );
    await expect(dialog).toContainText("The matching-record limit was reached.");
    await expect(dialog.getByRole("region", { name: "Projection preview" })).toContainText(
      "50 omitted from the preview",
    );
    const partial = [...snapshots]
      .reverse()
      .find((snapshot) => snapshot.operation?.state === "partial")?.operation;
    expect(partial?.reason).toBe("record-limit");
    expect(partial?.counts.countedRecords).toBe(250);
    expect(partial?.result?.preview).toHaveLength(200);
    await page.keyboard.press("Escape");
    const locked = await page.request.post(`${gateway.origin}/__streamskope_session/lock`, {
      headers: { origin: gateway.origin },
      data: {},
    });
    expect(locked.status()).toBe(200);
    await expect.poll(() => snapshots.at(-1)?.operation?.state).toBe("revoked");
    expect(snapshots.at(-1)?.operation?.result).toBeNull();
    await page.goto(gateway.origin);
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    await page.getByRole("button", { name: "Connect profile Local aio" }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openTopicDetail(page, fixture.config.topic);
    await expect(summary).toHaveCount(0);
    expect(problems).toEqual([]);
    await info.attach("analysis-counts", {
      body: Buffer.from(JSON.stringify({ complete: terminal, partial }, null, 2)),
      contentType: "application/json",
    });
  } finally {
    await page.close();
    for (const stop of unsubscribe) stop();
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      (): Promise<void> => producer?.close(true) ?? Promise.resolve(),
      (): Promise<void> => readiness?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
