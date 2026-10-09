import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { Producer } from "@platformatic/kafka";

import { parseHostEvent, type HostEvent } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { launchProductWebFixture } from "../support/product-web-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { fixtureClientOptions } from "../support/kafka-fixture";
import {
  fetchTopicMessages,
  observeBrowserDiagnostics,
  openTopicDetail,
} from "../support/workbench-browser";

async function port(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  if (address === null || typeof address === "string") throw new Error("No fixture port");
  return address.port;
}
test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("decodes, compares, traces and previews a schema sample in the real browser host", async ({
  page,
}, testInfo) => {
  test.setTimeout(240_000);
  page.setDefaultTimeout(30_000);
  page.setDefaultNavigationTimeout(60_000);
  const fixture = await startStructuredBrowserFixture();
  const backend = createKafkaBackend();
  let launch: Awaited<ReturnType<typeof launchProductWebFixture>> | undefined;
  const diagnostics = observeBrowserDiagnostics(page);
  try {
    launch = await launchProductWebFixture({
      backend,
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    let preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.getByRole("tab", { name: "Records", exact: true }).click();
    await preferences.getByRole("combobox", { name: "Default key encoding" }).click();
    await page.getByRole("option", { name: "UTF-8 text", exact: true }).click();
    await preferences.getByRole("button", { name: "Save record encodings" }).click();
    await expect(preferences.getByRole("status")).toBeVisible();
    await preferences.getByRole("button", { name: "Close", exact: true }).click();
    await page.reload();
    await page.getByRole("button", { name: "Preferences", exact: true }).click();
    preferences = page.getByRole("dialog", { name: "Workbench Preferences" });
    await preferences.getByRole("tab", { name: "Records", exact: true }).click();
    await expect(preferences.getByRole("combobox", { name: "Default key encoding" })).toHaveText(
      "UTF-8 text",
    );
    await preferences.getByRole("button", { name: "Close", exact: true }).click();
    try {
      await connectLocalProfile(page, fixture.connection);
    } catch (cause) {
      if (diagnostics.problems.length > 0) {
        throw new Error(`Browser connection workflow failed: ${diagnostics.problems.join("\n")}`, {
          cause,
        });
      }
      throw cause;
    }
    await fetchTopicMessages(page, fixture.config.topic);
    const grid = page.getByRole("grid", { name: "Kafka messages" });
    await grid.getByText(fixture.config.seedPayload, { exact: true }).first().click();
    const inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Decoded", exact: true }).click();
    await expect(inspector.getByLabel("Decoded JSON")).toBeVisible();
    await expect(inspector.getByText("Encoding: UTF-8 JSON", { exact: true })).toBeVisible();
    const downloadStarted = page.waitForEvent("download");
    await page.getByRole("button", { name: "Export records" }).click();
    await page.getByRole("menuitem", { name: "Current page JSON" }).click();
    const downloadPath = await (await downloadStarted).path();
    if (downloadPath === null) throw new Error("Structured export was not retained.");
    expect(JSON.parse(await readFile(downloadPath, "utf8"))).toMatchObject({
      schemaVersion: 3,
      messages: [
        {
          payload: fixture.config.seedPayload,
          structured: {
            key: { state: "decoded", codec: "utf8", text: "streamskope-seed" },
            value: { state: "decoded", codec: "json", json: fixture.config.seedPayload },
          },
        },
      ],
    });
    const tabs = inspector.getByRole("tablist", { name: "Message evidence" });
    const rectangles = await tabs.getByRole("tab").evaluateAll((nodes) =>
      nodes.map((node) => {
        const { x, width } = node.getBoundingClientRect();
        return { x, width };
      }),
    );
    for (let index = 1; index < rectangles.length; index++)
      expect(rectangles[index]!.x).toBeGreaterThanOrEqual(
        rectangles[index - 1]!.x + rectangles[index - 1]!.width - 1,
      );
    await page.screenshot({
      path: testInfo.outputPath("decoded-record.png"),
      animations: "disabled",
    });
    await inspector.getByRole("tab", { name: "Compare", exact: true }).click();
    await inspector.getByRole("button", { name: "Pin as baseline" }).click();
    await inspector.getByRole("button", { name: "Compare records" }).click();
    await expect(inspector.getByText(/No differences/u)).toBeVisible();
    await expect(page.getByLabel("Consumption status")).toContainText("Streaming");
    await page.getByRole("button", { name: "Trace correlation" }).click();
    const trace = page.getByRole("dialog", { name: "Trace a correlation ID" });
    await trace.getByRole("combobox", { name: "Correlation source" }).click();
    await page.getByRole("option", { name: "Key (saved encoding)" }).click();
    await trace.getByRole("textbox", { name: "Exact correlation value" }).fill("streamskope-seed");
    await trace.getByRole("button", { name: "Start trace" }).click();
    await expect(trace.getByText(/1 matching record/u)).toBeVisible();
    await expect(trace.getByRole("table", { name: "Correlation matches" })).toContainText(
      fixture.config.topic,
    );
    await expect(trace.getByText(/All requested retained offset ranges reached/u)).toBeVisible();
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("correlation-trace.png"),
      animations: "disabled",
    });
    await trace.getByRole("button", { name: "Close", exact: true }).click();
    await expect(page.getByLabel("Consumption status")).toContainText("Streaming");
    await page
      .getByRole("navigation", { name: "StreamSkope resources" })
      .getByRole("button", { name: "Schema Registry" })
      .click();
    await page.getByRole("button", { name: fixture.config.schemaSubject, exact: true }).click();
    await page.getByRole("button", { name: "Show reference tree" }).click();
    await expect(page.getByText(/does not establish producer/u)).toBeVisible();
    await page.getByRole("button", { name: "Generate samples" }).click();
    const sample = page.getByRole("dialog", { name: new RegExp(`Schema samples`) });
    await sample.getByRole("spinbutton", { name: "Sample count" }).fill("2");
    await sample.getByRole("button", { name: "Generate preview" }).click();
    await expect(sample.getByText(/2 valid samples/u)).toBeVisible();
    await expect(sample.getByLabel("Generated sample")).toBeVisible();
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("schema-samples.png"),
      animations: "disabled",
    });
    await sample.getByRole("button", { name: "Close", exact: true }).click();
    expect(diagnostics.problems).toEqual([]);
  } finally {
    if (diagnostics.problems.length > 0)
      await testInfo.attach("browser-diagnostics", {
        body: JSON.stringify(diagnostics.problems),
        contentType: "application/json",
      });
    await disposeNativeFixtureResources([
      (): Promise<void> => launch?.close() ?? backend.shutdown(),
      (): Promise<void> => fixture.dispose(),
    ]);
  }
});

test("continues a protected finite read into replacement pages with fixed ranges and cumulative coverage", async ({
  page,
}, testInfo) => {
  test.setTimeout(180_000);
  page.setDefaultTimeout(30_000);
  page.setDefaultNavigationTimeout(60_000);
  const fixture = await startStructuredBrowserFixture();
  const backend = createKafkaBackend();
  const states: Extract<HostEvent, { event: "consumption.state" }>["payload"][] = [];
  backend.subscribe((wire) => {
    const event = parseHostEvent(JSON.parse(JSON.stringify(wire)));
    if (event.event === "consumption.state") states.push(event.payload);
  });
  let producer: Producer<Buffer, Buffer, Buffer, Buffer> | undefined;
  let launch: Awaited<ReturnType<typeof launchProductWebFixture>> | undefined;
  const diagnostics = observeBrowserDiagnostics(page);
  const secret = `private-continuation-${randomUUID()}`;
  try {
    producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
      ...(await fixtureClientOptions(
        fixture.connection,
        fixture.config,
        `continuation-browser-${randomUUID()}`,
      )),
      autocreateTopics: false,
    });
    await producer.send({
      messages: Array.from({ length: 20 }, (_, index) => ({
        topic: fixture.config.topic,
        partition: 0,
        key: Buffer.from(`resume-${String(index + 1)}`),
        value: Buffer.from(JSON.stringify({ position: index + 1, secret })),
      })),
    });
    launch = await launchProductWebFixture({
      backend,
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
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
    const grid = page.getByRole("grid", { name: "Kafka messages" });
    const progress = page.getByLabel("Cumulative read progress");
    const coverage = page.getByRole("region", { name: "Read coverage" });
    await expect(progress).toContainText(
      "Pass 1 · Total: 10 records scanned; 10 records returned.",
    );
    await expect(grid.getByText("streamskope-seed", { exact: true })).toBeVisible();
    await expect(
      page.getByLabel("Showing 10 of 10 retained messages", { exact: true }),
    ).toBeVisible();
    await expect(coverage).toContainText("replaces this result page");
    const initial = states.find(
      (state) => state.request?.topic === fixture.config.topic && state.state === "complete",
    );
    expect(initial?.request).toMatchObject({
      topic: fixture.config.topic,
      mode: "time-window",
      maxMessages: 10,
    });
    expect(initial?.coverage?.partitions).toEqual([
      { partition: 0, startOffset: "0", endOffset: "21", nextOffset: "10" },
    ]);
    await producer.send({
      messages: [
        {
          topic: fixture.config.topic,
          partition: 0,
          key: Buffer.from("late-arrival"),
          value: Buffer.from('{"position":21}'),
        },
      ],
    });
    await page.getByRole("button", { name: "Continue read", exact: true }).click();
    await expect(progress).toContainText(
      "Pass 2 · Total: 20 records scanned; 20 records returned.",
    );
    await expect(grid.getByText("resume-10", { exact: true })).toBeVisible();
    await expect(grid.getByText("streamskope-seed", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("resume-1", { exact: true })).toHaveCount(0);
    await expect(
      page.getByLabel("Showing 10 of 10 retained messages", { exact: true }),
    ).toBeVisible();
    await expect(grid).not.toContainText(secret);
    const downloadStarted = page.waitForEvent("download");
    await page.getByRole("button", { name: "Export records" }).click();
    await page.getByRole("menuitem", { name: "Current page JSON" }).click();
    const file = await (await downloadStarted).path();
    if (file === null) throw new Error("Continued page export was not retained.");
    const exported = await readFile(file, "utf8");
    expect(exported).not.toContain(secret);
    expect(JSON.parse(exported)).toMatchObject({
      schemaVersion: 3,
      retainedMessageCount: 10,
      exportedMessageCount: 10,
      messages: Array.from({ length: 10 }, (_, index) => ({
        offset: String(index + 10),
        payload: JSON.stringify({ position: index + 10, secret: "[MASKED]" }),
        original: { state: "unavailable", reason: "masked" },
        structured: { protection: "masked" },
      })),
    });
    await page.getByRole("button", { name: "Continue read", exact: true }).click();
    await expect(progress).toContainText(
      "Pass 3 · Total: 21 records scanned; 21 records returned.",
    );
    await expect(grid.getByText("resume-20", { exact: true })).toBeVisible();
    await expect(grid.getByText("resume-10", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("late-arrival", { exact: true })).toHaveCount(0);
    await expect(
      page.getByLabel("Showing 1 of 1 retained messages", { exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: "Continue read", exact: true })).toHaveCount(0);
    await expect(coverage).toContainText("Requested offset ranges reached");
    await coverage.getByText("Partition coverage", { exact: true }).click();
    await expect(coverage).toContainText("Partition 0: requested [0, 21); reached 21");
    const completed = states.filter(
      (state) => state.request?.topic === fixture.config.topic && state.state === "complete",
    );
    expect(completed.map((state) => state.request)).toEqual([
      initial?.request,
      initial?.request,
      initial?.request,
    ]);
    expect(completed.map((state) => state.searchProgress?.pass)).toEqual([1, 2, 3]);
    expect(completed.at(-1)?.searchProgress?.continuation).toBeNull();
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Read coverage"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("continued-protected-read.png"),
      animations: "disabled",
    });
    expect(diagnostics.problems).toEqual([]);
  } finally {
    if (diagnostics.problems.length > 0)
      await testInfo.attach("browser-diagnostics", {
        body: JSON.stringify(diagnostics.problems),
        contentType: "application/json",
      });
    await disposeNativeFixtureResources([
      (): Promise<void> => launch?.close() ?? backend.shutdown(),
      (): Promise<void> => producer?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture.dispose(),
    ]);
  }
});
