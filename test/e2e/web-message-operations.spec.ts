import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
  type StreamSkopeBackend,
} from "../../src/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { expectWorkbenchReady } from "../support/workbench-browser";

class MessageOperationBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  private sequence = 0;

  emit(event: Omit<HostEvent, "sequence" | "version">): void {
    const sequenced = {
      ...event,
      sequence: this.sequence,
      version: HOST_PROTOCOL_VERSION,
    } as HostEvent;
    this.sequence += 1;
    for (const listener of this.listeners) {
      listener(sequenced);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `message-e2e-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

const backend = new MessageOperationBackend();
let launch: RunningWebDevelopment | undefined;

const zeroLiveEvaluation: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 12,
  errorCount: 0,
  errors: [],
  evaluatedRules: 1,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(
  id: string,
  key: string,
  overrides: Partial<KafkaExploredMessage> = {},
): KafkaExploredMessage {
  const payload = JSON.stringify({ key, source: "browser-message-fixture" });
  return {
    headers: { "content-type": "application/json" },
    id,
    key,
    offset: id,
    originalByteSize: new TextEncoder().encode(payload).byteLength,
    partition: 0,
    payload,
    preview: payload,
    ruleEvaluation: zeroLiveEvaluation,
    timestamp: "2026-07-25T10:40:00.000Z",
    topic: "orders",
    truncated: false,
    ...overrides,
  };
}

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolveListen();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected a temporary TCP endpoint.");
  }
  return address.port;
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) {
    return;
  }
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolveClose();
      } else {
        reject(error);
      }
    });
  });
}

async function reservePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await closeServer(server);
  return port;
}

function activeLaunch(): RunningWebDevelopment {
  if (launch === undefined) {
    throw new Error("Web development launch is not ready.");
  }
  return launch;
}

test.describe("StreamSkope browser message operations", () => {
  test.describe.configure({ mode: "serial" });

  test.beforeAll(async () => {
    launch = await launchWebDevelopment({
      backend,
      hostPort: await reservePort(),
      rendererPort: await reservePort(),
      rendererRoot: resolve(process.cwd()),
    });
  });

  test.afterAll(async () => {
    await launch?.close();
  });

  test("filters, copies, edits and exports one retained snapshot without backend work", async ({
    context,
    page,
  }, testInfo) => {
    test.setTimeout(75_000);
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await context.grantPermissions(["clipboard-read", "clipboard-write"], {
      origin: new URL(activeLaunch().browserUrl).origin,
    });
    await page.goto(activeLaunch().browserUrl);
    await expectWorkbenchReady(page);

    backend.emit({
      event: "connection.state",
      payload: { connectionName: "Message fixture", state: "connected" },
    });
    backend.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-07-26T03:00:00.000Z",
        state: "ready",
        topics: ["orders"],
      },
    });
    await page.getByRole("button", { name: "orders", exact: true }).click();

    const exactPayload = '  {"status":"APPROVED","evidence":"private-payload"}  ';
    const alpha = message("101", "Order-Alpha", {
      offset: "101",
      originalByteSize: new TextEncoder().encode(exactPayload).byteLength,
      partition: 2,
      payload: exactPayload,
      preview: exactPayload,
      ruleEvaluation: {
        ...zeroLiveEvaluation,
        activeMatchCount: 1,
        activeMatches: [{ level: "warn", name: "Approved order" }],
        highestActiveSeverity: "warn",
      },
      timestamp: "2026-07-25T10:41:00.000Z",
    });
    const beta = message("202", "Order-Beta", {
      offset: "202",
      partition: 3,
      payload: '{"status":"rejected"}',
      preview: '{"status":"rejected"}',
      timestamp: "2026-07-25T10:42:00.000Z",
    });
    const previewOnly = message("303", "Order-Preview", {
      offset: "303",
      originalByteSize: 2_000_000,
      partition: 3,
      payload: null,
      preview: "retained preview only",
      timestamp: "2026-07-25T10:43:00.000Z",
      truncated: true,
    });
    backend.emit({
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 3,
        request: {
          maxMessages: 1_000,
          mode: "tail",
          topic: "orders",
        },
        ruleEvaluation: {
          applicableRules: 1,
          omittedRules: 0,
          state: "ready",
        },
        state: "streaming",
      },
    });
    backend.emit({
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages: [alpha, beta, previewOnly],
        topic: "orders",
      },
    });

    await expect(page.getByText("3 / 3", { exact: true })).toBeVisible();
    const filterToggle = page.getByRole("button", { name: "Show message filters" });
    await filterToggle.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("button", { name: "Hide message filters" })).toBeFocused();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("button", { name: "Show message filters" })).toBeFocused();
    await page.keyboard.press("Enter");

    await page.getByRole("textbox", { name: "Timestamp contains" }).fill("10:41");
    await page.getByRole("spinbutton", { name: "Partition" }).fill("2");
    await page.getByRole("textbox", { name: "Offset contains" }).fill("01");
    await page.getByRole("textbox", { name: "Key contains" }).fill("alpha");
    await page
      .getByRole("textbox", { name: "Value or retained preview contains" })
      .fill("approved");
    await page.getByRole("checkbox", { name: "Rule matches only" }).check();

    const grid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(page.getByText("1 / 3", { exact: true })).toBeVisible();
    await expect(page.getByText("6 active filters", { exact: true })).toBeVisible();
    await expect(grid.getByText("Order-Alpha", { exact: true })).toBeVisible();
    await expect(grid.getByText("Order-Beta", { exact: true })).toHaveCount(0);

    const lateAlpha = message("1101", "Order-Alpha-Late", {
      ...alpha,
      id: "1101",
      key: "Order-Alpha-Late",
      offset: "1101",
      timestamp: "2026-07-25T10:41:30.000Z",
    });
    backend.emit({
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages: [lateAlpha],
        topic: "orders",
      },
    });
    await expect(page.getByText("2 / 4", { exact: true })).toBeVisible();

    await grid.getByText("Order-Alpha", { exact: true }).click();
    const inspector = page.getByRole("complementary", { name: "Message inspector" });
    const gridBoundsWithInspector = await grid.boundingBox();
    const rulesColumnBounds = await grid.getByRole("columnheader", { name: "Rules" }).boundingBox();
    if (gridBoundsWithInspector === null || rulesColumnBounds === null) {
      throw new Error("The message grid or Rules column has no visible bounds.");
    }
    expect(rulesColumnBounds.x + rulesColumnBounds.width).toBeLessThanOrEqual(
      gridBoundsWithInspector.x + gridBoundsWithInspector.width + 1,
    );
    const messageControls = page.getByRole("group", { name: "Message controls" });
    const inspectorHeader = inspector
      .getByRole("heading", { name: "Message details" })
      .locator("..");
    const inspectorHeaderBounds = await inspectorHeader.boundingBox();
    const controlsBounds = await messageControls.boundingBox();
    expect(inspectorHeaderBounds).not.toBeNull();
    expect(controlsBounds).not.toBeNull();
    expect(inspectorHeaderBounds?.height).toBe(controlsBounds?.height);
    const recordHeading = await inspector
      .getByRole("heading", { name: "Record", exact: true })
      .evaluate((element) => {
        const range = document.createRange();
        range.selectNodeContents(element);
        return range.getBoundingClientRect().x;
      });
    const topicLabel = await inspector.getByText("Topic", { exact: true }).boundingBox();
    expect(recordHeading).not.toBeNull();
    expect(topicLabel).not.toBeNull();
    expect(recordHeading).toBe(topicLabel?.x);
    await page.screenshot({ path: testInfo.outputPath("message-inspector-metadata.png") });
    await expect(messageControls.getByRole("button", { name: "Stop tail orders" })).toBeVisible();
    await expect(
      messageControls.getByRole("button", { name: "Hide message filters" }),
    ).toBeVisible();
    await expect(
      messageControls.getByRole("button", { name: "Export filtered JSON" }),
    ).toBeVisible();
    await expect(messageControls.getByLabel("Consumption status")).toBeVisible();
    await expect(messageControls.getByRole("combobox", { name: "Read mode" })).toHaveCount(0);
    await expect(messageControls.getByRole("combobox", { name: "Record limit" })).toHaveCount(0);
    await inspector.getByRole("tab", { name: "Value" }).click();
    await inspector.getByRole("button", { name: "Copy value" }).click();
    await expect
      .poll(async () => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(exactPayload);
    await expect(inspector.getByRole("status")).toContainText("Value copied to clipboard.");

    const openEditor = inspector.getByRole("button", { name: "Open in editor" });
    await openEditor.click();
    let scratchValue = page.getByRole("textbox", { name: "Scratch message value" });
    await expect(scratchValue).toHaveValue(exactPayload);
    await scratchValue.fill("local scratch change");
    await page.keyboard.press("Escape");
    await expect(openEditor).toBeFocused();
    await openEditor.click();
    scratchValue = page.getByRole("textbox", { name: "Scratch message value" });
    await expect(scratchValue).toHaveValue(exactPayload);
    await page.getByRole("button", { name: "Close editor" }).click();

    const commandCountBeforeLocalOperations = backend.commands.length;
    const downloadStarted = page.waitForEvent("download");
    await page.getByRole("button", { name: "Export filtered JSON" }).click();
    const download = await downloadStarted;
    expect(download.suggestedFilename()).toBe("streamskope-orders-messages.json");
    const downloadPath = await download.path();
    if (downloadPath === null) {
      throw new Error("The browser did not retain the filtered-message export.");
    }
    const exported = JSON.parse(await readFile(downloadPath, "utf8")) as {
      readonly exportedMessageCount: number;
      readonly filters: { readonly key: string };
      readonly messages: readonly { readonly key: string | null }[];
      readonly retainedMessageCount: number;
      readonly schemaVersion: number;
      readonly stale: boolean;
    };
    expect(exported).toMatchObject({
      exportedMessageCount: 2,
      filters: { key: "alpha" },
      retainedMessageCount: 4,
      schemaVersion: 1,
      stale: false,
    });
    expect(exported.messages.map((record) => record.key)).toEqual([
      "Order-Alpha-Late",
      "Order-Alpha",
    ]);
    await expect(page.getByRole("status").filter({ hasText: "download started" })).toContainText(
      "Filtered message JSON download started.",
    );

    await page.evaluate(() => {
      const scope = globalThis as typeof globalThis & {
        __streamSkopeCreateObjectUrl?: typeof URL.createObjectURL;
      };
      scope.__streamSkopeCreateObjectUrl = URL.createObjectURL.bind(URL);
      URL.createObjectURL = (): string => {
        throw new Error("blocked: private-payload");
      };
    });
    await page.getByRole("button", { name: "Export filtered JSON" }).click();
    const transferError = page.getByRole("alert").filter({
      hasText: "The filtered JSON export failed. No file was saved. Retry the export.",
    });
    await expect(transferError).toBeVisible();
    await expect(transferError).not.toContainText("private-payload");
    await page.evaluate(() => {
      const scope = globalThis as typeof globalThis & {
        __streamSkopeCreateObjectUrl?: typeof URL.createObjectURL;
      };
      if (scope.__streamSkopeCreateObjectUrl !== undefined) {
        URL.createObjectURL = scope.__streamSkopeCreateObjectUrl;
        delete scope.__streamSkopeCreateObjectUrl;
      }
    });

    await page.getByRole("textbox", { name: "Key contains" }).fill("does-not-exist");
    await expect(
      page.getByText("No messages match the current filters", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("The selected message is hidden by the current filters.", { exact: true }),
    ).toBeVisible();
    await expect(inspector).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Export filtered JSON" })).toBeDisabled();
    await page.getByRole("button", { name: "Clear message filters" }).click();
    await expect(page.getByLabel("Showing 4 of 4 retained messages")).toHaveText("4 / 4");
    const restoredInspector = page.getByRole("complementary", { name: "Message inspector" });
    await restoredInspector.getByRole("tab", { name: "Key" }).click();
    await expect(restoredInspector).toContainText("Order-Alpha");
    expect(backend.commands).toHaveLength(commandCountBeforeLocalOperations);

    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("message-operations-light.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("message-operations-dark.png"),
    });
  });
});
