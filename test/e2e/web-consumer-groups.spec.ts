import { createServer, type Server } from "node:http";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type HostEventListener,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupInventorySnapshot,
  type StreamSkopeBackend,
} from "../../src/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import {
  expectNoHorizontalOverflow,
  expectWorkbenchReady,
  observeBrowserDiagnostics,
  openWorkbenchResource,
} from "../support/workbench-browser";

class ConsumerGroupReviewBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  private sequence = 0;

  emit(event: Omit<HostEvent, "sequence" | "version">): void {
    const sequenced = {
      ...event,
      sequence: this.sequence++,
      version: HOST_PROTOCOL_VERSION,
    } as HostEvent;
    for (const listener of this.listeners) {
      listener(sequenced);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (
      command.command === "preferences.get" ||
      command.command === "preferences.reset" ||
      command.command === "preferences.update"
    ) {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: {
          correlationId: `consumer-group-review-${command.id}`,
          snapshot: {
            preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
            store: { durability: "session", state: "ready" },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      });
    }
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `consumer-group-review-${command.id}` },
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

const backend = new ConsumerGroupReviewBackend();
let launch: RunningWebDevelopment | undefined;

const denied: HostError = {
  activeStateChanged: false,
  code: "AUTHORIZATION_DENIED",
  correlationId: "consumer-groups-denied",
  recovery: "Grant DescribeGroups permission and retry.",
  retryable: false,
  stage: "broker",
  summary: "Kafka denied consumer-group access.",
};

const failed: HostError = {
  activeStateChanged: false,
  code: "TIMEOUT",
  correlationId: "consumer-groups-timeout",
  recovery: "Verify broker reachability and retry.",
  retryable: true,
  stage: "broker",
  summary: "Consumer-group metadata timed out.",
};

const readyInventory: KafkaConsumerGroupInventorySnapshot = {
  connectionName: "Local AIO",
  groups: [
    {
      groupType: "consumer",
      id: "billing-workers",
      protocolType: "consumer",
      state: "empty",
    },
    {
      groupType: "consumer",
      id: "orders-workers",
      protocolType: "consumer",
      state: "stable",
    },
  ],
  omittedGroups: 0,
  refreshedAt: "2026-08-12T09:01:00.000Z",
  state: "ready",
};

const readyDetail: KafkaConsumerGroupDetailSnapshot = {
  connectionName: "Local AIO",
  group: {
    id: "orders-workers",
    members: [
      {
        assignments: [{ partitions: [0, 1], topic: "orders.events" }],
        clientHost: "/10.0.0.8",
        clientId: "orders-worker-1",
        groupInstanceId: null,
        id: "member-1",
      },
    ],
    offsets: [
      {
        committedOffset: "9007199254740993",
        endOffset: "9007199254741000",
        lag: "7",
        partition: 0,
        topic: "orders.events",
      },
      {
        committedOffset: "12",
        endOffset: "12",
        lag: "0",
        partition: 1,
        topic: "orders.events",
      },
    ],
    omittedAssignments: 0,
    omittedMembers: 0,
    omittedOffsets: 0,
    protocol: "range",
    protocolType: "consumer",
    state: "stable",
  },
  groupId: "orders-workers",
  refreshedAt: "2026-08-12T09:01:01.000Z",
  state: "ready",
};

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

async function reservePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolveClose();
      } else {
        reject(error);
      }
    });
  });
  return port;
}

function activeLaunch(): RunningWebDevelopment {
  if (launch === undefined) {
    throw new Error("Consumer-group browser launch is unavailable.");
  }
  return launch;
}

function connect(): void {
  backend.emit({
    event: "connection.state",
    payload: { connectionName: "Local AIO", state: "connected" },
  });
}

function emitInventory(snapshot: KafkaConsumerGroupInventorySnapshot): void {
  backend.emit({ event: "consumerGroups.changed", payload: snapshot });
}

function emitDetail(snapshot: KafkaConsumerGroupDetailSnapshot): void {
  backend.emit({ event: "consumerGroup.changed", payload: snapshot });
}

async function openWorkbench(page: Page, colorScheme: "dark" | "light"): Promise<void> {
  await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
  await page.goto(activeLaunch().browserUrl);
  await expectWorkbenchReady(page);
}

async function expectNoSeriousAccessibilityViolations(page: Page): Promise<void> {
  const audit = await new AxeBuilder({ page }).analyze();
  expect(
    audit.violations.filter(
      (violation) => violation.impact === "critical" || violation.impact === "serious",
    ),
  ).toEqual([]);
}

test.describe("consumer-group visual workflow", () => {
  test.describe.configure({ mode: "serial", timeout: 60_000 });

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

  test("records honest wide states and the selected group hierarchy", async ({
    page,
  }, testInfo) => {
    const diagnostics = observeBrowserDiagnostics(page);
    await page.setViewportSize({ height: 900, width: 1440 });
    await openWorkbench(page, "light");

    const resources = page.getByRole("navigation", { name: "StreamSkope resources" });
    await expect(resources.getByRole("button", { name: "Consumer Groups" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );
    await expect(page.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-groups-disconnected-light.png"),
    });

    const listCount = backend.commands.filter(
      (command) => command.command === "consumerGroups.list",
    ).length;
    connect();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await expect(resources.getByRole("button", { name: "Topics" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    await openWorkbenchResource(page, "Consumer Groups");
    let inventory = page.getByRole("main", { name: "Consumer groups page" });
    await expect
      .poll(
        () =>
          backend.commands.filter((command) => command.command === "consumerGroups.list").length,
      )
      .toBe(listCount + 1);
    emitInventory({
      connectionName: "Local AIO",
      groups: [],
      omittedGroups: 0,
      refreshedAt: null,
      state: "loading",
    });
    await expect(
      inventory.getByRole("status", { name: "Consumer-group list status" }),
    ).toContainText("Loading consumer groups");
    emitInventory(readyInventory);
    await expect(inventory.getByRole("button", { name: "orders-workers" })).toBeVisible();
    await expect(page.getByRole("contentinfo")).toContainText("2 consumer groups");
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-groups-inventory-light.png"),
    });

    await inventory.getByRole("button", { name: "orders-workers" }).click();
    emitDetail({
      connectionName: "Local AIO",
      group: null,
      groupId: "orders-workers",
      refreshedAt: null,
      state: "loading",
    });
    await expect(page.getByRole("region", { name: "Consumer group workspace" })).toContainText(
      "Loading consumer group",
    );
    emitDetail(readyDetail);

    const workspace = page.getByRole("region", { name: "Consumer group workspace" });
    await expect(workspace.getByRole("heading", { name: "Group status" })).toBeVisible();
    await expect(workspace.getByRole("heading", { name: "Members" })).toBeVisible();
    await expect(workspace.getByRole("heading", { name: "Offsets and lag" })).toBeVisible();
    await expect(workspace.getByRole("grid", { name: "Consumer group offsets" })).toContainText(
      "9007199254740993",
    );
    await expect(workspace.getByRole("button", { name: /reset|delete/i })).toHaveCount(0);
    await expectNoHorizontalOverflow(page);
    await expectNoSeriousAccessibilityViolations(page);
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-group-detail-light.png"),
    });

    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    await expect(page.locator("html")).toHaveAttribute("data-mui-color-scheme", "dark");
    await expectNoSeriousAccessibilityViolations(page);
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-group-detail-dark.png"),
    });

    await page
      .getByRole("navigation", { name: "Breadcrumb" })
      .getByRole("button", { name: "Consumer Groups" })
      .click();
    inventory = page.getByRole("main", { name: "Consumer groups page" });
    emitInventory({
      connectionName: "Local AIO",
      groups: [],
      omittedGroups: 0,
      refreshedAt: "2026-08-12T09:02:00.000Z",
      state: "empty",
    });
    await expect(inventory).toContainText("No consumer groups found");
    emitInventory({
      connectionName: "Local AIO",
      error: denied,
      groups: [],
      omittedGroups: 0,
      refreshedAt: null,
      state: "denied",
    });
    await expect(inventory).toContainText("Consumer-group access denied");
    await expect(inventory).toContainText(denied.recovery);
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-groups-denied-dark.png"),
    });
    emitInventory({
      connectionName: "Local AIO",
      error: failed,
      groups: [],
      omittedGroups: 0,
      refreshedAt: null,
      state: "failed",
    });
    await expect(inventory).toContainText("Consumer groups unavailable");
    await expect(inventory).toContainText(failed.recovery);
    expect(diagnostics.problems).toEqual([]);
  });

  test("keeps the compact workspace primary and exposes resources in one drawer", async ({
    page,
  }, testInfo) => {
    const diagnostics = observeBrowserDiagnostics(page);
    await page.setViewportSize({ height: 600, width: 800 });
    await openWorkbench(page, "light");
    connect();
    emitInventory(readyInventory);

    await openWorkbenchResource(page, "Consumer Groups");
    await page
      .getByRole("main", { name: "Consumer groups page" })
      .getByRole("button", { name: "orders-workers" })
      .click();
    emitDetail(readyDetail);

    const workspace = page.getByRole("region", { name: "Consumer group workspace" });
    await expect(workspace).toBeVisible();
    const offsets = workspace.getByRole("grid", { name: "Consumer group offsets" });
    await expect(offsets).toBeVisible();
    await expect(page.getByRole("navigation", { name: "StreamSkope resources" })).toHaveCount(0);
    await expectNoHorizontalOverflow(page);
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-group-detail-compact.png"),
    });
    await offsets.scrollIntoViewIfNeeded();
    await expect(offsets).toContainText("9007199254740993");
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-group-offsets-compact.png"),
    });

    await page.getByRole("button", { name: "Open Kafka resources" }).click();
    await expect(page.getByRole("navigation", { name: "StreamSkope resources" })).toBeVisible();
    await page.screenshot({
      fullPage: true,
      path: testInfo.outputPath("consumer-groups-drawer-compact.png"),
    });
    await expectNoSeriousAccessibilityViolations(page);
    expect(diagnostics.problems).toEqual([]);
  });
});
