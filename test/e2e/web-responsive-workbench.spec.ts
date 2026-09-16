import { createServer, type Server } from "node:http";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Locator, type Page } from "@playwright/test";

import {
  HOST_PROTOCOL_VERSION,
  HOST_ACTIVITY_HISTORY_LIMIT,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupInventorySnapshot,
  type KafkaExploredMessage,
  type ProfileSummary,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import {
  expectNoHorizontalOverflow,
  observeBrowserDiagnostics,
  openTopicTask,
} from "../support/workbench-browser";

class ResponsiveWorkbenchBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  private sequence = 0;

  emit(event: Omit<HostEvent, "sequence" | "version">): void {
    const sequenced = {
      ...event,
      sequence: this.sequence++,
      version: HOST_PROTOCOL_VERSION,
    } as HostEvent;
    for (const listener of this.listeners) listener(sequenced);
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "preferences.get") {
      return Promise.resolve({
        command: command.command,
        id: command.id,
        ok: true,
        result: {
          correlationId: `responsive-${command.id}`,
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
      result: { correlationId: `responsive-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  resetCommands(): void {
    this.commands.length = 0;
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

const backend = new ResponsiveWorkbenchBackend();
let launch: RunningWebDevelopment | undefined;

const localProfile: ProfileSummary = {
  active: false,
  brokers: ["clab.orb.local:19093"],
  createdAt: "2026-08-12T08:00:00.000Z",
  id: "local-aio",
  name: "Local AIO Kafka",
  trust: {
    kind: "pem",
    label: "Repository fixture CA",
    materialPresent: true,
    passwordPresent: false,
  },
  updatedAt: "2026-08-12T08:00:00.000Z",
};

const readyGroups: KafkaConsumerGroupInventorySnapshot = {
  connectionName: "Local AIO Kafka",
  groups: [
    {
      groupType: "consumer",
      id: "orders-workers",
      protocolType: "consumer",
      state: "stable",
    },
  ],
  omittedGroups: 0,
  refreshedAt: "2026-08-12T08:04:00.000Z",
  state: "ready",
};

const readyGroup: KafkaConsumerGroupDetailSnapshot = {
  connectionName: "Local AIO Kafka",
  group: {
    id: "orders-workers",
    members: [
      {
        assignments: [{ partitions: [0], topic: "orders.events" }],
        clientHost: "/10.0.0.8",
        clientId: "orders-worker-1",
        groupInstanceId: null,
        id: "member-1",
      },
    ],
    offsets: [
      {
        committedOffset: "42",
        endOffset: "49",
        lag: "7",
        partition: 0,
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
  refreshedAt: "2026-08-12T08:04:01.000Z",
  state: "ready",
};

function message(): KafkaExploredMessage {
  const payload = '{"order_id":"ord-1042","status":"accepted"}';
  return {
    headers: { "content-type": "application/json" },
    id: "orders-0-42",
    key: "ord-1042",
    offset: "42",
    originalByteSize: payload.length,
    partition: 0,
    payload,
    preview: payload,
    ruleEvaluation: {
      activeMatchCount: 0,
      activeMatches: [],
      durationMicros: 14,
      errorCount: 0,
      errors: [],
      evaluatedRules: 0,
      omittedEvidence: 0,
      omittedRules: 0,
      state: "evaluated",
      suppressedMatchCount: 0,
      suppressedMatches: [],
    },
    timestamp: "2026-08-12T08:03:00.000Z",
    topic: "orders.events",
    truncated: false,
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

async function reservePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => {
      if (error === undefined) resolveClose();
      else reject(error);
    });
  });
  return port;
}

function activeLaunch(): RunningWebDevelopment {
  if (launch === undefined) throw new Error("Responsive browser launch is unavailable.");
  return launch;
}

async function openDesktop(page: Page): Promise<void> {
  backend.resetCommands();
  await page.setViewportSize({ height: 900, width: 1440 });
  await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
  await page.goto(activeLaunch().browserUrl);
  await expect(page.getByRole("banner", { name: "StreamSkope application bar" })).toContainText(
    "StreamSkope",
  );
  await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
}

function emitProfiles(): void {
  backend.emit({
    event: "profiles.changed",
    payload: {
      profiles: [localProfile],
      store: { durability: "session", protection: "memory", state: "ready" },
    },
  });
}

function connectWithTopics(): void {
  backend.emit({
    event: "connection.state",
    payload: { connectionName: "Local AIO Kafka", state: "connected" },
  });
  backend.emit({
    event: "topics.changed",
    payload: {
      refreshedAt: "2026-08-12T08:02:00.000Z",
      state: "ready",
      topics: ["audit.events", "orders.events"],
    },
  });
}

async function expectAccessible(page: Page): Promise<void> {
  const audit = await new AxeBuilder({ page }).analyze();
  expect(audit.violations).toEqual([]);
}

async function visibleTextBounds(
  container: Locator,
  expectedText: string,
): Promise<{
  readonly height: number;
  readonly width: number;
  readonly x: number;
  readonly y: number;
}> {
  return container.evaluate((element, text) => {
    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node !== null; node = walker.nextNode()) {
      const content = node.textContent ?? "";
      const start = content.indexOf(text);
      if (start < 0) continue;
      const range = document.createRange();
      range.setStart(node, start);
      range.setEnd(node, start + text.length);
      const bounds = range.getBoundingClientRect();
      return { height: bounds.height, width: bounds.width, x: bounds.x, y: bounds.y };
    }
    throw new Error(`Visible text ${text} was not found in the accessible cell.`);
  }, expectedText);
}

test.describe("StreamSkope Redpanda-style responsive workbench", () => {
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

  for (const wrap of [false, true]) {
    test(`keeps log reading stable and exposes active filters outside disclosure (wrap=${wrap})`, async ({
      page,
    }) => {
      await openDesktop(page);
      emitProfiles();
      await page.getByRole("button", { name: "Expand Activity" }).click();
      if (wrap) {
        await page.getByRole("button", { name: "Filter", exact: true }).click();
        await page.getByRole("checkbox", { name: "Wrap lines" }).check();
        await page.getByRole("button", { name: "Filter", exact: true }).click();
      }
      const emitLog = (index: number): void =>
        backend.emit({
          event: "activity.recorded",
          payload: {
            id: `review-${String(index)}`,
            timestamp: "2026-09-14T12:00:00Z",
            severity: "info",
            operation: "Review fixture",
            object: "Synthetic logs",
            outcome: "succeeded",
            correlationId: `review-${String(index)}`,
            detail: `Synthetic event ${String(index)}`,
          },
        });
      for (let index = 0; index < 80; index += 1) emitLog(index);
      const raw = page.getByRole("log", { name: "Raw activity log" });
      await expect(raw).toContainText("Synthetic event 79");
      await raw.evaluate((element) => {
        element.scrollTop = 0;
      });
      await expect(page.getByRole("button", { name: "Resume live" })).toBeVisible();
      emitLog(80);
      await expect(raw).toContainText("Synthetic event 80");
      expect(await raw.evaluate((element) => element.scrollTop)).toBe(0);
      for (let index = 81; index < HOST_ACTIVITY_HISTORY_LIMIT; index += 1) emitLog(index);
      await expect(raw).toContainText(`Synthetic event ${HOST_ACTIVITY_HISTORY_LIMIT - 1}`);
      const eventPosition = async (scroll = false): Promise<number> =>
        raw.evaluate((element, move) => {
          const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
          let node: Node | null;
          while ((node = walker.nextNode()) !== null) {
            const index = node.textContent?.indexOf('msg="Synthetic event 50"') ?? -1;
            if (index < 0) continue;
            const range = document.createRange();
            range.setStart(node, index);
            range.setEnd(node, index + 10);
            const offset = range.getBoundingClientRect().top - element.getBoundingClientRect().top;
            if (move) element.scrollTop += offset - 40;
            return range.getBoundingClientRect().top - element.getBoundingClientRect().top;
          }
          throw new Error("Retained event missing");
        }, scroll);
      await eventPosition(true);
      await expect(page.getByRole("button", { name: "Resume live" })).toBeVisible();
      const beforeEviction = await eventPosition();
      for (
        let index = HOST_ACTIVITY_HISTORY_LIMIT;
        index < HOST_ACTIVITY_HISTORY_LIMIT + 20;
        index += 1
      )
        emitLog(index);
      await expect(raw).toContainText(`Synthetic event ${HOST_ACTIVITY_HISTORY_LIMIT + 19}`);
      expect(Math.abs((await eventPosition()) - beforeEviction)).toBeLessThanOrEqual(1);
      await raw.evaluate((element) => {
        element.scrollTop = 0;
      });
      await expect.poll(() => raw.evaluate((element) => element.scrollTop)).toBe(0);
      emitLog(HOST_ACTIVITY_HISTORY_LIMIT + 20);
      await expect(
        page.getByText(/Previously viewed events are no longer retained/u),
      ).toBeVisible();
      await page.getByRole("button", { name: "Resume live" }).click();
      await expect
        .poll(() =>
          raw.evaluate((element) =>
            Math.abs(element.scrollHeight - element.clientHeight - element.scrollTop),
          ),
        )
        .toBeLessThanOrEqual(1);
      await page.getByRole("button", { name: "Filter", exact: true }).click();
      await page
        .getByRole("searchbox", { name: "Search activity" })
        .fill(`Synthetic event ${HOST_ACTIVITY_HISTORY_LIMIT + 19}`);
      await page.getByRole("button", { name: "Filter (1)" }).click();
      await expect(raw).not.toContainText("Synthetic event 79");
      await expect(page.getByRole("button", { name: "Filter (1)" })).toHaveAttribute(
        "aria-expanded",
        "false",
      );
      await expectAccessible(page);
    });
  }

  test("keeps profile evidence reachable with an expanded dock in both themes", async ({
    page,
  }, testInfo) => {
    await openDesktop(page);
    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [
          {
            ...localProfile,
            oauth: {
              clientId: "review",
              clientSecretPresent: true,
              scope: "kafka",
              tokenEndpoint: `https://auth.example.test/${"long-path/".repeat(12)}token`,
            },
          },
        ],
        store: { durability: "session", protection: "memory", state: "ready" },
      },
    });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.getByRole("button", { name: "Expand Activity" }).click();
    const workspace = page.getByRole("region", { name: "Connection profile workspace" });
    await expect(workspace).toContainText("Cluster services");
    const count = backend.commands.length;
    for (const mode of ["light", "dark"] as const) {
      await page.emulateMedia({ colorScheme: mode });
      const geometry = await workspace.evaluate((element) => {
        element.scrollTop = element.scrollHeight;
        return {
          content: element.scrollHeight,
          height: element.clientHeight,
          offset: element.scrollTop,
          overflow: getComputedStyle(element).overflowY,
        };
      });
      expect(geometry.overflow).toBe("auto");
      expect(geometry.offset + geometry.height).toBeGreaterThanOrEqual(geometry.content - 1);
      await expectNoHorizontalOverflow(page);
      await expectAccessible(page);
      await page.screenshot({ path: testInfo.outputPath(`profile-scroll-${mode}.png`) });
    }
    expect(
      backend.commands
        .slice(count)
        .filter(
          (command) =>
            command.command === "profiles.connect" || command.command === "profiles.test",
        ),
    ).toHaveLength(0);
  });

  test("renders one persistent resource hierarchy and honest disconnected pages", async ({
    page,
  }, testInfo) => {
    const diagnostics = observeBrowserDiagnostics(page);
    await openDesktop(page);
    emitProfiles();

    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    for (const resource of [
      "Overview",
      "Topics",
      "Consumer Groups",
      "Schema Registry",
      "Access Control Lists",
      "Transforms",
      "Connection Profiles",
    ]) {
      await expect(navigation.getByRole("button", { name: resource })).toBeVisible();
    }
    for (const unsupported of ["Kafka Connect"]) {
      await expect(navigation.getByRole("button", { name: unsupported })).toHaveCount(0);
    }
    await expect(
      navigation.getByRole("button", { name: "Connection Profiles" }),
    ).not.toHaveAttribute("aria-disabled");
    for (const resource of [
      "Overview",
      "Topics",
      "Consumer Groups",
      "Schema Registry",
      "Access Control Lists",
      "Transforms",
    ]) {
      await expect(navigation.getByRole("button", { name: resource })).toHaveAttribute(
        "aria-disabled",
        "true",
      );
    }
    await expect(page.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    await expect(
      page
        .getByRole("banner", { name: "StreamSkope application bar" })
        .getByRole("button", { name: /Connection context:/ }),
    ).toHaveCount(0);
    await expect(page.getByRole("status", { name: "Selected profile state" })).toHaveCount(0);
    await expect(page.getByRole("status", { name: "Connection status" })).toHaveCount(1);
    await expect(page.getByRole("status", { name: "Connection status" })).toContainText(
      "Disconnected",
    );
    await expect(
      page.getByRole("button", { name: "Select profile Local AIO Kafka" }),
    ).toBeVisible();
    const profileSearch = page.getByRole("searchbox", { name: "Search profiles" });
    const addProfile = page.getByRole("button", { name: "Add profile" });
    const profileSearchGeometry = await profileSearch.evaluate((element) => {
      const control = element.closest(".MuiOutlinedInput-root");
      if (!(control instanceof HTMLElement)) throw new Error("Profile search control is missing.");
      const style = getComputedStyle(control);
      return {
        bottomLeftRadius: style.borderBottomLeftRadius,
        height: control.getBoundingClientRect().height,
        topLeftRadius: style.borderTopLeftRadius,
      };
    });
    const addProfileGeometry = await addProfile.evaluate((element) => {
      const style = getComputedStyle(element);
      return {
        bottomRightRadius: style.borderBottomRightRadius,
        height: element.getBoundingClientRect().height,
        topRightRadius: style.borderTopRightRadius,
      };
    });
    expect(addProfileGeometry.topRightRadius).toBe(profileSearchGeometry.topLeftRadius);
    expect(addProfileGeometry.bottomRightRadius).toBe(profileSearchGeometry.bottomLeftRadius);
    expect(addProfileGeometry.height).toBe(profileSearchGeometry.height);
    const profileWorkspace = page.getByLabel("Connection profile workspace");
    await expect(profileWorkspace).toContainText("Kafka connection");
    await expect(profileWorkspace).toContainText("Transport and profile");
    const profileViewport = await profileWorkspace.evaluate((element) => ({
      clientHeight: element.clientHeight,
      scrollHeight: element.scrollHeight,
    }));
    expect(profileViewport.scrollHeight).toBeLessThanOrEqual(profileViewport.clientHeight + 1);

    const commandCount = backend.commands.length;
    await expect(page.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    expect(backend.commands.slice(commandCount).map(({ command }) => command)).not.toContain(
      "messages.start",
    );

    await expectNoHorizontalOverflow(page);
    await expectAccessible(page);
    expect(diagnostics.problems).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("resource-shell-disconnected-light.png"),
    });
  });

  test.describe("retina product evidence", () => {
    test.use({ deviceScaleFactor: 2 });
    test("uses topic inventory and object-scoped tabs with one activation read", async ({
      page,
    }, testInfo) => {
      test.setTimeout(60_000);
      const diagnostics = observeBrowserDiagnostics(page);
      await openDesktop(page);
      emitProfiles();
      connectWithTopics();

      await expect(page.getByRole("main", { name: "Topics page" })).toBeVisible();
      await expect(page.getByRole("grid", { name: "Kafka topics" })).toContainText("orders.events");
      const topicDataPlane = page.getByTestId("topic-inventory-data-plane");
      await expect(topicDataPlane).toBeVisible();
      const topicDataPlaneBounds = await topicDataPlane.boundingBox();
      expect(topicDataPlaneBounds?.height).toBeGreaterThan(400);
      await page.screenshot({
        animations: "disabled",
        path: testInfo.outputPath("topic-inventory-light.png"),
      });
      const preOpenCommands = backend.commands.length;
      await page.getByRole("button", { name: "orders.events" }).click();
      await expect(page.getByRole("main", { name: "Topic detail page" })).toBeVisible();
      const tabs = page.getByRole("tablist", { name: "Topic sections" });
      await expect(tabs.getByRole("tab")).toHaveCount(5);
      expect(backend.commands.slice(preOpenCommands).map(({ command }) => command)).toContain(
        "messages.start",
      );

      for (const [tab, region, capture] of [
        ["Messages", "Message workspace", "topic-messages-light.png"],
        ["Monitor", "Stream Monitor workspace", "topic-monitor-light.png"],
        ["Latency", "Latency workspace", "topic-latency-light.png"],
        ["Rules", "Rule workspace", "topic-rules-light.png"],
        ["Configuration", "Topic configuration workspace", "topic-configuration-light.png"],
      ] as const) {
        await openTopicTask(page, tab);
        await expect(page.getByRole("region", { name: region })).toBeVisible();
        const controls = page.getByRole("group", {
          name: {
            Messages: "Message controls",
            Monitor: "Monitor controls",
            Latency: "Latency controls",
            Rules: "Rule controls",
            Configuration: "Configuration controls",
          }[tab],
          exact: true,
        });
        await expect(controls).toBeVisible();
        const toolbarBounds = await controls.boundingBox();
        const tabBounds = await tabs.boundingBox();
        expect(toolbarBounds?.height).toBe(44);
        expect(
          Math.abs((toolbarBounds?.y ?? 0) - (tabBounds?.y ?? 0) - (tabBounds?.height ?? 0)),
        ).toBeLessThanOrEqual(1);
        expect(await controls.evaluate((element) => getComputedStyle(element).paddingLeft)).toBe(
          "12px",
        );
        await page.screenshot({ animations: "disabled", path: testInfo.outputPath(capture) });
      }
      expect(backend.commands.filter(({ command }) => command === "messages.start")).toHaveLength(
        1,
      );

      await page.setViewportSize({ width: 800, height: 600 });
      await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
      for (const tab of ["Messages", "Monitor", "Latency", "Rules", "Configuration"] as const) {
        await openTopicTask(page, tab);
        await expectNoHorizontalOverflow(page);
        expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
        await page.screenshot({
          animations: "disabled",
          path: testInfo.outputPath(`topic-${tab.toLowerCase()}-compact-dark.png`),
        });
      }
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });

      await openTopicTask(page, "Messages");
      await expect
        .poll(() => backend.commands.filter(({ command }) => command === "messages.start").length)
        .toBe(1);
      backend.emit({
        event: "consumption.state",
        payload: {
          droppedMessages: 0,
          receivedMessages: 1,
          request: { maxMessages: 1_000, mode: "tail", topic: "orders.events" },
          ruleEvaluation: { applicableRules: 0, omittedRules: 0, state: "ready" },
          state: "streaming",
        },
      });
      backend.emit({
        event: "messages.batch",
        payload: { droppedMessages: 0, messages: [message()], topic: "orders.events" },
      });
      const grid = page.getByRole("grid", { name: "Kafka messages" });
      await expect(grid).toBeVisible();
      for (const value of [
        "2026-08-12T08:03:00.000Z",
        "ord-1042",
        '{"order_id":"ord-1042","status":"accepted"}',
        "0",
        "42",
        "—",
      ]) {
        const cell = grid.getByRole("gridcell", { exact: true, name: value });
        const cellBounds = await cell.boundingBox();
        const textBounds = await visibleTextBounds(cell, value);
        expect(cellBounds).not.toBeNull();
        const verticalOffset = Math.abs(
          textBounds.y +
            textBounds.height / 2 -
            ((cellBounds?.y ?? 0) + (cellBounds?.height ?? 0) / 2),
        );
        expect(
          verticalOffset,
          `${value} must be vertically centered in its message cell`,
        ).toBeLessThanOrEqual(0.5);
      }
      await grid.getByRole("gridcell", { exact: true, name: "ord-1042" }).click();
      await expect(page.getByRole("complementary", { name: "Message inspector" })).toBeVisible();

      await expectAccessible(page);
      expect(diagnostics.problems).toEqual([]);
      await page.screenshot({
        animations: "disabled",
        path: testInfo.outputPath("topic-message-inspector-light.png"),
      });
      const inspector = page.getByRole("complementary", { name: "Message inspector" });
      await inspector.getByRole("tab", { name: "Value", exact: true }).click();
      await expect(inspector.getByLabel("Value evidence")).toContainText("ord-1042");
      await expect(inspector.getByLabel("Value evidence")).toContainText("accepted");
      await page.screenshot({
        animations: "disabled",
        path: testInfo.outputPath("topic-message-value-light.png"),
      });
      await inspector.getByRole("tab", { name: "Metadata", exact: true }).click();
      await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
      await expect(page.locator("html")).toHaveAttribute("data-mui-color-scheme", "dark");
      await expectAccessible(page);
      await page.screenshot({
        animations: "disabled",
        path: testInfo.outputPath("topic-message-inspector-dark.png"),
      });
    });
  });

  test("presents consumer groups as an inventory followed by one detail page", async ({
    page,
  }, testInfo) => {
    await openDesktop(page);
    connectWithTopics();
    const topicGrid = page.getByRole("grid", { name: "Kafka topics" });
    await expect(topicGrid).toContainText("orders.events");
    const topicHeaderBounds = await topicGrid.getByRole("columnheader").first().boundingBox();
    const topicRowBounds = await topicGrid.getByRole("row").nth(1).boundingBox();
    const topicAction = topicGrid.getByRole("button", { name: "orders.events" });
    const topicCell = topicGrid.getByRole("gridcell", { name: "orders.events" });
    const topicActionBounds = await topicAction.boundingBox();
    const topicCellBounds = await topicCell.boundingBox();
    const topicDataPlaneBounds = await page.getByTestId("topic-inventory-data-plane").boundingBox();
    expect(topicHeaderBounds).not.toBeNull();
    expect(topicRowBounds).not.toBeNull();
    expect(topicActionBounds).not.toBeNull();
    expect(topicCellBounds).not.toBeNull();
    expect(topicActionBounds?.height).toBeLessThanOrEqual(topicCellBounds?.height ?? 0);
    expect(
      Math.abs(
        (topicActionBounds?.y ?? 0) +
          (topicActionBounds?.height ?? 0) / 2 -
          ((topicCellBounds?.y ?? 0) + (topicCellBounds?.height ?? 0) / 2),
      ),
    ).toBeLessThanOrEqual(0.5);
    expect(topicDataPlaneBounds).not.toBeNull();

    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    await navigation.getByRole("button", { name: "Consumer Groups" }).click();
    backend.emit({ event: "consumerGroups.changed", payload: readyGroups });

    const inventory = page.getByRole("main", { name: "Consumer groups page" });
    const groupGrid = inventory.getByRole("grid", { name: "Kafka consumer groups" });
    await expect(groupGrid).toContainText("orders-workers");
    const groupHeaderBounds = await groupGrid.getByRole("columnheader").first().boundingBox();
    const groupRowBounds = await groupGrid.getByRole("row").nth(1).boundingBox();
    const groupAction = groupGrid.getByRole("button", { name: "orders-workers" });
    const groupCell = groupGrid.getByRole("gridcell", { name: "orders-workers" });
    const groupActionBounds = await groupAction.boundingBox();
    const groupCellBounds = await groupCell.boundingBox();
    expect(groupHeaderBounds?.height).toBe(topicHeaderBounds?.height);
    expect(groupRowBounds?.height).toBe(topicRowBounds?.height);
    expect(groupActionBounds?.height).toBeLessThanOrEqual(groupCellBounds?.height ?? 0);
    expect(
      Math.abs(
        (groupActionBounds?.y ?? 0) +
          (groupActionBounds?.height ?? 0) / 2 -
          ((groupCellBounds?.y ?? 0) + (groupCellBounds?.height ?? 0) / 2),
      ),
    ).toBeLessThanOrEqual(0.5);
    const groupDataPlaneBounds = await page
      .getByTestId("consumer-group-inventory-data-plane")
      .boundingBox();
    expect(groupDataPlaneBounds?.height).toBeGreaterThan(400);
    expect(groupDataPlaneBounds?.height).toBe(topicDataPlaneBounds?.height);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("consumer-groups-inventory-light.png"),
    });
    await inventory.getByRole("button", { name: "orders-workers" }).click();
    backend.emit({ event: "consumerGroup.changed", payload: readyGroup });

    const detail = page.getByRole("main", { name: "Consumer group detail page" });
    await expect(detail.getByRole("heading", { name: "orders-workers" })).toBeVisible();
    await expect(detail.getByRole("grid", { name: "Consumer group members" })).toContainText(
      "orders-worker-1",
    );
    await expect(detail.getByRole("grid", { name: "Consumer group offsets" })).toContainText("7");
    await expectAccessible(page);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("consumer-group-detail-light.png"),
    });
  });

  test("keeps the main resource page primary at 800 by 600", async ({ page }, testInfo) => {
    backend.resetCommands();
    await page.setViewportSize({ height: 600, width: 800 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await page.goto(activeLaunch().browserUrl);

    const banner = page.getByRole("banner");
    await expect(banner).toContainText("StreamSkope");
    await expect(page.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
    await banner.getByRole("button", { name: "Open Kafka resources" }).click();
    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    const drawer = page.getByRole("presentation").filter({ has: navigation });
    await expect(drawer).toBeVisible();
    await expect(navigation).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(drawer).toBeHidden();
    await page.getByRole("button", { name: "Expand Activity" }).click();
    const activity = page.getByRole("complementary", { name: "Activity log" });
    await expect(activity).toBeVisible();
    backend.emit({
      event: "activity.recorded",
      payload: {
        correlationId: "compact-review-failure",
        detail: "Kafka refused the connection; no active connection changed.",
        id: "compact-review-failure",
        object: "Local AIO Kafka",
        operation: "Test profile connection",
        outcome: "failed",
        severity: "error",
        timestamp: "2026-08-12T08:05:00.000Z",
      },
    });
    await expect(activity.getByRole("log", { name: "Raw activity log" })).toContainText(
      "Kafka refused the connection",
    );
    await expect(page.getByRole("heading", { name: "Raw logs" })).toBeVisible();
    await expectNoHorizontalOverflow(page);
    await expectAccessible(page);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("compact-activity-light.png"),
    });
  });
});
