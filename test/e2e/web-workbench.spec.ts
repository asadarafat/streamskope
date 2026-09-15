import { createServer, type Server } from "node:http";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page, type TestInfo } from "@playwright/test";

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

class WorkbenchBackend implements StreamSkopeBackend {
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
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `e2e-${command.id}` },
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

const backend = new WorkbenchBackend();
let launch: RunningWebDevelopment | undefined;

const zeroLiveEvaluation: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 17,
  errorCount: 0,
  errors: [],
  evaluatedRules: 2,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function exploredMessage(
  id: string,
  key: string,
  ruleEvaluation: KafkaLiveRuleEvaluation,
): KafkaExploredMessage {
  const payload = JSON.stringify({ key, source: "browser-rule-fixture" });
  return {
    headers: { "content-type": "application/json" },
    id,
    key,
    offset: id,
    originalByteSize: payload.length,
    partition: 0,
    payload,
    preview: payload,
    ruleEvaluation,
    timestamp: "2026-07-25T23:00:00.000Z",
    topic: "orders",
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

async function openWorkbench(page: Page): Promise<void> {
  await page.goto(activeLaunch().browserUrl);
  await expectWorkbenchReady(page);
  await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
}

async function latestCommandId(commandName: HostCommand["command"]): Promise<string> {
  await expect
    .poll(() => backend.commands.some((candidate) => candidate.command === commandName))
    .toBe(true);
  const command = [...backend.commands]
    .reverse()
    .find((candidate) => candidate.command === commandName);
  if (command === undefined) {
    throw new Error(`Expected ${commandName}.`);
  }
  return command.id;
}

async function captureTheme(
  page: Page,
  colorScheme: "dark" | "light",
  testInfo: TestInfo,
  previousBackground?: string,
): Promise<string> {
  await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
  await expect
    .poll(() => {
      return page.evaluate(() => {
        return globalThis.matchMedia("(prefers-color-scheme: dark)").matches;
      });
    })
    .toBe(colorScheme === "dark");
  await expect(page.getByRole("main", { name: "Connection profiles page" })).toBeVisible();
  const body = page.locator("body");
  if (previousBackground !== undefined) {
    await expect
      .poll(() => {
        return body.evaluate((element) => {
          return getComputedStyle(element).backgroundColor;
        });
      })
      .not.toBe(previousBackground);
  }
  await page.screenshot({
    animations: "disabled",
    path: testInfo.outputPath(`workbench-${colorScheme}.png`),
  });
  const accessibility = await new AxeBuilder({ page }).analyze();
  expect(accessibility.violations).toEqual([]);
  return body.evaluate((element) => {
    return getComputedStyle(element).backgroundColor;
  });
}

test.describe("StreamSkope browser workbench", () => {
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

  test("operates session rules by keyboard at the minimum desktop size", async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await openWorkbench(page);
    backend.emit({
      event: "rules.changed",
      payload: {
        rules: [],
        store: { durability: "session", state: "ready" },
      },
    });
    backend.emit({
      event: "connection.state",
      payload: { connectionName: "Browser fixture", state: "connected" },
    });
    backend.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-07-28T12:00:00.000Z",
        state: "ready",
        topics: ["orders"],
      },
    });
    await page.getByRole("button", { name: "orders", exact: true }).click();

    const rulesTab = page
      .getByRole("tablist", { name: "Topic sections" })
      .getByRole("tab", { name: "Rules" });
    await rulesTab.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("status", { name: "Rule storage status" })).toContainText(
      "Session-only rules",
    );

    const createRule = page.getByRole("button", { name: "Create rule" });
    await createRule.focus();
    await page.keyboard.press("Enter");
    const name = page.getByRole("textbox", { name: "Rule name" });
    await expect(name).toBeFocused();
    await page.keyboard.insertText("Browser ready orders");
    await page.getByRole("textbox", { name: "JSONPath expression" }).focus();
    await page.keyboard.insertText('$.status == "ready"');
    await page.getByRole("textbox", { name: "Topic filter" }).focus();
    await page.keyboard.insertText("orders");

    const validate = page.getByRole("button", { name: "Validate rule" });
    await validate.focus();
    await page.keyboard.press("Enter");
    const validationId = await latestCommandId("rules.validate");
    backend.emit({
      event: "rules.evaluation",
      payload: {
        kind: "validation",
        requestId: validationId,
        results: [{ name: "Browser ready orders", outcome: "valid" }],
      },
    });
    await expect(page.getByRole("status", { name: "Rule result" })).toContainText("Rule is valid");

    const testTab = page.getByRole("tab", { name: "Test" });
    await testTab.focus();
    await page.keyboard.press("Enter");
    const sample = page.getByRole("textbox", { name: "Sample JSON" });
    await sample.focus();
    await page.keyboard.press("ControlOrMeta+A");
    await page.keyboard.insertText('{"status":"ready"}');
    const evaluate = page.getByRole("button", {
      name: "Evaluate selected rule",
    });
    await evaluate.focus();
    await page.keyboard.press("Enter");
    const evaluationId = await latestCommandId("rules.evaluate");
    backend.emit({
      event: "rules.evaluation",
      payload: {
        kind: "evaluation",
        requestId: evaluationId,
        results: [{ name: "Browser ready orders", outcome: "matched" }],
      },
    });
    await expect(page.getByRole("status", { name: "Rule result" })).toContainText("Matched");

    const detailsTab = page.getByRole("tab", { name: "Details" });
    await detailsTab.focus();
    await page.keyboard.press("Enter");
    const save = page.getByRole("button", { name: "Save rule" });
    await save.focus();
    await page.keyboard.press("Enter");
    backend.emit({
      event: "rules.changed",
      payload: {
        rules: [
          {
            cooldownMs: 0,
            enabled: true,
            expression: '$.status == "ready"',
            level: "info",
            name: "Browser ready orders",
            topic: "orders",
          },
        ],
        store: { durability: "session", state: "ready" },
      },
    });
    await expect(
      page.getByRole("button", { name: "Browser ready orders, info, enabled" }),
    ).toBeVisible();
    await expect(createRule).toBeFocused();
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("rules-session-light.png"),
    });

    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("rules-session-dark.png"),
    });
  });

  test("keeps profile permutations understandable, accessible and bounded", async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ height: 650, width: 1000 });
    await openWorkbench(page);
    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: {
          durability: "session",
          protection: "memory",
          state: "ready",
        },
      },
    });

    await expect(page.getByText("Add a connection profile to connect to Kafka.")).toBeVisible();
    await expect(page.getByRole("status", { name: "Profile storage status" })).toContainText(
      "Session-only",
    );
    const addProfile = page.getByRole("button", { name: "Add profile" });
    await expect(addProfile).toBeEnabled();
    await addProfile.focus();
    await page.keyboard.press("Enter");
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await expect(editor).toBeVisible();
    await expect(editor.getByRole("textbox", { name: "Profile name" })).toBeFocused();
    await expect(editor.getByRole("textbox", { name: "Trust material label" })).toHaveCount(0);
    await expect(editor.getByRole("heading", { name: "Authentication" })).toBeVisible();
    const authenticationSwitch = editor.getByRole("switch", {
      name: "Use OAuth OAUTHBEARER",
    });
    const profileActions = editor.getByRole("group", { name: "Profile actions" });
    const [authenticationBounds, actionBounds] = await Promise.all([
      authenticationSwitch.boundingBox(),
      profileActions.boundingBox(),
    ]);
    expect(authenticationBounds).not.toBeNull();
    expect(actionBounds).not.toBeNull();
    expect(authenticationBounds!.y + authenticationBounds!.height).toBeLessThanOrEqual(
      actionBounds!.y,
    );
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("profiles-add-dialog.png"),
    });
    await page.keyboard.press("Escape");
    await expect(editor).toBeHidden();
    await expect(addProfile).toBeFocused();

    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [
          {
            active: false,
            brokers: ["127.0.0.1:9093"],
            createdAt: "2026-07-25T10:00:00.000Z",
            id: "profile-local",
            name: "Local aio",
            oauth: {
              clientId: "admin",
              clientSecretPresent: true,
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:5000/token",
            },
            trust: {
              kind: "pkcs12",
              label: "kafka.truststore.jks",
              materialPresent: true,
              passwordPresent: true,
            },
            updatedAt: "2026-07-25T10:00:00.000Z",
          },
          {
            active: false,
            brokers: ["broker.example.test:9094"],
            createdAt: "2026-07-25T11:00:00.000Z",
            id: "profile-staging",
            name: "Staging",
            trust: {
              kind: "pem",
              label: "staging-ca.pem",
              materialPresent: true,
              passwordPresent: false,
            },
            updatedAt: "2026-07-25T11:00:00.000Z",
          },
        ],
        store: {
          durability: "session",
          protection: "memory",
          state: "ready",
        },
      },
    });
    const profileList = page.getByRole("list", { name: "Kafka connection profiles" });
    await expect(profileList.getByText("Local aio")).toBeVisible();
    const stagingProfile = profileList.getByRole("button", { name: "Select profile Staging" });
    await expect(stagingProfile).toBeVisible();
    await expect(profileList).not.toContainText("kafka.truststore.jks");
    await expect(profileList).not.toContainText("staging-ca.pem");
    await page.getByRole("searchbox", { name: "Search profiles" }).fill("9094");
    await expect(stagingProfile).toBeVisible();
    await expect(profileList.getByText("Local aio")).toBeHidden();
    await expect(page.getByRole("heading", { name: "Staging", exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Local aio", exact: true })).toHaveCount(0);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("profiles-filtered-list.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("profiles-filtered-list-dark.png"),
    });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);

    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: {
          durability: "durable",
          protection: "unavailable",
          recovery: "Unlock the operating-system credential store, then restart StreamSkope.",
          state: "unavailable",
        },
      },
    });
    const unavailable = page.getByRole("alert");
    await expect(unavailable).toContainText("Profile storage unavailable");
    await expect(unavailable).toContainText(
      "Unlock the operating-system credential store, then restart StreamSkope.",
    );
    await expect(page.getByRole("button", { name: "Add profile" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Configure ad hoc connection" })).toHaveCount(0);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });

  test("keeps legacy catalogs out of the generic manual profile workflow", async ({
    page,
  }, testInfo) => {
    await openWorkbench(page);
    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: { durability: "session", protection: "memory", state: "ready" },
      },
    });
    await page.getByRole("button", { name: "Add profile" }).click();
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    await expect(editor.getByRole("combobox", { name: "Trust material format" })).toContainText(
      "PEM certificate",
    );
    await expect(
      editor.getByRole("button", { name: "Manage retrieval profiles", exact: true }),
    ).toHaveCount(0);
    await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    await expect(
      editor.getByRole("button", { name: "Manage retrieval profiles", exact: true }),
    ).toBeVisible();
    await expect(editor.getByRole("button", { name: "Manage connection templates" })).toHaveCount(
      0,
    );
    await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
    await expect(
      editor.getByRole("button", { name: "Choose OAuth endpoint template" }),
    ).toHaveCount(0);
    await expect(editor.getByRole("textbox", { name: "OAuth token endpoint" })).toHaveValue("");
    const recipeRequests = backend.commands.filter(
      (command) => command.command === "recipes.list",
    ).length;
    await editor.getByRole("button", { name: "Manage retrieval profiles", exact: true }).click();
    await expect
      .poll(() => backend.commands.filter((command) => command.command === "recipes.list").length)
      .toBeGreaterThan(recipeRequests);
    backend.emit({
      event: "recipes.changed",
      payload: { recipes: [], store: { durability: "session", state: "ready" } },
    });
    const manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles", exact: true });
    await manager.getByRole("button", { name: "Built-in profiles" }).click();
    await page.getByRole("menuitem", { name: "nsp-26-04" }).focus();
    await page.keyboard.press("Enter");
    await expect(manager.getByLabel("Retrieval profile name", { exact: true })).toHaveValue(
      "nsp-26-04",
    );
    await expect(manager.getByLabel("Material command", { exact: true })).toHaveValue(
      /kubectl exec/,
    );
    await expect(manager.getByLabel("Password command", { exact: true })).toHaveValue(
      /kubectl get secret/,
    );
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("nsp-26-04-review.png"),
    });
    await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
    await page.getByRole("button", { name: "Discard changes", exact: true }).click();
    expect(
      backend.commands.filter(
        (command) =>
          command.command.startsWith("trustAcquisition.") &&
          !["trustAcquisition.capabilities", "trustAcquisition.editor.open"].includes(
            command.command,
          ),
      ),
    ).toEqual([]);
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
    expect(
      backend.commands.filter((command) =>
        [
          "connection.connect",
          "connection.test",
          "profiles.create",
          "profiles.update",
          "profiles.test",
        ].includes(command.command),
      ),
    ).toEqual([]);
  });

  test("guards topic configuration changes and history at the minimum desktop size", async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await openWorkbench(page);
    backend.emit({
      event: "connection.state",
      payload: { connectionName: "Configuration fixture", state: "connected" },
    });
    backend.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-07-25T23:30:00.000Z",
        state: "ready",
        topics: ["orders"],
      },
    });
    await page.getByRole("button", { name: "orders", exact: true }).click();

    const workspaceTabs = page.getByRole("tablist", {
      name: "Topic sections",
    });
    const configurationTab = workspaceTabs.getByRole("tab", {
      name: "Configuration",
    });
    await configurationTab.focus();
    await page.keyboard.press("Enter");
    await expect(configurationTab).toHaveAttribute("aria-selected", "true");
    await latestCommandId("topicConfiguration.load");

    backend.emit({
      event: "topicConfiguration.changed",
      payload: {
        connectionName: "Configuration fixture",
        entries: [
          {
            documentation: "Retention policy",
            isDefault: false,
            isSensitive: false,
            name: "cleanup.policy",
            readOnly: true,
            source: "topic",
            synonyms: [],
            type: "list",
            value: "delete",
          },
          {
            documentation: "Retention duration",
            isDefault: false,
            isSensitive: false,
            name: "retention.ms",
            readOnly: false,
            source: "topic",
            synonyms: [],
            type: "long",
            value: "86400000",
          },
          {
            documentation: "Segment duration",
            isDefault: false,
            isSensitive: false,
            name: "segment.ms",
            readOnly: false,
            source: "topic",
            synonyms: [],
            type: "long",
            value: "1800000",
          },
        ],
        refreshedAt: "2026-07-25T23:30:01.000Z",
        state: "ready",
        topic: "orders",
      },
    });

    const grid = page.getByRole("grid", { name: "Topic configuration entries" });
    await grid.getByRole("gridcell", { name: "retention.ms" }).click();
    const proposedValue = page.getByRole("textbox", { name: "Proposed value" });
    const gridBounds = await grid.boundingBox();
    const proposedValueBounds = await proposedValue.boundingBox();
    if (gridBounds === null || proposedValueBounds === null) {
      throw new Error("The configuration table or selected-entry editor has no visible bounds.");
    }
    expect(proposedValueBounds.x).toBeGreaterThanOrEqual(gridBounds.x + gridBounds.width - 1);
    await proposedValue.fill("604800000");
    await page.getByRole("button", { name: "Queue change" }).click();
    await expect(page.getByRole("heading", { name: "1 pending change" })).toBeVisible();

    const dryRun = page.getByRole("button", { name: "Dry-run changes" });
    const applyChanges = page.getByRole("button", { name: "Apply changes" });
    const dryRunBounds = await dryRun.boundingBox();
    const applyChangesBounds = await applyChanges.boundingBox();
    if (dryRunBounds === null || applyChangesBounds === null) {
      throw new Error("The configuration mutation actions have no visible bounds.");
    }
    expect(applyChangesBounds.y).toBeCloseTo(dryRunBounds.y, 0);
    expect(applyChangesBounds.height).toBeCloseTo(dryRunBounds.height, 0);
    expect(applyChangesBounds.x).toBeGreaterThanOrEqual(dryRunBounds.x + dryRunBounds.width);
    await dryRun.focus();
    await page.keyboard.press("Enter");
    await expect
      .poll(() => {
        const command = [...backend.commands]
          .reverse()
          .find((candidate) => candidate.command === "topicConfiguration.validate");
        return command?.payload;
      })
      .toEqual({
        changes: [
          {
            isSensitive: false,
            name: "retention.ms",
            value: "604800000",
          },
        ],
        topic: "orders",
      });

    const applyCountBeforeCancel = backend.commands.filter(
      (command) => command.command === "topicConfiguration.apply",
    ).length;
    await applyChanges.click();
    let confirmation = page.getByRole("dialog", {
      name: "Apply topic configuration?",
    });
    await expect(confirmation).toContainText("orders");
    await expect(confirmation).toContainText("Configuration fixture");
    await expect(confirmation).toContainText("retention.ms");
    await confirmation.getByRole("button", { name: "Cancel" }).click();
    await expect(confirmation).toBeHidden();
    expect(
      backend.commands.filter((command) => command.command === "topicConfiguration.apply"),
    ).toHaveLength(applyCountBeforeCancel);

    await applyChanges.click();
    confirmation = page.getByRole("dialog", {
      name: "Apply topic configuration?",
    });
    await confirmation.getByRole("button", { name: "Apply named changes" }).click();
    await expect
      .poll(() => {
        const command = [...backend.commands]
          .reverse()
          .find((candidate) => candidate.command === "topicConfiguration.apply");
        return command?.payload;
      })
      .toEqual({
        changes: [
          {
            isSensitive: false,
            name: "retention.ms",
            value: "604800000",
          },
        ],
        topic: "orders",
      });
    await expect(page.getByText("No pending changes", { exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Configuration history" }).click();
    await latestCommandId("topicConfiguration.history");
    backend.emit({
      event: "topicConfiguration.history",
      payload: {
        connectionName: "Configuration fixture",
        entries: [
          {
            action: "apply",
            at: "2026-07-25T23:30:02.000Z",
            changes: [
              {
                from: "86400000",
                isSensitive: false,
                name: "retention.ms",
                to: "604800000",
                wasDefault: false,
              },
            ],
            connectionName: "Configuration fixture",
            connectionTarget: "broker:9093",
            id: "configuration-history-e2e",
            success: true,
            topic: "orders",
          },
        ],
        store: { durability: "session", state: "ready" },
        topic: "orders",
      },
    });
    const history = page.getByRole("dialog", { name: "Configuration history" });
    await expect(history).toContainText("Session-only history");
    await expect(history).toContainText("retention.ms");
    await expect(history).toContainText("604800000");
    await history.getByRole("button", { name: "Close" }).click();
    await expect(history).toBeHidden();

    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("topic-configuration-light.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("topic-configuration-dark.png"),
    });
  });

  test("explores live rule evidence by keyboard at the minimum desktop size", async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await openWorkbench(page);

    backend.emit({
      event: "connection.state",
      payload: { connectionName: "Rule fixture", state: "connected" },
    });
    backend.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-07-25T23:00:00.000Z",
        state: "ready",
        topics: ["orders"],
      },
    });
    await page.getByRole("button", { name: "orders", exact: true }).click();

    backend.emit({
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 2,
        request: {
          maxMessages: 1_000,
          mode: "tail",
          topic: "orders",
        },
        ruleEvaluation: {
          applicableRules: 52,
          omittedRules: 2,
          state: "partial",
        },
        state: "streaming",
      },
    });
    backend.emit({
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages: [
          exploredMessage("rule-active", "active-browser-key", {
            ...zeroLiveEvaluation,
            activeMatchCount: 2,
            activeMatches: [
              { level: "warn", name: "Slow order" },
              { level: "error", name: "Critical order" },
            ],
            highestActiveSeverity: "error",
          }),
          exploredMessage("rule-suppressed", "suppressed-browser-key", {
            ...zeroLiveEvaluation,
            evaluatedRules: 1,
            suppressedMatchCount: 1,
            suppressedMatches: [{ level: "warn", name: "Cooldown order" }],
          }),
          exploredMessage("rule-partial", "partial-browser-key", {
            ...zeroLiveEvaluation,
            errorCount: 1,
            errors: [{ diagnostic: "Rule evaluation failed.", name: "Broken order rule" }],
            evaluatedRules: 1,
            state: "partial",
          }),
          exploredMessage("rule-unavailable", "unavailable-browser-key", {
            activeMatchCount: 0,
            activeMatches: [],
            durationMicros: 0,
            errorCount: 0,
            errors: [],
            evaluatedRules: 0,
            omittedEvidence: 0,
            omittedRules: 0,
            reason: "payload-limit-exceeded",
            state: "unavailable",
            suppressedMatchCount: 0,
            suppressedMatches: [],
          }),
        ],
        topic: "orders",
      },
    });

    const grid = page.getByRole("grid", { name: "Kafka messages" });
    await expect(page.getByRole("alert")).toContainText("2 applicable live rules were omitted");

    const filterDisclosure = page.getByRole("button", { name: "Show message filters" });
    await filterDisclosure.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("button", { name: "Hide message filters" })).toBeFocused();
    const activeOnly = page.getByRole("checkbox", { name: "Rule matches only" });
    await activeOnly.focus();
    await page.keyboard.press("Space");
    await expect(activeOnly).toBeChecked();
    await expect(grid.getByText("active-browser-key", { exact: true })).toBeVisible();
    await expect(grid.getByText("suppressed-browser-key", { exact: true })).toHaveCount(0);

    await page.keyboard.press("Tab");
    await expect(page.locator('[role="columnheader"]:focus')).toBeVisible();
    await page.keyboard.press("ArrowDown");
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("ArrowRight");
    await page.keyboard.press("ArrowRight");
    await expect(page.locator('[role="gridcell"]:focus')).toContainText("2 matches · Error");
    await page.keyboard.press("Shift+Space");

    let inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { exact: true, name: "Rules" }).click();
    await expect(inspector).toContainText("2 active matches");
    await expect(inspector).toContainText("Critical order · Error");
    await inspector.getByRole("tab", { name: "Value" }).click();
    const rawTab = inspector.getByRole("tab", { name: "Raw" });
    await rawTab.focus();
    await page.keyboard.press("Enter");
    await expect(inspector).toContainText('"source":"browser-rule-fixture"');
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("live-rules-matched-light.png"),
    });

    await activeOnly.focus();
    await page.keyboard.press("Space");
    await expect(activeOnly).not.toBeChecked();
    await expect(grid.getByText("suppressed-browser-key", { exact: true })).toBeVisible();
    await expect(inspector).toContainText("active-browser-key");

    const closeInspector = inspector.getByRole("button", { name: "Close inspector" });
    await closeInspector.focus();
    await page.keyboard.press("Enter");
    await grid.getByText("suppressed-browser-key", { exact: true }).click();
    inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Rules", exact: true }).click();
    await expect(inspector).toContainText("1 suppressed match");
    await expect(inspector).toContainText("Cooldown-suppressed matches");
    await expect(inspector).toContainText("Cooldown order · Warn");

    await inspector.getByRole("button", { name: "Close inspector" }).click();
    await grid.getByText("partial-browser-key", { exact: true }).click();
    inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Rules" }).click();
    await expect(inspector).toContainText("Partial evaluation");
    await expect(inspector).toContainText("Broken order rule: Rule evaluation failed.");

    await inspector.getByRole("button", { name: "Close inspector" }).click();
    await grid.getByText("unavailable-browser-key", { exact: true }).click();
    inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Rules" }).click();
    await expect(inspector).toContainText("Live evaluation limit exceeded");
    await expect(inspector).toContainText("The complete payload remains available");

    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("live-rules-unavailable-dark.png"),
    });
  });

  test("does not report Connected before a confirming event", async ({ page }) => {
    await openWorkbench(page);

    backend.emit({
      event: "connection.state",
      payload: {
        connectionName: "Local validation",
        state: "connecting",
      },
    });
    const announcedStatus = page.getByRole("status", {
      name: "Connection status",
    });
    await expect(announcedStatus).toContainText("Connecting");
    await expect(announcedStatus).not.toContainText("Connected");

    backend.emit({
      event: "connection.state",
      payload: {
        connectionName: "Local validation",
        state: "connected",
      },
    });
    await expect(announcedStatus).toContainText("Connected · Local validation");

    backend.emit({
      event: "backend.availability",
      payload: {
        recovery: "Restart the local application host.",
        state: "unavailable",
      },
    });
    await expect(announcedStatus).toContainText("Last confirmed connected · Local validation");
    await expect(page.getByRole("contentinfo")).toContainText("Host unavailable");
    await expect(page.getByRole("contentinfo")).toContainText("Data is stale");
    await expect(page.getByRole("button", { name: "Reload workbench" })).toBeVisible();
  });

  test("shows timestamped safe operational activity on demand", async ({ page }) => {
    await openWorkbench(page);
    backend.emit({
      event: "activity.recorded",
      payload: {
        correlationId: "correlation-e2e",
        detail: "The local backend accepted a connection test.",
        id: "activity-e2e",
        object: "Local validation",
        operation: "Connection test",
        outcome: "succeeded",
        severity: "info",
        timestamp: "2026-07-25T12:00:00.000Z",
      },
    });

    const activitySession = page.getByRole("region", { name: "Activity dock" });
    await activitySession.getByRole("button", { name: "Expand Activity" }).click();
    const activity = page.getByRole("complementary", { name: "Activity log" });
    await expect(activitySession.getByRole("heading", { name: "Raw logs" })).toBeVisible();
    await expect(activitySession.getByRole("tablist")).toHaveCount(0);
    const rawLog = activity.getByRole("log", { name: "Raw activity log" });
    await expect(rawLog).toContainText('time="2026-07-25T12:00:00.000Z"');
    await expect(rawLog).toContainText('operation="Connection test"');
    await expect(rawLog).toContainText("outcome=succeeded");
    await expect(rawLog).toContainText("The local backend accepted a connection test.");
    await expect(activity.getByRole("region", { name: "Activity entry detail" })).toHaveCount(0);
  });

  test("passes automated accessibility checks in light and dark modes", async ({
    page,
  }, testInfo) => {
    await page.emulateMedia({ colorScheme: "light" });
    await openWorkbench(page);
    const lightBackground = await captureTheme(page, "light", testInfo);
    const darkBackground = await captureTheme(page, "dark", testInfo, lightBackground);

    expect(darkBackground).not.toBe(lightBackground);
  });
});
