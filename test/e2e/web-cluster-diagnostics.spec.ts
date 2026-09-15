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
  type HostTextDocument,
  type KafkaClusterDiagnosticsSnapshot,
  type StreamSkopeBackend,
} from "../../src/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { openProfileActions, openWorkbenchResource } from "../support/workbench-browser";

const profile = {
  brokers: ["127.0.0.1:19093"],
  id: "profile-local",
  name: "Local validation",
} as const;

const document = {
  cluster: {
    brokers: [
      { host: "kafka-1", nodeId: 1, port: 9093, rack: null },
      { host: "kafka-2", nodeId: 2, port: 9094, rack: "rack-b" },
    ],
    clusterId: "fixture-cluster",
    configuration: [
      {
        documentation: "Default partition count.",
        isDefault: true,
        isSensitive: false,
        name: "num.partitions",
        readOnly: false,
        source: "default",
        synonyms: [],
        type: "int",
        value: "3",
      },
      {
        documentation: null,
        isDefault: false,
        isSensitive: true,
        name: "ssl.keystore.password",
        readOnly: true,
        source: "static-broker",
        synonyms: [],
        type: "password",
        value: null,
      },
    ],
    configurationSourceBrokerId: 1,
    controllerId: 1,
  },
  endpoint: "127.0.0.1:19093",
  fetchedAt: "2026-07-25T13:00:00.000Z",
  profile,
} as const;

const exportContent = `${JSON.stringify(document, null, 2)}\n`;
const exportDocument: HostTextDocument = {
  byteSize: new TextEncoder().encode(exportContent).byteLength,
  content: exportContent,
  fileName: "streamskope-cluster-fixture-cluster.json",
  mediaType: "application/json",
};

class ClusterWorkbenchBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  private sequence = 0;

  emit(event: Omit<HostEvent, "sequence" | "version">): void {
    const value = {
      ...event,
      sequence: this.sequence++,
      version: HOST_PROTOCOL_VERSION,
    } as HostEvent;
    for (const listener of this.listeners) {
      listener(value);
    }
  }

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve(
      command.command === "clusterDetails.export"
        ? {
            command: command.command,
            id: command.id,
            ok: true,
            result: {
              correlationId: `cluster-${command.id}`,
              document: exportDocument,
            },
            version: HOST_PROTOCOL_VERSION,
          }
        : {
            command: command.command,
            id: command.id,
            ok: true,
            result: { correlationId: `cluster-${command.id}` },
            version: HOST_PROTOCOL_VERSION,
          },
    );
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

const backend = new ClusterWorkbenchBackend();
let launch: RunningWebDevelopment | undefined;

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

function publishSnapshot(snapshot: KafkaClusterDiagnosticsSnapshot): void {
  backend.emit({
    event: "clusterDetails.changed",
    payload: snapshot,
  });
}

test.describe("StreamSkope cluster-details browser workflow", () => {
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

  test("keeps every diagnostic state bounded, accessible, explicit and non-polling", async ({
    context,
    page,
  }, testInfo) => {
    test.setTimeout(75_000);
    if (launch === undefined) {
      throw new Error("Web development launch is unavailable.");
    }
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await context.grantPermissions(["clipboard-read", "clipboard-write"], {
      origin: new URL(launch.browserUrl).origin,
    });
    await page.goto(launch.browserUrl);
    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [
          {
            active: true,
            brokers: profile.brokers,
            createdAt: "2026-07-25T12:00:00.000Z",
            id: profile.id,
            name: profile.name,
            trust: {
              kind: "pem",
              label: "fixture-ca.pem",
              materialPresent: true,
              passwordPresent: false,
            },
            updatedAt: "2026-07-25T12:00:00.000Z",
          },
        ],
        store: {
          durability: "session",
          protection: "memory",
          state: "ready",
        },
      },
    });
    backend.emit({
      event: "connection.state",
      payload: { connectionName: profile.name, state: "connected" },
    });

    await openWorkbenchResource(page, "Connection Profiles");
    const profileActionsButton = page.getByRole("button", {
      name: "More actions for profile Local validation",
    });
    const openButton = (await openProfileActions(page, "Local validation")).getByRole("menuitem", {
      name: "Cluster detail",
    });
    await expect(openButton).toBeEnabled();
    const loadCountBefore = backend.commands.filter(
      (command) => command.command === "clusterDetails.load",
    ).length;
    await openButton.click();
    await expect
      .poll(
        () =>
          backend.commands.filter((command) => command.command === "clusterDetails.load").length,
      )
      .toBe(loadCountBefore + 1);
    publishSnapshot({ ...document, state: "ready" });

    const dialog = page.getByRole("dialog", {
      name: "Cluster details — Local validation",
    });
    await expect(dialog).toContainText("fixture-cluster");
    await expect(dialog).toContainText("Broker 1");
    await expect(dialog.getByRole("table", { name: "Kafka cluster brokers" })).toBeVisible();
    const grid = dialog.getByRole("grid", { name: "Broker configuration entries" });
    await expect(grid.getByRole("gridcell", { name: "••••" })).toBeVisible();
    const bounds = await dialog.boundingBox();
    expect(bounds).not.toBeNull();
    expect((bounds?.x ?? -1) >= 0).toBe(true);
    expect((bounds?.y ?? -1) >= 0).toBe(true);
    expect((bounds?.x ?? 0) + (bounds?.width ?? 0)).toBeLessThanOrEqual(1000);
    expect((bounds?.y ?? 0) + (bounds?.height ?? 0)).toBeLessThanOrEqual(650);
    expect(
      await page.evaluate(
        () => globalThis.document.documentElement.scrollWidth <= globalThis.innerWidth,
      ),
    ).toBe(true);

    const filter = dialog.getByRole("searchbox", { name: "Filter broker configuration" });
    await filter.fill("ssl");
    await expect(grid.getByRole("gridcell", { name: "ssl.keystore.password" })).toBeVisible();
    await expect(grid.getByRole("gridcell", { name: "num.partitions" })).toHaveCount(0);
    expect(
      backend.commands.filter((command) => command.command === "clusterDetails.load"),
    ).toHaveLength(loadCountBefore + 1);

    const refresh = dialog.getByRole("button", { name: "Refresh cluster details" });
    await refresh.focus();
    await page.keyboard.press("Tab");
    await expect(dialog.getByRole("button", { name: "Copy cluster details JSON" })).toBeFocused();
    await page.keyboard.press("Tab");
    await expect(dialog.getByRole("button", { name: "Close" })).toBeFocused();
    await page.keyboard.press("Tab");
    await expect(
      dialog.getByRole("button", { name: "Download cluster details JSON" }),
    ).toBeFocused();

    await dialog.getByRole("button", { name: "Copy cluster details JSON" }).click();
    await expect(dialog.getByRole("status")).toContainText("Cluster JSON copied.");
    await expect
      .poll(async () => page.evaluate(() => navigator.clipboard.readText()))
      .toBe(exportContent);

    const downloadStarted = page.waitForEvent("download");
    await dialog.getByRole("button", { name: "Download cluster details JSON" }).click();
    const download = await downloadStarted;
    expect(download.suggestedFilename()).toBe(exportDocument.fileName);
    const downloadPath = await download.path();
    if (downloadPath === null) {
      throw new Error("The browser did not retain the downloaded cluster JSON.");
    }
    await expect(readFile(downloadPath, "utf8")).resolves.toBe(exportContent);

    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("cluster-details-ready-light.png"),
    });
    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("cluster-details-ready-dark.png"),
    });

    publishSnapshot({
      ...document,
      cluster: {
        ...document.cluster,
        configuration: [],
        configurationIssue: {
          code: "authorization-denied",
          recovery: "Request DESCRIBE_CONFIGS.",
          summary: "Broker configuration is not permitted for this connection.",
        },
      },
      state: "partial",
    });
    await expect(dialog.getByRole("alert")).toContainText("Broker configuration is not permitted");
    await expect(
      dialog.getByRole("button", { name: "Download cluster details JSON" }),
    ).toBeEnabled();

    const timeout = {
      activeStateChanged: false,
      code: "TIMEOUT",
      correlationId: "cluster-timeout",
      recovery: "Retry cluster details.",
      retryable: true,
      stage: "broker",
      summary: "Kafka broker metadata access timed out.",
    } as const;
    publishSnapshot({ ...document, error: timeout, state: "stale" });
    await expect(dialog.getByRole("alert")).toContainText("Displayed cluster data is stale");
    await expect(
      dialog.getByRole("button", { name: "Download cluster details JSON" }),
    ).toBeDisabled();

    publishSnapshot({
      cluster: null,
      endpoint: document.endpoint,
      error: timeout,
      fetchedAt: null,
      profile,
      state: "failed",
    });
    await expect(dialog.getByRole("alert")).toContainText(
      "Kafka broker metadata access timed out.",
    );
    await expect(dialog).not.toContainText("fixture-cluster");

    publishSnapshot({
      cluster: null,
      endpoint: document.endpoint,
      fetchedAt: null,
      profile,
      state: "loading",
    });
    await expect(dialog.getByRole("alert")).toContainText("Loading fresh cluster metadata");
    await expect(refresh).toBeDisabled();

    publishSnapshot({ ...document, state: "ready" });
    await expect(dialog).toContainText("fixture-cluster");
    await expect(refresh).toBeEnabled();
    await refresh.click();
    await expect
      .poll(
        () =>
          backend.commands.filter((command) => command.command === "clusterDetails.load").length,
      )
      .toBe(loadCountBefore + 2);
    publishSnapshot({ ...document, state: "ready" });
    await expect(refresh).toBeEnabled();

    await refresh.focus();
    await page.keyboard.press("Escape");
    await expect(dialog).toBeHidden();
    await expect(profileActionsButton).toBeFocused();
  });
});
