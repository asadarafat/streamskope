import { createServer, type Server } from "node:http";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeBackend,
} from "../../src/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";

class StreamMonitorBackend implements StreamSkopeBackend {
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
      result: { correlationId: `monitor-${command.id}` },
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

const backend = new StreamMonitorBackend();
let launch: RunningWebDevelopment | undefined;

function activeLaunch(): RunningWebDevelopment {
  if (launch === undefined) {
    throw new Error("Stream Monitor web launch is not ready.");
  }
  return launch;
}

test.describe("StreamSkope browser Stream Monitor", () => {
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

  test("keeps Stream Monitor keyboard-operable, accessible and bounded at minimum size", async ({
    page,
  }, testInfo) => {
    test.setTimeout(45_000);
    await page.setViewportSize({ height: 650, width: 1000 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await page.goto(activeLaunch().browserUrl);
    await expect(page.getByRole("banner", { name: "StreamSkope application bar" })).toContainText(
      "StreamSkope",
    );
    await expect
      .poll(() => {
        const commands = new Set(backend.commands.map((candidate) => candidate.command));
        return (
          commands.has("profiles.list") &&
          commands.has("rules.list") &&
          commands.has("templates.list")
        );
      })
      .toBe(true);

    backend.emit({
      event: "connection.state",
      payload: { connectionName: "Monitor fixture", state: "connected" },
    });
    await expect
      .poll(() => backend.commands.some((candidate) => candidate.command === "topics.list"))
      .toBe(true);
    backend.emit({
      event: "topics.changed",
      payload: {
        refreshedAt: "2026-07-26T12:00:00.000Z",
        state: "ready",
        topics: ["orders"],
      },
    });
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
          applicableRules: 0,
          omittedRules: 0,
          state: "ready",
        },
        state: "streaming",
      },
    });
    const queueDepths = [0, 4, 2, 8, 3, 10, 4, 1];
    const deliveryRates = [20, 34, 28, 51, 37, 62, 46, 55];
    const emitStreamSample = (sample: number, currentMessages: number): void => {
      backend.emit({
        event: "streamMetrics.changed",
        payload: {
          connectionName: "Monitor fixture",
          delivery: {
            batchCount: sample + 1,
            batchSize: 200,
            deliveredMessages: (sample + 1) * 2,
            historySamples: 50,
            intervalMs: 20,
            lastBatchMessages: 2,
            messagesPerSecond: deliveryRates[sample] ?? 0,
            publicationDurationMs: 0.55 + sample * 0.08,
            queueWaitMs: 0.9 + currentMessages * 0.12,
            receivedMessages: (sample + 1) * 2 + currentMessages,
            tuningSource: "confirmed",
          },
          queue: {
            capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
            capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
            currentBytes: currentMessages * 128,
            currentMessages,
            droppedMessages: 0,
            droppedPerSecond: 0,
            droppedSincePrevious: 0,
            peakBytes: 1_280,
            peakMessages: 10,
          },
          request: {
            maxMessages: 1_000,
            mode: "tail",
            topic: "orders",
          },
          sampledAt: `2026-07-26T12:00:${String(sample + 1).padStart(2, "0")}.000Z`,
          state: "streaming",
          status: "nominal",
        },
      });
    };

    const resourceNavigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    const navigation = await resourceNavigation.boundingBox();
    if (navigation === null) {
      throw new Error("Kafka resource navigation has no visible bounds.");
    }
    for (const name of ["Connection Profiles", "Topics"]) {
      const modeBounds = await resourceNavigation.getByRole("button", { name }).boundingBox();
      if (modeBounds === null) {
        throw new Error(`${String(name)} resource mode has no visible bounds.`);
      }
      expect(modeBounds.x).toBeGreaterThanOrEqual(navigation.x);
      expect(modeBounds.x + modeBounds.width).toBeLessThanOrEqual(navigation.x + navigation.width);
    }
    const topic = page.getByRole("button", { name: "orders", exact: true });
    await topic.focus();
    await page.keyboard.press("Enter");
    const commandCount = backend.commands.length;
    const openMonitor = page
      .getByRole("tablist", { name: "Topic sections" })
      .getByRole("tab", { name: "Monitor" });
    await openMonitor.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("heading", { name: "Stream Monitor" })).toBeVisible();
    for (const [sample, currentMessages] of queueDepths.entries()) {
      emitStreamSample(sample, currentMessages);
      await expect(page.getByRole("region", { name: /^Queue depth trend\./u })).toHaveAttribute(
        "aria-label",
        new RegExp(`Queue: ${String(sample + 1)} ${sample === 0 ? "sample" : "samples"}`, "u"),
      );
    }
    const queuePlot = page.getByRole("group", { name: "Queue depth trend plot" });
    await expect(queuePlot).toHaveAttribute("tabindex", "0");
    expect(await queuePlot.locator('[tabindex="0"]').count()).toBe(0);
    await queuePlot.focus();
    await page.keyboard.press("End");
    await expect(
      page.getByRole("region", { name: /^Queue depth trend\./u }).getByRole("status"),
    ).toContainText("Queue at");
    await page.keyboard.press("Tab");
    await expect(queuePlot).not.toBeFocused();
    for (const sectionName of [
      "Context",
      "Host queue",
      "Delivery",
      "Renderer",
      "Recent host samples",
      "Recent renderer samples",
    ]) {
      const section = page.getByRole("region", { exact: true, name: sectionName });
      const headingTextStart = await section
        .getByRole("heading", { name: sectionName })
        .evaluate(
          (element) =>
            element.getBoundingClientRect().x + parseFloat(getComputedStyle(element).paddingLeft),
        );
      const childTextStart = await section
        .locator("dt, th")
        .first()
        .evaluate(
          (element) =>
            element.getBoundingClientRect().x + parseFloat(getComputedStyle(element).paddingLeft),
        );
      expect(childTextStart).toBe(headingTextStart);
    }
    const monitorStatus = page.getByRole("status", { name: "Stream monitor status" });
    await expect
      .poll(async () => monitorStatus.textContent())
      .toMatch(/^(?:Backpressure|Nominal)$/u);
    if ((await monitorStatus.textContent()) === "Backpressure") {
      await expect(page.getByRole("region", { name: "Stream monitor", exact: true })).toContainText(
        /(?:Host-event-to-commit|Message filtering|Message workspace render|Renderer)/u,
      );
    }
    await expect(page.getByRole("region", { name: "Message workspace" })).toHaveCount(0);
    expect(backend.commands).toHaveLength(commandCount);

    await expect(page.getByRole("button", { name: /Stop tail/u })).toHaveCount(0);
    await expect(
      page
        .getByRole("region", { name: "Stream Monitor workspace" })
        .getByRole("button", { name: /Stop/u }),
    ).toHaveCount(0);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("stream-monitor-light.png"),
    });

    const messagesTab = page
      .getByRole("tablist", { name: "Topic sections" })
      .getByRole("tab", { name: "Messages" });
    await messagesTab.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("region", { name: "Message workspace" })).toBeVisible();
    let stop = page.getByRole("button", { name: /Stop tail/u });
    await expect(stop).toBeVisible();
    await expect(stop).toHaveText("Stop tail");
    const topicPageBounds = await page
      .getByRole("main", { name: "Topic detail page" })
      .boundingBox();
    const stopBounds = await stop.boundingBox();
    if (topicPageBounds === null || stopBounds === null) {
      throw new Error("The topic detail or message-owned stop action has no visible bounds.");
    }
    expect(stopBounds.x).toBeGreaterThanOrEqual(topicPageBounds.x);
    expect(stopBounds.x + stopBounds.width).toBeLessThanOrEqual(
      topicPageBounds.x + topicPageBounds.width,
    );
    await openMonitor.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("heading", { name: "Stream Monitor" })).toBeVisible();

    await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: testInfo.outputPath("stream-monitor-dark.png"),
    });

    const priorStops = backend.commands.filter(
      (candidate) => candidate.command === "messages.stop",
    ).length;
    await messagesTab.focus();
    await page.keyboard.press("Enter");
    stop = page.getByRole("button", { name: /Stop tail/u });
    await stop.focus();
    await page.keyboard.press("Enter");
    await expect
      .poll(
        () => backend.commands.filter((candidate) => candidate.command === "messages.stop").length,
      )
      .toBe(priorStops + 1);
  });
});
