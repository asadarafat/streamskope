import { createServer } from "node:net";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { launchProductWebFixture } from "../support/product-web-fixture";
import { startPrometheusFixture } from "../support/prometheus-fixture";
import { observeBrowserDiagnostics } from "../support/workbench-browser";

async function port(): Promise<number> {
  const server = createServer();
  await new Promise<void>((ready) => server.listen(0, "127.0.0.1", ready));
  const address = server.address();
  await new Promise<void>((closed) => server.close(() => closed()));
  if (!address || typeof address === "string") throw new Error("Browser fixture port unavailable.");
  return address.port;
}
test.use({ actionTimeout: 10_000, trace: "off", viewport: { width: 1440, height: 1000 } });
test("reads real optional metrics with honest source coverage and no automatic exporter reads across navigation or reconnect", async ({
  page,
}, testInfo) => {
  test.setTimeout(180_000);
  const fixture = await startPrometheusFixture(),
    backend = createKafkaBackend();
  const diagnostics = observeBrowserDiagnostics(page),
    commands: string[] = [];
  const rejectedRequests: { command: string; status: number }[] = [];
  const metricsResponses: unknown[] = [],
    responseReads: Promise<void>[] = [];
  page.on("response", (response): void => {
    if (!new URL(response.url()).pathname.endsWith("/commands")) return;
    const body = response.request().postDataJSON() as { command?: unknown } | null;
    if (response.status() >= 400 && typeof body?.command === "string")
      rejectedRequests.push({ command: body.command, status: response.status() });
    if (body?.command === "observations.metrics.capture") {
      responseReads.push(
        (async (): Promise<void> => {
          try {
            const result = parseHostCommandResponse(await response.json());
            if (!result.ok) metricsResponses.push({ ok: false, code: result.error.code });
            else if (result.command === "observations.metrics.capture") {
              const snapshot = result.result.snapshot;
              metricsResponses.push({ ok: true, snapshot });
            }
          } catch {
            metricsResponses.push({ parsed: false, status: response.status() });
          }
        })(),
      );
    }
  });
  page.on("request", (request) => {
    if (request.method() === "POST" && new URL(request.url()).pathname.endsWith("/commands")) {
      const body = request.postDataJSON() as { command?: unknown } | null;
      if (typeof body?.command === "string") commands.push(body.command);
    }
  });
  let launch: Awaited<ReturnType<typeof launchProductWebFixture>> | undefined;
  try {
    // These JMX gauges are registered when a real user topic initializes partition metadata.
    await fixture.broker.admin.createTopics({
      topics: ["browser-metrics-replicated"],
      replicas: -1,
      partitions: -1,
      assignments: [{ partition: 0, brokers: [1, 2] }],
    });
    launch = await launchProductWebFixture({
      backend,
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    const created = await backend.execute({
      command: "profiles.create",
      id: "metrics-profile",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: {
          name: "Metrics fixture",
          transport: "plaintext",
          brokers: fixture.broker.connection.brokers,
          services: { metrics: { baseUrl: fixture.endpoint, authentication: "none" } },
        },
      },
    });
    expect(created.ok).toBe(true);
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Metrics fixture" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const nav = page.getByRole("navigation", { name: "StreamSkope resources" });
    await nav.getByRole("button", { name: "Observed health", exact: true }).click();
    const panel = page.getByRole("main", { name: "Observed health page" });
    await panel.getByRole("button", { name: "Prometheus metrics (optional)" }).click();
    await expect(panel).toContainText(fixture.endpoint);
    expect(commands.filter((c) => c === "observations.metrics.capture")).toHaveLength(0);
    await panel.getByLabel("Exporter job").fill(fixture.job);
    await panel.getByLabel("Exporter instance").fill(fixture.broker.metricsInstances[0]!);
    const table = panel.getByRole("table", { name: "External Kafka metrics" });
    await expect(async () => {
      await panel.getByRole("button", { name: "Read metrics", exact: true }).click();
      await expect(table.getByRole("cell", { name: /^Captured\b/u })).toHaveCount(2);
    }).toPass({ timeout: 20_000, intervals: [1000] });
    await expect(table.getByRole("cell", { name: "0", exact: true })).toHaveCount(2);
    await expect(panel).toContainText("identity is not bound to your connected cluster");
    await expect(panel.getByText(/^Read two Kafka JMX partition-count gauges/u)).toBeInViewport({
      ratio: 1,
    });
    await expect(panel.getByText(`Endpoint: ${fixture.endpoint}`, { exact: true })).toBeInViewport({
      ratio: 1,
    });
    await page.screenshot({ path: testInfo.outputPath("external-metrics.png"), fullPage: true });
    await testInfo.attach("external-metrics", {
      path: testInfo.outputPath("external-metrics.png"),
      contentType: "image/png",
    });
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await panel.getByLabel("Exporter instance").fill("unconfigured-exporter:9404");
    await panel.getByRole("button", { name: "Read metrics", exact: true }).click();
    await expect(table.getByText("Missing series", { exact: true })).toHaveCount(2);
    await expect(table.getByRole("cell", { name: "Unknown", exact: true })).toHaveCount(2);
    const attempts = commands.filter((c) => c === "observations.metrics.capture").length;
    await nav.getByRole("button", { name: "Topics", exact: true }).click();
    await nav.getByRole("button", { name: "Observed health", exact: true }).click();
    await panel.getByRole("button", { name: "Prometheus metrics (optional)" }).click();
    await expect(table.getByText("Not read", { exact: true })).toHaveCount(2);
    const disconnected = await backend.execute({
      command: "connection.disconnect",
      id: "metrics-disconnect",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    expect(disconnected.ok).toBe(true);
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    expect(commands.filter((c) => c === "observations.metrics.capture")).toHaveLength(attempts);
    expect(diagnostics.problems).toEqual([]);
  } finally {
    await Promise.all(responseReads);
    const panelText = await page
      .getByRole("main", { name: "Observed health page" })
      .innerText()
      .catch(() => "Observed health is not mounted.");
    await testInfo.attach("metrics-browser-diagnostics", {
      body: JSON.stringify({
        problems: diagnostics.problems,
        rejectedRequests,
        commands,
        metricsResponses,
        panelText,
      }),
      contentType: "application/json",
    });
    try {
      await backend.shutdown();
    } finally {
      try {
        await launch?.close();
      } finally {
        await fixture.dispose();
      }
    }
  }
});
