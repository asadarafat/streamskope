import { createServer } from "node:net";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { launchWebDevelopment } from "../../src/platform/dev-host";
import { provisionSeededFixtureTopic } from "../support/kafka-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { fetchTopicMessages, observeBrowserDiagnostics } from "../support/workbench-browser";

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
  test.setTimeout(120_000);
  const fixture = await provisionSeededFixtureTopic();
  const backend = createKafkaBackend();
  const launch = await launchWebDevelopment({
    backend,
    hostPort: await port(),
    rendererPort: await port(),
    rendererRoot: resolve(process.cwd()),
  });
  try {
    const diagnostics = observeBrowserDiagnostics(page);
    await page.goto(launch.browserUrl);
    await connectLocalProfile(page);
    await fetchTopicMessages(page, fixture.config.topic);
    const grid = page.getByRole("grid", { name: "Kafka messages" });
    await grid.getByText(fixture.config.seedPayload, { exact: true }).first().click();
    const inspector = page.getByRole("complementary", { name: "Message inspector" });
    await inspector.getByRole("tab", { name: "Decoded", exact: true }).click();
    await inspector.getByRole("button", { name: "Decode record" }).click();
    await expect(inspector.getByLabel("Decoded JSON")).toBeVisible();
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
    await page.getByRole("option", { name: "Key (UTF-8)" }).click();
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
    await launch.close();
    await fixture.dispose();
  }
});
