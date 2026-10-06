import { createServer } from "node:net";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { launchProductWebFixture } from "../support/product-web-fixture";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { startConnectFixture } from "../support/connect-fixture";
import { observeBrowserDiagnostics } from "../support/workbench-browser";

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

test.use({ actionTimeout: 10_000, trace: "off", viewport: { width: 1440, height: 1000 } });
test("reviews real Connect creation and a snapshot promotion without writing before confirmation", async ({
  page,
}, testInfo) => {
  test.setTimeout(240_000);
  const fixture = await startAuthorizationFixture();
  const worker = await startConnectFixture(fixture.connection.brokers[0]!).catch(
    async (error: unknown) => {
      await fixture.dispose();
      throw error;
    },
  );
  const topic = "browser-integrations";
  const name = "browser-owned-sink";
  const backend = createKafkaBackend();
  const diagnostics = observeBrowserDiagnostics(page);
  let launch: Awaited<ReturnType<typeof launchProductWebFixture>> | undefined;
  try {
    await fixture.admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    launch = await launchProductWebFixture({
      backend,
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible();
    const created = await backend.execute({
      command: "profiles.create",
      id: "owned-integration-profile",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: {
          name: "Owned integrations",
          transport: "plaintext",
          brokers: fixture.connection.brokers,
          services: { connect: { baseUrl: worker.url, authentication: "none" } },
        },
      },
    });
    expect(created.ok).toBe(true);
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Owned integrations" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    await navigation.getByRole("button", { name: "Kafka Connect", exact: true }).click();
    const connect = page.getByRole("main", { name: "Kafka Connect page" });
    await expect(connect).toContainText("org.apache.kafka.connect.file.FileStreamSinkConnector");
    await connect.getByRole("textbox", { name: "Connector name", exact: true }).fill(name);
    await connect
      .getByRole("textbox", { name: "Connector configuration (JSON string map)", exact: true })
      .fill(
        JSON.stringify({
          "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
          "tasks.max": "1",
          topics: topic,
          file: "/tmp/browser-owned-sink.txt",
        }),
      );
    await connect.getByRole("button", { name: "Validate configuration" }).click();
    await expect(connect).toContainText("Validation passed. No connector change was made.");
    expect(await (await fetch(`${worker.url}/connectors`)).json()).toEqual([]);
    await connect.getByRole("button", { name: "Review action" }).click();
    const createConfirmation = `create ${name}`;
    const createInput = connect.getByRole("textbox", {
      name: `Type ${createConfirmation} to confirm`,
    });
    await expect(createInput).toBeVisible();
    const applyConnect = connect.getByRole("button", { name: "Apply reviewed action" });
    await expect(applyConnect).toBeDisabled();
    expect(await (await fetch(`${worker.url}/connectors`)).json()).toEqual([]);
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Kafka Connect page"]').analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("connect-review.png"),
      animations: "disabled",
    });
    await createInput.fill(createConfirmation);
    await applyConnect.click();
    await expect(connect).toContainText("acknowledged:");
    await expect(applyConnect).toBeDisabled();
    await expect
      .poll(async (): Promise<unknown> => (await fetch(`${worker.url}/connectors`)).json(), {
        timeout: 15_000,
      })
      .toEqual([name]);

    const capture = await backend.execute({
      command: "environments.capture",
      id: "owned-browser-snapshot",
      version: HOST_PROTOCOL_VERSION,
      payload: { topics: [topic], profile: null },
    });
    if (!capture.ok) throw new Error("Could not capture owned fixture.");
    const baseline = capture.result.snapshot;
    const intent = {
      ...baseline,
      topics: baseline.topics.map((t) => ({
        ...t,
        configs: t.configs.map((c) => (c.key === "retention.ms" ? { ...c, value: "3600000" } : c)),
      })),
    };
    expect(baseline.topics[0]?.configs.find((c) => c.key === "retention.ms")?.value).not.toBe(
      "3600000",
    );
    await navigation.getByRole("button", { name: "Compare environments", exact: true }).click();
    const comparison = page.getByRole("main", { name: "Environment comparison page" });
    await comparison
      .getByRole("textbox", { name: "Import source snapshot JSON" })
      .fill(JSON.stringify(intent));
    await comparison.getByRole("button", { name: "Use imported source" }).click();
    await comparison.getByRole("button", { name: "Capture destination and compare" }).click();
    await expect(comparison.getByRole("table", { name: "Environment differences" })).toContainText(
      "retention.ms",
    );
    await comparison.getByRole("checkbox", { name: `Promote ${topic} retention.ms` }).check();
    await comparison.getByRole("button", { name: "Review selected promotion" }).click();
    const promotionConfirmation = `promote 1 settings to ${baseline.clusterId}`;
    const promotionInput = comparison.getByRole("textbox", {
      name: `Type ${promotionConfirmation} to confirm`,
    });
    await expect(promotionInput).toBeVisible();
    const applyPromotion = comparison.getByRole("button", { name: "Apply reviewed promotion" });
    await expect(applyPromotion).toBeDisabled();
    const before = await backend.execute({
      command: "environments.capture",
      id: "owned-browser-before-apply",
      version: HOST_PROTOCOL_VERSION,
      payload: { topics: [topic], profile: null },
    });
    if (!before.ok) throw new Error("Could not verify owned fixture.");
    expect(before.result.snapshot.topics).toEqual(baseline.topics);
    expect(
      (
        await new AxeBuilder({ page })
          .include('[aria-label="Environment comparison page"]')
          .analyze()
      ).violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("environment-review.png"),
      animations: "disabled",
    });
    await promotionInput.fill(promotionConfirmation);
    await applyPromotion.click();
    await expect(comparison).toContainText(`${topic}: acknowledged; read-back verified`);
    await expect(applyPromotion).toBeDisabled();
    const after = await backend.execute({
      command: "environments.capture",
      id: "owned-browser-after-apply",
      version: HOST_PROTOCOL_VERSION,
      payload: { topics: [topic], profile: null },
    });
    if (!after.ok) throw new Error("Could not verify promoted fixture.");
    expect(after.result.snapshot.topics).toEqual(intent.topics);
    expect(diagnostics.problems).toEqual([]);
  } finally {
    await testInfo.attach("browser-diagnostics", {
      body: Buffer.from(JSON.stringify(diagnostics, null, 2)),
      contentType: "application/json",
    });
    await launch?.close();
    await backend.shutdown();
    await worker.dispose();
    await fixture.dispose();
  }
});
