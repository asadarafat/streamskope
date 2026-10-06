import { createServer } from "node:net";
import { resolve } from "node:path";

import { Producer } from "@platformatic/kafka";
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { launchProductWebFixture } from "../support/product-web-fixture";
import { ConnectHttpAdapter, ConnectHttpError } from "../../src/features/kafka/engine/connect-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
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

test.use({ actionTimeout: 10_000, trace: "off", viewport: { width: 1600, height: 1100 } });
test("observes a real rising-lag fixture, backtests its forecast, stops polling and discovers bounded lineage", async ({
  page,
}, testInfo) => {
  test.setTimeout(300_000);
  const fixture = await startAuthorizationFixture();
  const worker = await startConnectFixture(fixture.connection.brokers[0]!).catch(
    async (error: unknown) => {
      await fixture.dispose();
      throw error;
    },
  );
  const topic = "browser-observations",
    groupId = "browser-observed-group",
    connector = "browser-lineage-sink";
  const backend = createKafkaBackend(),
    diagnostics = observeBrowserDiagnostics(page);
  const producer = new Producer({
    clientId: "observation-browser-fixture",
    bootstrapBrokers: [...fixture.connection.brokers],
    retries: 0,
    autocreateTopics: false,
  });
  const connect = new ConnectHttpAdapter(new NodeBoundedJsonHttp()),
    context = {
      baseUrl: worker.url,
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
    };
  let launch: Awaited<ReturnType<typeof launchProductWebFixture>> | undefined;
  try {
    await fixture.admin.createTopics({ topics: [{ topic, partitions: 2, replicas: 1 }] });
    await expect
      .poll(
        async (): Promise<boolean> => {
          try {
            await fixture.admin.alterConsumerGroupOffsets({
              groupId,
              topics: [
                {
                  name: topic,
                  partitionOffsets: [
                    { partition: 0, offset: 0n },
                    { partition: 1, offset: 0n },
                  ],
                },
              ],
            });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15000 },
      )
      .toBe(true);
    await producer.send({
      messages: Array.from({ length: 20 }, (_, i) => ({
        topic,
        partition: i % 2,
        key: Buffer.from("fixture-key"),
        value: Buffer.from('{"id":42}'),
      })),
    });
    await connect.apply(
      context,
      {
        name: connector,
        action: "create",
        config: {
          "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
          "tasks.max": "1",
          topics: topic,
          file: "/tmp/browser-lineage.txt",
        },
      },
      AbortSignal.timeout(15000),
    );
    await expect
      .poll(
        async () => {
          try {
            return (await connect.load(context, connector, AbortSignal.timeout(10000)))?.detail
              .tasks[0]?.state;
          } catch (error) {
            // Creation is asynchronous; config can exist before the status record.
            if (error instanceof ConnectHttpError && error.status === 404) return "PENDING";
            throw error;
          }
        },
        { timeout: 45000 },
      )
      .toBe("RUNNING");
    launch = await launchProductWebFixture({
      backend,
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible();
    expect(
      (
        await backend.execute({
          command: "profiles.create",
          id: "observation-profile",
          version: HOST_PROTOCOL_VERSION,
          payload: {
            profile: {
              name: "Observation fixture",
              transport: "plaintext",
              brokers: fixture.connection.brokers,
              services: { connect: { baseUrl: worker.url, authentication: "none" } },
            },
          },
        })
      ).ok,
    ).toBe(true);
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Observation fixture" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    await navigation.getByRole("button", { name: "Observed health", exact: true }).click();
    const health = page.getByRole("main", { name: "Observed health page" });
    await health.getByLabel("Observed topic", { exact: true }).fill(topic);
    await health.getByLabel("Observed consumer group (optional)").fill(groupId);
    await health.getByRole("button", { name: "History and collection settings" }).click();
    await health.getByLabel("Lag alert threshold (optional)").fill("15");
    await health
      .getByRole("checkbox", { name: "Sample records for size and key distribution" })
      .check();
    await health.getByRole("button", { name: "Start observing" }).click();
    await expect(health).toContainText("1 samples in the recent continuous segment", {
      timeout: 20000,
    });
    for (let i = 1; i < 9; i++) {
      await producer.send({
        messages: Array.from({ length: 20 }, () => ({
          topic,
          partition: 0,
          key: Buffer.from("fixture-key"),
          value: Buffer.from('{"id":42}'),
          // Controlled CreateTime keeps the next bounded window deterministic while
          // real offset growth and the ten-second polling cadence remain under test.
          timestamp: BigInt(Date.now() + 1_000),
        })),
      });
      await expect(health).toContainText(`${i + 1} samples in the recent continuous segment`, {
        timeout: 20000,
      });
    }
    await health.getByRole("button", { name: "Stop observing" }).click();
    await health.getByRole("button", { name: "Analysis details", exact: true }).click();
    await expect(health).toContainText("Projected lag:");
    await expect(health).toContainText("Skew suspected");
    await expect(health).toContainText("Hot key suspected");
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Observed health page"]').analyze())
        .violations,
    ).toEqual([]);
    await health
      .getByRole("heading", { name: "Explain these observations" })
      .scrollIntoViewIfNeeded();
    await page.screenshot({
      path: testInfo.outputPath("observation-analysis.png"),
      animations: "disabled",
    });
    const before = await backend.execute({
      command: "observations.history",
      id: "before-stop",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    if (!before.ok) throw new Error("No history");
    const samples = before.result.snapshot.series[0]!.samples;
    expect(samples).toHaveLength(9);
    expect(samples.map((s) => s.partitions.reduce((sum, p) => sum + Number(p.lag), 0))).toEqual([
      20, 40, 60, 80, 100, 120, 140, 160, 180,
    ]);
    await testInfo.attach("observed-lag-evidence", {
      body: Buffer.from(JSON.stringify(before.result.snapshot)),
      contentType: "application/json",
    });
    await navigation.getByRole("button", { name: "Relationships", exact: true }).click();
    const relations = page.getByRole("main", { name: "Relationships page" });
    await relations.getByLabel("Topics (up to three, comma-separated)").fill(topic);
    await relations.getByRole("button", { name: "Discover relationships" }).click();
    await expect(relations.getByRole("table", { name: "Relationship evidence" })).toContainText(
      connector,
      { timeout: 15000 },
    );
    await expect(relations.getByRole("table", { name: "Relationship evidence" })).toContainText(
      groupId,
    );
    await expect(relations.getByRole("table", { name: "Relationship coverage" })).toContainText(
      "not-configured",
    );
    await relations.getByRole("button", { name: `Show relationships for ${groupId}` }).click();
    await expect(relations.getByRole("table", { name: "Relationship evidence" })).not.toContainText(
      connector,
    );
    await relations.getByRole("button", { name: "Show all relationships" }).click();
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Relationships page"]').analyze())
        .violations,
    ).toEqual([]);
    await relations.getByRole("heading", { name: "Observed lineage" }).scrollIntoViewIfNeeded();
    await page.screenshot({
      path: testInfo.outputPath("relationship-lineage.png"),
      animations: "disabled",
    });
    await page.waitForTimeout(11_000);
    const after = await backend.execute({
      command: "observations.history",
      id: "after-stop",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    expect(after.ok && after.result.snapshot.series[0]?.samples.length).toBe(9);
    expect(diagnostics.problems).toEqual([]);
  } finally {
    await testInfo.attach("browser-diagnostics", {
      body: Buffer.from(JSON.stringify(diagnostics)),
      contentType: "application/json",
    });
    await launch?.close();
    await backend.shutdown();
    await producer.close();
    await worker.dispose();
    await fixture.dispose();
  }
});
