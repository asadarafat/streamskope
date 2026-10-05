import { createServer } from "node:net";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { Producer } from "@platformatic/kafka";
import { expect, test, type Locator } from "@playwright/test";

import { HOST_PROTOCOL_VERSION, type HostCommand } from "../../src/features/kafka/contracts";
import { MemoryObservationStore } from "../../src/features/kafka/application/observation-store";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { launchWebDevelopment, type DevelopmentBackend } from "../../src/platform/dev-host";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { testHostExecute } from "../support/host-response";
import { observeBrowserDiagnostics } from "../support/workbench-browser";

async function port(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => (error ? reject(error) : resolveClose()));
  });
  if (address === null || typeof address === "string") throw new Error("No fixture port.");
  return address.port;
}

class RecoverableHistoryStore extends MemoryObservationStore {
  unavailable = false;
  override load(): ReturnType<MemoryObservationStore["load"]> {
    if (this.unavailable) return Promise.reject(new Error("Injected unavailable history."));
    return super.load();
  }
}

async function choose(input: Locator, value: string): Promise<void> {
  await input.fill(value);
  await input.press("ArrowDown");
  await input.press("Enter");
}

test.use({ actionTimeout: 10_000, viewport: { width: 1000, height: 700 } });
test("investigates real lag and sampled records, respects cooldown, and recovers without presenting retained evidence as current", async ({
  page,
}, testInfo) => {
  // Includes isolated broker startup and cold Vite compilation; only four captures,
  // rather than the separate nine-sample forecasting qualification, run here.
  test.setTimeout(240_000);
  const fixture = await startAuthorizationFixture();
  const store = new RecoverableHistoryStore();
  const real = createKafkaBackend({ observationStore: store });
  const commands: HostCommand[] = [];
  let failNextCapture = false;
  const backend: DevelopmentBackend = {
    execute: testHostExecute(async (command) => {
      commands.push(command);
      if (command.command === "observations.capture" && failNextCapture) {
        failNextCapture = false;
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: false,
          error: {
            code: "TIMEOUT",
            stage: "kafka",
            correlationId: "injected-observation-timeout",
            retryable: true,
            activeStateChanged: false,
            summary: "The observation deadline expired before collection completed.",
            recovery: "Check broker reachability and retry the observation.",
          },
        };
      }
      return real.execute(command);
    }),
    subscribe: (listener) => real.subscribe(listener),
    shutdown: () => real.shutdown(),
  };
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "observed-health-recovery-seed",
    retries: 0,
    autocreateTopics: false,
  });
  const diagnostics = observeBrowserDiagnostics(page);
  const topic = "browser-health-investigation",
    groupId = "browser-health-workers";
  let launch: Awaited<ReturnType<typeof launchWebDevelopment>> | undefined;
  let failure: { readonly cause: unknown } | undefined;
  try {
    await fixture.admin.createTopics({ topics: [{ topic, partitions: 2, replicas: 1 }] });
    await expect
      .poll(
        async () => {
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
        { timeout: 15_000 },
      )
      .toBe(true);
    launch = await launchWebDevelopment({
      backend,
      hostPort: await port(),
      rendererPort: await port(),
      rendererRoot: resolve(process.cwd()),
    });
    await page.goto(launch.browserUrl);
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible();
    const created = await backend.execute({
      command: "profiles.create",
      id: "health-recovery-profile",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: {
          name: "Health investigation fixture",
          transport: "plaintext",
          brokers: fixture.connection.brokers,
        },
      },
    });
    expect(created.ok).toBe(true);
    const connect = page.getByRole("button", {
      name: "Connect insecure plaintext profile Health investigation fixture",
    });
    await connect.click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    const openHealth = async (): Promise<Locator> => {
      await navigation.getByRole("button", { name: "Observed health", exact: true }).click();
      return page.getByRole("main", { name: "Observed health page" });
    };
    let health = await openHealth();
    await choose(health.getByRole("combobox", { name: "Observed topic", exact: true }), topic);
    await choose(
      health.getByRole("combobox", { name: "Observed consumer group (optional)" }),
      groupId,
    );
    await health.getByRole("button", { name: "History and collection settings" }).click();
    await health.getByLabel("Lag alert threshold (optional)").fill("15");
    await health.getByRole("button", { name: "History and collection settings" }).click();
    await health
      .getByRole("checkbox", { name: "Sample records for size and key distribution" })
      .check();
    const capture = (): Locator => health.getByRole("button", { name: "Capture observation" });
    // Seed only after cold renderer startup so the protected sixty-second sample
    // remains deterministic without extending its production sampling budget.
    await producer.send({
      messages: Array.from({ length: 40 }, (_, index) => ({
        topic,
        partition: index % 2,
        ...(index % 2 === 0 ? { key: Buffer.from("shared-investigation-key") } : {}),
        value: Buffer.from(`{"fixtureRecord":${index}}`),
      })),
    });
    await capture().click();
    await expect(health.getByRole("region", { name: "Observation summary" })).toContainText("40", {
      timeout: 20_000,
    });
    await expect(health.getByRole("region", { name: "Observation findings" })).toContainText(
      /lag.*15|15.*lag/iu,
    );
    await expect(capture()).toBeDisabled();
    const captureCount = commands.filter(
      (command) => command.command === "observations.capture",
    ).length;
    await capture().focus();
    await page.keyboard.press("Enter");
    expect(commands.filter((command) => command.command === "observations.capture")).toHaveLength(
      captureCount,
    );
    await expect(health.getByRole("table", { name: "Observed partition positions" })).toContainText(
      "20",
    );
    await health.getByRole("checkbox", { name: "Only partitions with gaps or lag" }).check();
    await expect(health.getByRole("table", { name: "Observed partition positions" })).toContainText(
      "20",
    );
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Observed health page"]').analyze())
        .violations,
    ).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await health.evaluate((element) => {
      element.scrollTop = 0;
    });
    await page.screenshot({
      path: testInfo.outputPath("observed-health-narrow.png"),
      animations: "disabled",
    });
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.screenshot({
      path: testInfo.outputPath("observed-health-desktop.png"),
      animations: "disabled",
    });
    await page.setViewportSize({ width: 1000, height: 700 });

    await health.getByRole("button", { name: "Analysis details", exact: true }).click();
    await health.getByRole("button", { name: "Find sampled record partition 0 offset 0" }).click();
    await expect(page.getByRole("main", { name: "Topic detail page" })).toContainText(topic);
    await expect
      .poll(() => {
        const read = commands.filter((command) => command.command === "messages.start").at(-1);
        return read?.payload;
      })
      .toMatchObject({
        topic,
        mode: "time-window",
        maxMessages: 1,
        search: {
          key: "",
          value: "",
          offset: "",
          timestamp: "",
          partition: 0,
          offsetExact: "0",
        },
      } satisfies Partial<Extract<HostCommand, { command: "messages.start" }>["payload"]>);
    const messageWorkspace = page.getByRole("region", { name: "Message workspace" });
    await expect(messageWorkspace).toContainText("Partition 0 · Offset 0", { timeout: 15_000 });
    await messageWorkspace.getByRole("tab", { name: "Value", exact: true }).click();
    const valueEvidence = messageWorkspace.getByRole("region", {
      name: "Message evidence content",
    });
    await expect(valueEvidence).toContainText(/"fixtureRecord"\s*:\s*0\b/u);
    await expect(valueEvidence).not.toContainText(/"fixtureRecord"\s*:\s*20\b/u);
    await expect(valueEvidence).not.toContainText(/"fixtureRecord"\s*:\s*1\b/u);

    health = await openHealth();
    await choose(health.getByRole("combobox", { name: "Observed topic", exact: true }), topic);
    await choose(
      health.getByRole("combobox", { name: "Observed consumer group (optional)" }),
      groupId,
    );
    await expect(
      health.getByRole("button", { name: `Inspect consumer group ${groupId}` }),
    ).toBeDisabled();
    await expect(capture()).toBeEnabled({ timeout: 15_000 });
    await capture().click();
    await expect(
      health.getByRole("button", { name: `Inspect consumer group ${groupId}` }),
    ).toBeEnabled({ timeout: 20_000 });
    await health.getByRole("button", { name: `Inspect consumer group ${groupId}` }).click();
    await expect(page.getByRole("main", { name: "Consumer group detail page" })).toContainText(
      groupId,
    );

    health = await openHealth();
    await choose(health.getByRole("combobox", { name: "Observed topic", exact: true }), topic);
    await choose(
      health.getByRole("combobox", { name: "Observed consumer group (optional)" }),
      groupId,
    );
    await expect(capture()).toBeEnabled({ timeout: 15_000 });
    failNextCapture = true;
    await capture().click();
    await expect(health.getByRole("alert").filter({ hasText: "deadline expired" })).toBeVisible();
    const retry = health.getByRole("button", { name: "Retry observation" });
    await expect(retry).toBeDisabled();
    await expect(retry).toBeEnabled({ timeout: 15_000 });
    await retry.click();
    await expect(health.getByRole("alert").filter({ hasText: "deadline expired" })).toHaveCount(0);
    await expect(health.getByRole("button", { name: `Inspect topic ${topic}` })).toBeEnabled({
      timeout: 20_000,
    });
    await health.getByRole("button", { name: `Inspect topic ${topic}` }).click();
    await expect(page.getByRole("main", { name: "Topic detail page" })).toContainText(topic);

    store.unavailable = true;
    health = await openHealth();
    await choose(health.getByRole("combobox", { name: "Observed topic", exact: true }), topic);
    await choose(
      health.getByRole("combobox", { name: "Observed consumer group (optional)" }),
      groupId,
    );
    await expect(
      health.getByRole("alert").filter({ hasText: /history.*unavailable|history.*read/iu }),
    ).toBeVisible();
    await expect(capture()).toBeDisabled();
    store.unavailable = false;
    await health.getByRole("button", { name: "Reload history", exact: true }).click();
    await expect(
      health.getByRole("alert").filter({ hasText: /history.*unavailable|history.*read/iu }),
    ).toHaveCount(0);
    await expect(health.getByRole("button", { name: `Inspect topic ${topic}` })).toBeDisabled();
    const profileConnect = commands.find((command) => command.command === "profiles.connect");
    if (!profileConnect || profileConnect.command !== "profiles.connect")
      throw new Error("The fixture did not connect a saved profile.");
    expect(
      (
        await backend.execute({
          command: "connection.disconnect",
          id: "health-recovery-disconnect",
          version: HOST_PROTOCOL_VERSION,
          payload: {},
        })
      ).ok,
    ).toBe(true);
    await expect(page.getByLabel("Connection status")).toContainText(/Disconnected|Idle/iu);
    await expect(capture()).toBeDisabled();
    const capturesBeforeReconnect = commands.filter(
      (command) => command.command === "observations.capture",
    ).length;
    expect(
      (
        await backend.execute({
          ...profileConnect,
          id: "health-recovery-reconnect",
        })
      ).ok,
    ).toBe(true);
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await expect(page.getByRole("main", { name: "Topics page", exact: true })).toBeVisible();
    health = await openHealth();
    await choose(health.getByRole("combobox", { name: "Observed topic", exact: true }), topic);
    await choose(
      health.getByRole("combobox", { name: "Observed consumer group (optional)" }),
      groupId,
    );
    await expect(health.getByRole("region", { name: "Observation summary" })).toContainText(
      "Retained evidence",
    );
    await expect(health.getByRole("button", { name: `Inspect topic ${topic}` })).toBeDisabled();
    await expect(
      health.getByRole("button", { name: `Inspect consumer group ${groupId}` }),
    ).toBeDisabled();
    await expect(capture()).toBeEnabled({ timeout: 15_000 });
    expect(commands.filter((command) => command.command === "observations.capture")).toHaveLength(
      capturesBeforeReconnect,
    );
    await capture().click();
    await expect(health.getByRole("button", { name: `Inspect topic ${topic}` })).toBeEnabled({
      timeout: 20_000,
    });
    const retained = await store.load();
    expect(retained.series[0]?.samples).toHaveLength(4);
    await store.commit({
      ...retained,
      series: retained.series.map((series) => ({
        ...series,
        samples: series.samples.map((sample) => ({
          ...sample,
          startedAt: sample.startedAt - 60_000,
          observedAt: sample.observedAt - 60_000,
          records: sample.records
            ? {
                ...sample.records,
                startTimeMs: sample.records.startTimeMs - 60_000,
                endTimeMs: sample.records.endTimeMs - 60_000,
              }
            : null,
        })),
      })),
    });
    await health.getByRole("button", { name: "History and collection settings" }).click();
    await health.getByRole("button", { name: "Reload retained history", exact: true }).click();
    await expect(health.getByRole("region", { name: "Observation summary" })).toContainText(
      "Stale evidence",
    );
    await expect(health.getByRole("button", { name: `Inspect topic ${topic}` })).toBeDisabled();
    await page.screenshot({
      path: testInfo.outputPath("observed-health-recovery.png"),
      animations: "disabled",
    });
    expect(diagnostics.problems).toEqual([]);
    await testInfo.attach("qualification-scope", {
      body: Buffer.from(
        JSON.stringify({
          realKafka: ["lag", "sampled-record-read", "topic-and-group-drilldowns"],
          injectedFailures: ["capture-timeout", "history-load-failure"],
          retainedSamples: retained.series[0]?.samples.length,
          captureCommands: commands.filter((command) => command.command === "observations.capture")
            .length,
        }),
      ),
      contentType: "application/json",
    });
  } catch (error) {
    failure = { cause: error };
  } finally {
    await testInfo.attach("browser-diagnostics", {
      body: Buffer.from(JSON.stringify(diagnostics)),
      contentType: "application/json",
    });
    // The development host owns backend shutdown; closing it and the backend
    // concurrently would double-close codec workers. Always dispose the broker,
    // including when development-host cleanup itself fails.
    const cleanup = await Promise.allSettled([
      launch === undefined ? real.shutdown() : launch.close(),
      producer.close(),
    ]);
    cleanup.push(...(await Promise.allSettled([fixture.dispose()])));
    const failures = cleanup.flatMap((result) =>
      result.status === "rejected" ? [result.reason as unknown] : [],
    );
    if (failures.length && failure === undefined)
      failure = {
        cause: new AggregateError(
          failures,
          "The observation browser fixture did not shut down cleanly.",
        ),
      };
  }
  if (failure !== undefined) throw failure.cause;
});
