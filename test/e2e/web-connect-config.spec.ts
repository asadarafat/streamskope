import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { build } from "vite";
import { Producer } from "@platformatic/kafka";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { startConnectFixture } from "../support/connect-fixture";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway, type WebGatewayRuntime } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import {
  observeBrowserDiagnostics,
  expectNoHorizontalOverflow,
} from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("keyboard-operable Connect set/remove review preserves secrets and keeps one actual receipt", async ({
  page,
}, info) => {
  test.setTimeout(240000);
  page.setDefaultTimeout(30000);
  const fixture = await startAuthorizationFixture(),
    name = `browser-connect-${randomUUID()}`;
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-connect-vault-")),
    rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-connect-"));
  let worker: Awaited<ReturnType<typeof startConnectFixture>> | undefined,
    gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined,
    runtime: WebGatewayRuntime | undefined;
  const commands: string[] = [],
    failures: unknown[] = [],
    diagnostics = observeBrowserDiagnostics(page),
    privateValue = `connect-private-${randomUUID()}`;
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const p = request.postDataJSON() as { command?: unknown } | null;
    if (typeof p?.command === "string") commands.push(p.command);
  });
  const actualConfig = async (): Promise<Record<string, string>> => {
    const r = await fetch(`${worker!.url}/connectors/${name}/config`);
    expect(r.ok).toBe(true);
    return (await r.json()) as Record<string, string>;
  };
  const actualOffset = async (): Promise<number | null> => {
    const response = await fetch(`${worker!.url}/connectors/${name}/offsets`);
    expect(response.ok).toBe(true);
    const body = (await response.json()) as {
      offsets: readonly { offset: { kafka_offset?: number } | null }[];
    };
    return body.offsets[0]?.offset?.kafka_offset ?? null;
  };
  try {
    await fixture.admin.createTopics({
      topics: ["connect-browser-input"],
      partitions: 1,
      replicas: 1,
    });
    worker = await startConnectFixture(fixture.connection.brokers[0]!);
    const response = await fetch(`${worker.url}/connectors`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name,
        config: {
          "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
          "tasks.max": "1",
          topics: "connect-browser-input",
          file: "/tmp/browser-connect-owned.txt",
          "fixture.protected.value": privateValue,
          "fixture.remove.value": "explicit-only",
        },
      }),
    });
    expect(response.ok).toBe(true);
    await expect
      .poll(
        async () => {
          const r = await fetch(`${worker!.url}/connectors/${name}/status`);
          if (r.status === 404) return null;
          expect(r.ok).toBe(true);
          const p = (await r.json()) as { tasks?: { state: string }[] };
          return p.tasks?.[0]?.state ?? null;
        },
        { timeout: 45000 },
      )
      .toBe("RUNNING");
    const producer = new Producer({
      bootstrapBrokers: [...fixture.connection.brokers],
      clientId: "browser-connect-offsets",
    });
    try {
      await producer.send({
        messages: [
          { topic: "connect-browser-input", value: Buffer.from("first") },
          { topic: "connect-browser-input", value: Buffer.from("second") },
        ],
      });
    } finally {
      await producer.close();
    }
    await expect.poll(actualOffset, { timeout: 30_000 }).toBe(2);
    await build({
      configFile: resolve("config/vite.config.ts"),
      logLevel: "silent",
      build: { outDir: rendererRoot },
    });
    gateway = await startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: async (value, mode): Promise<WebGatewayRuntime> => {
        runtime = await openBrowserRuntime(dataRoot, value, mode);
        return runtime;
      },
    });
    if (!gateway.setupCodePath) throw new Error("No setup code");
    await page.goto(gateway.origin);
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    const passphrase = `fixture-vault-${randomUUID()}`;
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible();
    expect(
      await runtime!.providers.get("kafka")!.dispatch({
        command: "profiles.create",
        id: "owned-connect-profile",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profile: {
            name: "Owned Connect fixture",
            transport: "plaintext",
            brokers: fixture.connection.brokers,
            services: { connect: { baseUrl: worker.url, authentication: "none" } },
          },
        },
      }),
    ).toMatchObject({ ok: true });
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Owned Connect fixture" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    await navigation.getByRole("button", { name: "Kafka Connect", exact: true }).click();
    const main = page.getByRole("main", { name: "Kafka Connect page" });
    await expect(main).toContainText("Connection: Owned Connect fixture");
    await main.getByRole("combobox", { name: "Existing connector", exact: true }).click();
    await page.getByRole("option", { name, exact: true }).click();
    const config = main.getByRole("textbox", {
        name: "Configuration changes (JSON string map)",
        exact: true,
      }),
      remove = main.getByRole("textbox", {
        name: "Fields to remove (JSON string array)",
        exact: true,
      });
    await expect(config).toHaveValue("{}");
    await expect(remove).toHaveValue("[]");
    expect(await main.innerText()).not.toContain(privateValue);
    await config.fill('{"tasks.max":"2"}');
    await remove.fill('["fixture.remove.value"]');
    await main.getByRole("button", { name: "Validate configuration", exact: true }).click();
    await expect(main).toContainText("Validation passed. No connector change was made.");
    expect((await actualConfig())["fixture.remove.value"]).toBe("explicit-only");
    await main.getByRole("button", { name: "Review action", exact: true }).focus();
    await page.keyboard.press("Enter");
    await expect(main).toContainText("Set fields: tasks.max.");
    await expect(main).toContainText("Remove fields: fixture.remove.value.");
    expect(commands.filter((c) => c === "connect.apply")).toHaveLength(0);
    const apply = main.getByRole("button", { name: "Apply reviewed action", exact: true });
    await expect(apply).toBeDisabled();
    const confirmation = main.getByRole("textbox", {
      name: `Type update ${name} to confirm`,
      exact: true,
    });
    await confirmation.fill(name);
    await expect(apply).toBeDisabled();
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Kafka Connect page"]').analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({
      path: info.outputPath("connect-set-remove-review.png"),
      animations: "disabled",
    });
    await confirmation.fill(`update ${name}`);
    await apply.focus();
    await page.keyboard.press("Enter");
    await expect(main).toContainText("acknowledged:");
    await expect(main).toContainText("Original request cleanup: confirmed.");
    await expect(apply).toBeDisabled();
    await expect(remove).toBeDisabled();
    await expect
      .poll(
        async () => ({
          removed: Object.hasOwn(await actualConfig(), "fixture.remove.value"),
          tasks: (await actualConfig())["tasks.max"],
        }),
        { timeout: 15000 },
      )
      .toEqual({ removed: false, tasks: "2" });
    expect((await actualConfig())["fixture.protected.value"]).toBe(privateValue);
    expect(await main.innerText()).not.toContain(privateValue);
    expect(commands.filter((c) => c === "connect.apply")).toHaveLength(1);
    await main.getByRole("button", { name: "Refresh connectors", exact: true }).click();
    await expect(main).toContainText("acknowledged:");
    expect(commands.filter((c) => c === "connect.apply")).toHaveLength(1);
    await page.setViewportSize({ width: 390, height: 844 });
    await expectNoHorizontalOverflow(page);
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Kafka Connect page"]').analyze())
        .violations,
    ).toEqual([]);
    await main.getByText(/^acknowledged:/u).scrollIntoViewIfNeeded();
    await expect(main.getByText(/^acknowledged:/u)).toBeVisible();
    await page.screenshot({
      path: info.outputPath("connect-mobile-receipt.png"),
      animations: "disabled",
    });
    await main
      .getByRole("button", { name: "Dismiss receipt and start another review", exact: true })
      .click();
    await expect(main.getByText(/^acknowledged:/u)).toHaveCount(0);
    await expect(remove).toBeEnabled();
    await page.setViewportSize({ width: 1440, height: 1000 });
    await main.getByRole("combobox", { name: "Action", exact: true }).click();
    await page.getByRole("option", { name: "stop", exact: true }).click();
    await main.getByRole("button", { name: "Review action", exact: true }).click();
    await main
      .getByRole("textbox", { name: `Type stop ${name} to confirm`, exact: true })
      .fill(`stop ${name}`);
    await apply.click();
    await expect(main).toContainText("acknowledged:");
    await expect
      .poll(
        async () => {
          const response = await fetch(`${worker!.url}/connectors/${name}/status`);
          expect(response.ok).toBe(true);
          return ((await response.json()) as { connector: { state: string } }).connector.state;
        },
        { timeout: 15_000 },
      )
      .toBe("STOPPED");
    const offsets = main.getByRole("region", { name: `Connector offsets ${name}`, exact: true });
    await offsets.getByRole("button", { name: "Inspect connector offsets", exact: true }).focus();
    await page.keyboard.press("Enter");
    await expect(offsets).toContainText("available ·");
    await expect(offsets).toContainText("connector STOPPED");
    await offsets.getByRole("textbox", { name: "New offset position", exact: true }).fill("1");
    await offsets.getByRole("button", { name: "Review offset change", exact: true }).focus();
    await page.keyboard.press("Enter");
    await expect(offsets).toContainText("2 → 1");
    expect(await actualOffset()).toBe(2);
    expect(commands.filter((command) => command === "connect.offsets.apply")).toHaveLength(0);
    const applyOffset = offsets.getByRole("button", {
      name: "Apply reviewed offset change",
      exact: true,
    });
    await expect(applyOffset).toBeDisabled();
    expect(
      (await new AxeBuilder({ page }).include(`[aria-label="Connector offsets ${name}"]`).analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({
      path: info.outputPath("connect-offset-review.png"),
      animations: "disabled",
    });
    await offsets
      .getByRole("textbox", { name: "Confirm exact offset change", exact: true })
      .fill(`set OFFSETS ${name}`);
    await applyOffset.focus();
    await page.keyboard.press("Enter");
    await expect(offsets).toContainText(
      "acknowledged · dispatch attempted · readback verified · cleanup confirmed",
    );
    expect(await actualOffset()).toBe(1);
    await expect(applyOffset).toBeDisabled();
    expect(commands.filter((command) => command === "connect.offsets.apply")).toHaveLength(1);
    await page.setViewportSize({ width: 390, height: 844 });
    await expectNoHorizontalOverflow(page);
    expect(
      (await new AxeBuilder({ page }).include(`[aria-label="Connector offsets ${name}"]`).analyze())
        .violations,
    ).toEqual([]);
    await offsets.getByText(/^acknowledged · dispatch/u).scrollIntoViewIfNeeded();
    await page.screenshot({
      path: info.outputPath("connect-offset-mobile-receipt.png"),
      animations: "disabled",
    });
    expect(await main.innerText()).not.toContain(privateValue);
    await offsets.getByRole("button", { name: "Dismiss offset receipt", exact: true }).click();
    await expect(
      offsets.getByRole("button", { name: "Inspect connector offsets", exact: true }),
    ).toBeEnabled();
    expect(diagnostics.problems).toEqual([]);
  } catch (error) {
    failures.push(error);
  } finally {
    await info.attach("browser-diagnostics", {
      body: Buffer.from(JSON.stringify({ problems: diagnostics.problems, commands }, null, 2)),
      contentType: "application/json",
    });
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => page.close(),
        (): Promise<void> => gateway?.close() ?? Promise.resolve(),
        (): Promise<void> => worker?.dispose() ?? Promise.resolve(),
        (): Promise<void> => fixture.dispose(),
        (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
        (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Connect browser flow or owned cleanup failed", {
      cause: failures[0],
    });
});
