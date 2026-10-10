import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { build } from "vite";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway, type WebGatewayRuntime } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { observeBrowserDiagnostics, openWorkbenchResource } from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("keyboard-operable reviewed topic expansion and UUID deletion preserve visible receipts and refresh real inventory", async ({
  page,
}, info) => {
  test.setTimeout(180_000);
  const fixture = await startAuthorizationFixture(),
    topic = `browser-admin-${randomUUID()}`;
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-topic-admin-")),
    rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-topic-admin-"));
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined,
    runtime: WebGatewayRuntime | undefined;
  const diagnostics = observeBrowserDiagnostics(page),
    commands: string[] = [];
  const failures: unknown[] = [];
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  try {
    await fixture.admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
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
    const passphrase = `fixture-vault-${randomUUID()}`;
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible();
    expect(
      await runtime!.providers.get("kafka")!.dispatch({
        command: "profiles.create",
        id: "owned-topic-profile",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profile: {
            name: "Owned topic fixture",
            transport: "plaintext",
            brokers: fixture.connection.brokers,
          },
        },
      }),
    ).toMatchObject({ ok: true });
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Owned topic fixture" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openWorkbenchResource(page, "Topics");
    await page.getByRole("button", { name: topic, exact: true }).click();
    const manage = page.getByRole("button", { name: "Manage topic…" });
    await manage.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", { name: "Review topic change" });
    await dialog.getByRole("textbox", { name: "New total partitions" }).fill("2");
    await dialog.getByRole("button", { name: "Review change", exact: true }).click();
    await expect(dialog.getByText(/Current partitions: 1/u)).toBeVisible();
    expect(
      (
        await fixture.admin.metadata({
          topics: [topic],
          forceUpdate: true,
          autocreateTopics: false,
        })
      ).topics.get(topic)?.partitionsCount,
    ).toBe(1);
    expect(commands.filter((c) => c === "topics.change.apply")).toHaveLength(0);
    await expect(dialog.getByRole("button", { name: "Apply reviewed change" })).toBeDisabled();
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: info.outputPath("topic-expansion-review.png"),
      animations: "disabled",
    });
    await dialog
      .getByRole("textbox", { name: "Confirm exact topic change" })
      .fill(`EXPAND ${topic} TO 2`);
    await dialog.getByRole("button", { name: "Apply reviewed change" }).click();
    await expect(dialog.getByText(/acknowledged · readback/u)).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Apply reviewed change" })).toBeDisabled();
    await expect
      .poll(
        async () =>
          (
            await fixture.admin.metadata({
              topics: [topic],
              forceUpdate: true,
              autocreateTopics: false,
            })
          ).topics.get(topic)?.partitionsCount,
      )
      .toBe(2);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: `Stop tail ${topic}`, exact: true }).click();
    await expect(
      page.getByRole("button", { name: `Start tail ${topic}`, exact: true }),
    ).toBeVisible();
    await manage.click();
    await dialog.getByRole("combobox", { name: "Action" }).click();
    await page.getByRole("option", { name: "Delete topic", exact: true }).click();
    await dialog.getByRole("button", { name: "Review change", exact: true }).click();
    await expect(dialog.getByText(/Current partitions: 2/u)).toBeVisible();
    await expect(dialog.getByText(/There is no undo/u)).toBeVisible();
    await dialog
      .getByRole("textbox", { name: "Confirm exact topic change" })
      .fill(`DELETE ${topic}`);
    await dialog.getByRole("button", { name: "Apply reviewed change" }).click();
    await expect(dialog.getByText(/acknowledged · readback/u)).toBeVisible();
    await expect.poll(() => fixture.admin.listTopics()).not.toContain(topic);
    await page.screenshot({
      path: info.outputPath("topic-deletion-receipt.png"),
      animations: "disabled",
    });
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await expect(page.getByRole("main", { name: "Topics page" })).toBeVisible();
    await expect(page.getByRole("button", { name: topic, exact: true })).not.toBeVisible();
    expect(commands.filter((c) => c === "topics.change.apply")).toHaveLength(2);
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
        (): Promise<void> => fixture.dispose(),
        (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
        (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Browser topic administration or owned cleanup failed", {
      cause: failures[0],
    });
});
