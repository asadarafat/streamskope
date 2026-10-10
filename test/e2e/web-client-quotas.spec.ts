import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { ClientQuotaMatchTypes } from "@platformatic/kafka";
import { build } from "vite";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway, type WebGatewayRuntime } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import {
  observeBrowserDiagnostics,
  openWorkbenchResource,
  waitForSettledDialog,
  expectNoHorizontalOverflow,
} from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("keyboard-operable exact quota review preserves explicit keys, distinguishes defaults and keeps actual receipts visible", async ({
  page,
}, info) => {
  test.setTimeout(180000);
  const fixture = await startAuthorizationFixture(),
    name = `browser-quota-${randomUUID()}`;
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-quota-admin-")),
    rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-quota-admin-"));
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined,
    runtime: WebGatewayRuntime | undefined;
  const diagnostics = observeBrowserDiagnostics(page),
    commands: string[] = [],
    failures: unknown[] = [];
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  const values = async (): Promise<unknown> =>
    (
      await fixture.admin.describeClientQuotas({
        strict: true,
        components: [{ entityType: "user", matchType: ClientQuotaMatchTypes.EXACT, match: name }],
      })
    )[0]?.values.sort((a, b) => a.key.localeCompare(b.key));
  try {
    await fixture.admin.alterClientQuotas({
      validateOnly: false,
      entries: [
        {
          entities: [{ entityType: "user", entityName: name }],
          ops: [
            { key: "producer_byte_rate", remove: false, value: 1000 },
            { key: "consumer_byte_rate", remove: false, value: 900 },
          ],
        },
      ],
    });
    await expect.poll(values, { timeout: 15000 }).toEqual([
      { key: "consumer_byte_rate", value: 900 },
      { key: "producer_byte_rate", value: 1000 },
    ]);
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
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible({
      timeout: 30000,
    });
    expect(
      await runtime!.providers.get("kafka")!.dispatch({
        command: "profiles.create",
        id: "owned-quota-profile",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profile: {
            name: "Owned quota fixture",
            transport: "plaintext",
            brokers: fixture.connection.brokers,
          },
        },
      }),
    ).toMatchObject({ ok: true });
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Owned quota fixture" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openWorkbenchResource(page, "Overview");
    const action = page.getByRole("button", { name: "Client quotas…", exact: true });
    await action.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", {
      name: "Inspect and review client quotas",
      exact: true,
    });
    await dialog.getByRole("textbox", { name: "Exact Kafka user", exact: true }).fill(name);
    await dialog.getByRole("button", { name: "Inspect exact quotas", exact: true }).click();
    await expect(dialog.getByRole("table", { name: "Explicit client quotas" })).toContainText(
      "1000",
    );
    await expect(dialog.getByRole("table", { name: "Explicit client quotas" })).toContainText(
      "900",
    );
    await dialog.getByRole("combobox", { name: "Change producer_byte_rate", exact: true }).click();
    await page.getByRole("option", { name: "Set explicit value", exact: true }).click();
    await dialog.getByRole("textbox", { name: "Value producer_byte_rate", exact: true }).fill("0");
    await dialog.getByRole("button", { name: "Review quota changes", exact: true }).click();
    await expect(dialog.getByText(/Set requires a finite positive number/u)).toBeVisible();
    expect(commands.filter((c) => c === "quotas.change.review")).toHaveLength(0);
    await dialog
      .getByRole("textbox", { name: "Value producer_byte_rate", exact: true })
      .fill("512");
    await dialog.getByRole("button", { name: "Review quota changes", exact: true }).click();
    await expect(
      dialog.getByRole("table", { name: "Reviewed client quota changes" }),
    ).toContainText("1000");
    expect(await values()).toEqual([
      { key: "consumer_byte_rate", value: 900 },
      { key: "producer_byte_rate", value: 1000 },
    ]);
    expect(commands.filter((c) => c === "quotas.change.apply")).toHaveLength(0);
    const apply = dialog.getByRole("button", { name: "Apply reviewed quota changes", exact: true });
    await expect(apply).toBeDisabled();
    await waitForSettledDialog(dialog);
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({ path: info.outputPath("quota-review.png"), animations: "disabled" });
    const confirmation = dialog.getByRole("textbox", {
      name: "Confirm exact quota change",
      exact: true,
    });
    await confirmation.fill(name);
    await expect(apply).toBeDisabled();
    await confirmation.fill(`ALTER QUOTAS user=${JSON.stringify(name)}`);
    await apply.focus();
    await page.keyboard.press("Enter");
    await expect(
      dialog.getByText(/acknowledged · readback (verified|different) · cleanup confirmed/u),
    ).toBeVisible();
    await expect(apply).toBeDisabled();
    await expect.poll(values, { timeout: 15000 }).toEqual([
      { key: "consumer_byte_rate", value: 900 },
      { key: "producer_byte_rate", value: 512 },
    ]);
    expect(commands.filter((c) => c === "quotas.change.apply")).toHaveLength(1);
    await page.screenshot({ path: info.outputPath("quota-receipt.png"), animations: "disabled" });
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await action.click();
    await dialog.getByRole("checkbox", { name: "Include user", exact: true }).uncheck();
    await dialog.getByRole("checkbox", { name: "Include client ID", exact: true }).check();
    await dialog.getByRole("checkbox", { name: "Default client-ID entry", exact: true }).check();
    await dialog.getByRole("button", { name: "Inspect exact quotas", exact: true }).click();
    await expect(dialog.getByText(/No explicit quotas were returned/u)).toBeVisible();
    await expect(dialog.getByText(/Default entries can affect many clients/u)).toBeVisible();
    await expect(
      dialog.getByRole("button", { name: "Review quota changes", exact: true }),
    ).toBeDisabled();
    await page.setViewportSize({ width: 390, height: 844 });
    await waitForSettledDialog(dialog);
    await expectNoHorizontalOverflow(page);
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: info.outputPath("quota-mobile-inspection.png"),
      animations: "disabled",
    });
    expect(commands.filter((c) => c === "quotas.change.apply")).toHaveLength(1);
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
    throw new AggregateError(failures, "Browser quota administration or owned cleanup failed", {
      cause: failures[0],
    });
});
