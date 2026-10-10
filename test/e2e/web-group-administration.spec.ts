import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Locator } from "@playwright/test";
import { Producer } from "@platformatic/kafka";
import { build } from "vite";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway, type WebGatewayRuntime } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { observeBrowserDiagnostics, openWorkbenchResource } from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
async function settledDialog(dialog: Locator): Promise<void> {
  // Audit the visible result, including its ancestor fade and floating labels.
  // Every finding from the settled audit still fails the test.
  await expect
    .poll(() =>
      dialog.evaluate((element) => {
        for (let parent: Element | null = element; parent; parent = parent.parentElement) {
          if (
            getComputedStyle(parent).opacity !== "1" ||
            parent.getAnimations().some((animation) => animation.playState === "running")
          )
            return false;
        }
        return element
          .getAnimations({ subtree: true })
          .every((animation) => animation.playState !== "running");
      }),
    )
    .toBe(true);
}
test("keyboard-operable reviewed group selectors and deletion preserve visible receipts and refresh real inventory", async ({
  page,
}, info) => {
  test.setTimeout(180_000);
  const fixture = await startAuthorizationFixture(),
    topic = `browser-groups-${randomUUID()}`,
    groupId = `browser-group-${randomUUID()}`;
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "group-browser-seed",
    idempotent: true,
    retries: 0,
  });
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-group-admin-")),
    rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-group-admin-"));
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
    await producer.send({
      messages: [{ topic, partition: 0, value: Buffer.from('{"event":"group-preview"}') }],
    });
    await expect
      .poll(
        async () => {
          try {
            await fixture.admin.alterConsumerGroupOffsets({
              groupId,
              topics: [{ name: topic, partitionOffsets: [{ partition: 0, offset: 1n }] }],
            });
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 15000, intervals: [250] },
      )
      .toBe(true);

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
      timeout: 30_000,
    });
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

    await openWorkbenchResource(page, "Consumer Groups");
    await page.getByRole("button", { name: groupId, exact: true }).click();
    const reset = page.getByRole("button", { name: "Reset offsets…" });
    await expect(reset).toBeEnabled();
    await reset.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", { name: "Review consumer offset reset" });
    await dialog.getByRole("checkbox", { name: `Reset ${topic}:0` }).check();
    await dialog.getByRole("combobox", { name: "Reset position" }).click();
    await page.getByRole("option", { name: "Earliest retained", exact: true }).click();
    await dialog.getByRole("button", { name: "Preview reset" }).click();
    await expect(dialog.getByRole("table", { name: "Offset reset preview" })).toBeVisible();
    await expect(dialog.getByText(/group Empty/u)).toBeVisible();
    expect(
      (
        await fixture.admin.listConsumerGroupOffsets({ groups: [groupId], requireStable: false })
      )[0]!.topics[0]!.partitions[0]!.committedOffset,
    ).toBe(1n);
    expect(commands.filter((c) => c === "consumerGroups.reset.apply")).toHaveLength(0);
    await settledDialog(dialog);
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: info.outputPath("group-reset-review.png"),
      animations: "disabled",
    });
    await dialog.getByRole("textbox", { name: "Type the exact group ID to confirm" }).fill(groupId);
    await dialog.getByRole("button", { name: "Apply reviewed reset" }).click();
    await expect(dialog.getByRole("table", { name: "Offset reset results" })).toContainText(
      "acknowledged",
    );
    await expect(dialog.getByRole("table", { name: "Offset reset results" })).toContainText(
      "confirmed",
    );
    await expect(dialog.getByRole("button", { name: "Apply reviewed reset" })).toBeDisabled();
    expect(
      (
        await fixture.admin.listConsumerGroupOffsets({ groups: [groupId], requireStable: false })
      )[0]!.topics[0]!.partitions[0]!.committedOffset,
    ).toBe(0n);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Delete group…" }).click();
    const deletion = page.getByRole("dialog", { name: "Review consumer group deletion" });
    await deletion.getByRole("button", { name: "Review group deletion", exact: true }).click();
    await expect(
      deletion.getByRole("textbox", { name: "Confirm exact group deletion" }),
    ).toBeVisible();
    expect((await fixture.admin.listGroups()).has(groupId)).toBe(true);
    await settledDialog(deletion);
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await deletion
      .getByRole("textbox", { name: "Confirm exact group deletion" })
      .fill(`DELETE GROUP ${groupId}`);
    await deletion.getByRole("button", { name: "Delete reviewed group", exact: true }).click();
    await expect(deletion.getByText(/acknowledged · readback/u)).toBeVisible();
    await page.screenshot({
      path: info.outputPath("group-deletion-receipt.png"),
      animations: "disabled",
    });
    await expect(deletion.getByRole("button", { name: "Delete reviewed group" })).toBeDisabled();
    await expect
      .poll(async () => (await fixture.admin.listGroups()).has(groupId), { timeout: 10000 })
      .toBe(false);
    await deletion.getByRole("button", { name: "Close", exact: true }).click();
    await expect(page.getByLabel("Consumer groups page", { exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: groupId, exact: true })).toHaveCount(0);
    expect(commands.filter((c) => c === "consumerGroups.delete.apply")).toHaveLength(1);
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
        (): Promise<void> => producer.close(),
        (): Promise<void> => fixture.dispose(),
        (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
        (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Browser group administration or owned cleanup failed", {
      cause: failures[0],
    });
});
