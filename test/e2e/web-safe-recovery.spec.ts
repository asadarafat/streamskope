import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Producer } from "@platformatic/kafka";
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { build } from "vite";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway, type WebGatewayRuntime } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import {
  fetchTopicMessages,
  observeBrowserDiagnostics,
  openWorkbenchResource,
} from "../support/workbench-browser";

test.use({ actionTimeout: 10_000, trace: "off", viewport: { width: 1440, height: 1000 } });
test("reviews and applies replay, offset recovery and ACL changes through the real browser host", async ({
  page,
}, testInfo) => {
  test.setTimeout(180_000);
  const fixture = await startAuthorizationFixture();
  const topic = "recovery-source",
    target = "recovery-copy",
    groupId = "recovery-owned-group";
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-repair-browser-"));
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-repair-"));
  let runtime: WebGatewayRuntime | undefined;
  const diagnostics = observeBrowserDiagnostics(page);
  const httpProblems: string[] = [];
  page.on("response", (response) => {
    if (response.status() >= 400)
      httpProblems.push(`${response.status()} ${new URL(response.url()).pathname}`);
  });
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  try {
    await build({
      configFile: resolve("config/vite.config.ts"),
      logLevel: "silent",
      build: { outDir: rendererRoot },
    });
    await fixture.admin.createTopics({ topics: [topic, target], partitions: 1, replicas: 1 });
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
        { timeout: 30_000 },
      )
      .toBe(true);
    // Topic metadata can precede leader readiness on a fresh broker. Fixture seeding
    // permits bounded transport retries; reviewed recovery itself never resends.
    const producer = new Producer({
      bootstrapBrokers: [...fixture.connection.brokers],
      clientId: "browser-recovery-seed",
      retries: 2,
    });
    try {
      await producer.send({
        messages: [
          {
            topic,
            partition: 0,
            key: Buffer.from("owned-key"),
            value: Buffer.from('{"event":"fixture"}'),
          },
        ],
      });
    } finally {
      await producer.close();
    }
    gateway = await startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: async (value, mode) => {
        runtime = await openBrowserRuntime(dataRoot, value, mode);
        return runtime;
      },
    });
    if (!gateway.setupCodePath) throw new Error("Fresh vault omitted setup code");
    await page.goto(gateway.origin);
    const passphrase = `test-vault-${randomUUID()}`;
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await expect(page.getByRole("button", { name: "Add connection" })).toBeVisible();
    const created = await runtime!.providers.get("kafka")!.dispatch({
      command: "profiles.create",
      id: "owned-recovery-profile",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: {
          name: "Owned recovery",
          transport: "plaintext",
          brokers: fixture.connection.brokers,
        },
      },
    });
    expect(created).toMatchObject({ ok: true });
    await page
      .getByRole("button", { name: "Connect insecure plaintext profile Owned recovery" })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await fetchTopicMessages(page, topic);
    await page
      .getByRole("grid", { name: "Kafka messages" })
      .getByText('{"event":"fixture"}', { exact: true })
      .first()
      .click();
    await page.getByRole("button", { name: "Replay…" }).click();
    const replay = page.getByRole("dialog", { name: "Copy or replay selected records" });
    await replay.getByRole("textbox", { name: "Destination topic" }).fill(target);
    await replay.getByRole("checkbox", { name: "Transform structured value" }).check();
    await replay
      .getByRole("textbox", { name: "Value JSON Pointer edits" })
      .fill(JSON.stringify([{ op: "set", path: "/event", json: '"replayed"' }]));
    await replay.getByRole("button", { name: "Preview replay" }).click();
    const replayConfirmation = `Owned recovery / ${target} / 0`;
    await expect(
      replay.getByRole("textbox", { name: `Type ${replayConfirmation} to confirm` }),
    ).toBeVisible();
    await expect(replay.getByText("Verified writer mappings", { exact: true })).toBeVisible();
    await expect(
      replay.getByRole("textbox", { name: "Find literal UTF-8 value text" }),
    ).toBeDisabled();
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("replay-review.png"),
      animations: "disabled",
    });
    await replay
      .getByRole("textbox", { name: `Type ${replayConfirmation} to confirm` })
      .fill(replayConfirmation);
    await replay.getByRole("button", { name: "Apply reviewed replay" }).click();
    await expect(
      replay.getByText(/1 acknowledged; 0 unknown; 0 rejected; 0 unsent/u),
    ).toBeVisible();
    await replay.getByRole("button", { name: "Close", exact: true }).click();
    await expect(replay).not.toBeVisible();
    await expect(page.getByLabel("Backend status")).toContainText("Host ready");
    await expect(page.getByLabel("Consumption status")).toContainText("Streaming");

    await page.getByRole("button", { name: "Repair history", exact: true }).click();
    const history = page.getByRole("dialog", { name: "Repair jobs and receipts" });
    await expect(history.getByText(/Protected, durable host storage/u)).toBeVisible();
    await expect(
      history.getByText(`Owned recovery / ${target} / 0`, { exact: true }),
    ).toBeVisible();
    await expect(history.getByRole("cell", { name: "acknowledged", exact: true })).toBeVisible();
    await expect(history.getByRole("cell", { name: `${target}/0@0`, exact: true })).toBeVisible();
    await history.getByRole("button", { name: "Recovery controls" }).click();
    await history.getByRole("textbox", { name: "Destination offset to inspect" }).fill("0");
    await history.getByRole("button", { name: "Inspect destination offset" }).click();
    await expect(history.getByText(/Record 1 at destination offset 0: equivalent/u)).toBeVisible();
    const repairFile = join(dataRoot, "history", "kafka-repair-jobs.json");
    const protectedRecovery = await readFile(repairFile, "utf8");
    expect(protectedRecovery).not.toContain("replayed");
    const protectedEnvelope: unknown = JSON.parse(protectedRecovery);
    expect(protectedEnvelope).toMatchObject({ schemaVersion: 3 });
    await expect(
      history.getByRole("button", { name: "Review definitely unsent records" }),
    ).toBeDisabled();
    await history.getByRole("button", { name: "Close recovery controls" }).click();
    // Contrast checks require the dialog's entrance fade to have finished.
    await expect(history.locator("..")).toHaveCSS("opacity", "1");
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await history.getByRole("button", { name: "Refresh", exact: true }).click();
    await expect(history.getByRole("cell", { name: `${target}/0@0`, exact: true })).toBeVisible();
    await history.getByRole("button", { name: "Close", exact: true }).click();
    await expect(history).not.toBeVisible();

    await openWorkbenchResource(page, "Consumer Groups");
    await page.getByRole("button", { name: groupId, exact: true }).click();
    await page.getByRole("button", { name: "Reset offsets…" }).click();
    const reset = page.getByRole("dialog", { name: "Review consumer offset reset" });
    await reset.getByRole("checkbox", { name: `Reset ${topic}:0` }).check();
    await reset.getByRole("textbox", { name: `Next offset ${topic}:0` }).fill("0");
    await reset.getByRole("button", { name: "Preview reset" }).click();
    await expect(reset.getByRole("table", { name: "Offset reset preview" })).toBeVisible();
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath("offset-review.png"),
      animations: "disabled",
    });
    await reset.getByRole("textbox", { name: "Type the exact group ID to confirm" }).fill(groupId);
    await reset.getByRole("button", { name: "Apply reviewed reset" }).click();
    await expect(reset.getByRole("table", { name: "Offset reset results" })).toContainText(
      "acknowledged",
    );
    await reset.getByRole("button", { name: "Close", exact: true }).click();
    await expect(reset).not.toBeVisible();

    await page
      .getByRole("navigation", { name: "StreamSkope resources" })
      .getByRole("button", { name: "Access Control Lists" })
      .click();
    await page.getByRole("button", { name: "Create ACL", exact: true }).click();
    const create = page.getByRole("dialog", { name: "Create exact ACL binding" });
    await create.getByRole("textbox", { name: "Resource name" }).fill(target);
    await create.getByRole("textbox", { name: "Principal", exact: true }).fill("User:ANONYMOUS");
    await create.getByRole("button", { name: "Create ACL", exact: true }).click();
    const acl = page.getByRole("dialog", { name: "Review ACL create" });
    await acl.getByRole("textbox", { name: "Client IP seen by Kafka" }).fill("127.0.0.1");
    await acl.getByRole("button", { name: "Preview ACL change" }).click();
    await expect(acl.getByRole("table", { name: "Before access evidence" })).toBeVisible();
    await expect(acl.getByRole("table", { name: "After access evidence" })).toBeVisible();
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({ path: testInfo.outputPath("acl-review.png"), animations: "disabled" });
    await acl
      .getByRole("textbox", { name: "Exact change confirmation" })
      .fill(`create TOPIC | LITERAL | ${target} | User:ANONYMOUS | * | READ | ALLOW`);
    await acl.getByRole("button", { name: "Apply reviewed ACL change" }).click();
    await expect(acl.getByText(/acknowledged · verified/u)).toBeVisible();
    await expect(acl.getByRole("button", { name: "Apply reviewed ACL change" })).toBeDisabled();
    await acl.getByRole("button", { name: "Close", exact: true }).click();
    await expect(acl).not.toBeVisible();
    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Repair history", exact: true }).click();
    await expect(history.getByText(/Record 1 at destination offset 0: equivalent/u)).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath("repair-history-profiles.png"),
      animations: "disabled",
    });
    await history.getByRole("button", { name: "Close", exact: true }).click();
    expect(diagnostics.problems).toEqual([]);
  } finally {
    await testInfo.attach("browser-diagnostics", {
      body: Buffer.from(JSON.stringify({ problems: diagnostics.problems, httpProblems }, null, 2)),
      contentType: "application/json",
    });
    await disposeNativeFixtureResources([
      (): Promise<void> => gateway?.close() ?? Promise.resolve(),
      (): Promise<void> => fixture.dispose(),
      (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
      (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
    ]);
  }
});
