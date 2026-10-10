import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test } from "@playwright/test";
import { build } from "vite";

import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startRegistryBrowserFixture } from "../support/registry-browser-fixture";
import { configureLocalConnection } from "../support/web-profile-workflow";
import { openWorkbenchResource } from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("reviews real subject policy, confirms override, refreshes without another write and restores inheritance", async ({
  page,
}, info) => {
  test.setTimeout(240000);
  page.setDefaultTimeout(30000);
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-policy-vault-")),
    rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-policy-")),
    commands: string[] = [],
    errors: string[] = [],
    failures: unknown[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  let fixture: Awaited<ReturnType<typeof startRegistryBrowserFixture>> | undefined,
    gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  try {
    await build({
      configFile: resolve("config/vite.config.ts"),
      logLevel: "silent",
      build: { outDir: rendererRoot },
    });
    fixture = await startRegistryBrowserFixture();
    gateway = await startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: (value, mode) => openBrowserRuntime(dataRoot, value, mode),
    });
    if (!gateway.setupCodePath) throw new Error("Fresh vault omitted setup code");
    const passphrase = `test-vault-${randomUUID()}`;
    await page.goto(gateway.origin);
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await configureLocalConnection(page, undefined, fixture.connection);
    const profile = page.getByRole("dialog", { name: "Add Kafka profile" });
    await profile.getByRole("combobox", { name: "Schema Registry authentication" }).click();
    await page.getByRole("option", { name: "No HTTP authorization", exact: true }).click();
    await profile.getByRole("button", { name: "Save profile", exact: true }).click();
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openWorkbenchResource(page, "Schema Registry");
    const subject = fixture.config.schemaSubject;
    await page.getByRole("button", { name: subject, exact: true }).click();
    await page.getByRole("button", { name: "Compatibility policy", exact: true }).click();
    let dialog = page.getByRole("dialog", {
      name: `Compatibility policy — ${subject}`,
      exact: true,
    });
    await expect(
      dialog.getByRole("table", { name: "Compatibility policy comparison" }),
    ).toContainText("Inherited");
    await dialog.getByRole("combobox", { name: "Subject policy" }).click();
    await page.getByRole("option", { name: "FULL_TRANSITIVE", exact: true }).click();
    await dialog.getByRole("button", { name: "Review policy change", exact: true }).click();
    await expect(dialog.getByRole("table")).toContainText("After");
    expect(commands.filter((x) => x === "schemas.policy.apply")).toHaveLength(0);
    await expect(dialog.getByRole("button", { name: "Apply reviewed policy" })).toBeDisabled();
    await dialog.getByLabel(`Type ${subject} to confirm policy change`).fill(subject);
    await page.screenshot({
      path: info.outputPath("schema-policy-reviewed.png"),
      animations: "disabled",
    });
    await dialog.getByRole("button", { name: "Apply reviewed policy" }).click();
    await expect(dialog.getByText("acknowledged · verified", { exact: true })).toBeVisible();
    const http = new NodeBoundedJsonHttp(),
      base = fixture.connection.schemaRegistryEndpoint!,
      signal = AbortSignal.timeout(15000);
    expect(
      (
        await http.request({
          method: "GET",
          url: `${base}/config/${subject}?defaultToGlobal=false`,
          signal,
        })
      ).body,
    ).toMatchObject({ compatibilityLevel: "FULL_TRANSITIVE" });
    await dialog.getByRole("button", { name: "Read current policy", exact: true }).click();
    await expect(dialog.getByRole("table")).toContainText("FULL_TRANSITIVE");
    expect(commands.filter((x) => x === "schemas.policy.apply")).toHaveLength(1);
    await dialog.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Compatibility policy", exact: true }).click();
    dialog = page.getByRole("dialog", { name: `Compatibility policy — ${subject}`, exact: true });
    await expect(dialog.getByRole("table")).toContainText("FULL_TRANSITIVE");
    await dialog.getByRole("combobox", { name: "Subject policy" }).click();
    await page.getByRole("option", { name: "Inherit global default", exact: true }).click();
    await dialog.getByRole("button", { name: "Review policy change", exact: true }).click();
    await dialog.getByLabel(`Type ${subject} to confirm policy change`).fill(subject);
    await dialog.getByRole("button", { name: "Apply reviewed policy", exact: true }).click();
    await expect(dialog.getByText("acknowledged · verified", { exact: true })).toBeVisible();
    await expect(dialog.getByText("Read back: BACKWARD (global default).")).toBeVisible();
    const override = await http.request({
      method: "GET",
      url: `${base}/config/${subject}?defaultToGlobal=false`,
      signal,
    });
    expect(override.status).toBe(404);
    expect(
      (await http.request({ method: "GET", url: `${base}/config`, signal })).body,
    ).toMatchObject({ compatibilityLevel: "BACKWARD" });
    expect(commands.filter((x) => x === "schemas.policy.apply")).toHaveLength(2);
    expect(
      commands.filter((x) => x === "schemas.change.apply" || x === "schemas.register"),
    ).toHaveLength(0);
    expect(errors).toEqual([]);
    await page.screenshot({
      path: info.outputPath("schema-policy-inherited.png"),
      animations: "disabled",
    });
  } catch (error) {
    failures.push(error);
  } finally {
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => page.close(),
        (): Promise<void> => gateway?.close() ?? Promise.resolve(),
        (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
        (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
        (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Browser policy or owned cleanup failed", {
      cause: failures[0],
    });
});
