import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { expect, test } from "@playwright/test";
import { Admin, Consumer, type MessagesStream } from "@platformatic/kafka";
import avro from "avsc";
import { build } from "vite";

import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { startWebGateway } from "../../src/platform/node/web-gateway";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { fixtureClientOptions } from "../support/kafka-fixture";
import { disposeNativeFixtureResources } from "../support/native-kafka-fixture";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";
import { connectLocalProfile } from "../support/web-profile-workflow";
import { openWorkbenchResource } from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("authors an edited record through the production browser and publishes only after destination confirmation", async ({
  page,
}, info) => {
  test.setTimeout(240_000);
  page.setDefaultTimeout(30_000);
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-author-vault-"));
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-author-"));
  const commands: string[] = [],
    errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  let fixture: Awaited<ReturnType<typeof startStructuredBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let admin: Admin | undefined, consumer: Consumer<Buffer, Buffer, Buffer, Buffer> | undefined;
  const topic = `browser-author-${randomUUID()}`;
  let topicCreated = false;
  let stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | undefined;
  let timeoutId: NodeJS.Timeout | undefined;
  const failures: unknown[] = [];
  try {
    await build({
      configFile: resolve("config/vite.config.ts"),
      logLevel: "silent",
      build: { outDir: rendererRoot },
    });
    fixture = await startStructuredBrowserFixture();
    const options = await fixtureClientOptions(fixture.connection, fixture.config, topic);
    admin = new Admin(options);
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    topicCreated = true;
    gateway = await startWebGateway({
      port: 0,
      hostname: "127.0.0.1",
      publicOrigin: "http://127.0.0.1:0",
      rendererRoot,
      dataRoot,
      inspectVault: () => inspectPassphraseVault(dataRoot),
      openRuntime: (value, mode) => openBrowserRuntime(dataRoot, value, mode),
    });
    if (!gateway.setupCodePath) throw new Error("Fresh vault omitted setup code.");
    const passphrase = `test-vault-${randomUUID()}`;
    await page.goto(gateway.origin);
    await page
      .getByLabel("Setup code", { exact: true })
      .fill((await readFile(gateway.setupCodePath, "utf8")).trim());
    await page.getByLabel("Vault passphrase", { exact: true }).fill(passphrase);
    await page.getByLabel("Confirm vault passphrase", { exact: true }).fill(passphrase);
    await page.getByRole("button", { name: "Create vault", exact: true }).click();
    await connectLocalProfile(page, fixture.connection);
    await openWorkbenchResource(page, "Schema Registry");
    await page.getByRole("button", { name: fixture.config.schemaSubject, exact: true }).click();
    await page.getByRole("button", { name: "Author record", exact: true }).click();
    const dialog = page.getByRole("dialog", {
      name: `Author record — ${fixture.config.schemaSubject}@1`,
      exact: true,
    });
    await dialog.getByRole("button", { name: "Start from one sample", exact: true }).click();
    await expect(
      dialog.getByRole("button", { name: "Validate payload", exact: true }),
    ).toBeEnabled();
    await expect(
      dialog.getByRole("button", { name: "Publish reviewed batch", exact: true }),
    ).toHaveCount(0);
    const payload = {
      source: "schema-authoring-browser",
      sequence: "9223372036854775807",
      message: "manually edited",
    };
    await dialog
      .getByRole("textbox", { name: "Record payload JSON", exact: true })
      .fill(JSON.stringify(payload));
    await dialog.getByRole("button", { name: "Validate payload", exact: true }).click();
    await expect(dialog.getByText(/Validated against/u)).toBeVisible();
    await dialog.getByRole("textbox", { name: "Destination topic", exact: true }).fill(topic);
    await dialog.getByRole("button", { name: "Review batch destination", exact: true }).click();
    await expect(
      dialog.getByRole("button", { name: "Publish reviewed batch", exact: true }),
    ).toBeDisabled();
    // Editing the value invalidates the validation and its previously reviewed destination.
    await dialog
      .getByRole("textbox", { name: "Record payload JSON", exact: true })
      .fill(JSON.stringify({ ...payload, message: "final edited value" }));
    await expect(
      dialog.getByRole("button", { name: "Publish reviewed batch", exact: true }),
    ).toHaveCount(0);
    expect(commands.filter((command) => command === "records.batch.apply")).toEqual([]);
    const offsets = await admin.listOffsets({
      topics: [{ name: topic, partitions: [{ partitionIndex: 0, timestamp: -1n }] }],
    });
    expect(offsets).toMatchObject([{ partitions: [{ offset: 0n }] }]);
    await dialog.getByRole("button", { name: "Validate payload", exact: true }).click();
    await expect(dialog.getByText(/Validated against/u)).toBeVisible();
    await dialog.getByRole("textbox", { name: "Destination topic", exact: true }).fill(topic);
    await dialog.getByRole("button", { name: "Review batch destination", exact: true }).click();
    await dialog
      .getByRole("textbox", { name: "Type destination topic to confirm", exact: true })
      .fill(topic);
    await page.screenshot({
      path: info.outputPath("schema-authoring-reviewed.png"),
      animations: "disabled",
    });
    await dialog.getByRole("button", { name: "Publish reviewed batch", exact: true }).click();
    await expect(
      dialog.getByText(/1 acknowledged, 0 rejected, 0 uncertain, 0 unsent/u),
    ).toBeVisible();
    consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
      ...options,
      groupId: `author-${randomUUID()}`,
      autocreateTopics: false,
    });
    stream = await consumer.consume({ topics: [topic], mode: "earliest", maxWaitTime: 1000 });
    const first = await Promise.race([
      stream[Symbol.asyncIterator]().next(),
      new Promise<never>((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("Authored broker record was not observed.")),
          15_000,
        );
      }),
    ]);
    if (first.done || !first.value.value) throw new Error("Authored value missing.");
    const wire = first.value.value;
    expect(wire.subarray(0, 5).toString("hex")).toBe("0000000001");
    const long = avro.types.LongType.__with({
      fromBuffer: (b: Buffer) => b.readBigInt64LE().toString(),
      toBuffer: () => Buffer.alloc(8),
      fromJSON: String,
      toJSON: String,
      isValid: (v: unknown) => typeof v === "string",
      compare: () => 0,
    });
    const type = avro.Type.forSchema(JSON.parse(fixture.config.schemaDefinition) as avro.Schema, {
      typeHook: (s) => (s === "long" ? long : undefined),
    });
    expect(type.fromBuffer(wire.subarray(5))).toMatchObject({
      ...payload,
      message: "final edited value",
    });
    expect(commands.filter((command) => command === "records.batch.apply")).toHaveLength(1);
    expect(errors).toEqual([]);
  } catch (error) {
    failures.push(error);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    try {
      await disposeNativeFixtureResources([
        (): Promise<void> => page.close(),
        (): Promise<void> => gateway?.close() ?? Promise.resolve(),
        (): Promise<void> => stream?.close() ?? Promise.resolve(),
        (): Promise<void> => consumer?.close() ?? Promise.resolve(),
        async (): Promise<void> => {
          if (admin && topicCreated) await admin.deleteTopics({ topics: [topic] });
        },
        (): Promise<void> => admin?.close() ?? Promise.resolve(),
        (): Promise<void> => fixture?.dispose() ?? Promise.resolve(),
        (): Promise<void> => rm(rendererRoot, { recursive: true, force: true }),
        (): Promise<void> => rm(dataRoot, { recursive: true, force: true }),
      ]);
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length)
    throw new AggregateError(failures, "Browser authoring or owned cleanup failed", {
      cause: failures[0],
    });
});
