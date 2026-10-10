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
import { startRegistryBrowserFixture } from "../support/registry-browser-fixture";
import { configureLocalConnection } from "../support/web-profile-workflow";
import { openWorkbenchResource } from "../support/workbench-browser";

test.use({ trace: "off", viewport: { width: 1440, height: 1000 } });
test("evolves a registered schema through the production browser, confirms readback and authors its new writer", async ({
  page,
}, info) => {
  test.setTimeout(240_000);
  page.setDefaultTimeout(30_000);
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-author-vault-"));
  const rendererRoot = await mkdtemp(join(resolve("dist"), "renderer-evolution-"));
  const commands: string[] = [],
    errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/commands"))
      return;
    const body = request.postDataJSON() as { command?: unknown } | null;
    if (typeof body?.command === "string") commands.push(body.command);
  });
  let fixture: Awaited<ReturnType<typeof startRegistryBrowserFixture>> | undefined;
  let gateway: Awaited<ReturnType<typeof startWebGateway>> | undefined;
  let admin: Admin | undefined, consumer: Consumer<Buffer, Buffer, Buffer, Buffer> | undefined;
  const topic = `browser-evolution-${randomUUID()}`;
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
    fixture = await startRegistryBrowserFixture();
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
    await configureLocalConnection(page, undefined, fixture.connection);
    const profile = page.getByRole("dialog", { name: "Add Kafka profile" });
    await profile.getByRole("combobox", { name: "Schema Registry authentication" }).click();
    await page.getByRole("option", { name: "No HTTP authorization", exact: true }).click();
    await profile.getByRole("button", { name: "Save profile", exact: true }).click();
    await page.getByRole("button", { name: "Connect profile Local aio", exact: true }).click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openWorkbenchResource(page, "Schema Registry");
    await page.getByRole("button", { name: fixture.config.schemaSubject, exact: true }).click();
    await page.getByRole("button", { name: "Evolve selected schema", exact: true }).click();
    const evolution = page.getByRole("dialog", {
      name: `Evolve schema — ${fixture.config.schemaSubject}@1`,
      exact: true,
    });
    const schema = JSON.parse(fixture.config.schemaDefinition) as { fields: unknown[] };
    schema.fields.push({ name: "note", type: "string", default: "" });
    const evolvedDefinition = JSON.stringify(schema);
    await evolution
      .getByRole("textbox", { name: "Proposed schema", exact: true })
      .fill(evolvedDefinition);
    await evolution.getByRole("button", { name: "Review schema change", exact: true }).click();
    await expect(
      evolution.getByText(
        "Registry compatibility passed for this draft under the current policy.",
        { exact: true },
      ),
    ).toBeVisible();
    await expect(evolution.getByRole("table", { name: "Differences" })).toBeVisible();
    await expect(
      evolution.getByRole("button", { name: "Register reviewed schema", exact: true }),
    ).toBeDisabled();
    expect(commands.filter((command) => command === "schemas.change.apply")).toEqual([]);
    await evolution
      .getByRole("textbox", {
        name: `Type ${fixture.config.schemaSubject} to confirm registration`,
        exact: true,
      })
      .fill(fixture.config.schemaSubject);
    await page.screenshot({
      path: info.outputPath("schema-evolution-reviewed.png"),
      animations: "disabled",
    });
    await evolution.getByRole("button", { name: "Register reviewed schema", exact: true }).click();
    await expect(evolution.getByRole("status")).toContainText(
      "Registry acknowledged registration and its writer ID was read back",
    );
    const writerResponse = await page.request.get(
      `${fixture.connection.schemaRegistryEndpoint}/subjects/${encodeURIComponent(fixture.config.schemaSubject)}/versions/latest`,
    );
    expect(writerResponse.ok()).toBe(true);
    const writer = (await writerResponse.json()) as { id: number; version: number };
    expect(writer.version).toBe(2);
    await evolution.getByRole("button", { name: "Refresh subject", exact: true }).click();
    await page.getByRole("button", { name: "Author record", exact: true }).click();
    const dialog = page.getByRole("dialog", {
      name: `Author record — ${fixture.config.schemaSubject}@2`,
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
      note: "evolved writer",
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
      path: info.outputPath("schema-evolution-publication.png"),
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
    expect(wire[0]).toBe(0);
    expect(wire.readUInt32BE(1)).toBe(writer.id);
    const long = avro.types.LongType.__with({
      fromBuffer: (b: Buffer) => b.readBigInt64LE().toString(),
      toBuffer: () => Buffer.alloc(8),
      fromJSON: String,
      toJSON: String,
      isValid: (v: unknown) => typeof v === "string",
      compare: () => 0,
    });
    const type = avro.Type.forSchema(JSON.parse(evolvedDefinition) as avro.Schema, {
      typeHook: (s) => (s === "long" ? long : undefined),
    });
    expect(type.fromBuffer(wire.subarray(5))).toMatchObject({
      ...payload,
      message: "final edited value",
      note: "evolved writer",
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
