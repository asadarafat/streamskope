import { mkdir, mkdtemp, readFile, writeFile, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

import { Admin, Producer } from "@platformatic/kafka";
import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommandResponse,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { runReadOnlyCli } from "../../src/platform/node/read-only-cli";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import {
  loadFixtureConfig,
  loadFixtureConnection,
  fixtureClientOptions,
  fetchFixtureToken,
} from "../support/kafka-fixture";

it("generates a real Registry client, round-trips Kafka bytes and exports protected query results over TLS/OAuth", async () => {
  const config = await loadFixtureConfig(),
    fixture = await loadFixtureConnection();
  if (!fixture.schemaRegistryEndpoint) throw new Error("Registry required");
  const ca = await readFile(fixture.caPath, "utf8");
  const connection: SecureConnectionInput = {
    name: "Developer qualification",
    brokers: [fixture.kafkaEndpoint],
    tls: { enabled: true, caPem: ca },
    oauth: {
      clientId: config.oauthClientId,
      clientSecret: config.oauthClientSecret,
      scope: config.oauthScope,
      tokenEndpoint: fixture.oauthEndpoint,
    },
    services: {
      schemaRegistry: { baseUrl: fixture.schemaRegistryEndpoint, authentication: "oauth" },
    },
  };
  const options = await fixtureClientOptions(fixture, config, `developer-${randomUUID()}`);
  const admin = new Admin({ ...options, retries: 0 }),
    producer = new Producer<Buffer | null, Buffer, Buffer, Buffer>({
      ...options,
      retries: 0,
      autocreateTopics: false,
      repeatOnStaleMetadata: false,
    });
  const backend = createKafkaBackend(),
    registry = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp());
  const context = {
    baseUrl: fixture.schemaRegistryEndpoint,
    authorization: async (): Promise<string> =>
      `Bearer ${await fetchFixtureToken(fixture, config)}`,
  };
  const topic = `streamskope-developer-${randomUUID()}`,
    subject = `${topic}-value`,
    signal = AbortSignal.timeout(60000);
  await mkdir(".artifacts", { recursive: true });
  const folder = await mkdtemp(join(process.cwd(), ".artifacts", "developer-real-"));
  let created = false,
    registered = false;
  try {
    const schema = JSON.stringify({
      type: "object",
      properties: { id: { type: "integer" }, label: { type: "string" } },
      required: ["id", "label"],
      additionalProperties: false,
    });
    const registration = await registry.register(
      context,
      { subject, version: "latest", normalize: false, schemaType: "JSON", schema, references: [] },
      signal,
    );
    registered = true;
    expect(
      await backend.execute({
        command: "connection.connect",
        id: "connect",
        version: HOST_PROTOCOL_VERSION,
        payload: connection,
      }),
    ).toMatchObject({ ok: true });
    const generated = parseHostCommandResponse(
      await backend.execute({
        command: "schemas.client",
        id: "client",
        version: HOST_PROTOCOL_VERSION,
        payload: { subject, version: 1 },
      }),
    );
    if (!generated.ok || generated.command !== "schemas.client")
      throw new Error(!generated.ok ? generated.error.summary : "Unexpected client response");
    expect(generated.result.client).toMatchObject({
      subject,
      version: 1,
      schemaId: registration.id,
      generator: "Ajv 8.20.0 standalone (MIT)",
    });
    const modulePath = join(folder, "client.cjs");
    await writeFile(modulePath, generated.result.client.source);
    await promisify(execFile)(process.execPath, ["--check", modulePath]);
    const client = createRequire(import.meta.url)(modulePath) as {
      send(producer: unknown, topic: string, value: unknown): Promise<unknown>;
      decode(value: Buffer): unknown;
    };
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    created = true;
    const value = { id: 42, label: "generated" };
    await client.send(producer, topic, value);
    await producer.send({
      messages: [
        {
          topic,
          key: Buffer.from("private-key"),
          value: Buffer.from('{"id":1,"label":"visible","private":"secret-value"}'),
          headers: new Map([[Buffer.from("secret"), Buffer.from("secret-header")]]),
        },
        {
          topic,
          key: Buffer.from("other-key"),
          value: Buffer.from('{"id":2,"label":"other","private":"secret-value"}'),
        },
      ],
    });
    const active = await new StreamSkopeKafkaEngine().openConnection(connection, signal);
    try {
      const stream = await active.openMessageStream(
        { topic, mode: "earliest", maxMessages: 1 },
        signal,
      );
      const received: unknown[] = [];
      try {
        for await (const record of stream) {
          if (record.original?.state !== "complete" || record.original.value === null)
            throw new Error("Original bytes missing");
          received.push(client.decode(Buffer.from(record.original.value, "base64")));
        }
      } finally {
        await stream.close();
      }
      expect(received).toEqual([value]);
    } finally {
      await active.close();
    }
    const protection = {
      readOnly: true,
      maskKey: true,
      maskHeaders: ["secret"],
      valuePaths: ["/private"],
    };
    const cliConfig = { connection, protection };
    const inspected: unknown[] = [];
    await runReadOnlyCli(
      "inspect",
      cliConfig,
      undefined,
      {
        write: (v): Promise<void> => {
          inspected.push(v);
          return Promise.resolve();
        },
      },
      signal,
    );
    expect(inspected).toEqual([
      expect.objectContaining({
        kind: "inspection",
        topics: expect.arrayContaining([topic]) as unknown,
      }),
    ]);
    const query = { topic, mode: "earliest", maxMessages: 10 };
    const results: unknown[] = [];
    const collect = {
      write: (v: unknown): Promise<void> => {
        results.push(v);
        return Promise.resolve();
      },
    };
    await runReadOnlyCli("query", cliConfig, query, collect, signal);
    expect(results).toHaveLength(4);
    expect(results[1]).toMatchObject({
      kind: "record",
      record: {
        offset: "1",
        key: "[MASKED]",
        headers: { secret: "[MASKED]" },
        original: { state: "unavailable", reason: "masked" },
        payload: '{"id":1,"label":"visible","private":"[MASKED]"}',
        structured: {
          protection: "masked",
          value: {
            state: "decoded",
            codec: "json",
            json: '{"id":1,"label":"visible","private":"[MASKED]"}',
          },
        },
      },
    });
    expect(results[3]).toMatchObject({
      kind: "summary",
      count: 3,
      complete: true,
      coverage: { scannedRecords: 3, matchedRecords: 3, reason: "range-complete" },
    });
    const search = { key: "", value: "secret-value", offset: "", timestamp: "", partition: null };
    const protectedRecords = [...results];
    results.length = 0;
    await runReadOnlyCli("query", cliConfig, { ...query, search }, collect, signal);
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({
      kind: "summary",
      count: 0,
      coverage: { scannedRecords: 3, matchedRecords: 0 },
    });
    expect(JSON.stringify(results)).not.toContain("secret-value");
    const unmasked: unknown[] = [];
    await runReadOnlyCli(
      "query",
      {
        connection,
        protection: { readOnly: true, maskKey: false, maskHeaders: [], valuePaths: [] },
      },
      { ...query, search: { ...search, value: "visible" } },
      {
        write: (v): Promise<void> => {
          unmasked.push(v);
          return Promise.resolve();
        },
      },
      signal,
    );
    expect(unmasked).toHaveLength(2);
    expect(unmasked[0]).toMatchObject({ kind: "record", record: { offset: "1" } });
    expect(unmasked[1]).toMatchObject({
      kind: "summary",
      count: 1,
      complete: true,
      coverage: { scannedRecords: 3, matchedRecords: 1 },
    });
    const privatePath = join(folder, "config.json"),
      queryPath = join(folder, "query.json"),
      output = join(folder, "export.ndjson");
    await writeFile(privatePath, JSON.stringify(cliConfig), { mode: 0o600 });
    await writeFile(queryPath, JSON.stringify(query));
    const executed = await promisify(execFile)(
      process.execPath,
      [
        "--import",
        "tsx",
        "tools/cli.ts",
        "export",
        "--config",
        privatePath,
        "--query",
        queryPath,
        "--output",
        output,
      ],
      { timeout: 30000 },
    );
    expect(executed.stderr).toBe("");
    expect(executed.stdout).toBe("");
    const exported = await readFile(output, "utf8");
    expect(exported).not.toContain("secret-value");
    expect(exported).not.toContain("private-key");
    expect(exported).not.toContain("secret-header");
    const lines = exported
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as unknown);
    expect(lines.slice(0, 3)).toEqual(protectedRecords.slice(0, 3));
    expect(lines[3]).toMatchObject({
      kind: "summary",
      operation: "export",
      count: 3,
      complete: true,
    });
    if (process.platform !== "win32") expect((await stat(output)).mode & 0o777).toBe(0o600);
  } finally {
    await backend.shutdown();
    await producer.close();
    try {
      if (created) await admin.deleteTopics({ topics: [topic] });
    } finally {
      await admin.close();
    }
    if (registered)
      await registry.delete(
        context,
        { target: { kind: "subject", subject }, mode: "permanent", confirmation: subject },
        AbortSignal.timeout(10000),
      );
    await rm(folder, { recursive: true, force: true });
  }
}, 90000);
