import { createHash, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { Admin, Producer } from "@platformatic/kafka";
import { afterAll, beforeAll, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseHostEvent,
  type HostCommand,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import type {
  RecordExportInput,
  RecordExportOperation,
  RecordExportSnapshot,
} from "../../src/features/kafka/contracts/record-export";
import {
  parseRecordExportReceipt,
  parseRecordExportSnapshot,
} from "../../src/features/kafka/contracts/record-export-validation";
import { createKafkaBackend, type NodeKafkaBackend } from "../../src/platform/node/kafka-backend";
import { fetchFixtureToken, loadFixtureConfig } from "../support/kafka-fixture";
import {
  disposeNativeFixtureResources,
  startNativeKafkaFixture,
} from "../support/native-kafka-fixture";
import { createSchemaRegistryProtocolFixture } from "../support/schema-registry-protocol-fixture";

let fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>>;
beforeAll(async () => {
  fixture = await startNativeKafkaFixture();
}, 180_000);
afterAll(async () => {
  await fixture?.dispose();
}, 30_000);

interface RecordInput {
  readonly partition: number;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers?: Map<Buffer, Buffer>;
}
interface TopicContext {
  readonly topic: string;
  readonly backend: NodeKafkaBackend;
  readonly snapshots: RecordExportSnapshot[];
  readonly connect: () => Promise<void>;
  readonly seed: (records: readonly RecordInput[]) => Promise<void>;
  readonly execute: (
    command: HostCommand["command"],
    payload: unknown,
  ) => ReturnType<NodeKafkaBackend["execute"]>;
  readonly start: (overrides?: Partial<RecordExportInput>) => Promise<string>;
  readonly finished: (jobId: string) => Promise<RecordExportOperation>;
}
async function withTopic(work: (context: TopicContext) => Promise<void>): Promise<void> {
  const config = await loadFixtureConfig();
  const connection = {
    kafkaEndpoint: fixture.environment.STREAMSKOPE_TEST_KAFKA_ENDPOINT!,
    oauthEndpoint: fixture.environment.STREAMSKOPE_TEST_OAUTH_ENDPOINT!,
    caPath: fixture.environment.STREAMSKOPE_TEST_CA_PATH!,
  };
  const caPem = await readFile(connection.caPath, "utf8");
  const options = {
    bootstrapBrokers: [connection.kafkaEndpoint],
    clientId: `streaming-export-${randomUUID()}`,
    retries: 0,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    sasl: { mechanism: "OAUTHBEARER" as const, token: await fetchFixtureToken(connection, config) },
    tls: { ca: [caPem], rejectUnauthorized: true },
  };
  const admin = new Admin(options);
  const producer = new Producer<Buffer | null, Buffer | null, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
  });
  const backend = createKafkaBackend();
  const snapshots: RecordExportSnapshot[] = [];
  backend.subscribe((wire) => {
    const event = parseHostEvent(JSON.parse(JSON.stringify(wire)));
    if (event.event === "records.export.changed") snapshots.push(event.payload);
  });
  const registry = createSchemaRegistryProtocolFixture([
    {
      id: 7,
      subject: "event-avro",
      version: 1,
      schemaType: "AVRO",
      references: [],
      schema:
        '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}',
    },
    {
      id: 8,
      subject: "event-protobuf",
      version: 1,
      schemaType: "PROTOBUF",
      references: [],
      schema: 'syntax="proto3"; message Event { int64 id=1; string name=2; }',
    },
  ]);
  const topic = `streaming-export-${randomUUID()}`;
  const execute: TopicContext["execute"] = (command, payload) =>
    backend.execute({
      command,
      payload,
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
    } as HostCommand);
  let created = false;
  try {
    const registryUrl = await registry.listen();
    const input: SecureConnectionInput = {
      name: "Streaming export fixture",
      brokers: [connection.kafkaEndpoint],
      oauth: {
        clientId: config.oauthClientId,
        clientSecret: config.oauthClientSecret,
        scope: config.oauthScope,
        tokenEndpoint: connection.oauthEndpoint,
      },
      tls: { enabled: true, caPem },
      services: { schemaRegistry: { baseUrl: registryUrl, authentication: "none" } },
    };
    const topics = await admin.createTopics({ topics: [topic], partitions: 3, replicas: 1 });
    created = true;
    const topicId = topics.find((item) => item.name === topic)?.id;
    await vi.waitFor(
      async () => {
        const metadata = await producer.metadata({
          topics: [topic],
          forceUpdate: true,
          autocreateTopics: false,
        });
        const item = metadata.topics.get(topic);
        expect(item?.id).toBe(topicId);
        expect(item?.partitions).toHaveLength(3);
        for (const partition of item?.partitions ?? [])
          expect(partition.isr).toContain(partition.leader);
      },
      { timeout: 15_000, interval: 100 },
    );
    await work({
      topic,
      backend,
      snapshots,
      execute,
      connect: async (): Promise<void> => {
        const response = await execute("connection.connect", input);
        expect(response, JSON.stringify(response)).toMatchObject({ ok: true });
      },
      seed: async (records): Promise<void> => {
        for (let offset = 0; offset < records.length; offset += 250)
          await producer.send({
            messages: records.slice(offset, offset + 250).map((record) => ({ topic, ...record })),
          });
      },
      start: async (overrides = {}): Promise<string> => {
        const request: RecordExportInput = {
          requestId: randomUUID(),
          topic,
          range: { mode: "earliest" },
          format: "jsonl",
          maxRecords: 100_000,
          search: { key: "", value: "", offset: "", timestamp: "", partition: null },
          ...overrides,
        };
        const response = await execute("records.export.start", request);
        expect(response, JSON.stringify(response)).toMatchObject({ ok: true });
        if (!response.ok || response.command !== "records.export.start")
          throw new Error("Export admission failed.");
        const snapshot = parseRecordExportSnapshot(response.result.snapshot);
        return snapshot.operation!.jobId;
      },
      finished: async (jobId): Promise<RecordExportOperation> => {
        let operation: RecordExportOperation | undefined;
        await vi.waitFor(
          () => {
            operation =
              snapshots
                .map((snapshot) => snapshot.operation)
                .find(
                  (item) =>
                    item?.jobId === jobId &&
                    ["completed", "partial", "failed"].includes(item.state),
                ) ?? undefined;
            expect(operation, "Export must report a terminal operation").toBeDefined();
          },
          { timeout: 240_000, interval: 100 },
        );
        expect(operation, JSON.stringify(operation)).not.toMatchObject({ state: "failed" });
        return operation!;
      },
    });
  } finally {
    await disposeNativeFixtureResources([
      (): Promise<void> => backend.shutdown(),
      (): Promise<void> => producer.close(),
      async (): Promise<void> => {
        if (created) await admin.deleteTopics({ topics: [topic] });
      },
      (): Promise<void> => admin.close(),
      (): Promise<void> => registry.close(),
    ]);
  }
}

async function readArtifact(
  backend: NodeKafkaBackend,
  artifactId: string,
  part: "data" | "receipt",
  consume: (chunk: Uint8Array) => void,
): Promise<{ bytes: number; sha256: string }> {
  const hash = createHash("sha256");
  let bytes = 0;
  await backend.exportFiles.withDownload(
    { artifactId, part },
    { signal: new AbortController().signal, assertCurrent: (): void => undefined },
    async (chunks): Promise<void> => {
      for await (const chunk of chunks) {
        bytes += chunk.byteLength;
        hash.update(chunk);
        consume(chunk);
      }
    },
  );
  return { bytes, sha256: hash.digest("hex") };
}

it("streams a three-partition canonical JSONL artifact larger than the renderer memory budget and excludes later appends", async () => {
  await withTopic(async (context) => {
    const count = 3_003;
    const padding = "x".repeat(7_200);
    for (let base = 0; base < count; base += 250)
      await context.seed(
        Array.from({ length: Math.min(250, count - base) }, (_, index) => ({
          partition: (base + index) % 3,
          key: Buffer.from(`key-${String(base + index)}`),
          value: Buffer.from(JSON.stringify({ id: base + index, padding })),
        })),
      );
    await context.connect();
    const jobId = await context.start();
    await vi.waitFor(
      () => {
        expect(
          context.snapshots.some(
            (snapshot) =>
              snapshot.operation?.jobId === jobId && snapshot.operation.counts.scannedRecords > 0,
          ),
        ).toBe(true);
      },
      { timeout: 30_000 },
    );
    await context.seed(
      [0, 1, 2].map((partition) => ({
        partition,
        key: Buffer.from("late"),
        value: Buffer.from('{"late":true}'),
      })),
    );
    const operation = await context.finished(jobId);
    expect(operation).toMatchObject({
      state: "completed",
      reason: "range-complete",
      counts: { writtenRecords: count, scannedRecords: count },
    });
    expect(operation.counts.passes).toBeGreaterThan(1);
    expect(operation.artifact?.output.bytes).toBeGreaterThan(64 * 1_048_576);
    expect(
      operation.coverage?.partitions.map((part) => ({
        partition: part.partition,
        start: part.startOffset,
        end: part.endOffset,
        next: part.nextOffset,
      })),
    ).toEqual([0, 1, 2].map((partition) => ({ partition, start: "0", end: "1001", next: "1001" })));
    const locators = new Set<string>();
    const decoder = new TextDecoder();
    let carry = "";
    const output = await readArtifact(
      context.backend,
      operation.artifact!.artifactId,
      "data",
      (chunk) => {
        carry += decoder.decode(chunk, { stream: true });
        while (carry.includes("\n")) {
          const end = carry.indexOf("\n");
          const row = JSON.parse(carry.slice(0, end)) as {
            partition: number;
            offset: string;
            structured: { value: { json: string } };
            original: { value: string };
          };
          carry = carry.slice(end + 1);
          const decoded = JSON.parse(row.structured.value.json) as { id: number; padding: string };
          expect(decoded.padding).toBe(padding);
          expect(JSON.parse(Buffer.from(row.original.value, "base64").toString("utf8"))).toEqual(
            decoded,
          );
          const locator = `${String(row.partition)}:${row.offset}`;
          expect(locators.has(locator)).toBe(false);
          locators.add(locator);
        }
      },
    );
    expect(carry + decoder.decode()).toBe("");
    expect(locators.size).toBe(count);
    expect(output).toEqual({
      bytes: operation.artifact!.output.bytes,
      sha256: operation.artifact!.output.sha256,
    });
    const receiptChunks: Uint8Array[] = [];
    const receiptBytes = await readArtifact(
      context.backend,
      operation.artifact!.artifactId,
      "receipt",
      (chunk) => receiptChunks.push(chunk),
    );
    const receipt = parseRecordExportReceipt(
      JSON.parse(Buffer.concat(receiptChunks).toString("utf8")),
    );
    expect(receipt.counts).toEqual(operation.counts);
    expect(receipt.coverage).toEqual(operation.coverage);
    expect(receipt.output.sha256).toBe(output.sha256);
    expect(receiptBytes).toEqual({
      bytes: operation.artifact!.receiptBytes,
      sha256: operation.artifact!.receiptSha256,
    });
    expect(await context.execute("records.export.discard", { jobId })).toMatchObject({ ok: true });
    expect(() =>
      context.backend.exportFiles.describe({
        artifactId: operation.artifact!.artifactId,
        part: "data",
      }),
    ).toThrow();
  });
}, 300_000);

it("exports protected mixed schemas, malformed records, tombstones and duplicate headers using the production reader", async () => {
  await withTopic(async (context) => {
    const duplicateHeaders = new Map([
      [Buffer.from("cid"), Buffer.from("one")],
      [Buffer.from("cid"), Buffer.from("two")],
    ]);
    await context.seed([
      {
        partition: 0,
        key: Buffer.from("sensitive-key"),
        value: Buffer.from('{"id":1,"name":"sensitive-name"}'),
        headers: duplicateHeaders,
      },
      { partition: 1, key: null, value: Buffer.from("000000000702046f6b", "hex") },
      { partition: 2, key: null, value: Buffer.from("000000000800080112026f6b", "hex") },
      { partition: 0, key: null, value: Buffer.from('{"malformed"') },
      { partition: 1, key: null, value: null },
    ]);
    expect(
      await context.execute("preferences.update", {
        patch: {
          protection: {
            ...KAFKA_RECORD_PROTECTION_DEFAULTS,
            maskKey: true,
            maskHeaders: ["cid"],
            valuePaths: ["/name"],
          },
        },
      }),
    ).toMatchObject({ ok: true });
    await context.connect();
    const operation = await context.finished(await context.start());
    expect(operation).toMatchObject({
      state: "completed",
      counts: { writtenRecords: 5, originalUnavailableRecords: 5 },
    });
    const chunks: Uint8Array[] = [];
    await readArtifact(context.backend, operation.artifact!.artifactId, "data", (chunk) =>
      chunks.push(chunk),
    );
    const text = Buffer.concat(chunks).toString("utf8");
    expect(text).not.toContain("sensitive-key");
    expect(text).not.toContain("sensitive-name");
    const rows = text
      .trimEnd()
      .split("\n")
      .map(
        (line) =>
          JSON.parse(line) as {
            structured: {
              protection: string;
              value: { state: string; text?: string; writerSchema?: { id: number } };
              headers: { key: string; value: string }[];
            };
            original: { state: string; reason: string };
          },
      );
    expect(
      rows.every(
        (row) => row.structured.protection === "masked" && row.original.reason === "masked",
      ),
    ).toBe(true);
    expect(
      rows.filter(
        (row) =>
          row.structured.value.writerSchema?.id === 7 ||
          row.structured.value.writerSchema?.id === 8,
      ),
    ).toHaveLength(2);
    expect(rows.some((row) => row.structured.value.state === "null")).toBe(true);
    expect(rows.some((row) => row.structured.value.state === "masked")).toBe(true);
    expect(rows.find((row) => row.structured.headers.length === 2)?.structured.headers).toEqual([
      { key: "cid", value: "[MASKED]", error: null },
      { key: "cid", value: "[MASKED]", error: null },
    ]);
    await context.execute("connection.disconnect", {});
    expect(() =>
      context.backend.exportFiles.describe({
        artifactId: operation.artifact!.artifactId,
        part: "data",
      }),
    ).toThrow();
  });
}, 90_000);
