import { describe, expect, it } from "vitest";

import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  compileKafkaSearchFilter,
  type KafkaExploredMessage,
  type KafkaMessage,
  type KafkaRuleDefinition,
} from "../../src/features/kafka/contracts";
import type { RecordDecodeResult } from "../../src/features/kafka/contracts/record-codec";
import { compareDocuments } from "../../src/features/kafka/contracts/document-diff";
import type { CorrelationTraceInput } from "../../src/features/kafka/contracts/correlation-trace";
import { RecordCodecService } from "../../src/features/kafka/application/record-codec-service";
import type {
  RegisteredSchema,
  SchemaLookupPort,
} from "../../src/features/kafka/application/record-codec-types";
import { StructuredRecordService } from "../../src/features/kafka/application/structured-record-service";
import { protectKafkaRecord } from "../../src/features/kafka/application/record-protection";
import { matchCorrelation } from "../../src/features/kafka/application/correlation-selector";
import { InMemoryKafkaRuleStore } from "../../src/features/kafka/application/in-memory-rule-store";
import { KafkaLiveRuleRuntime } from "../../src/features/kafka/application/live-rule-runtime";
import { KafkaRuleService } from "../../src/features/kafka/application/rule-service";
import { parseStructuredRecord } from "../../src/features/kafka/engine/record-codec-parser";
import { translateKafkaRecord } from "../../src/features/kafka/engine/message-record";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine/rule-evaluator";
import {
  createKafkaMessageExportDocument,
  initialKafkaMessageFilters,
  selectKafkaQueryMessages,
} from "../../src/features/kafka/ui/message-operations";
import { evaluatedRuleResult } from "../support/workbench-fixtures";

const integer = "9223372036854775807";
const schemas: Readonly<Record<number, RegisteredSchema>> = {
  7: {
    id: 7,
    schemaType: "AVRO",
    references: [],
    schema:
      '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}',
  },
  8: {
    id: 8,
    schemaType: "AVRO",
    references: [],
    schema:
      '{"type":"record","name":"Event","fields":[{"name":"name","type":"string"},{"name":"id","type":"long"}]}',
  },
  9: {
    id: 9,
    schemaType: "PROTOBUF",
    references: [],
    schema: 'syntax="proto3"; message Event { int64 id=1; string name=2; }',
  },
};
const context = {
  baseUrl: "https://registry.example.test",
  authorization: (): Promise<undefined> => Promise.resolve(undefined),
};
const signal = (): AbortSignal => new AbortController().signal;
const expected = JSON.stringify({ id: integer, name: "ok" });
const frames = [
  { name: "JSON", bytes: Buffer.from(`{"id":${integer},"name":"ok"}`), schemaId: null },
  {
    name: "Avro writer 7",
    bytes: Buffer.from("0000000007feffffffffffffffff01046f6b", "hex"),
    schemaId: 7,
  },
  {
    name: "Avro writer 8",
    bytes: Buffer.from("0000000008046f6bfeffffffffffffffff01", "hex"),
    schemaId: 8,
  },
  {
    name: "Protobuf writer 9",
    bytes: Buffer.from("00000000090008ffffffffffffffff7f12026f6b", "hex"),
    schemaId: 9,
  },
] as const;

function services(): { service: StructuredRecordService; lookups: number[] } {
  const lookups: number[] = [];
  const lookup: SchemaLookupPort = {
    byId: (_context, id): Promise<RegisteredSchema> => {
      lookups.push(id);
      const result = schemas[id];
      return result
        ? Promise.resolve(result)
        : Promise.reject(new Error("Unavailable fixture schema"));
    },
    byVersion: (): Promise<RegisteredSchema> =>
      Promise.reject(new Error("No references in these independent fixtures")),
  };
  const codec = new RecordCodecService(lookup, {
    decode: (input, bundle): Promise<RecordDecodeResult> =>
      Promise.resolve(parseStructuredRecord({ input, bundle })),
  });
  return { service: new StructuredRecordService(codec), lookups };
}
function record(value: Buffer | null, offset = 0n): KafkaMessage {
  return translateKafkaRecord(
    {
      topic: "events",
      partition: 0,
      offset,
      timestamp: 1_500n,
      key: Buffer.from("key"),
      value,
      headers: new Map(),
      headerEntries: [
        [Buffer.from("cid"), Buffer.from("wrong")],
        [Buffer.from("cid"), Buffer.from(integer)],
        [Buffer.from("cid"), null],
        [Buffer.from("empty"), Buffer.alloc(0)],
      ],
    },
    "events",
  );
}
const explored = (message: KafkaMessage): KafkaExploredMessage => ({
  ...message,
  ruleEvaluation: evaluatedRuleResult,
});
const trace = (
  value = integer,
  source: "header" | "key" | "payload" = "payload",
  path = "/id",
): CorrelationTraceInput => ({
  traceId: "consistency",
  topics: ["events"],
  startTimeMs: 1_000,
  endTimeMs: 2_000,
  value,
  selector: { source, path, format: "json" },
});
async function rules(expression: string): Promise<KafkaLiveRuleRuntime> {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const definition: KafkaRuleDefinition = {
    name: "Consistency",
    expression,
    cooldownMs: 0,
    level: "info",
    enabled: true,
  };
  const runtime = new KafkaLiveRuleRuntime(
    new KafkaRuleService(
      new InMemoryKafkaRuleStore(
        { durability: "session", state: "ready" },
        { rules: [definition] },
      ),
      evaluator,
    ),
    evaluator,
  );
  await runtime.prepare("events");
  return runtime;
}
function exported(message: KafkaMessage): Record<string, unknown> {
  const document = createKafkaMessageExportDocument({
    topic: "events",
    filters: initialKafkaMessageFilters,
    messages: [explored(message)],
    retainedMessageCount: 1,
    stale: false,
  });
  return JSON.parse(document.content) as Record<string, unknown>;
}

describe("one record interpretation across supported workflows", () => {
  it.each(frames)(
    "keeps $name values identical for display, filters, rules, comparison, tracing and export",
    async ({ bytes, schemaId }) => {
      const source = record(bytes);
      const original = JSON.stringify(source.original);
      const { service } = services();
      const message = await service.prepare(
        source,
        { key: "auto", value: "auto" },
        context,
        signal(),
      );
      const field = message.structured?.value;
      expect(field?.state).toBe("decoded");
      if (field?.state !== "decoded") throw new Error("Projection missing");
      expect(JSON.parse(field.json!)).toEqual(JSON.parse(expected));
      expect(field.writerSchema?.id ?? null).toBe(schemaId);
      expect(message.payload).toBe(field.text);
      expect(JSON.parse(message.payload!)).toEqual(JSON.parse(expected));
      const filters = { ...initialKafkaMessageFilters, expression: `$.id == "${integer}"` };
      expect(compileKafkaSearchFilter(filters)(message)).toBe("matched");
      expect(selectKafkaQueryMessages([explored(message)], filters)).toMatchObject({
        unavailable: 0,
        messages: [message],
      });
      expect((await rules(filters.expression)).evaluate(message)).toMatchObject({
        activeMatchCount: 1,
        errorCount: 0,
      });
      expect(compareDocuments(expected, field.json!, "json")).toEqual({ rows: [], limited: false });
      expect(await matchCorrelation(message, trace(), undefined, null, signal())).toBe("matched");
      expect(
        await matchCorrelation(message, trace(integer, "header", "cid"), undefined, null, signal()),
      ).toBe("matched");
      expect(exported(message)).toMatchObject({
        messages: [
          { payload: message.payload, structured: message.structured, original: message.original },
        ],
      });
      expect(JSON.stringify(message.original)).toBe(original);
      expect(JSON.stringify(source.original)).toBe(original);
    },
  );

  it("preserves each writer identity in one mixed-schema read without topic-level schema guessing", async () => {
    const { service, lookups } = services();
    const results = [];
    for (const [index, input] of frames.entries()) {
      results.push(
        await service.prepare(
          record(input.bytes, BigInt(index)),
          { key: "auto", value: "auto" },
          context,
          signal(),
        ),
      );
    }
    expect(lookups).toEqual([7, 8, 9]);
    expect(results.map((message) => message.structured?.value.writerSchema?.id ?? null)).toEqual([
      null,
      7,
      8,
      9,
    ]);
    expect(results.map((message) => JSON.parse(message.payload!) as unknown)).toEqual(
      frames.map(() => JSON.parse(expected) as unknown),
    );
  });

  it.each([
    ["malformed JSON", Buffer.from('{"id":')],
    ["malformed UTF-8", Buffer.from([0xff, 0xfe])],
    ["short Confluent framing", Buffer.from([0, 0, 0])],
    ["missing writer schema", Buffer.from("000000006301", "hex")],
    ["malformed Avro", Buffer.from("000000000780", "hex")],
  ])(
    "keeps %s unavailable without mislabelling it as a tombstone or negative field match",
    async (_name, bytes) => {
      const { service } = services();
      const source = record(bytes);
      const message = await service.prepare(
        source,
        { key: "auto", value: "auto" },
        context,
        signal(),
      );
      expect(message.structured?.value.state).toBe("error");
      expect(message.original).toEqual(source.original);
      expect(
        compileKafkaSearchFilter({
          ...initialKafkaMessageFilters,
          expression: '$.id == "not-present"',
        })(message),
      ).toBe("unavailable");
      expect(await matchCorrelation(message, trace(), undefined, null, signal())).toBe(
        "unavailable",
      );
      expect(exported(message)).toMatchObject({
        messages: [{ structured: { value: { state: "error" } } }],
      });
    },
  );

  it("distinguishes Kafka tombstones, empty text and JSON null across projection and export", async () => {
    const { service } = services();
    const results = [];
    for (const value of [null, Buffer.alloc(0), Buffer.from("null")]) {
      results.push(
        await service.prepare(record(value), { key: "auto", value: "auto" }, context, signal()),
      );
    }
    expect(results[0]!.structured?.value.state).toBe("null");
    expect(results[1]!.structured?.value).toMatchObject({ state: "decoded", text: "", json: null });
    expect(results[2]!.structured?.value).toMatchObject({ state: "decoded", json: "null" });
    expect(
      results.map((message) =>
        message.original?.state === "complete" ? message.original.value : undefined,
      ),
    ).toEqual([null, "", "bnVsbA=="]);
    expect(results.map((message) => exported(message))).toMatchObject([
      { messages: [{ structured: { value: { state: "null" } } }] },
      { messages: [{ structured: { value: { state: "decoded", text: "", json: null } } }] },
      { messages: [{ structured: { value: { state: "decoded", json: "null" } } }] },
    ]);
  });

  it("retains ordered duplicate, null and empty headers through protection and export", async () => {
    const { service } = services();
    const message = await service.prepare(
      record(frames[0].bytes),
      { key: "auto", value: "auto" },
      context,
      signal(),
    );
    expect(message.structured?.headers).toEqual([
      { key: "cid", value: "wrong", error: null },
      { key: "cid", value: integer, error: null },
      { key: "cid", value: null, error: null },
      { key: "empty", value: "", error: null },
    ]);
    const masked = protectKafkaRecord(message, {
      ...KAFKA_RECORD_PROTECTION_DEFAULTS,
      maskHeaders: ["cid"],
    });
    expect(masked.original).toEqual({ state: "unavailable", reason: "masked" });
    expect(masked.structured?.headers).toEqual([
      { key: "cid", value: "[MASKED]", error: null },
      { key: "cid", value: "[MASKED]", error: null },
      { key: "cid", value: null, error: null },
      { key: "empty", value: "", error: null },
    ]);
    expect(exported(masked)).toMatchObject({
      messages: [{ structured: { headers: masked.structured?.headers } }],
    });
    expect(message.structured?.headers[1]?.value).toBe(integer);
  });

  it("keeps invalid header bytes explicit without inventing a decoded value or disclosing masked originals", async () => {
    const { service } = services();
    const source = translateKafkaRecord(
      {
        topic: "events",
        partition: 0,
        offset: 0n,
        timestamp: 1_500n,
        key: null,
        value: Buffer.from("{}"),
        headers: new Map(),
        headerEntries: [
          [Buffer.from("token"), Buffer.from([0xff, 0x00])],
          [Buffer.from([0xff]), Buffer.from("private-header")],
        ],
      },
      "events",
    );
    const message = await service.prepare(
      source,
      { key: "auto", value: "auto" },
      context,
      signal(),
    );
    expect(message.structured?.headers).toHaveLength(2);
    expect(message.structured?.headers[0]?.key).toBe("token");
    for (const header of message.structured!.headers) {
      expect(header.value).toBeNull();
      expect(header.error).not.toBeNull();
    }
    expect(
      await matchCorrelation(
        message,
        trace("private-header", "header", "token"),
        undefined,
        null,
        signal(),
      ),
    ).toBe("unavailable");
    const masked = protectKafkaRecord(message, {
      ...KAFKA_RECORD_PROTECTION_DEFAULTS,
      maskHeaders: ["token"],
    });
    expect(masked.original).toEqual({ state: "unavailable", reason: "masked" });
    expect(JSON.stringify(masked)).not.toContain("private-header");
    expect(JSON.stringify(exported(masked))).not.toContain(
      Buffer.from("private-header").toString("base64"),
    );
    expect(message.original).toEqual(source.original);
  });

  it.each(frames)(
    "applies $name field masking before any visible workflow while retaining the source unchanged",
    async ({ bytes }) => {
      const { service } = services();
      const source = record(bytes);
      const message = await service.prepare(
        source,
        { key: "auto", value: "auto" },
        context,
        signal(),
      );
      const masked = protectKafkaRecord(message, {
        ...KAFKA_RECORD_PROTECTION_DEFAULTS,
        maskKey: true,
        maskHeaders: ["cid"],
        valuePaths: ["/id"],
      });
      expect(masked.structured?.protection).toBe("masked");
      expect(JSON.parse(masked.payload!)).toEqual({ id: "[MASKED]", name: "ok" });
      expect(
        compileKafkaSearchFilter({ ...initialKafkaMessageFilters, expression: '$.name == "ok"' })(
          masked,
        ),
      ).toBe("matched");
      expect(
        compileKafkaSearchFilter({
          ...initialKafkaMessageFilters,
          expression: `$.id == "${integer}"`,
        })(masked),
      ).toBe("not-matched");
      expect((await rules('$.name == "ok"')).evaluate(masked)).toMatchObject({
        activeMatchCount: 1,
      });
      expect(
        await matchCorrelation(masked, trace("ok", "payload", "/name"), undefined, null, signal()),
      ).toBe("matched");
      expect(await matchCorrelation(masked, trace(), undefined, null, signal())).not.toBe(
        "matched",
      );
      expect(JSON.stringify(exported(masked))).not.toContain(integer);
      expect(JSON.stringify(masked)).not.toContain(integer);
      expect(message.original).toEqual(source.original);
      expect(JSON.parse(message.payload!)).toMatchObject({ id: integer });
    },
  );

  it("records the actual writer identity when a manual codec disagrees with the Registry", async () => {
    const { service } = services();
    const source = record(frames[3].bytes);
    const message = await service.prepare(
      source,
      { key: "utf8", value: "avro" },
      context,
      signal(),
    );
    expect(message.structured?.value).toMatchObject({
      state: "error",
      codec: "avro",
      code: "schema-type",
      writerSchema: { id: 9, format: "protobuf", registry: new URL(context.baseUrl).toString() },
    });
    expect(message.original).toEqual(source.original);
  });

  it("honors manual text and bytes overrides without Registry lookup or changing originals", async () => {
    const { service, lookups } = services();
    const source = record(frames[0].bytes);
    const text = await service.prepare(source, { key: "utf8", value: "utf8" }, context, signal());
    expect(text.structured?.value).toMatchObject({
      state: "decoded",
      codec: "utf8",
      text: frames[0].bytes.toString("utf8"),
      json: null,
      writerSchema: null,
    });
    const binary = await service.prepare(
      record(frames[1].bytes),
      { key: "bytes", value: "bytes" },
      context,
      signal(),
    );
    expect(binary.structured?.value).toMatchObject({
      state: "decoded",
      codec: "bytes",
      text: frames[1].bytes.toString("base64"),
      json: null,
      writerSchema: null,
    });
    expect(lookups).toEqual([]);
    expect(text.original).toEqual(source.original);
  });
});
