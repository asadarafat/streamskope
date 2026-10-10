import avro from "avsc";
import protobuf from "protobufjs";
import Ajv from "ajv";
import { expect, it } from "vitest";

import { authorSchemaRecord } from "../../src/features/kafka/engine/schema-record-encoder";
import {
  parseSchemaAuthoringInput,
  parseSchemaAuthoringResult,
  type SchemaAuthoringResult,
} from "../../src/features/kafka/contracts/schema-authoring";
import type { CodecSchemaBundle } from "../../src/features/kafka/application/record-codec-types";
import { createHostRecordCodec } from "../../src/platform/node/record-codec";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";

const input = { subject: "writer", version: 1, schemaId: 17, messageType: "", payload: "{}" };
function bundle(schemaType: "AVRO" | "PROTOBUF" | "JSON", schema: string): CodecSchemaBundle {
  return { root: { id: 17, schemaType, references: [], schema }, dependencies: [] };
}
function author(
  graph: CodecSchemaBundle,
  payload: string,
  messageType = "",
): SchemaAuthoringResult {
  return authorSchemaRecord({
    kind: "author",
    input: { ...input, payload, messageType },
    bundle: graph,
  });
}
function bytes(result: SchemaAuthoringResult): Buffer {
  expect(result.state).toBe("valid");
  if (result.state !== "valid") throw new Error(JSON.stringify(result.issues));
  return Buffer.from(result.record.value!, "base64");
}
it("independently decodes edited Avro reference values, exact int64, named branches and byte strings", () => {
  const detail = '{"type":"record","name":"Detail","fields":[{"name":"id","type":"long"}]}';
  const schema =
    '{"type":"record","name":"Event","fields":[{"name":"detail","type":"Detail"},{"name":"choice","type":["null","string"]},{"name":"bytes","type":"bytes"}]}';
  const graph = {
    root: {
      ...bundle("AVRO", schema).root,
      references: [{ name: "Detail", subject: "details", version: 1 }],
    },
    dependencies: [
      {
        name: "Detail",
        schema: { id: 18, schemaType: "AVRO" as const, references: [], schema: detail },
      },
    ],
  };
  const result = author(
    graph,
    '{"detail":{"id":"9223372036854775807"},"choice":{"string":"edited"},"bytes":"ÿ\\u0000"}',
  );
  const wire = bytes(result);
  expect(wire.subarray(0, 5).toString("hex")).toBe("0000000011");
  // Independent decoder keeps int64 as its raw eight-byte representation.
  const long = avro.types.LongType.__with({
    fromBuffer: (b: Buffer) => b.readBigInt64LE(),
    toBuffer: () => Buffer.alloc(8),
    fromJSON: (v: string) => BigInt(v),
    toJSON: (v: bigint) => v.toString(),
    isValid: (v: unknown) => typeof v === "bigint",
    compare: () => 0,
  });
  const registry = {
    Detail: avro.Type.forSchema(JSON.parse(detail) as avro.Schema, {
      typeHook: (s) => (s === "long" ? long : undefined),
    }),
  };
  const type = avro.Type.forSchema(JSON.parse(schema) as avro.Schema, {
    registry,
    wrapUnions: "always",
  });
  expect(type.fromBuffer(wire.subarray(5))).toMatchObject({
    detail: { id: 9223372036854775807n },
    choice: { string: "edited" },
    bytes: Buffer.from([255, 0]),
  });
});
it("frames top-level Avro bytes directly and preserves logical types' underlying values", () => {
  expect(bytes(author(bundle("AVRO", '"bytes"'), '"\\u0000ÿ"')).subarray(5)).toEqual(
    Buffer.from([0, 255]),
  );
  const result = author(
    bundle("AVRO", '{"type":"long","logicalType":"timestamp-millis"}'),
    '"1700000000000"',
  );
  const independent = avro.Type.forSchema("long");
  expect(independent.fromBuffer(bytes(result).subarray(5))).toBe(1700000000000);
});
it.each([
  ['{"known":1,"foreign":"private-payload-sentinel"}', "schema", "/foreign"],
  ['{"known":9007199254740993}', "precision", ""],
  ['{"known":true}', "schema", ""],
  ["{", "json", ""],
])("rejects invalid Avro input safely: %s", (payload, code, path) => {
  const result = author(
    bundle("AVRO", '{"type":"record","name":"R","fields":[{"name":"known","type":"int"}]}'),
    payload,
  );
  expect(result).toMatchObject({ state: "invalid", issues: [{ code, path }] });
  expect(JSON.stringify(result)).not.toContain("private-payload-sentinel");
  expect(result).not.toHaveProperty("record");
});
it("encodes the selected nested Protobuf message without conversion loss", () => {
  const schema =
    'syntax="proto3"; package author; message Other { string ignored=1; } message Outer { message Event { int64 id=1; bytes data=2; oneof choice { string text=3; bool enabled=4; } map<string,int32> counts=5; } }';
  const result = author(
    bundle("PROTOBUF", schema),
    '{"id":"9223372036854775807","data":"AP8=","text":"edited","counts":{"a":7}}',
    "author.Outer.Event",
  );
  const wire = bytes(result);
  expect(wire.subarray(5, 8).toString("hex")).toBe("040200");
  const independent = protobuf
    .parse(schema, { keepCase: true })
    .root.lookupType("author.Outer.Event");
  expect(
    independent.toObject(independent.decode(wire.subarray(8)), { longs: String, bytes: String }),
  ).toEqual({ id: "9223372036854775807", data: "AP8=", text: "edited", counts: { a: 7 } });
});
it.each([
  '{"id":3}',
  '{"id":"9223372036854775808"}',
  '{"amount":"3"}',
  '{"amount":2147483648}',
  '{"state":7}',
  '{"data":"%%%"}',
  '{"a":"first","b":true}',
  '{"undeclared":1}',
  '{"nested":{"undeclared":1}}',
])("refuses Protobuf coercion/unknown fields/invalid oneof: %s", (payload) => {
  const schema =
    'syntax="proto3"; message R { int64 id=1; int32 amount=2; enum State { OFF=0; ON=1; } State state=3; bytes data=4; oneof choice { string a=5; bool b=6; } message Child { string name=1; } Child nested=7; }';
  expect(author(bundle("PROTOBUF", schema), payload)).toMatchObject({ state: "invalid" });
});
it("validates edited JSON draft07 against an independently compiled referenced schema", () => {
  const graph = {
    root: {
      ...bundle(
        "JSON",
        '{"type":"object","required":["id"],"additionalProperties":false,"properties":{"id":{"$ref":"detail"}}}',
      ).root,
      references: [{ name: "detail", subject: "detail", version: 1 }],
    },
    dependencies: [
      {
        name: "detail",
        schema: {
          id: 19,
          schemaType: "JSON" as const,
          references: [],
          schema: '{"type":"string","pattern":"^[A-Z]{4}$"}',
        },
      },
    ],
  };
  const result = author(graph, '{"id":"TEST"}');
  const wire = bytes(result);
  expect(wire.toString()).toBe('{"id":"TEST"}');
  const independent = new Ajv()
    .addSchema(JSON.parse(graph.dependencies[0]!.schema.schema) as object, "detail")
    .compile(JSON.parse(graph.root.schema) as object);
  expect(independent(JSON.parse(wire.toString()) as unknown)).toBe(true);
  expect(author(graph, '{"id":"private-payload-sentinel"}')).toEqual({
    state: "invalid",
    issues: [{ code: "schema", path: "/id", detail: "Payload failed JSON Schema pattern." }],
  });
});
it("bounds worker input/output and rejects unsupported external references/dialects", async () => {
  expect(() =>
    parseSchemaAuthoringInput({ ...input, payload: '"' + "€".repeat(6000) + '"' }),
  ).toThrow();
  for (const schema of [
    '{"$ref":"https://untrusted.invalid/schema"}',
    '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}',
  ])
    expect(author(bundle("JSON", schema), '"value"')).toMatchObject({
      state: "invalid",
      issues: [{ code: "unsupported" }],
    });
  const worker = createHostRecordCodec();
  const result = await worker.author(
    { ...input, payload: '"edited"' },
    bundle("AVRO", '"string"'),
    new AbortController().signal,
  );
  expect(bytes(result).subarray(5)).toEqual(Buffer.from([12, ...Buffer.from("edited")]));
});
it("seals input/result contracts and validates authoring through the current host protocol", () => {
  expect(() => parseSchemaAuthoringInput({ ...input, version: "latest" })).toThrow();
  expect(() => parseSchemaAuthoringInput({ ...input, credentials: "never" })).toThrow();
  const command = parseHostCommand({
    command: "schemas.author",
    id: "author-1",
    version: HOST_PROTOCOL_VERSION,
    payload: input,
  });
  expect(command.command).toBe("schemas.author");
  const result = author(bundle("JSON", "{}"), "{}");
  expect(
    parseHostCommandResponse({
      command: "schemas.author",
      id: "author-1",
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "correlation", authoring: result },
    }),
  ).toMatchObject({ ok: true });
  expect(() => parseSchemaAuthoringResult({ state: "invalid", issues: [] })).toThrow();
  expect(() => parseSchemaAuthoringResult({ ...result, credentials: "never" })).toThrow();
});

it("rejects a valid-state response whose wire header or projection disagrees with its writer", () => {
  const result = author(bundle("AVRO", '"string"'), '"edited"');
  if (result.state !== "valid") throw new Error("Valid Avro fixture unavailable.");
  expect(() =>
    parseSchemaAuthoringResult({ ...result, writer: { ...result.writer, id: 99 } }),
  ).toThrow();
  expect(() => parseSchemaAuthoringResult({ ...result, messageType: "Unexpected" })).toThrow();
  const json = author(bundle("JSON", "{}"), "{}");
  if (json.state !== "valid") throw new Error("Valid JSON fixture unavailable.");
  expect(() => parseSchemaAuthoringResult({ ...json, json: "[]" })).toThrow();
});
