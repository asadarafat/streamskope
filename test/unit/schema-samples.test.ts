import avro from "avsc";
import Ajv from "ajv";
import { expect, it } from "vitest";

import { generateSchemaSamples } from "../../src/features/kafka/engine/schema-sample-parser";
import { parseStructuredRecord } from "../../src/features/kafka/engine/record-codec-parser";
import {
  parseSchemaSampleInput,
  parseRecordBatchInput,
  parseRecordBatchOutcome,
  type SchemaSamples,
  type SchemaSampleInput,
} from "../../src/features/kafka/contracts/schema-samples";
import type { CodecSchemaBundle } from "../../src/features/kafka/application/record-codec-types";
import { createHostRecordCodec } from "../../src/platform/node/record-codec";

const input: SchemaSampleInput = {
  subject: "fixture",
  version: 1,
  seed: 123,
  count: 5,
  messageType: "",
};
function generate(
  bundle: CodecSchemaBundle,
  overrides: Partial<SchemaSampleInput> = {},
): SchemaSamples {
  const result = generateSchemaSamples({
    kind: "generate",
    input: { ...input, ...overrides },
    bundle,
  });
  expect(result.ok, result.ok ? undefined : result.detail).toBe(true);
  if (!result.ok) throw new Error(result.detail);
  return result.samples;
}
it("generates reproducible Avro records with defaults, enums, unions and a declared reference", () => {
  const bundle: CodecSchemaBundle = {
    root: {
      id: 7,
      schemaType: "AVRO",
      schema:
        '{"type":"record","name":"Event","fields":[{"name":"nested","type":"Detail"},{"name":"optional","type":["null","string"]},{"name":"status","type":{"type":"enum","name":"State","symbols":["ON","OFF"]}},{"name":"fixed","type":"int","default":42}]}',
      references: [{ name: "Detail", subject: "detail", version: 1 }],
    },
    dependencies: [
      {
        name: "Detail",
        schema: {
          id: 8,
          schemaType: "AVRO",
          references: [],
          schema: '{"type":"record","name":"Detail","fields":[{"name":"name","type":"string"}]}',
        },
      },
    ],
  };
  const first = generate(bundle);
  expect(generate(bundle)).toEqual(first);
  expect(generate(bundle, { seed: 124 })).not.toEqual(first);
  const registry = {
    Detail: avro.Type.forSchema(JSON.parse(bundle.dependencies[0]!.schema.schema) as avro.Schema),
  };
  const independent = avro.Type.forSchema(JSON.parse(bundle.root.schema) as avro.Schema, {
    registry,
    wrapUnions: "always",
  });
  for (const sample of first.samples) {
    const bytes = Buffer.from(sample.record.value!, "base64");
    expect(bytes.readUInt32BE(1)).toBe(7);
    const decoded: unknown = independent.fromBuffer(bytes.subarray(5));
    expect(independent.isValid(decoded)).toBe(true);
    expect(decoded).toMatchObject({ fixed: 42 });
  }
});
it("frames the selected nested Protobuf message and preserves explicit defaults and oneof selection", () => {
  const bundle: CodecSchemaBundle = {
    root: {
      id: 9,
      schemaType: "PROTOBUF",
      references: [],
      schema:
        'syntax="proto2"; package fixture; message Other { required int32 other=1; } message Outer { message Event { required int32 count=1 [default=7]; oneof detail { string a=2; int64 b=3; } } }',
    },
    dependencies: [],
  };
  const samples = generate(bundle, { messageType: "fixture.Outer.Event" });
  for (const sample of samples.samples) {
    expect(Buffer.from(sample.record.value!, "base64").subarray(5, 8).toString("hex")).toBe(
      "040200",
    );
    const parsed = parseStructuredRecord({
      input: { format: "protobuf", bytes: sample.record.value },
      bundle,
    });
    expect(parsed.state).toBe("decoded");
    if (parsed.state === "decoded") {
      const value = JSON.parse(parsed.json) as Record<string, unknown>;
      expect(value.count).toBe(7);
      expect(Object.keys(value).filter((key) => key === "a" || key === "b")).toHaveLength(1);
    }
  }
  expect(
    generateSchemaSamples({
      kind: "generate",
      input: { ...input, messageType: "unknown" },
      bundle,
    }),
  ).toMatchObject({ ok: false });
});
it("validates JSON samples independently and rejects unsupported constraints instead of inventing valid-looking data", () => {
  const definition = {
    type: "object",
    additionalProperties: false,
    required: ["id", "status", "child"],
    properties: {
      id: { type: "integer", minimum: 5, maximum: 15 },
      status: { enum: ["ok", "pending"], default: "ok" },
      child: { $ref: "#/definitions/detail" },
    },
    definitions: { detail: { type: "string", minLength: 3, maxLength: 8 } },
  };
  const bundle: CodecSchemaBundle = {
    root: { id: 10, schemaType: "JSON", references: [], schema: JSON.stringify(definition) },
    dependencies: [],
  };
  const samples = generate(bundle);
  const validate = new Ajv().compile(definition);
  for (const sample of samples.samples) {
    const value: unknown = JSON.parse(sample.json);
    expect(validate(value)).toBe(true);
    expect(Buffer.from(sample.record.value!, "base64").toString()).toBe(sample.json);
    expect(value).toMatchObject({ status: "ok" });
  }
  const unsupported = {
    ...bundle,
    root: { ...bundle.root, schema: '{"type":"string","pattern":"^[A-Z]{4}$"}' },
  };
  expect(generateSchemaSamples({ kind: "generate", input, bundle: unsupported })).toMatchObject({
    ok: false,
    detail: "Unsupported JSON Schema keyword: pattern",
  });
});
it("enforces count, bytes and accounting contracts and runs the isolated generator worker", async () => {
  expect(() => parseSchemaSampleInput({ ...input, count: 51 })).toThrow();
  expect(() => parseSchemaSampleInput({ ...input, seed: -1 })).toThrow();
  expect(() =>
    parseRecordBatchInput({
      topic: "x",
      partition: 0,
      ratePerSecond: 11,
      records: [{ state: "complete", encoding: "base64", key: null, value: "e30=", headers: [] }],
    }),
  ).toThrow();
  expect(() =>
    parseRecordBatchOutcome({ total: 5, unsent: 4, outcomes: [], stopReason: "complete" }),
  ).toThrow();
  const result = await createHostRecordCodec().generate(
    input,
    { root: { id: 1, schemaType: "AVRO", references: [], schema: '"string"' }, dependencies: [] },
    new AbortController().signal,
  );
  expect(result.samples).toHaveLength(5);
});

it("generates exact Avro long values and refuses unsupported logical semantics", () => {
  const bundle: CodecSchemaBundle = {
    root: { id: 11, schemaType: "AVRO", references: [], schema: '"long"' },
    dependencies: [],
  };
  for (const item of generate(bundle).samples) {
    const decoded = parseStructuredRecord({
      input: { format: "avro", bytes: item.record.value },
      bundle,
    });
    expect(decoded.state).toBe("decoded");
    if (decoded.state === "decoded") expect(JSON.parse(decoded.json)).toMatch(/^-?\d+$/u);
  }
  const result = generateSchemaSamples({
    kind: "generate",
    input,
    bundle: {
      ...bundle,
      root: { ...bundle.root, schema: '{"type":"long","logicalType":"timestamp-millis"}' },
    },
  });
  expect(result).toMatchObject({
    ok: false,
    detail: "Avro logical-type sample generation is not supported.",
  });
  expect(() =>
    parseRecordBatchOutcome({ total: 1, unsent: 1, outcomes: [], stopReason: "complete" }),
  ).toThrow();
});
