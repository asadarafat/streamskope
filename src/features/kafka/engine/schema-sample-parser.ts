import avro from "avsc";
import protobuf from "protobufjs";

import {
  parseSchemaSamples,
  parseSchemaSampleInput,
  type SchemaSampleInput,
  type SchemaSamples,
  SCHEMA_SAMPLE_LIMITS as limits,
} from "../contracts/schema-samples";
import type { CodecSchemaBundle } from "../application/record-codec-types";

import { avroType, boundedJson } from "./record-codec-parser";
import { createSchemaRecordEncoder, selectProtobufMessage } from "./schema-record-encoder";
import { jsonSampleGenerator } from "./schema-sample-json";

export interface SchemaSampleWorkerInput {
  readonly kind: "generate";
  readonly input: SchemaSampleInput;
  readonly bundle: CodecSchemaBundle;
}
export type SchemaSampleWorkerResult =
  | { readonly ok: true; readonly samples: SchemaSamples }
  | { readonly ok: false; readonly detail: string };
export function generateSchemaSamples({
  input,
  bundle,
}: SchemaSampleWorkerInput): SchemaSampleWorkerResult {
  try {
    input = parseSchemaSampleInput(input);
    let seed = input.seed >>> 0;
    const random = (): number => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return seed / 0x100000000;
    };
    const pick = <T>(values: readonly T[]): T => {
      if (!values.length) throw new Error("Empty schema choices cannot generate a value.");
      return values[Math.floor(random() * values.length)]!;
    };
    let fields = 0;
    const count = (depth: number): void => {
      if (depth > limits.depth || ++fields > 2_000)
        throw new Error("Sample structure exceeds its depth or field budget.");
    };
    const avroValue = (type: avro.Type, depth: number): unknown => {
      count(depth);
      if (type instanceof avro.types.RecordType) {
        if (type.fields.length > limits.fields) throw new Error("Too many Avro fields.");
        return Object.fromEntries(
          type.fields.map((field) => {
            const fallback: unknown = field.defaultValue();
            return [
              field.name,
              fallback === undefined ? avroValue(field.type, depth + 1) : fallback,
            ];
          }),
        );
      }
      if (type instanceof avro.types.WrappedUnionType) {
        const selected = pick(type.types);
        const value = avroValue(selected, depth + 1);
        return selected.wrap(value) as unknown;
      }
      if (type instanceof avro.types.EnumType) return pick(type.symbols);
      if (type instanceof avro.types.ArrayType)
        return [avroValue(type.itemsType, depth + 1), avroValue(type.itemsType, depth + 1)];
      if (type instanceof avro.types.MapType)
        return { sample: avroValue(type.valuesType as avro.Type, depth + 1) };
      if (type instanceof avro.types.FixedType) {
        if (type.size > 1_024) throw new Error("Fixed samples exceed 1,024 bytes.");
        return Buffer.alloc(type.size, Math.floor(random() * 256));
      }
      switch (type.typeName) {
        case "null":
          return null;
        case "boolean":
          return random() < 0.5;
        case "int":
          return Math.floor(random() * 2_001) - 1_000;
        case "long":
        case "abstract:long":
          return String(Math.floor(random() * 2_001) - 1_000);
        case "float":
        case "double":
          return Math.floor(random() * 2001) / 10;
        case "string":
          return `sample-${Math.floor(random() * 1_000_000).toString(36)}`;
        case "bytes":
          return Buffer.from([Math.floor(random() * 256), Math.floor(random() * 256)]);
        default:
          throw new Error("Unsupported Avro generation type.");
      }
    };
    const protoValue = (type: protobuf.Type, depth: number): Record<string, unknown> => {
      count(depth);
      if (type.fieldsArray.length > limits.fields) throw new Error("Too many Protobuf fields.");
      const choices = new Map(type.oneofsArray.map((group) => [group.name, pick(group.oneof)]));
      return Object.fromEntries(
        type.fieldsArray
          .filter((field) => !field.partOf || choices.get(field.partOf.name) === field.name)
          .map((field) => {
            const scalar = (): unknown => {
              if (field.resolvedType instanceof protobuf.Type)
                return protoValue(field.resolvedType, depth + 1);
              if (field.options?.default !== undefined) return field.defaultValue as unknown;
              if (field.resolvedType instanceof protobuf.Enum)
                return pick(Object.values(field.resolvedType.values));
              if (field.type === "string")
                return `sample-${Math.floor(random() * 1_000_000).toString(36)}`;
              if (field.type === "bool") return random() < 0.5;
              if (field.type === "bytes") return Buffer.from([Math.floor(random() * 256)]);
              return /64$/u.test(field.type)
                ? String(Math.floor(random() * 1_000))
                : Math.floor(random() * 1_000);
            };
            if (field instanceof protobuf.MapField) {
              const map = field;
              return [
                field.name,
                {
                  [map.keyType === "bool" ? "true" : map.keyType === "string" ? "sample" : "1"]:
                    scalar(),
                },
              ];
            }
            return [field.name, field.repeated ? [scalar(), scalar()] : scalar()];
          }),
      );
    };
    let next: () => { json: string; wire: Buffer };
    const encode = createSchemaRecordEncoder(bundle, input.messageType);
    let encoding: string;
    if (bundle.root.schemaType === "AVRO") {
      const checkLogical = (value: unknown, depth: number): void => {
        if (depth > 32) throw new Error("Avro schema is too deeply nested.");
        if (!value || typeof value !== "object") return;
        if (Object.hasOwn(value, "logicalType"))
          throw new Error("Avro logical-type sample generation is not supported.");
        for (const child of Object.values(value)) checkLogical(child, depth + 1);
      };
      for (const schema of [bundle.root, ...bundle.dependencies.map((item) => item.schema)])
        checkLogical(JSON.parse(schema.schema) as unknown, 0);
      const type = avroType(bundle);
      encoding = "Confluent Avro (writer schema ID)";
      next = (): { json: string; wire: Buffer } => {
        const value = avroValue(type, 0);
        if (!type.isValid(value)) throw new Error("Generated Avro did not validate.");
        return encode(type.toString(value));
      };
    } else if (bundle.root.schemaType === "PROTOBUF") {
      const { type } = selectProtobufMessage(bundle, input.messageType);
      encoding = `Confluent Protobuf ${type.fullName} (writer schema ID and message indexes)`;
      next = (): { json: string; wire: Buffer } => {
        const value = type.fromObject(protoValue(type, 0));
        if (type.verify(value)) throw new Error("Generated Protobuf did not validate.");
        return encode(
          boundedJson(
            type.toObject(value, { longs: String, enums: String, bytes: String, defaults: false }),
          ),
        );
      };
    } else {
      const generate = jsonSampleGenerator(bundle, random);
      encoding = "UTF-8 JSON validated against JSON Schema draft-07 (no wire header)";
      next = (): { json: string; wire: Buffer } => {
        const json = boundedJson(generate());
        return encode(json);
      };
    }
    const samples = Array.from({ length: input.count }, () => {
      fields = 0;
      const { json, wire } = next();
      return {
        json,
        record: {
          state: "complete" as const,
          encoding: "base64" as const,
          key: null,
          value: wire.toString("base64"),
          headers: [],
        },
      };
    });
    return {
      ok: true,
      samples: parseSchemaSamples({
        schema: { subject: input.subject, version: input.version },
        schemaId: bundle.root.id,
        encoding,
        seed: input.seed,
        samples,
      }),
    };
  } catch (error) {
    return {
      ok: false,
      detail: error instanceof Error ? error.message.slice(0, 256) : "Schema generation failed.",
    };
  }
}
