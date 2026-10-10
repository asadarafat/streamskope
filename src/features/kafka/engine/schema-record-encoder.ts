import protobuf from "protobufjs";

import { boundedRecordJson } from "../application/record-json";
import type { CodecSchemaBundle } from "../application/record-codec-types";
import {
  parseSchemaAuthoringInput,
  parseSchemaAuthoringResult,
  SCHEMA_AUTHORING_LIMITS,
  type SchemaAuthoringInput,
  type SchemaAuthoringResult,
} from "../contracts/schema-authoring";

import { avroType, protobufTypes } from "./record-codec-parser";
import { compileJsonSchema } from "./schema-json-validation";
import {
  checkAvroFields,
  checkProtobufPayload,
  invalidPayload,
  SchemaPayloadError,
} from "./schema-authoring-validation";

export interface SchemaAuthoringWorkerInput {
  readonly kind: "author";
  readonly input: SchemaAuthoringInput;
  readonly bundle: CodecSchemaBundle;
}
export interface EncodedSchemaValue {
  readonly json: string;
  readonly wire: Buffer;
  readonly encoding: string;
  readonly messageType: string | null;
}
function frame(bundle: CodecSchemaBundle, body: Uint8Array): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt32BE(bundle.root.id, 1);
  return Buffer.concat([header, body]);
}
export function selectProtobufMessage(
  bundle: CodecSchemaBundle,
  name: string,
): { type: protobuf.Type; indexes: number[] } {
  let selected: { type: protobuf.Type; indexes: number[] } | undefined;
  const visit = (types: readonly protobuf.Type[], parent: readonly number[]): void => {
    for (const [index, type] of types.entries()) {
      const indexes = [...parent, index];
      if (
        (!name && parent.length === 0 && index === 0) ||
        type.fullName.replace(/^\./u, "") === name.replace(/^\./u, "")
      )
        selected = { type, indexes };
      if (parent.length < 8)
        visit(
          type.nestedArray.filter(
            (child): child is protobuf.Type => child instanceof protobuf.Type,
          ),
          indexes,
        );
    }
  };
  visit(protobufTypes(bundle), []);
  if (!selected)
    invalidPayload("", "schema", "Choose a message type declared in the writer schema.");
  return selected;
}
function messagePrefix(indexes: readonly number[]): Buffer {
  if (indexes.length === 1 && indexes[0] === 0) return Buffer.from([0]);
  const varint = (value: number): number[] => {
    let remaining = value * 2;
    const bytes: number[] = [];
    do {
      const part = remaining & 127;
      remaining >>>= 7;
      bytes.push(part | (remaining ? 128 : 0));
    } while (remaining);
    return bytes;
  };
  return Buffer.from([indexes.length, ...indexes].flatMap(varint));
}

/** One worker-owned encoder for generated and edited payloads; no Registry or broker writes. */
export function createSchemaRecordEncoder(
  bundle: CodecSchemaBundle,
  messageType: string,
): (payload: string) => EncodedSchemaValue {
  let encode: (value: unknown, source: string) => EncodedSchemaValue;
  if (bundle.root.id < 1 || bundle.root.id > 0x7fffffff)
    invalidPayload("", "schema", "Writer schema ID is invalid.");
  if (bundle.root.schemaType === "AVRO") {
    const type = avroType(bundle);
    encode = (value, source): EncodedSchemaValue => {
      checkAvroFields(type, value);
      let converted: unknown;
      try {
        converted = type.fromString(source) as unknown;
      } catch {
        invalidPayload(
          "",
          "schema",
          "Payload does not match the Avro JSON writer representation. Check required fields, branch names and field types.",
        );
      }
      if (!type.isValid(converted, { noUndeclaredFields: true }))
        invalidPayload("", "schema", "Payload does not validate against the Avro writer schema.");
      const body = type.toBuffer(converted);
      return {
        json: type.toString(type.fromBuffer(body)),
        wire: frame(bundle, type.typeName === "bytes" ? (converted as Buffer) : body),
        encoding:
          "Confluent Avro (writer schema ID); Avro JSON branches and byte strings; logical types use underlying storage values",
        messageType: null,
      };
    };
  } else if (bundle.root.schemaType === "PROTOBUF") {
    const selected = selectProtobufMessage(bundle, messageType);
    encode = (value): EncodedSchemaValue => {
      checkProtobufPayload(selected.type, value);
      const converted = selected.type.fromObject(value as Record<string, unknown>);
      if (selected.type.verify(converted))
        invalidPayload("", "schema", "Payload does not validate against the writer message.");
      const body = selected.type.encode(converted).finish();
      return {
        json: boundedRecordJson(
          selected.type.toObject(selected.type.decode(body), {
            longs: String,
            enums: String,
            bytes: String,
            defaults: false,
          }),
        ),
        wire: frame(bundle, Buffer.concat([messagePrefix(selected.indexes), body])),
        encoding:
          "Confluent Protobuf (writer schema ID and message indexes); decimal-string int64, Base64 bytes and declared enum names",
        messageType: selected.type.fullName,
      };
    };
  } else {
    const validate = compileJsonSchema(bundle);
    encode = (value): EncodedSchemaValue => {
      if (!validate(value)) {
        const error = validate.errors?.[0];
        invalidPayload(
          error?.instancePath ?? "",
          "schema",
          `Payload failed JSON Schema ${error?.keyword ?? "validation"}.`,
        );
      }
      const json = boundedRecordJson(value);
      return {
        json,
        wire: Buffer.from(json),
        encoding: "UTF-8 JSON validated against JSON Schema draft-07 (no wire header)",
        messageType: null,
      };
    };
  }
  return (source): EncodedSchemaValue => {
    if (Buffer.byteLength(source) > SCHEMA_AUTHORING_LIMITS.payloadBytes)
      invalidPayload("", "limit", "Payload exceeds 16 KiB UTF-8.");
    let value: unknown;
    try {
      value = JSON.parse(source, (_key: string, item: unknown) => {
        if (
          typeof item === "number" &&
          (!Number.isFinite(item) || (Number.isInteger(item) && !Number.isSafeInteger(item)))
        )
          invalidPayload(
            "",
            "precision",
            "Use decimal strings for 64-bit fields; JSON numbers must be finite and exact safe integers.",
          );
        return item;
      }) as unknown;
    } catch (error) {
      if (error instanceof SchemaPayloadError) throw error;
      invalidPayload("", "json", "Payload must contain valid JSON.");
    }
    boundedRecordJson(value);
    const result = encode(value, source);
    if (result.wire.length > SCHEMA_AUTHORING_LIMITS.payloadBytes)
      invalidPayload("", "limit", "Encoded record exceeds 16 KiB.");
    return result;
  };
}
export function authorSchemaRecord({
  input,
  bundle,
}: SchemaAuthoringWorkerInput): SchemaAuthoringResult {
  try {
    input = parseSchemaAuthoringInput(input);
    if (input.schemaId !== bundle.root.id)
      invalidPayload(
        "",
        "schema",
        "The selected writer identity changed. Reload the subject before authoring again.",
      );
    const encoded = createSchemaRecordEncoder(bundle, input.messageType)(input.payload);
    return parseSchemaAuthoringResult({
      state: "valid",
      writer: {
        subject: input.subject,
        version: input.version,
        id: bundle.root.id,
        schemaType: bundle.root.schemaType,
      },
      messageType: encoded.messageType,
      json: encoded.json,
      encoding: encoded.encoding,
      record: {
        state: "complete",
        encoding: "base64",
        key: null,
        value: encoded.wire.toString("base64"),
        headers: [],
      },
    });
  } catch (error) {
    return {
      state: "invalid",
      issues: [
        error instanceof SchemaPayloadError
          ? error.issue
          : {
              path: "",
              code: error instanceof RangeError ? "limit" : "unsupported",
              detail:
                error instanceof RangeError
                  ? "Payload exceeds the bounded structure or output limits."
                  : "The writer schema, references or dialect cannot be compiled within supported authoring limits.",
            },
      ],
    };
  }
}
