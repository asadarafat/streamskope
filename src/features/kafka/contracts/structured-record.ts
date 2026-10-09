import {
  RECORD_CODEC_LIMITS,
  RECORD_DECODE_ERRORS,
  type RecordDecodeErrorCode,
} from "./record-codec";
import { KAFKA_ORIGINAL_RECORD_LIMITS } from "./record-bytes";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export const RECORD_CODEC_SELECTIONS = [
  "auto",
  "utf8",
  "json",
  "avro",
  "protobuf",
  "bytes",
] as const;
export type RecordCodecSelection = (typeof RECORD_CODEC_SELECTIONS)[number];
export type ResolvedRecordCodec = Exclude<RecordCodecSelection, "auto">;
export interface RecordCodecPreferences {
  readonly key: RecordCodecSelection;
  readonly value: RecordCodecSelection;
}
export const RECORD_CODEC_DEFAULTS: RecordCodecPreferences = Object.freeze({
  key: "auto",
  value: "auto",
});

/** Schema IDs are meaningful only within their originating Registry. */
export interface RecordWriterSchema {
  readonly id: number;
  readonly format: "avro" | "protobuf" | "unknown";
  readonly registry: string | null;
  readonly messageType: string | null;
}
export type RecordField =
  | {
      readonly state: "decoded";
      readonly codec: ResolvedRecordCodec;
      readonly text: string;
      readonly json: string | null;
      readonly writerSchema: RecordWriterSchema | null;
    }
  | { readonly state: "null"; readonly codec: RecordCodecSelection; readonly writerSchema: null }
  | {
      readonly state: "error";
      readonly codec: RecordCodecSelection;
      readonly writerSchema: RecordWriterSchema | null;
      readonly code: RecordDecodeErrorCode | "invalid-utf8" | "not-captured";
      readonly detail: string;
    }
  | {
      readonly state: "masked";
      readonly codec: RecordCodecSelection;
      readonly writerSchema: RecordWriterSchema | null;
    };

export interface StructuredRecordHeader {
  readonly key: string;
  readonly value: string | null;
  readonly error: string | null;
}

/** Travels with KafkaMessage.original; aliases are derived from this protected projection. */
export interface StructuredRecord {
  readonly version: 1;
  readonly key: RecordField;
  readonly value: RecordField;
  readonly headers: readonly StructuredRecordHeader[];
  readonly headersState: "complete" | "unavailable";
  readonly protection: "none" | "masked";
}

export function recordFieldText(field: RecordField): string | null {
  return field.state === "decoded" ? field.text : field.state === "masked" ? "[MASKED]" : null;
}

export function parseRecordCodecPreferences(
  value: unknown,
  path = "codecs",
): RecordCodecPreferences {
  const input = record(value, path);
  exactKeys(input, ["key", "value"], path);
  return {
    key: declaredValue(input.key, RECORD_CODEC_SELECTIONS, `${path}.key`),
    value: declaredValue(input.value, RECORD_CODEC_SELECTIONS, `${path}.value`),
  };
}

function parseWriter(value: unknown, path: string): RecordWriterSchema | null {
  if (value === null) return null;
  const input = record(value, path);
  exactKeys(input, ["id", "format", "registry", "messageType"], path);
  const registry = input.registry === null ? null : text(input.registry, `${path}.registry`, 2_048);
  if (registry !== null) {
    let url: URL;
    try {
      url = new URL(registry);
    } catch {
      throw new HostContractValidationError(`${path}.registry`, "must be an absolute Registry URL");
    }
    if (
      !["https:", "http:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new HostContractValidationError(
        `${path}.registry`,
        "must omit credentials, query and fragment",
      );
  }
  return {
    id: positiveBoundedInteger(input.id, `${path}.id`, 0x7fffffff),
    format: declaredValue(input.format, ["avro", "protobuf", "unknown"] as const, `${path}.format`),
    registry,
    messageType:
      input.messageType === null ? null : text(input.messageType, `${path}.messageType`, 512),
  };
}

export function parseRecordField(value: unknown, path: string): RecordField {
  const input = record(value, path);
  const codec = declaredValue(input.codec, RECORD_CODEC_SELECTIONS, `${path}.codec`);
  const writerSchema = parseWriter(input.writerSchema, `${path}.writerSchema`);
  switch (input.state) {
    case "null":
      exactKeys(input, ["state", "codec", "writerSchema"], path);
      if (writerSchema !== null)
        throw new HostContractValidationError(path, "a tombstone has no writer schema");
      return { state: "null", codec, writerSchema: null };
    case "masked":
      exactKeys(input, ["state", "codec", "writerSchema"], path);
      return { state: "masked", codec, writerSchema };
    case "error":
      exactKeys(input, ["state", "codec", "writerSchema", "code", "detail"], path);
      return {
        state: "error",
        codec,
        writerSchema,
        code: declaredValue(
          input.code,
          [...RECORD_DECODE_ERRORS, "invalid-utf8", "not-captured"] as const,
          `${path}.code`,
        ),
        detail: text(input.detail, `${path}.detail`, 1_024),
      };
    case "decoded": {
      exactKeys(input, ["state", "codec", "writerSchema", "text", "json"], path);
      if (codec === "auto")
        throw new HostContractValidationError(
          path,
          "a decoded field must identify its selected codec",
        );
      if (codec === "avro" || codec === "protobuf") {
        if (
          writerSchema === null ||
          writerSchema.format !== codec ||
          writerSchema.registry === null
        )
          throw new HostContractValidationError(
            path,
            "a decoded schema record must identify its writer format and Registry",
          );
      } else if (writerSchema !== null)
        throw new HostContractValidationError(
          path,
          "this codec does not declare a Registry writer schema",
        );
      const projection = boundedText(
        input.text,
        `${path}.text`,
        RECORD_CODEC_LIMITS.outputCharacters * 2,
      );
      const json =
        input.json === null
          ? null
          : boundedText(input.json, `${path}.json`, RECORD_CODEC_LIMITS.outputCharacters);
      if (["json", "avro", "protobuf"].includes(codec)) {
        if (json === null || json !== projection)
          throw new HostContractValidationError(
            path,
            "structured text must equal its canonical JSON projection",
          );
        try {
          JSON.parse(json);
        } catch {
          throw new HostContractValidationError(`${path}.json`, "must be valid JSON");
        }
      } else if (json !== null)
        throw new HostContractValidationError(path, "text and byte codecs do not declare JSON");
      return { state: "decoded", codec, writerSchema, text: projection, json };
    }
    default:
      throw new HostContractValidationError(
        `${path}.state`,
        "must identify an explicit record field state",
      );
  }
}

export function parseStructuredRecord(value: unknown, path = "structured"): StructuredRecord {
  const input = record(value, path);
  exactKeys(input, ["version", "key", "value", "headers", "headersState", "protection"], path);
  if (
    input.version !== 1 ||
    !Array.isArray(input.headers) ||
    input.headers.length > KAFKA_ORIGINAL_RECORD_LIMITS.headers
  )
    throw new HostContractValidationError(path, "must be a bounded version 1 structured record");
  const headersState = declaredValue(
    input.headersState,
    ["complete", "unavailable"] as const,
    `${path}.headersState`,
  );
  if (headersState === "unavailable" && input.headers.length !== 0)
    throw new HostContractValidationError(path, "unavailable headers must not invent an inventory");
  const headers = input.headers.map((value: unknown, index): StructuredRecordHeader => {
    const p = `${path}.headers[${index}]`;
    const header = record(value, p);
    exactKeys(header, ["key", "value", "error"], p);
    return {
      key: boundedText(header.key, `${p}.key`, KAFKA_ORIGINAL_RECORD_LIMITS.headerKeyBytes),
      value:
        header.value === null
          ? null
          : boundedText(header.value, `${p}.value`, KAFKA_ORIGINAL_RECORD_LIMITS.headerValueBytes),
      error: header.error === null ? null : text(header.error, `${p}.error`, 512),
    };
  });
  const key = parseRecordField(input.key, `${path}.key`);
  const decodedValue = parseRecordField(input.value, `${path}.value`);
  const protection = declaredValue(
    input.protection,
    ["none", "masked"] as const,
    `${path}.protection`,
  );
  if ((key.state === "masked" || decodedValue.state === "masked") && protection !== "masked")
    throw new HostContractValidationError(
      path,
      "masked fields require protected projection metadata",
    );
  return { version: 1, key, value: decodedValue, headers, headersState, protection };
}
