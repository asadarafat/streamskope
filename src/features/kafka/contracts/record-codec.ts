import { parseRecordBase64 } from "./record-bytes";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export const RECORD_FORMATS = ["json", "avro", "protobuf"] as const;
export type RecordFormat = (typeof RECORD_FORMATS)[number];
export const RECORD_CODEC_LIMITS = {
  bytes: 256 * 1_024,
  outputCharacters: 256 * 1_024,
  schemaBytes: 256 * 1_024,
  graphBytes: 1_048_576,
  schemaNodes: 32,
  referenceDepth: 8,
  cacheEntries: 32,
  cacheBytes: 2 * 1_048_576,
  jsonNodes: 20_000,
  jsonDepth: 32,
  workerMs: 3_000,
} as const;

export interface RecordDecodeInput {
  readonly format: RecordFormat;
  readonly bytes: string | null;
}

export const RECORD_DECODE_ERRORS = [
  "malformed",
  "schema-unavailable",
  "schema-type",
  "reference",
  "limit",
  "cancelled",
  "unavailable",
] as const;
export type RecordDecodeErrorCode = (typeof RECORD_DECODE_ERRORS)[number];
export type RecordDecodeResult =
  | {
      readonly state: "decoded";
      readonly format: RecordFormat;
      readonly json: string;
      readonly schemaId: number | null;
      readonly messageType: string | null;
      readonly notes: string;
    }
  | { readonly state: "null"; readonly format: RecordFormat }
  | {
      readonly state: "error";
      readonly format: RecordFormat;
      readonly code: RecordDecodeErrorCode;
      readonly detail: string;
    };

export function parseRecordDecodeInput(value: unknown): RecordDecodeInput {
  const input = record(value, "decode");
  exactKeys(input, ["format", "bytes"], "decode");
  return {
    format: declaredValue(input.format, RECORD_FORMATS, "decode.format"),
    bytes:
      input.bytes === null
        ? null
        : parseRecordBase64(input.bytes, "decode.bytes", RECORD_CODEC_LIMITS.bytes),
  };
}

export function parseRecordDecodeResult(value: unknown): RecordDecodeResult {
  const input = record(value, "decoded");
  const format = declaredValue(input.format, RECORD_FORMATS, "decoded.format");
  if (input.state === "null") {
    exactKeys(input, ["state", "format"], "decoded");
    return { state: "null", format };
  }
  if (input.state === "error") {
    exactKeys(input, ["state", "format", "code", "detail"], "decoded");
    return {
      state: "error",
      format,
      code: declaredValue(input.code, RECORD_DECODE_ERRORS, "decoded.code"),
      detail: text(input.detail, "decoded.detail", 1_024),
    };
  }
  exactKeys(input, ["state", "format", "json", "schemaId", "messageType", "notes"], "decoded");
  if (input.state !== "decoded")
    throw new HostContractValidationError("decoded.state", "must describe a decode result");
  const json = boundedText(input.json, "decoded.json", RECORD_CODEC_LIMITS.outputCharacters);
  try {
    JSON.parse(json);
  } catch {
    throw new HostContractValidationError("decoded.json", "must contain valid JSON");
  }
  return {
    state: "decoded",
    format,
    json,
    schemaId:
      input.schemaId === null
        ? null
        : positiveBoundedInteger(input.schemaId, "decoded.schemaId", 0x7fffffff),
    messageType:
      input.messageType === null ? null : text(input.messageType, "decoded.messageType", 512),
    notes: boundedText(input.notes, "decoded.notes", 1_024),
  };
}
