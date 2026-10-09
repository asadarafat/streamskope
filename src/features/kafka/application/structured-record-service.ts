import {
  KAFKA_MESSAGE_LIMITS,
  kafkaRawMessageRetainedBytes,
  type KafkaMessage,
} from "../contracts";
import type {
  RecordCodecPreferences,
  RecordCodecSelection,
  RecordField,
  RecordWriterSchema,
  StructuredRecord,
  StructuredRecordHeader,
} from "../contracts/structured-record";
import { recordFieldText } from "../contracts/structured-record";

import { normalizeRecordJson } from "./record-json";
import { RecordCodecService, SchemaResolutionError } from "./record-codec-service";
import type { KafkaClusterServiceContext } from "./types";

const bytesOf = (value: string): Uint8Array => Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
const utf8 = (bytes: Uint8Array): string =>
  new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

function registryIdentity(context: KafkaClusterServiceContext | null): string | null {
  if (!context) return null;
  const url = new URL(context.baseUrl);
  url.username = "";
  url.password = "";
  url.search = "";
  url.hash = "";
  return url.toString();
}

/** Compatibility fields are views of the same protected canonical record. */
export function projectStructuredRecord(
  message: KafkaMessage,
  structured: StructuredRecord,
): KafkaMessage {
  const payload = recordFieldText(structured.value);
  return {
    ...message,
    structured,
    key: recordFieldText(structured.key),
    payload,
    preview: (payload ?? "").slice(0, KAFKA_MESSAGE_LIMITS.previewBytes / 4),
    headers: Object.fromEntries(structured.headers.map((h) => [h.key, h.value ?? "(null)"])),
  };
}

/** Decoding happens once, before protection, search, rules or renderer delivery. */
export class StructuredRecordService {
  constructor(private readonly codec: RecordCodecService) {}

  async prepare(
    message: KafkaMessage,
    choices: RecordCodecPreferences,
    context: KafkaClusterServiceContext | null,
    signal: AbortSignal,
  ): Promise<KafkaMessage> {
    signal.throwIfAborted();
    const original = message.original;
    if (original?.state !== "complete") {
      const missing = (codec: RecordCodecSelection): RecordField => ({
        state: "error",
        codec,
        writerSchema: null,
        code: "not-captured",
        detail:
          "Complete original bytes are unavailable. A preview cannot establish the decoded value.",
      });
      return projectStructuredRecord(message, {
        version: 1,
        key: missing(choices.key),
        value: missing(choices.value),
        headers: [],
        headersState: "unavailable",
        protection: "none",
      });
    }
    // Capture immutable strings and a detached ordered header inventory before any await.
    const captured = Object.freeze({
      ...original,
      headers: Object.freeze(original.headers.map((h) => Object.freeze({ ...h }))),
    });
    const key = await this.field(captured.key, choices.key, context, signal);
    const value = await this.field(captured.value, choices.value, context, signal);
    signal.throwIfAborted();
    const headers: StructuredRecordHeader[] = captured.headers.map((h) => {
      let key: string;
      try {
        key = utf8(bytesOf(h.key));
      } catch {
        return {
          key: "[binary header]",
          value: null,
          error: "Header name is not valid UTF-8; inspect original bytes.",
        };
      }
      try {
        return {
          key,
          value: h.value === null ? null : utf8(bytesOf(h.value)),
          error: null,
        };
      } catch {
        return {
          key,
          value: null,
          error: "Header value is not valid UTF-8; inspect original bytes.",
        };
      }
    });
    const prepared = projectStructuredRecord(
      { ...message, original: captured },
      { version: 1, key, value, headers, headersState: "complete", protection: "none" },
    );
    if (kafkaRawMessageRetainedBytes(prepared) <= KAFKA_MESSAGE_LIMITS.messageBytes)
      return prepared;
    const limited = (field: RecordField): RecordField =>
      field.state === "null"
        ? field
        : {
            state: "error",
            codec: field.codec,
            writerSchema: field.writerSchema,
            code: "limit",
            detail:
              "The decoded projection exceeds the retained record budget. Original bytes remain unchanged.",
          };
    return projectStructuredRecord(
      { ...prepared, payloadTruncated: true, truncated: true },
      { ...prepared.structured!, key: limited(key), value: limited(value) },
    );
  }

  private async field(
    encoded: string | null,
    selection: RecordCodecSelection,
    context: KafkaClusterServiceContext | null,
    signal: AbortSignal,
  ): Promise<RecordField> {
    if (encoded === null) return { state: "null", codec: selection, writerSchema: null };
    signal.throwIfAborted();
    const bytes = bytesOf(encoded);
    let codec = selection;
    let writerSchema: RecordWriterSchema | null = null;
    const error = (
      code: Extract<RecordField, { state: "error" }>["code"],
      detail: string,
    ): RecordField => ({ state: "error", codec, writerSchema, code, detail });
    if (selection === "bytes")
      return { state: "decoded", codec: "bytes", text: encoded, json: null, writerSchema: null };
    const framed = bytes.length >= 5 && bytes[0] === 0;
    if (
      (selection === "auto" && bytes[0] === 0) ||
      selection === "avro" ||
      selection === "protobuf"
    ) {
      if (!framed)
        return error(
          "malformed",
          "Expected Confluent magic byte 0 and a four-byte writer schema ID. Choose the actual writer encoding.",
        );
      const id = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(1);
      if (id < 1 || id > 0x7fffffff)
        return error("malformed", "The Confluent frame contains an invalid writer schema ID.");
      writerSchema = {
        id,
        format: "unknown",
        registry: registryIdentity(context),
        messageType: null,
      };
      if (!context)
        return error(
          "schema-unavailable",
          "Configure this connection's Schema Registry to resolve the writer schema, or select a manual codec.",
        );
      try {
        const bundle = await this.codec.resolve(context, id, signal);
        if (bundle.root.schemaType !== "AVRO" && bundle.root.schemaType !== "PROTOBUF")
          return error(
            "schema-type",
            "The Registry writer schema is not a supported Avro or Protobuf type. Select the declared encoding.",
          );
        const writerFormat = bundle.root.schemaType === "AVRO" ? "avro" : "protobuf";
        writerSchema = { ...writerSchema, format: writerFormat };
        if (selection === "auto") codec = writerFormat;
        else if (selection !== writerFormat)
          return error(
            "schema-type",
            "The selected codec differs from the Registry writer schema. Select automatic detection or the declared writer format.",
          );
      } catch (cause) {
        signal.throwIfAborted();
        return error(
          cause instanceof SchemaResolutionError ? cause.code : "schema-unavailable",
          "The writer schema could not be resolved within the configured Registry and bounded reference limits.",
        );
      }
      const result = await this.codec.decode(
        { format: codec as "avro" | "protobuf", bytes: encoded },
        context,
        signal,
      );
      signal.throwIfAborted();
      if (result.state === "error") return error(result.code, result.detail);
      if (result.state !== "decoded")
        return error("malformed", "The writer decoder returned no projection for non-null bytes.");
      return {
        state: "decoded",
        codec: result.format,
        json: result.json,
        text: result.json,
        writerSchema: { ...writerSchema, messageType: result.messageType },
      };
    }
    let text: string;
    try {
      text = utf8(bytes);
    } catch {
      return error(
        "invalid-utf8",
        "The record is not valid UTF-8. Choose original bytes or its declared writer encoding.",
      );
    }
    if (selection === "utf8")
      return { state: "decoded", codec: "utf8", text, json: null, writerSchema: null };
    try {
      const json = normalizeRecordJson(text);
      return { state: "decoded", codec: "json", text: json, json, writerSchema: null };
    } catch (cause) {
      if (selection === "json" || /^\s*(?:\{|\[)/u.test(text) || cause instanceof RangeError) {
        codec = "json";
        return error(
          cause instanceof RangeError ? "limit" : "malformed",
          cause instanceof RangeError
            ? "The JSON projection exceeds bounded structure or output limits."
            : "The record is not valid JSON. Select UTF-8 text only if this is the intended writer encoding.",
        );
      }
      return { state: "decoded", codec: "utf8", text, json: null, writerSchema: null };
    }
  }
}
