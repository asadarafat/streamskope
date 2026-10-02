import {
  KAFKA_MESSAGE_LIMITS,
  kafkaRawMessageRetainedBytes,
  utf8ByteLength,
  type KafkaMessage,
} from "../contracts";
import { KAFKA_ORIGINAL_RECORD_LIMITS, type KafkaOriginalRecord } from "../contracts/record-bytes";

import type { KafkaRawMessage } from "./types";

function decodePrefix(buffer: Buffer | null | undefined, maximumBytes: number): string {
  if (buffer == null) return "";
  const decoded = buffer.subarray(0, maximumBytes).toString("utf8");
  if (utf8ByteLength(decoded) <= maximumBytes) return decoded;
  const retained: string[] = [];
  let retainedBytes = 0;
  for (const character of decoded) {
    const bytes = Buffer.byteLength(character, "utf8");
    if (retainedBytes + bytes > maximumBytes) break;
    retained.push(character);
    retainedBytes += bytes;
  }
  return retained.join("");
}

function originalRecord(raw: KafkaRawMessage): KafkaOriginalRecord {
  const headers = raw.headerEntries ?? [...raw.headers];
  let bytes = (raw.key?.byteLength ?? 0) + (raw.value?.byteLength ?? 0);
  if (headers.length > KAFKA_ORIGINAL_RECORD_LIMITS.headers)
    return { state: "unavailable", reason: "size-limit" };
  for (const [key, value] of headers) {
    bytes += key.byteLength + (value?.byteLength ?? 0);
    if (
      key.byteLength > KAFKA_ORIGINAL_RECORD_LIMITS.headerKeyBytes ||
      (value?.byteLength ?? 0) > KAFKA_ORIGINAL_RECORD_LIMITS.headerValueBytes
    )
      return { state: "unavailable", reason: "size-limit" };
  }
  if (bytes > KAFKA_ORIGINAL_RECORD_LIMITS.bytes)
    return { state: "unavailable", reason: "size-limit" };
  return {
    state: "complete",
    encoding: "base64",
    key: raw.key?.toString("base64") ?? null,
    value: raw.value?.toString("base64") ?? null,
    headers: headers.map(([key, value]) => ({
      key: key.toString("base64"),
      value: value?.toString("base64") ?? null,
    })),
  };
}

export function translateKafkaRecord(raw: KafkaRawMessage, expectedTopic: string): KafkaMessage {
  if (raw.topic !== expectedTopic)
    throw new Error(`Kafka returned a record for unexpected topic ${raw.topic}.`);
  if (!Number.isSafeInteger(raw.partition) || raw.partition < 0 || raw.offset < 0n)
    throw new Error("Kafka returned invalid partition or offset metadata.");
  const timestamp = Number(raw.timestamp);
  if (
    !Number.isSafeInteger(timestamp) ||
    timestamp < -8_640_000_000_000_000 ||
    timestamp > 8_640_000_000_000_000
  )
    throw new Error("Kafka returned an invalid record timestamp.");

  const sourceHeaders = raw.headerEntries ?? [...raw.headers];
  const headers: Array<readonly [string, string]> = [];
  let headerBytes = 0;
  let truncated = sourceHeaders.length > KAFKA_MESSAGE_LIMITS.headerCount;
  for (const [key, value] of sourceHeaders.slice(0, KAFKA_MESSAGE_LIMITS.headerCount)) {
    const name = decodePrefix(key, KAFKA_MESSAGE_LIMITS.headerKeyBytes);
    const text =
      value == null ? "(null)" : decodePrefix(value, KAFKA_MESSAGE_LIMITS.headerValueBytes);
    headerBytes += utf8ByteLength(name) + utf8ByteLength(text);
    if (headerBytes > KAFKA_ORIGINAL_RECORD_LIMITS.headerPreviewBytes) {
      truncated = true;
      break;
    }
    if (
      key.byteLength > KAFKA_MESSAGE_LIMITS.headerKeyBytes ||
      (value?.byteLength ?? 0) > KAFKA_MESSAGE_LIMITS.headerValueBytes ||
      (value != null &&
        utf8ByteLength(value.toString("utf8")) > KAFKA_MESSAGE_LIMITS.headerValueBytes)
    )
      truncated = true;
    headers.push([name, text]);
  }
  const contentOversized =
    (raw.key?.byteLength ?? 0) + (raw.value?.byteLength ?? 0) > KAFKA_MESSAGE_LIMITS.messageBytes;
  const key =
    (raw.key?.byteLength ?? 0) > KAFKA_MESSAGE_LIMITS.messageBytes
      ? null
      : (raw.key?.toString("utf8") ?? null);
  const value = contentOversized ? null : (raw.value?.toString("utf8") ?? null);
  const message: KafkaMessage = {
    headers: Object.fromEntries(headers),
    id: `${raw.topic}:${raw.partition}:${raw.offset.toString()}`,
    key,
    offset: raw.offset.toString(),
    originalByteSize: (raw.key?.byteLength ?? 0) + (raw.value?.byteLength ?? 0),
    original: originalRecord(raw),
    partition: raw.partition,
    payload: value,
    payloadTruncated: contentOversized && raw.value != null,
    preview: decodePrefix(raw.value, KAFKA_MESSAGE_LIMITS.previewBytes),
    timestamp: new Date(timestamp).toISOString(),
    topic: raw.topic,
    truncated: truncated || contentOversized,
  };
  if (kafkaRawMessageRetainedBytes(message) <= KAFKA_MESSAGE_LIMITS.messageBytes) return message;
  return {
    ...message,
    key: utf8ByteLength(key) <= KAFKA_MESSAGE_LIMITS.previewBytes ? key : null,
    payload: null,
    payloadTruncated: raw.value != null,
    truncated: true,
  };
}
