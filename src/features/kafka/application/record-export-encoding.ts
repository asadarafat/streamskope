import type { KafkaMessage } from "../contracts";
import type { RecordExportFormat } from "../contracts/record-export";

const encoder = new TextEncoder();
const columns = [
  "topic_json",
  "partition",
  "offset_json",
  "timestamp_json",
  "key_json",
  "value_json",
  "headers_state",
  "headers_json",
  "protection",
  "original_json",
  "original_byte_size",
  "record_byte_size",
  "truncated",
  "payload_truncated",
] as const;

/** RFC 4180 quoting follows JSON encoding, so arbitrary text cannot become a formula cell. */
function csvCell(value: string): string {
  return /[",\r\n]/u.test(value) ? `"${value.replaceAll('"', '""')}"` : value;
}

export function recordExportHeader(format: RecordExportFormat): Uint8Array {
  return encoder.encode(format === "csv" ? `${columns.join(",")}\r\n` : "");
}

/** The protected canonical projection is the only decoded representation in either format. */
export function encodeRecordExportRow(
  message: KafkaMessage,
  format: RecordExportFormat,
): Uint8Array {
  const structured = message.structured;
  if (structured === undefined) throw new Error("A canonical record is required for range export.");
  const original = message.original ?? { state: "unavailable", reason: "not-captured" };
  if (structured.protection === "masked" && original.state === "complete")
    throw new Error("Protected records cannot export original bytes.");
  const row = {
    topic: message.topic,
    partition: message.partition,
    offset: message.offset,
    timestamp: message.timestamp,
    structured,
    original,
    originalByteSize: message.originalByteSize,
    recordByteSize: message.recordByteSize ?? null,
    truncated: message.truncated,
    payloadTruncated: message.payloadTruncated ?? false,
  };
  if (format === "jsonl") return encoder.encode(`${JSON.stringify(row)}\n`);
  return encoder.encode(
    [
      JSON.stringify(row.topic),
      String(row.partition),
      JSON.stringify(row.offset),
      JSON.stringify(row.timestamp),
      JSON.stringify(structured.key),
      JSON.stringify(structured.value),
      structured.headersState,
      JSON.stringify(structured.headers),
      structured.protection,
      JSON.stringify(original),
      String(row.originalByteSize),
      row.recordByteSize === null ? "null" : String(row.recordByteSize),
      String(row.truncated),
      String(row.payloadTruncated),
    ]
      .map(csvCell)
      .join(",") + "\r\n",
  );
}
