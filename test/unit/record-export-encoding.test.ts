import { describe, expect, it } from "vitest";

import type { KafkaMessage } from "../../src/features/kafka/contracts";
import {
  encodeRecordExportRow,
  recordExportHeader,
} from "../../src/features/kafka/application/record-export-encoding";

const message: KafkaMessage = {
  id: "record",
  topic: '=HYPERLINK("https://example.test")',
  partition: 2,
  offset: "9223372036854775807",
  timestamp: "2026-10-09T10:00:00.000Z",
  key: "=1+1",
  payload: null,
  preview: "",
  headers: {},
  originalByteSize: 4,
  recordByteSize: 14,
  truncated: false,
  structured: {
    version: 1,
    key: { state: "decoded", codec: "utf8", text: "=1+1", json: null, writerSchema: null },
    value: { state: "null", codec: "auto", writerSchema: null },
    headersState: "complete",
    protection: "none",
    headers: [
      { key: "same", value: "line1\r\nline2", error: null },
      { key: "same", value: null, error: null },
      { key: "same", value: "", error: null },
    ],
  },
  original: {
    state: "complete",
    encoding: "base64",
    key: "PTErMQ==",
    value: null,
    headers: [
      { key: "c2FtZQ==", value: "bGluZTFcclxubGluZTI=" },
      { key: "c2FtZQ==", value: null },
      { key: "c2FtZQ==", value: "" },
    ],
  },
};

/** Independent RFC 4180 reader, including quoted commas, CRLF and doubled quotes. */
function csv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === '"') {
      if (quoted && text[i + 1] === '"') {
        cell += '"';
        i++;
      } else quoted = !quoted;
    } else if (!quoted && char === ",") {
      row.push(cell);
      cell = "";
    } else if (!quoted && char === "\r" && text[i + 1] === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      i++;
    } else cell += char;
  }
  expect(quoted).toBe(false);
  return rows;
}

describe("canonical streaming export formats", () => {
  it("preserves original bytes, ordered null/empty duplicate headers, tombstones and exact offsets in JSONL", () => {
    const text = new TextDecoder().decode(encodeRecordExportRow(message, "jsonl"));
    expect(text.split("\n")).toHaveLength(2);
    expect(JSON.parse(text)).toMatchObject({
      offset: message.offset,
      structured: message.structured,
      original: message.original,
    });
    expect(recordExportHeader("jsonl")).toHaveLength(0);
  });

  it("independently round-trips RFC CSV cells without coercing offsets or permitting formula-leading text", () => {
    const decoder = new TextDecoder();
    const [header, values] = csv(
      decoder.decode(recordExportHeader("csv")) +
        decoder.decode(encodeRecordExportRow(message, "csv")),
    );
    const parsed = Object.fromEntries(header!.map((key, index) => [key, values![index]]));
    expect(JSON.parse(parsed.topic_json!)).toBe(message.topic);
    expect(JSON.parse(parsed.offset_json!)).toBe(message.offset);
    expect(JSON.parse(parsed.key_json!)).toEqual(message.structured!.key);
    expect(JSON.parse(parsed.headers_json!)).toEqual(message.structured!.headers);
    expect(JSON.parse(parsed.original_json!)).toEqual(message.original);
    for (const value of values!) expect(value).not.toMatch(/^[=+\-@\t\r]/u);
  });

  it("keeps decode failures and masking explicit and refuses inconsistent protected original bytes", () => {
    const protectedRecord: KafkaMessage = {
      ...message,
      original: { state: "unavailable", reason: "masked" },
      structured: {
        ...message.structured!,
        protection: "masked",
        value: {
          state: "error",
          codec: "avro",
          writerSchema: {
            id: 7,
            format: "avro",
            registry: "https://registry.test",
            messageType: null,
          },
          code: "schema-unavailable",
          detail: "Schema unavailable.",
        },
      },
    };
    expect(
      JSON.parse(new TextDecoder().decode(encodeRecordExportRow(protectedRecord, "jsonl"))),
    ).toMatchObject({
      original: { state: "unavailable", reason: "masked" },
      structured: protectedRecord.structured,
    });
    expect(() =>
      encodeRecordExportRow({ ...protectedRecord, original: message.original! }, "csv"),
    ).toThrow("Protected records");
    const legacy = { ...message };
    delete legacy.structured;
    expect(() => encodeRecordExportRow(legacy, "jsonl")).toThrow("canonical record");
  });
});
