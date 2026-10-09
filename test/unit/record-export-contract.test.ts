import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  RECORD_EXPORT_LIMITS,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
  parseRecordExportInput,
  parseRecordExportReceipt,
  parseRecordExportSnapshot,
  type RecordExportInput,
  type RecordExportSnapshot,
} from "../../src/features/kafka/contracts";

const id = "2365efb2-58ad-4375-a9e6-3626a0026500";
const input: RecordExportInput = {
  requestId: id,
  topic: "orders",
  range: { mode: "earliest" },
  format: "jsonl",
  maxRecords: 100_000,
  search: { key: "", value: "", offset: "", timestamp: "", partition: null },
};
const empty: RecordExportSnapshot = { scopeId: id, revision: 0, available: true, operation: null };
const completed: RecordExportSnapshot = {
  ...empty,
  revision: 4,
  operation: {
    jobId: id,
    input,
    state: "completed",
    reason: "range-complete",
    error: null,
    limits: RECORD_EXPORT_LIMITS,
    source: { connectionName: "Lab", clusterId: "cluster", topicId: "topic" },
    settings: {
      codecs: { key: "auto", value: "auto" },
      protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
    },
    startedAt: "2026-10-09T10:00:00.000Z",
    completedAt: "2026-10-09T10:01:00.000Z",
    counts: {
      passes: 1,
      scannedRecords: 0,
      scannedBytes: 0,
      writtenRecords: 0,
      writtenBytes: 0,
      unavailableRecords: 0,
      decodeErrorRecords: 0,
      originalUnavailableRecords: 0,
    },
    coverage: {
      reason: "range-complete",
      scannedRecords: 0,
      scannedBytes: 0,
      matchedRecords: 0,
      unavailableRecords: 0,
      partitions: [],
    },
    artifact: {
      artifactId: id,
      output: { format: "jsonl", fileName: "orders.jsonl", bytes: 0, sha256: "0".repeat(64) },
      receiptBytes: 1_024,
      receiptSha256: "1".repeat(64),
      expiresAt: "2026-10-09T10:16:00.000Z",
    },
  },
};

describe("bounded export protocol", () => {
  it("requires a dedicated request identity and an explicit finite read range", () => {
    expect(parseRecordExportInput(input)).toEqual(input);
    const timed = { ...input, range: { mode: "time-window", startTimeMs: 1, endTimeMs: 2 } };
    expect(parseRecordExportInput(timed)).toEqual(timed);
    for (const change of [
      { requestId: "../artifact" },
      { requestId: "" },
      { topic: " " },
      { range: { mode: "tail" } },
      { range: { mode: "newest", maxMessages: 1000 } },
      { range: { mode: "time-window", startTimeMs: 2, endTimeMs: 1 } },
      { range: { mode: "time-window", startTimeMs: 1, endTimeMs: Number.MAX_SAFE_INTEGER } },
      { maxRecords: 0 },
      { maxRecords: 100_001 },
      { maxRecords: 1.5 },
      { format: "json" },
      { path: "/etc/passwd" },
      { search: { ...input.search, activeRuleMatchesOnly: true } },
    ])
      expect(() => parseRecordExportInput({ ...input, ...change })).toThrow();
  });

  it("roundtrips all commands, structured results and metadata events", () => {
    for (const [command, payload] of [
      ["records.export.start", input],
      ["records.export.status", {}],
      ["records.export.cancel", { jobId: id }],
      ["records.export.discard", { jobId: id }],
    ] as const) {
      const envelope = { id: "request", version: HOST_PROTOCOL_VERSION, command, payload };
      expect(parseHostCommand(envelope)).toEqual(envelope);
      const response = {
        id: "request",
        version: HOST_PROTOCOL_VERSION,
        command,
        ok: true,
        result: { correlationId: "correlation", snapshot: completed },
      };
      expect(parseHostCommandResponse(response)).toEqual(response);
      expect(() =>
        parseHostCommandResponse({ ...response, result: { correlationId: "correlation" } }),
      ).toThrow();
      expect(() => parseHostCommand({ ...envelope, version: 56 })).toThrow();
    }
    const event = {
      event: "records.export.changed",
      payload: completed,
      sequence: 12,
      version: HOST_PROTOCOL_VERSION,
    };
    expect(parseHostEvent(event)).toEqual(event);
    expect(() =>
      parseHostCommand({
        command: "records.export.discard",
        id: "discard",
        version: HOST_PROTOCOL_VERSION,
        payload: { artifactId: id },
      }),
    ).toThrow();
  });

  it("accepts empty JSONL and smaller host limits without allowing bytes or file paths in snapshots", () => {
    expect(parseRecordExportSnapshot(completed)).toEqual(completed);
    expect(parseRecordExportSnapshot(empty)).toEqual(empty);
    expect(
      parseRecordExportSnapshot({
        ...completed,
        operation: { ...completed.operation!, limits: { ...RECORD_EXPORT_LIMITS, bytes: 1_024 } },
      }).operation?.limits.bytes,
    ).toBe(1_024);
    for (const change of [
      { state: "reading" },
      { reason: "records-unavailable" },
      { artifact: null },
      { coverage: null },
      { coverage: { ...completed.operation!.coverage!, reason: "cancelled" } },
      { counts: { ...completed.operation!.counts, unavailableRecords: 1 } },
      { completedAt: null },
      { path: "/tmp/export" },
      { limits: { ...RECORD_EXPORT_LIMITS, bytes: RECORD_EXPORT_LIMITS.bytes + 1 } },
      { counts: { ...completed.operation!.counts, writtenRecords: 100_001 } },
      { artifact: { ...completed.operation!.artifact!, content: "record-plaintext" } },
      { artifact: { ...completed.operation!.artifact!, receiptSha256: "bad" } },
      {
        artifact: {
          ...completed.operation!.artifact!,
          output: { ...completed.operation!.artifact!.output, fileName: "../orders.jsonl" },
        },
      },
      {
        artifact: {
          ...completed.operation!.artifact!,
          output: { ...completed.operation!.artifact!.output, bytes: 1 },
        },
      },
    ])
      expect(() =>
        parseRecordExportSnapshot({
          ...completed,
          operation: { ...completed.operation!, ...change },
        }),
      ).toThrow();
  });

  it("receipts distinguish a completed range from honest partial output", () => {
    const {
      jobId,
      input: request,
      source,
      settings,
      limits,
      startedAt,
      completedAt,
      counts,
      coverage,
      artifact,
    } = completed.operation!;
    const receipt = {
      schema: "streamskope.record-export/v1",
      jobId,
      input: request,
      source,
      settings,
      limits,
      startedAt,
      completedAt,
      counts,
      coverage,
      output: artifact!.output,
      outcome: "complete",
      reason: "range-complete",
    };
    expect(parseRecordExportReceipt(receipt)).toEqual(receipt);
    expect(
      parseRecordExportReceipt({ ...receipt, outcome: "partial", reason: "cancelled" }).outcome,
    ).toBe("partial");
    expect(() => parseRecordExportReceipt({ ...receipt, reason: "record-limit" })).toThrow();
    expect(() => parseRecordExportReceipt({ ...receipt, coverage: null })).toThrow();
    expect(() =>
      parseRecordExportReceipt({ ...receipt, coverage: { ...coverage!, reason: "cancelled" } }),
    ).toThrow();
    expect(() =>
      parseRecordExportReceipt({ ...receipt, counts: { ...counts, unavailableRecords: 1 } }),
    ).toThrow();
    expect(() =>
      parseRecordExportReceipt({ ...receipt, output: { ...artifact!.output, bytes: 9 } }),
    ).toThrow();
  });
});
