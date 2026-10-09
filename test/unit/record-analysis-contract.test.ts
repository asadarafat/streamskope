import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  RECORD_ANALYSIS_LIMITS,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
  parseRecordAnalysisInput,
  parseRecordAnalysisSnapshot,
  type RecordAnalysisInput,
  type RecordAnalysisOperation,
  type RecordAnalysisSnapshot,
} from "../../src/features/kafka/contracts";

const id = "2365efb2-58ad-4375-a9e6-3626a0026500";
const input: RecordAnalysisInput = {
  requestId: id,
  topic: "orders",
  range: { mode: "earliest" },
  maxRecords: 100_000,
  search: { key: "", value: "", offset: "", timestamp: "", partition: null },
  columns: [{ id: "kind", label: "Kind", source: "value", path: "$.kind" }],
  groupBy: "kind",
};
const empty: RecordAnalysisSnapshot = { scopeId: id, revision: 0, operation: null };
function complete(): RecordAnalysisSnapshot {
  const preview = [
    {
      partition: 0,
      offset: "4",
      timestamp: "2026-10-09T10:00:00.000Z",
      cells: [{ state: "scalar" as const, value: "order" }],
    },
  ];
  return {
    ...empty,
    revision: 4,
    operation: {
      jobId: id,
      input,
      state: "completed",
      reason: "range-complete",
      error: null,
      limits: RECORD_ANALYSIS_LIMITS,
      source: { connectionName: "Lab", clusterId: "cluster", topicId: "topic" },
      settings: {
        codecs: { key: "auto", value: "auto" },
        protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
      },
      startedAt: "2026-10-09T10:00:00.000Z",
      completedAt: "2026-10-09T10:01:00.000Z",
      counts: {
        passes: 1,
        scannedRecords: 1,
        scannedBytes: 100,
        countedRecords: 1,
        unavailableRecords: 0,
      },
      coverage: {
        reason: "range-complete",
        scannedRecords: 1,
        scannedBytes: 100,
        matchedRecords: 1,
        unavailableRecords: 0,
        partitions: [{ partition: 0, startOffset: "4", endOffset: "5", nextOffset: "5" }],
      },
      result: {
        columns: [
          {
            columnId: "kind",
            scalar: 1,
            missing: 0,
            nullKey: 0,
            tombstone: 0,
            masked: 0,
            unavailable: 0,
          },
        ],
        grouping: {
          groups: [{ key: { state: "scalar", value: "order" }, count: 1 }],
          groupedRecords: 1,
          excluded: { masked: 0, unavailable: 0 },
        },
        preview,
        previewOmittedRecords: 0,
        previewBytes: Buffer.byteLength(JSON.stringify(preview)),
        workUnits: 100,
      },
    },
  };
}
function altered(change: Partial<RecordAnalysisOperation>): RecordAnalysisSnapshot {
  const snapshot = complete();
  return { ...snapshot, operation: { ...snapshot.operation!, ...change } };
}

describe("bounded analysis protocol", () => {
  it("accepts count-only and finite time ranges while rejecting executable or unbounded selectors", () => {
    expect(parseRecordAnalysisInput(input)).toEqual(input);
    const countOnly = {
      ...input,
      range: { mode: "time-window", startTimeMs: 1, endTimeMs: 2 },
      columns: [],
      groupBy: null,
    };
    expect(parseRecordAnalysisInput(countOnly)).toEqual(countOnly);
    for (const change of [
      { requestId: "../job" },
      { topic: " " },
      { range: { mode: "tail" } },
      { maxRecords: 0 },
      { maxRecords: 100_001 },
      { columns: [input.columns[0], input.columns[0]] },
      { groupBy: "absent" },
      { columns: [] },
      { code: "return record" },
    ])
      expect(() => parseRecordAnalysisInput({ ...input, ...change })).toThrow();
    for (const path of [
      "$.*",
      "$..kind",
      "$[?(@.kind)]",
      "headers.kind",
      "$.kind + 1",
      "$" + ".a".repeat(17),
      "$['" + "a".repeat(255) + "']",
    ])
      expect(() =>
        parseRecordAnalysisInput({ ...input, columns: [{ ...input.columns[0], path }] }),
      ).toThrow();
  });

  it("roundtrips every typed command, response and event and rejects the previous host protocol", () => {
    for (const [command, payload] of [
      ["records.analysis.start", input],
      ["records.analysis.status", {}],
      ["records.analysis.cancel", { jobId: id }],
      ["records.analysis.discard", { jobId: id }],
    ] as const) {
      const envelope = { id: "request", version: HOST_PROTOCOL_VERSION, command, payload };
      expect(parseHostCommand(envelope)).toEqual(envelope);
      expect(() => parseHostCommand({ ...envelope, version: HOST_PROTOCOL_VERSION - 1 })).toThrow();
      const response = {
        id: "request",
        version: HOST_PROTOCOL_VERSION,
        command,
        ok: true,
        result: { correlationId: "correlation", snapshot: complete() },
      };
      expect(parseHostCommandResponse(response)).toEqual(response);
    }
    const event = {
      event: "records.analysis.changed",
      payload: complete(),
      sequence: 12,
      version: HOST_PROTOCOL_VERSION,
    };
    expect(parseHostEvent(event)).toEqual(event);
    expect(parseRecordAnalysisSnapshot(empty)).toEqual(empty);
  });

  it("requires coherent counts, bounded results and effective field limits", () => {
    const operation = complete().operation!;
    expect(parseRecordAnalysisSnapshot(complete())).toEqual(complete());
    for (const change of [
      { state: "reading" as const },
      { counts: { ...operation.counts, countedRecords: 2 } },
      { counts: { ...operation.counts, unavailableRecords: 1 } },
      { counts: { ...operation.counts, scannedRecords: 0 } },
      { counts: { ...operation.counts, passes: 1001 } },
      { coverage: { ...operation.coverage!, scannedBytes: 99 } },
      { result: { ...operation.result!, previewBytes: 0 } },
      { result: { ...operation.result!, previewOmittedRecords: 1 } },
      { result: { ...operation.result!, workUnits: RECORD_ANALYSIS_LIMITS.work + 1 } },
      { limits: { ...RECORD_ANALYSIS_LIMITS, resultBytes: 100 } },
      { limits: { ...RECORD_ANALYSIS_LIMITS, pathCharacters: 2 } },
      { limits: { ...RECORD_ANALYSIS_LIMITS, recordWork: RECORD_ANALYSIS_LIMITS.recordWork + 1 } },
      {
        input: { ...input, columns: [{ ...input.columns[0]!, path: "$.kind.a" }] },
        limits: { ...RECORD_ANALYSIS_LIMITS, pathSegments: 1 },
      },
    ])
      expect(() => parseRecordAnalysisSnapshot(altered(change))).toThrow();
  });

  it("keeps failed/revoked operations distinct from bounded partial results", () => {
    const operation = complete().operation!;
    for (const reason of ["cancelled", "record-limit", "group-limit", "work-limit"] as const)
      expect(
        parseRecordAnalysisSnapshot(altered({ state: "partial", reason })).operation?.reason,
      ).toBe(reason);
    for (const reason of ["read-failed", "analysis-failed", "cleanup-failed", "revoked"] as const)
      expect(() => parseRecordAnalysisSnapshot(altered({ state: "partial", reason }))).toThrow();
    expect(() =>
      parseRecordAnalysisSnapshot(altered({ state: "completed", result: null })),
    ).toThrow();
    expect(() =>
      parseRecordAnalysisSnapshot(
        altered({ state: "failed", result: null, reason: "read-failed" }),
      ),
    ).toThrow();
    expect(() =>
      parseRecordAnalysisSnapshot(altered({ state: "revoked", reason: "revoked" })),
    ).toThrow();
    const revoked = altered({ state: "revoked", reason: "revoked", result: null, error: null });
    expect(parseRecordAnalysisSnapshot(revoked)).toEqual(revoked);
    expect(operation.error).toBeNull();
  });

  it("rejects preview locators outside acknowledged captured bounds and duplicate locators", () => {
    const operation = complete().operation!;
    const row = operation.result!.preview[0]!;
    for (const preview of [
      [{ ...row, offset: "3" }],
      [{ ...row, offset: "5" }],
      [{ ...row, partition: 1 }],
    ]) {
      expect(() =>
        parseRecordAnalysisSnapshot(
          altered({
            result: {
              ...operation.result!,
              preview,
              previewBytes: Buffer.byteLength(JSON.stringify(preview)),
            },
          }),
        ),
      ).toThrow();
    }
    expect(() =>
      parseRecordAnalysisSnapshot(
        altered({ state: "partial", reason: "checkpoint-unavailable", coverage: null }),
      ),
    ).toThrow();
    const duplicate = [row, row];
    expect(() =>
      parseRecordAnalysisSnapshot(
        altered({
          counts: { ...operation.counts, scannedRecords: 2, countedRecords: 2 },
          coverage: {
            ...operation.coverage!,
            scannedRecords: 2,
            matchedRecords: 2,
            partitions: [{ partition: 0, startOffset: "4", nextOffset: "6", endOffset: "6" }],
          },
          result: {
            ...operation.result!,
            columns: [{ ...operation.result!.columns[0]!, scalar: 2 }],
            grouping: {
              groups: [{ key: { state: "scalar", value: "order" }, count: 2 }],
              groupedRecords: 2,
              excluded: { masked: 0, unavailable: 0 },
            },
            preview: duplicate,
            previewBytes: Buffer.byteLength(JSON.stringify(duplicate)),
          },
        }),
      ),
    ).toThrow(/repeat record locators/u);
  });

  it("reconciles typed groups, field classifications and exclusions without merging unlike scalars", () => {
    const operation = complete().operation!;
    const result = operation.result!;
    for (const grouping of [
      {
        groups: [{ key: { state: "masked" }, count: 1 }],
        groupedRecords: 1,
        excluded: { masked: 0, unavailable: 0 },
      },
      { ...result.grouping!, excluded: { masked: 1, unavailable: 0 } },
      { ...result.grouping!, groups: [{ key: { state: "scalar", value: "order" }, count: 2 }] },
      { ...result.grouping!, groups: [...result.grouping!.groups, ...result.grouping!.groups] },
    ])
      expect(() =>
        parseRecordAnalysisSnapshot(
          altered({ result: { ...result, grouping } as RecordAnalysisOperation["result"] }),
        ),
      ).toThrow();
    const preview = [{ ...result.preview[0]!, cells: [{ state: "masked" as const }] }];
    const protectedResult = {
      ...result,
      columns: [{ ...result.columns[0]!, scalar: 0, masked: 1 }],
      grouping: { groups: [], groupedRecords: 0, excluded: { masked: 1, unavailable: 0 } },
      preview,
      previewBytes: Buffer.byteLength(JSON.stringify(preview)),
    };
    expect(parseRecordAnalysisSnapshot(altered({ result: protectedResult })).operation?.state).toBe(
      "completed",
    );
  });
  it("admits a bounded result alongside the full partition inventory and protected settings", () => {
    const operation = complete().operation!;
    const groups = Array.from({ length: 256 }, (_, index) => ({
      key: { state: "scalar" as const, value: `${String(index)}:${"x".repeat(750)}` },
      count: 1,
    }));
    const preview = groups.slice(0, 200).map((group, index) => ({
      partition: index,
      offset: "9223372036854775806",
      timestamp: "2026-10-09T10:00:00.000Z",
      cells: [group.key],
    }));
    const result = {
      columns: [{ ...operation.result!.columns[0]!, scalar: 256 }],
      grouping: { groups, groupedRecords: 256, excluded: { masked: 0, unavailable: 0 } },
      preview,
      previewOmittedRecords: 56,
      previewBytes: Buffer.byteLength(JSON.stringify(preview)),
      workUnits: 500_000,
    };
    const coverage = {
      reason: "range-complete" as const,
      matchedRecords: 256,
      scannedRecords: 256,
      scannedBytes: 256_000,
      unavailableRecords: 0,
      partitions: Array.from({ length: 2048 }, (_, partition) => ({
        partition,
        startOffset: "9223372036854775806",
        nextOffset: "9223372036854775807",
        endOffset: "9223372036854775807",
      })),
    };
    const snapshot = altered({
      result,
      coverage,
      counts: {
        ...operation.counts,
        countedRecords: 256,
        scannedRecords: 256,
        scannedBytes: 256_000,
      },
      settings: {
        ...operation.settings,
        protection: {
          ...KAFKA_RECORD_PROTECTION_DEFAULTS,
          valuePaths: Array.from(
            { length: 32 },
            (_, index) => `/${String(index)}${"\u0000".repeat(508)}`,
          ),
          maskHeaders: Array.from(
            { length: 32 },
            (_, index) => `${String(index)}${"\u0000".repeat(508)}`,
          ),
        },
      },
    });
    expect(Buffer.byteLength(JSON.stringify(snapshot))).toBeGreaterThan(512 * 1024);
    expect(Buffer.byteLength(JSON.stringify(snapshot))).toBeLessThan(1024 * 1024);
    expect(parseRecordAnalysisSnapshot(snapshot)).toEqual(snapshot);
  });
});
