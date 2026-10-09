import { describe, expect, it, vi } from "vitest";

import { RecordAnalysisAccumulator } from "../../src/features/kafka/application/record-analysis-accumulator";
import { RecordAnalysisProjection } from "../../src/features/kafka/application/record-analysis-projection";
import { protectKafkaRecord } from "../../src/features/kafka/application/record-protection";
import type { RecordReadSettings } from "../../src/features/kafka/contracts/finite-record-read";
import {
  RECORD_ANALYSIS_LIMITS,
  type RecordAnalysisInput,
  type RecordAnalysisLimits,
} from "../../src/features/kafka/contracts/record-analysis";
import { compileKafkaProjectionPath } from "../../src/features/kafka/contracts/rule-expression-parser";
import {
  KafkaRuleWorkBudget,
  readKafkaProjectionPath,
} from "../../src/features/kafka/contracts/rule-expression-evaluator";
import type { RecordField } from "../../src/features/kafka/contracts/structured-record";
import type { KafkaMessage } from "../../src/features/kafka/contracts/types";

const settings: RecordReadSettings = {
  codecs: { key: "auto", value: "auto" },
  protection: { readOnly: false, maskKey: false, maskHeaders: [], valuePaths: [] },
};
const input: RecordAnalysisInput = {
  requestId: "analysis",
  topic: "records",
  range: { mode: "earliest" },
  search: { key: "", value: "", partition: null, offset: "", timestamp: "" },
  maxRecords: 100,
  columns: [{ id: "field", label: "Field", source: "value", path: "$.field" }],
  groupBy: "field",
};
const nullField: RecordField = { state: "null", codec: "auto", writerSchema: null };
const decoded = (value: unknown): RecordField => ({
  state: "decoded",
  codec: "json",
  text: JSON.stringify(value),
  json: JSON.stringify(value),
  writerSchema: null,
});
function message(
  value: RecordField = decoded({ field: 1 }),
  key: RecordField = nullField,
): KafkaMessage {
  return {
    id: "1",
    topic: "records",
    partition: 0,
    offset: "1",
    timestamp: "2026-10-09T12:00:00.000Z",
    originalByteSize: 12,
    headers: {},
    key: "incorrect alias",
    payload: "incorrect alias",
    preview: "incorrect alias",
    truncated: false,
    original: {
      state: "complete",
      encoding: "base64",
      key: "c2VjcmV0",
      value: "c2VjcmV0",
      headers: [],
    },
    structured: {
      version: 1,
      key,
      value,
      headers: [],
      headersState: "complete",
      protection: "none",
    },
  };
}
const limits = (patch: Partial<RecordAnalysisLimits>): RecordAnalysisLimits => ({
  ...RECORD_ANALYSIS_LIMITS,
  ...patch,
});
const bytes = (value: unknown): number => Buffer.byteLength(JSON.stringify(value), "utf8");

describe("bounded scalar selectors", () => {
  it("uses the existing quoted-property and array-index grammar with own properties only", () => {
    const path = compileKafkaProjectionPath('$["a.b"][0]["a/b"]');
    expect(
      readKafkaProjectionPath(path, { "a.b": [{ "a/b": "exact" }] }, new KafkaRuleWorkBudget()),
    ).toEqual({ found: true, value: "exact" });
    const ownProto: unknown = JSON.parse('{"__proto__":{"field":7}}');
    expect(
      readKafkaProjectionPath(
        compileKafkaProjectionPath("$.__proto__.field"),
        ownProto,
        new KafkaRuleWorkBudget(),
      ),
    ).toEqual({ found: true, value: 7 });
    expect(
      readKafkaProjectionPath(
        compileKafkaProjectionPath("$.__proto__"),
        {},
        new KafkaRuleWorkBudget(),
      ),
    ).toEqual({ found: false });
    expect(
      readKafkaProjectionPath(
        compileKafkaProjectionPath("$.constructor"),
        {},
        new KafkaRuleWorkBudget(),
      ),
    ).toEqual({ found: false });
    const inheritedArray = new Array<unknown>(1);
    Object.setPrototypeOf(inheritedArray, { 0: "inherited" });
    expect(
      readKafkaProjectionPath(
        compileKafkaProjectionPath("$[0]"),
        inheritedArray,
        new KafkaRuleWorkBudget(),
      ),
    ).toEqual({ found: false });
  });

  it.each([
    "",
    "@.field",
    "$.field exists",
    "$.field == 1",
    "($.field)",
    "$[*]",
    "$..field",
    "$[?(@.field)]",
    "$.field || $.other",
    `$.${"x".repeat(255)}`,
    `$${".x".repeat(17)}`,
  ])("rejects non-scalar or over-limit path %s", (path) => {
    expect(() => compileKafkaProjectionPath(path)).toThrow();
  });

  it("parses each canonical source only once across selected columns and ignores aliases and original bytes", () => {
    const columns = [
      { id: "one", label: "One", source: "value", path: "$.field" },
      { id: "two", label: "Two", source: "value", path: "$.field" },
      { id: "key", label: "Key", source: "key", path: "$" },
    ] as const;
    const record = message(decoded({ field: "9007199254740993" }), decoded("key"));
    const before = JSON.stringify(record);
    const projection = new RecordAnalysisProjection(columns, settings);
    const parse = vi.spyOn(JSON, "parse");
    const cells = projection.project(record, new KafkaRuleWorkBudget());
    expect(parse).toHaveBeenCalledTimes(2);
    expect(cells).toEqual([
      { state: "scalar", value: "9007199254740993" },
      { state: "scalar", value: "9007199254740993" },
      { state: "scalar", value: "key" },
    ]);
    expect(JSON.stringify(record)).toBe(before);
  });

  it("classifies actual policy masks, escaped pointers and masked ancestors without treating ordinary sentinel text as hidden", () => {
    const policy = {
      ...settings.protection,
      valuePaths: ["/a~1b", "/secret", "/absent"],
      maskKey: true,
    };
    const captured = { ...settings, protection: policy };
    const record = protectKafkaRecord(
      message(
        decoded({ "a/b": { child: "private" }, secret: "private", literal: "[MASKED]" }),
        decoded("private"),
      ),
      policy,
    );
    const columns = [
      { id: "child", label: "Child", source: "value", path: '$["a/b"].child' },
      { id: "secret", label: "Secret", source: "value", path: "$.secret" },
      { id: "literal", label: "Literal", source: "value", path: "$.literal" },
      { id: "absent", label: "Absent", source: "value", path: "$.absent" },
      { id: "key", label: "Key", source: "key", path: "$" },
    ] as const;
    expect(
      new RecordAnalysisProjection(columns, captured).project(record, new KafkaRuleWorkBudget()),
    ).toEqual([
      { state: "masked" },
      { state: "masked" },
      { state: "scalar", value: "[MASKED]" },
      { state: "missing" },
      { state: "masked" },
    ]);
  });

  it("keeps arrays, objects, malformed JSON, byte codecs and uncaptured fields explicit", () => {
    const project = (field: RecordField): unknown =>
      new RecordAnalysisProjection([{ ...input.columns[0]!, path: "$" }], settings).project(
        message(field),
        new KafkaRuleWorkBudget(),
      )[0];
    expect(project(decoded([]))).toEqual({ state: "unavailable", reason: "array" });
    expect(project(decoded({}))).toEqual({ state: "unavailable", reason: "object" });
    expect(
      project({ state: "decoded", codec: "json", text: "{", json: "{", writerSchema: null }),
    ).toEqual({ state: "unavailable", reason: "decoding-error" });
    expect(
      project({
        state: "decoded",
        codec: "bytes",
        text: "Ynl0ZXM=",
        json: null,
        writerSchema: null,
      }),
    ).toEqual({ state: "unavailable", reason: "bytes" });
    expect(
      project({
        state: "decoded",
        codec: "utf8",
        text: "ordinary text",
        json: null,
        writerSchema: null,
      }),
    ).toEqual({ state: "scalar", value: "ordinary text" });
    const uncaptured = { ...message() };
    delete uncaptured.structured;
    expect(
      new RecordAnalysisProjection(input.columns, settings).project(
        uncaptured,
        new KafkaRuleWorkBudget(),
      ),
    ).toEqual([{ state: "unavailable", reason: "not-captured" }]);
    let nested: unknown = null;
    for (let depth = 0; depth < 70; depth++) nested = [nested];
    expect(project(decoded(nested))).toEqual({ state: "unavailable", reason: "sample-limit" });
  });

  it("projects UTF-8 keys and values only at the root without guessing JSON or coercing strings", () => {
    const text: RecordField = {
      state: "decoded",
      codec: "utf8",
      text: '{"field":123}',
      json: null,
      writerSchema: null,
    };
    const projection = new RecordAnalysisProjection(
      [
        { id: "text", label: "Text", source: "value", path: "$" },
        { id: "nested", label: "Nested", source: "value", path: "$.field" },
        { id: "key", label: "Key", source: "key", path: "$" },
      ],
      settings,
    );
    const key = { ...text, text: "123" };
    const parse = vi.spyOn(JSON, "parse");
    expect(projection.project(message(text, key), new KafkaRuleWorkBudget())).toEqual([
      { state: "scalar", value: '{"field":123}' },
      { state: "unavailable", reason: "not-json" },
      { state: "scalar", value: "123" },
    ]);
    expect(parse).not.toHaveBeenCalled();
  });
});

describe("atomic bounded analysis", () => {
  it("groups typed scalars, JSON null, missing paths and tombstones independently, excluding masked and undecodable values", () => {
    const accumulator = new RecordAnalysisAccumulator(input, settings);
    const fields = [
      decoded({ field: 1 }),
      decoded({ field: "1" }),
      decoded({ field: null }),
      decoded({}),
      nullField,
      {
        state: "error",
        codec: "json",
        code: "malformed",
        detail: "hidden diagnostic",
        writerSchema: null,
      } as RecordField,
      { state: "masked", codec: "json", writerSchema: null } as RecordField,
      decoded({ field: "[MASKED]" }),
    ];
    for (const field of fields) expect(accumulator.accept(message(field))).toBe("committed");
    const result = accumulator.snapshot();
    expect(accumulator.countedRecords).toBe(8);
    expect(result.columns).toEqual([
      {
        columnId: "field",
        scalar: 4,
        missing: 1,
        nullKey: 0,
        tombstone: 1,
        masked: 1,
        unavailable: 1,
      },
    ]);
    expect(result.grouping).toEqual({
      groups: [
        { key: { state: "scalar", value: 1 }, count: 1 },
        { key: { state: "scalar", value: "1" }, count: 1 },
        { key: { state: "scalar", value: null }, count: 1 },
        { key: { state: "missing" }, count: 1 },
        { key: { state: "tombstone" }, count: 1 },
        { key: { state: "scalar", value: "[MASKED]" }, count: 1 },
      ],
      groupedRecords: 6,
      excluded: { masked: 1, unavailable: 1 },
    });
    expect(JSON.stringify(result)).not.toContain("hidden diagnostic");
    expect(result.previewBytes).toBe(bytes(result.preview));
  });

  it("distinguishes absent Kafka keys from decoded JSON null and missing key properties", () => {
    const byKey = {
      ...input,
      columns: [{ id: "key", label: "Key", source: "key" as const, path: "$" }],
      groupBy: "key",
    };
    const accumulator = new RecordAnalysisAccumulator(byKey, settings);
    expect(accumulator.accept(message(decoded({}), nullField))).toBe("committed");
    expect(accumulator.accept(message(decoded({}), decoded(null)))).toBe("committed");
    expect(accumulator.snapshot().grouping?.groups).toEqual([
      { key: { state: "null-key" }, count: 1 },
      { key: { state: "scalar", value: null }, count: 1 },
    ]);
    expect(accumulator.snapshot().columns[0]?.nullKey).toBe(1);
  });

  it("refuses a new group atomically while an existing group remains countable", () => {
    const accumulator = new RecordAnalysisAccumulator(input, settings, limits({ groups: 1 }));
    expect(accumulator.accept(message(decoded({ field: "first" })))).toBe("committed");
    const before = accumulator.snapshot();
    expect(accumulator.accept(message(decoded({ field: "second" })))).toBe("group-limit");
    expect(accumulator.countedRecords).toBe(1);
    expect(accumulator.snapshot()).toEqual(before);
    expect(accumulator.accept(message(decoded({ field: "first" })))).toBe("committed");
    expect(accumulator.snapshot().grouping?.groups).toEqual([
      { key: { state: "scalar", value: "first" }, count: 2 },
    ]);
  });

  it("keeps exact grouping when a preview scalar exceeds its cell limit and refuses oversized group keys without truncating", () => {
    const value = "🧭".repeat(20);
    const groupBytes = bytes(["scalar", value]);
    const accumulator = new RecordAnalysisAccumulator(
      input,
      settings,
      limits({ cellBytes: 8, groupKeyBytes: groupBytes }),
    );
    expect(accumulator.accept(message(decoded({ field: value })))).toBe("committed");
    const result = accumulator.snapshot();
    expect(result.columns[0]?.scalar).toBe(1);
    expect(result.preview[0]?.cells).toEqual([{ state: "unavailable", reason: "value-limit" }]);
    expect(result.grouping?.groups).toEqual([{ key: { state: "scalar", value }, count: 1 }]);
    expect(accumulator.accept(message(decoded({ field: `${value}x` })))).toBe("group-key-limit");
    expect(accumulator.snapshot()).toEqual(result);
  });

  it("continues exact counts after the preview row or byte capacity is reached", () => {
    const first = new RecordAnalysisAccumulator(input, settings, limits({ previewRows: 1 }));
    for (let index = 0; index < 50; index++) expect(first.accept(message())).toBe("committed");
    const snapshot = first.snapshot();
    expect(first.countedRecords).toBe(50);
    expect(snapshot.preview).toHaveLength(1);
    expect(snapshot.previewOmittedRecords).toBe(49);
    expect(snapshot.grouping?.groups[0]?.count).toBe(50);
    const second = new RecordAnalysisAccumulator(
      input,
      settings,
      limits({ previewBytes: bytes(snapshot.preview) }),
    );
    expect(second.accept(message())).toBe("committed");
    expect(second.accept(message())).toBe("committed");
    expect(second.snapshot().preview).toHaveLength(1);
    expect(second.snapshot().previewOmittedRecords).toBe(1);
    expect(second.snapshot().previewBytes).toBe(bytes(snapshot.preview));
  });

  it("enforces the actual UTF-8 result boundary before any partial record is committed", () => {
    const record = message(decoded({ field: 'comma, quote" newline\n🧭' }));
    const measuring = new RecordAnalysisAccumulator(input, settings);
    expect(measuring.accept(record)).toBe("committed");
    const first = measuring.snapshot();
    const accumulator = new RecordAnalysisAccumulator(
      input,
      settings,
      limits({ resultBytes: bytes(first) }),
    );
    expect(accumulator.accept(record)).toBe("committed");
    expect(accumulator.accept(record)).toBe("result-byte-limit");
    expect(accumulator.countedRecords).toBe(1);
    expect(accumulator.snapshot()).toEqual(first);
  });

  it("charges shared parsing and evaluation work, and rejects the whole record on exhaustion", () => {
    const measuring = new RecordAnalysisAccumulator(input, settings);
    expect(measuring.accept(message())).toBe("committed");
    const work = measuring.snapshot().workUnits;
    const accumulator = new RecordAnalysisAccumulator(
      input,
      settings,
      limits({ work: work * 2 - 1 }),
    );
    expect(accumulator.accept(message())).toBe("committed");
    const before = accumulator.snapshot();
    expect(accumulator.accept(message())).toBe("work-limit");
    expect(accumulator.snapshot()).toEqual(before);
    expect(accumulator.countedRecords).toBe(1);
    const perRecord = new RecordAnalysisAccumulator(
      input,
      settings,
      limits({ recordWork: work - 1 }),
    );
    const empty = perRecord.snapshot();
    expect(perRecord.accept(message())).toBe("work-limit");
    expect(perRecord.snapshot()).toEqual(empty);
    expect(perRecord.countedRecords).toBe(0);
  });

  it("counts without touching canonical data when no projection was requested", () => {
    const accumulator = new RecordAnalysisAccumulator(
      { ...input, columns: [], groupBy: null },
      settings,
    );
    const record = message();
    Object.defineProperty(record, "structured", {
      get: (): never => {
        throw new Error("Canonical data must not be parsed for count-only.");
      },
    });
    expect(accumulator.accept(record)).toBe("committed");
    expect(accumulator.snapshot().columns).toEqual([]);
    expect(accumulator.snapshot().grouping).toBeNull();
    expect(accumulator.countedRecords).toBe(1);
  });

  it("does not expose mutable retained rows or group objects through snapshots", () => {
    const accumulator = new RecordAnalysisAccumulator(input, settings);
    accumulator.accept(message());
    const result = accumulator.snapshot();
    const before = JSON.stringify(result);
    Object.assign(result.preview[0]!.cells[0]!, { state: "scalar", value: "mutated" });
    Object.assign(result.grouping!.groups[0]!.key, { state: "scalar", value: "mutated" });
    Object.assign(result.columns[0]!, { scalar: 999 });
    expect(JSON.stringify(accumulator.snapshot())).toBe(before);
  });
});
