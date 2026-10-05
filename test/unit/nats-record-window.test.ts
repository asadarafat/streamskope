import { describe, expect, it } from "vitest";

import { appendNatsRecords, emptyNatsRecordWindow } from "../../src/features/nats/ui/record-window";
import { uiNatsRecord } from "../support/nats-ui-host-fixture";

describe("independent NATS viewer retention", () => {
  it("evicts the oldest records without replacing identity or mutating input", () => {
    const original = [uiNatsRecord("first"), uiNatsRecord("second")];
    const first = appendNatsRecords(emptyNatsRecordWindow(), original, {
      records: 2,
      bytes: 10000,
    });
    const next = appendNatsRecords(first, [uiNatsRecord("third")], { records: 2, bytes: 10000 });
    expect(next.records.map((record) => record.id)).toEqual(["second", "third"]);
    expect(next.evictedRecords).toBe(1);
    expect(first.records).toEqual(original);
  });
  it("bounds UTF-8 encoded retained evidence including escaping and metadata", () => {
    const first = uiNatsRecord("escaped", "generation-1", '\u0000"é');
    const second = uiNatsRecord("binary-like", "generation-1", "second");
    const secondBytes = new TextEncoder().encode(JSON.stringify(second)).length;
    const result = appendNatsRecords(emptyNatsRecordWindow(), [first, second], {
      records: 10,
      bytes: secondBytes,
    });
    expect(result.records).toEqual([second]);
    expect(result.bytes).toBe(secondBytes);
    expect(result.evictedRecords).toBe(1);
    expect(new TextEncoder().encode(JSON.stringify(first)).length).toBeGreaterThan(
      first.payloadBytes,
    );
  });
  it("accounts a record larger than the entire viewer budget as an eviction", () => {
    const result = appendNatsRecords(emptyNatsRecordWindow(), [uiNatsRecord()], {
      records: 2,
      bytes: 1,
    });
    expect(result).toEqual({ records: [], bytes: 0, evictedRecords: 1 });
  });
});
