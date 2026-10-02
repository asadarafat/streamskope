import { describe, expect, it } from "vitest";

import {
  parseKafkaFetchRequest,
  parseKafkaInvestigationQuery,
  parseKafkaQueryTimestamp,
} from "../../src/features/kafka/contracts";

describe("bounded investigation queries", () => {
  it("preserves a half-open historical request through a versioned round trip", () => {
    const query = {
      schemaVersion: 1,
      request: {
        topic: "orders",
        mode: "time-window",
        maxMessages: 100,
        startTimeMs: Date.parse("2026-07-24T14:03:00Z"),
        endTimeMs: Date.parse("2026-07-24T14:04:00Z"),
      },
    };
    expect(parseKafkaInvestigationQuery(JSON.parse(JSON.stringify(query)))).toEqual(query);
  });

  it.each(["tail", "newest", "earliest"])("preserves the existing %s read", (mode) => {
    const query = { schemaVersion: 1, request: { topic: "orders", mode, maxMessages: 10 } };
    expect(parseKafkaInvestigationQuery(query)).toEqual(query);
  });

  it.each([
    { schemaVersion: 2 },
    { credentials: { password: "not-query-data" } },
    { messages: [{ payload: "not-query-data" }] },
    { request: { topic: "orders", mode: "earliest", maxMessages: 1_001 } },
    { request: { topic: "", mode: "earliest", maxMessages: 1 } },
    { request: { topic: "x".repeat(513), mode: "earliest", maxMessages: 1 } },
    {
      request: {
        topic: "orders",
        mode: "time-window",
        maxMessages: 1,
        startTimeMs: 100,
        endTimeMs: 99,
      },
    },
  ])("rejects incompatible, excessive or undeclared query data %#", (override) => {
    expect(() =>
      parseKafkaInvestigationQuery({
        schemaVersion: 1,
        request: { topic: "orders", mode: "earliest", maxMessages: 10 },
        ...override,
      }),
    ).toThrow();
  });

  it("converts explicit offsets independent of the host time zone", () => {
    const expected = Date.parse("2026-07-24T14:03:00.125Z");
    expect(parseKafkaQueryTimestamp("2026-07-24T16:03:00.125+02:00", "Start time")).toBe(expected);
    expect(parseKafkaQueryTimestamp("2026-07-24T09:03:00.125-05:00", "Start time")).toBe(expected);
    expect(parseKafkaQueryTimestamp("2026-07-24T14:03:00.125Z", "Start time")).toBe(expected);
  });

  it.each([
    "2026-07-24T14:03:00", // ambiguous local time
    "07/24/2026 14:03:00", // locale-dependent date
    "2026-02-30T14:03:00Z", // Date.parse otherwise normalizes this to March
    "2026-07-24T24:00:00Z", // no silent next-day normalization
    "2026-07-24T14:60:00Z",
    "2026-07-24T14:03:60Z",
    "2026-07-24T14:03:00+24:00",
    "1969-12-31T23:59:59Z",
    "",
    "x".repeat(10_000),
  ])("rejects invalid or ambiguous timestamp %s", (value) => {
    expect(() => parseKafkaQueryTimestamp(value, "Start time")).toThrow();
  });

  it("rejects host timestamps outside the representable Date range", () => {
    expect(() =>
      parseKafkaFetchRequest({
        topic: "orders",
        mode: "time-window",
        maxMessages: 1,
        startTimeMs: 8_640_000_000_000_000,
        endTimeMs: 8_640_000_000_000_001,
      }),
    ).toThrow();
  });
});
