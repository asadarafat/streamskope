import { randomUUID } from "node:crypto";

import { expect, it } from "vitest";

import {
  createKafkaQueryLink,
  parseKafkaQueryTransfer,
  serializeKafkaQuery,
  KAFKA_QUERY_TRANSFER_LIMITS,
} from "../../src/features/kafka/contracts";

const query = {
  schemaVersion: 1,
  request: {
    mode: "time-window",
    topic: "orders",
    maxMessages: 42,
    startTimeMs: 1_780_000_000_000,
    endTimeMs: 1_780_000_060_000,
  },
  filters: {
    key: "incident-☕",
    value: "défaillance",
    offset: "",
    timestamp: "",
    partition: null,
    expression: '$.status == "failed"',
  },
} as const;

it("round-trips UTF-8 filters and exact absolute bounds through JSON, browser links and pasteable desktop links", () => {
  const json = serializeKafkaQuery(query);
  expect(parseKafkaQueryTransfer(json)).toEqual(query);
  for (const base of [
    "https://workbench.example.test/app/?ignored=true#old",
    "streamskope://app/",
  ]) {
    const link = createKafkaQueryLink(query, base);
    expect(new URL(link).search).toBe("");
    expect(parseKafkaQueryTransfer(link)).toEqual(query);
    expect(parseKafkaQueryTransfer(new URL(link).hash)).toEqual(query);
  }
});

const secretSentinel = `test-only-${randomUUID()}`;

it.each([
  { ...query, schemaVersion: 2 },
  { ...query, profileId: "local-profile" },
  { ...query, password: secretSentinel },
  { ...query, messages: [{ value: "private-message" }] },
  { ...query, request: { ...query.request, broker: "broker.invalid:9092" } },
  { ...query, filters: { ...query.filters, expression: "fetch('https://example.test')" } },
])(
  "rejects unsupported or non-configuration fields without echoing their content (%#)",
  (document) => {
    expect(() => parseKafkaQueryTransfer(JSON.stringify(document))).toThrow(
      /invalid or unsupported query/u,
    );
    try {
      parseKafkaQueryTransfer(JSON.stringify(document));
    } catch (error) {
      expect(String(error)).not.toContain(secretSentinel);
      expect(String(error)).not.toContain("private-message");
    }
  },
);

it.each([
  "https://user:password@example.test/#query=e30",
  "https://example.test/?token=secret#query=e30",
  "javascript:alert(1)",
  "file:///tmp/query.json#query=e30",
  "streamskope://other/#query=e30",
  "#query=%%%%",
  "#query=AA",
  "#query=////",
  "#query=__8",
  "[]",
])("rejects unsafe URLs, malformed encodings and invalid documents (%#)", (input) => {
  expect(() => parseKafkaQueryTransfer(input)).toThrow(/invalid or unsupported query/u);
});

it("enforces byte and link limits before parsing an oversized document", () => {
  expect(() =>
    parseKafkaQueryTransfer(" ".repeat(KAFKA_QUERY_TRANSFER_LIMITS.linkCharacters + 1)),
  ).toThrow();
  expect(() => parseKafkaQueryTransfer("{" + "☕".repeat(12_000) + "}")).toThrow();
  const oversized = btoa(" ".repeat(KAFKA_QUERY_TRANSFER_LIMITS.documentBytes + 1)).replace(
    /=+$/u,
    "",
  );
  expect(() => parseKafkaQueryTransfer(`#query=${oversized}`)).toThrow();
  expect(() => createKafkaQueryLink(query, "https://secret:password@example.test/")).toThrow();
});
