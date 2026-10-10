import { expect, it } from "vitest";

import { inspectConnectDlqEvidence } from "../../src/features/kafka/contracts/connect-dlq";
import type { KafkaMessage } from "../../src/features/kafka/contracts";
import type { StructuredRecordHeader } from "../../src/features/kafka/contracts/structured-record";

const values = {
  topic: "orders",
  partition: "0",
  offset: "9223372036854775807",
  "connector.name": "fixture-sink",
  "task.id": "0",
  stage: "VALUE_CONVERTER",
};
const headers = Object.entries(values).map(([key, value]) => ({
  key: "__connect.errors." + key,
  value,
  error: null,
}));
function message(
  entries: readonly StructuredRecordHeader[] = headers,
): Pick<KafkaMessage, "headers" | "truncated" | "structured"> {
  const field = { state: "null", codec: "auto", writerSchema: null } as const;
  return {
    truncated: false,
    headers: Object.fromEntries(entries.map((h) => [h.key, h.value ?? "(null)"])),
    structured: {
      version: 1,
      key: field,
      value: field,
      headers: entries,
      headersState: "complete",
      protection: "none",
    },
  };
}
it("uses the ordered projection instead of conflicting flattened aliases and preserves exact int64 offsets", () => {
  const m = message();
  expect(
    inspectConnectDlqEvidence({
      ...m,
      headers: { ...m.headers, "__connect.errors.topic": "forged" },
    }),
  ).toEqual({
    state: "reported",
    context: {
      topic: "orders",
      partition: "0",
      offset: values.offset,
      connector: "fixture-sink",
      task: "0",
      stage: "VALUE_CONVERTER",
    },
  });
});
it("does not read original bytes to fill missing or protected projections", () => {
  const m = message();
  expect(inspectConnectDlqEvidence({ headers: m.headers, truncated: m.truncated })).toEqual({
    state: "unavailable",
    reason: "incomplete",
  });
  const masked = message(
    headers.map((h) => (h.key.endsWith(".offset") ? { ...h, value: "[MASKED]" } : h)),
  );
  const withOriginal = Object.assign(masked, {
    original: {
      state: "complete",
      encoding: "base64",
      key: null,
      value: null,
      headers: headers.map((h) => ({ key: btoa(h.key), value: btoa(h.value) })),
    },
  });
  expect(inspectConnectDlqEvidence(withOriginal)).toEqual({
    state: "unavailable",
    reason: "protected",
  });
});
it.each(Object.keys(values))(
  "rejects duplicate reserved %s even when the flattened map looks valid",
  (field) => {
    const h = headers.find((h) => h.key === "__connect.errors." + field)!;
    expect(inspectConnectDlqEvidence(message([...headers, { ...h }]))).toEqual({
      state: "unavailable",
      reason: "ambiguous",
    });
  },
);
it.each([
  ["topic", "."],
  ["topic", ".."],
  ["partition", "2147483648"],
  ["partition", "01"],
  ["offset", "9223372036854775808"],
  ["offset", "-1"],
  ["offset", "00"],
  ["connector.name", "a".repeat(201)],
  ["connector.name", "bad\nname"],
  ["task.id", "2147483648"],
  ["stage", "UNKNOWN"],
  ["stage", "VALUE_CONVERTER\n"],
])("rejects invalid %s without clipping it into plausible context", (field, value) => {
  const changed = headers.map((h) => (h.key === "__connect.errors." + field ? { ...h, value } : h));
  expect(inspectConnectDlqEvidence(message(changed))).toEqual({
    state: "unavailable",
    reason: "invalid",
  });
});
it("preserves ordinary repeated and null headers without changing the record", () => {
  const m = message([
    ...headers,
    { key: "trace", value: "first", error: null },
    { key: "trace", value: null, error: null },
  ]);
  const before = structuredClone(m);
  expect(inspectConnectDlqEvidence(m).state).toBe("reported");
  expect(m).toEqual(before);
});
it("distinguishes absent context from truncated, incomplete or malformed reported context", () => {
  expect(inspectConnectDlqEvidence(message([]))).toEqual({ state: "absent" });
  expect(inspectConnectDlqEvidence({ ...message(), truncated: true })).toEqual({
    state: "unavailable",
    reason: "incomplete",
  });
  expect(inspectConnectDlqEvidence(message(headers.slice(1)))).toEqual({
    state: "unavailable",
    reason: "incomplete",
  });
  expect(
    inspectConnectDlqEvidence(
      message(headers.map((h) => (h.key.endsWith(".stage") ? { ...h, value: null } : h))),
    ),
  ).toEqual({ state: "unavailable", reason: "incomplete" });
  expect(
    inspectConnectDlqEvidence(
      message([...headers, { key: "[invalid]", value: null, error: "Invalid UTF8 header name" }]),
    ),
  ).toEqual({ state: "unavailable", reason: "invalid" });
});
