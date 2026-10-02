import { describe, expect, it } from "vitest";

import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseKafkaRecordProtection,
  type KafkaExploredMessage,
} from "../../src/features/kafka/contracts";
import { protectKafkaRecord } from "../../src/features/kafka/application/record-protection";
import { translateKafkaRecord } from "../../src/features/kafka/engine/message-record";
import {
  createKafkaMessageExportDocument,
  initialKafkaMessageFilters,
} from "../../src/features/kafka/ui/message-operations";

function record(
  value: Buffer | null = Buffer.from(
    '{"customer":{"email":"secret-email"},"items":[{"token":"secret-token"}],"public":"visible"}',
  ),
): ReturnType<typeof translateKafkaRecord> {
  return translateKafkaRecord(
    {
      topic: "orders",
      partition: 0,
      offset: 1n,
      timestamp: 0n,
      key: Buffer.from("secret-key"),
      value,
      headers: new Map(),
      headerEntries: [
        [Buffer.from("Authorization"), Buffer.from("secret-header")],
        [Buffer.from("Authorization"), Buffer.from("secret-header-2")],
        [Buffer.from("source"), Buffer.from("fixture")],
      ],
    },
    "orders",
  );
}
const policy = {
  ...KAFKA_RECORD_PROTECTION_DEFAULTS,
  maskKey: true,
  maskHeaders: ["Authorization"],
  valuePaths: ["/customer/email", "/items/0/token"],
};

describe("Kafka record disclosure", () => {
  it("removes selected content from every retained representation and the export document", () => {
    const original = record();
    const masked = protectKafkaRecord(original, policy);
    expect(masked.key).toBe("[MASKED]");
    expect(masked.headers).toEqual({ Authorization: "[MASKED]", source: "fixture" });
    expect(masked.payload).toBe(
      '{"customer":{"email":"[MASKED]"},"items":[{"token":"[MASKED]"}],"public":"visible"}',
    );
    expect(masked.original).toEqual({ state: "unavailable", reason: "masked" });
    const explored: KafkaExploredMessage = {
      ...masked,
      ruleEvaluation: {
        state: "evaluated",
        activeMatchCount: 0,
        activeMatches: [],
        durationMicros: 0,
        errorCount: 0,
        errors: [],
        evaluatedRules: 0,
        omittedEvidence: 0,
        omittedRules: 0,
        suppressedMatchCount: 0,
        suppressedMatches: [],
      },
    };
    const exported = createKafkaMessageExportDocument({
      topic: "orders",
      filters: initialKafkaMessageFilters,
      messages: [explored],
      retainedMessageCount: 1,
      stale: false,
    });
    expect(JSON.stringify(masked)).not.toContain("secret-");
    expect(exported.content).not.toContain("secret-");
    expect(exported.content).not.toContain(Buffer.from("secret-key").toString("base64"));
    expect(original.key).toBe("secret-key");
  });
  it.each([
    Buffer.from("secret-plain-text"),
    Buffer.from([0xff, 0x00]),
    Buffer.from('"secret-string"'),
  ])("withholds unstructured content or the whole configured value", (value) => {
    const masked = protectKafkaRecord(record(value), { ...policy, valuePaths: [""] });
    expect(masked.payload).toMatch(/\[MASKED\]/u);
    expect(JSON.stringify(masked)).not.toContain("secret-");
  });
  it("masks truncated previews while preserving a real tombstone", () => {
    const partial = {
      ...record(),
      payload: null,
      payloadTruncated: true,
      preview: "secret-incomplete",
    };
    expect(protectKafkaRecord(partial, policy).preview).toBe("[MASKED]");
    expect(protectKafkaRecord(record(null), policy).payload).toBeNull();
  });
  it("handles escaped JSON Pointer paths and literal prototype keys without mutation", () => {
    const message = record(
      Buffer.from('{"a/b":{"~x":"secret-a"},"__proto__":{"token":"secret-b"},"safe":1}'),
    );
    const result = protectKafkaRecord(message, {
      ...policy,
      valuePaths: ["/a~1b/~0x", "/__proto__/token"],
    });
    expect(JSON.stringify(result)).not.toContain("secret-");
    expect(result.payload).toContain('"safe":1');
    expect(Object.hasOwn({}, "token")).toBe(false);
  });
  it.each(["$.email", "/bad~escape", "/".repeat(17)])("rejects invalid mask path %s", (path) => {
    expect(() => parseKafkaRecordProtection({ ...policy, valuePaths: [path] })).toThrow();
  });
  it("rejects repeated rules and excessive rule sets explicitly", () => {
    expect(() =>
      parseKafkaRecordProtection({ ...policy, maskHeaders: ["token", "token"] }),
    ).toThrow();
    expect(() =>
      parseKafkaRecordProtection({
        ...policy,
        valuePaths: Array.from({ length: 33 }, (_, i) => `/${i}`),
      }),
    ).toThrow();
  });
});
