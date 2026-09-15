import { describe, expect, it } from "vitest";

import type { KafkaExploredMessage, KafkaLiveRuleEvaluation } from "../../src/kafka/contracts";
import {
  KAFKA_MESSAGE_OPERATION_LIMITS,
  KafkaMessageOperationError,
  createKafkaMessageExportDocument,
  initialKafkaMessageFilters,
} from "../../src/kafka/ui";

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(id: string, overrides: Partial<KafkaExploredMessage> = {}): KafkaExploredMessage {
  return {
    headers: { source: "fixture" },
    id,
    key: `key-${id}`,
    offset: id,
    originalByteSize: 14,
    partition: 0,
    payload: `{"id":"${id}"}`,
    preview: `{"id":"${id}"}`,
    ruleEvaluation: evaluated,
    timestamp: "2026-07-25T11:00:00.000Z",
    topic: "orders.eu",
    truncated: false,
    ...overrides,
  };
}

describe("Kafka filtered message export", () => {
  it("creates one exact deterministic document from the selected ordered snapshot", () => {
    const first = message("10", {
      headers: {
        Alpha: "uppercase",
        zeta: "last ASCII",
        ä: "non-ASCII",
        "😀": "supplementary",
      },
      key: " order-key ",
      offset: "10",
      partition: 2,
      payload: "  value with whitespace  ",
      preview: "  value with whitespace  ",
    });
    const filters = {
      ...initialKafkaMessageFilters,
      activeRuleMatchesOnly: true,
      key: "order",
      partition: 2,
      value: "value",
    };

    const document = createKafkaMessageExportDocument({
      filters,
      messages: [first],
      retainedMessageCount: 3,
      stale: false,
      topic: "orders.eu",
    });

    const expected = `{
  "schemaVersion": 1,
  "topic": "orders.eu",
  "filters": {
    "timestamp": "",
    "partition": 2,
    "offset": "",
    "key": "order",
    "value": "value",
    "activeRuleMatchesOnly": true
  },
  "retainedMessageCount": 3,
  "exportedMessageCount": 1,
  "stale": false,
  "messages": [
    {
      "timestamp": "2026-07-25T11:00:00.000Z",
      "partition": 2,
      "offset": "10",
      "key": " order-key ",
      "headers": {
        "Alpha": "uppercase",
        "zeta": "last ASCII",
        "ä": "non-ASCII",
        "😀": "supplementary"
      },
      "payload": "  value with whitespace  ",
      "preview": "  value with whitespace  ",
      "truncated": false,
      "originalByteSize": 14
    }
  ]
}
`;

    expect(document).toEqual({
      byteSize: new TextEncoder().encode(expected).byteLength,
      content: expected,
      fileName: "streamskope-orders.eu-messages.json",
      mediaType: "application/json",
    });
    expect(document.content).not.toContain("ruleEvaluation");
    expect(document.content).not.toContain('"id": "10"');
  });

  it("preserves null, truncated, stale, filter, and displayed-order evidence", () => {
    const truncated = message("2", {
      headers: { partial: "true" },
      originalByteSize: 2_000_000,
      partition: 3,
      payload: null,
      preview: "retained prefix",
      truncated: true,
    });
    const kafkaNull = message("1", {
      originalByteSize: 0,
      payload: null,
      preview: "",
    });

    const document = createKafkaMessageExportDocument({
      filters: {
        ...initialKafkaMessageFilters,
        offset: "1",
        timestamp: "2026",
      },
      messages: [truncated, kafkaNull],
      retainedMessageCount: 2,
      stale: true,
      topic: "orders.eu",
    });
    const parsed = JSON.parse(document.content) as {
      readonly filters: { readonly offset: string; readonly timestamp: string };
      readonly messages: readonly {
        readonly offset: string;
        readonly originalByteSize: number;
        readonly payload: string | null;
        readonly preview: string;
        readonly truncated: boolean;
      }[];
      readonly stale: boolean;
    };

    expect(parsed.stale).toBe(true);
    expect(parsed.filters).toMatchObject({ offset: "1", timestamp: "2026" });
    expect(parsed.messages.map((item) => item.offset)).toEqual(["2", "1"]);
    expect(parsed.messages[0]).toMatchObject({
      originalByteSize: 2_000_000,
      payload: null,
      preview: "retained prefix",
      truncated: true,
    });
    expect(parsed.messages[1]).toMatchObject({
      originalByteSize: 0,
      payload: null,
      preview: "",
      truncated: false,
    });
  });

  it("sanitizes hostile and empty topic filename segments predictably", () => {
    const hostile = message("1", { topic: "../../Orders / EU 🔒" });
    expect(
      createKafkaMessageExportDocument({
        filters: initialKafkaMessageFilters,
        messages: [hostile],
        retainedMessageCount: 1,
        stale: false,
        topic: "../../Orders / EU 🔒",
      }).fileName,
    ).toBe("streamskope-Orders-EU-messages.json");

    const punctuation = message("1", { topic: "///" });
    expect(
      createKafkaMessageExportDocument({
        filters: initialKafkaMessageFilters,
        messages: [punctuation],
        retainedMessageCount: 1,
        stale: false,
        topic: "///",
      }).fileName,
    ).toBe("streamskope-topic-messages.json");
  });

  it("rejects empty, inconsistent, too-many, over-filter, and oversized exports safely", () => {
    const operation = (run: () => unknown, code: string): void => {
      expect(run).toThrow(KafkaMessageOperationError);
      try {
        run();
      } catch (error) {
        expect(error).toMatchObject({ code });
        expect(String(error)).not.toContain("sensitive-export-marker");
      }
    };

    operation(
      () =>
        createKafkaMessageExportDocument({
          filters: initialKafkaMessageFilters,
          messages: [],
          retainedMessageCount: 2,
          stale: false,
          topic: "orders.eu",
        }),
      "NO_MESSAGES",
    );
    operation(
      () =>
        createKafkaMessageExportDocument({
          filters: initialKafkaMessageFilters,
          messages: [message("1", { topic: "other" })],
          retainedMessageCount: 1,
          stale: false,
          topic: "orders.eu",
        }),
      "INVALID_EXPORT",
    );
    operation(
      () =>
        createKafkaMessageExportDocument({
          filters: initialKafkaMessageFilters,
          messages: Array.from(
            { length: KAFKA_MESSAGE_OPERATION_LIMITS.exportMessages + 1 },
            (_value, index) => message(String(index)),
          ),
          retainedMessageCount: KAFKA_MESSAGE_OPERATION_LIMITS.exportMessages + 1,
          stale: false,
          topic: "orders.eu",
        }),
      "EXPORT_TOO_LARGE",
    );
    operation(
      () =>
        createKafkaMessageExportDocument({
          filters: {
            ...initialKafkaMessageFilters,
            key: "x".repeat(KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters + 1),
          },
          messages: [message("1")],
          retainedMessageCount: 1,
          stale: false,
          topic: "orders.eu",
        }),
      "INVALID_EXPORT",
    );
    operation(
      () =>
        createKafkaMessageExportDocument({
          filters: initialKafkaMessageFilters,
          messages: [
            message("1", {
              headers: {
                oversized: `sensitive-export-marker${"x".repeat(
                  KAFKA_MESSAGE_OPERATION_LIMITS.exportContentBytes,
                )}`,
              },
            }),
          ],
          retainedMessageCount: 1,
          stale: false,
          topic: "orders.eu",
        }),
      "EXPORT_TOO_LARGE",
    );
  });
});
