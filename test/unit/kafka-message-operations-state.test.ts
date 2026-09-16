import { describe, expect, it } from "vitest";

import type {
  KafkaExploredMessage,
  KafkaLiveRuleEvaluation,
} from "../../src/features/kafka/contracts";
import {
  KAFKA_MESSAGE_OPERATION_LIMITS,
  countActiveKafkaMessageFilters,
  initialKafkaMessageFilters,
  initialKafkaUiState,
  reduceKafkaUiState,
  selectFilteredKafkaMessages,
} from "../../src/features/kafka/ui";

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
  return Object.freeze({
    headers: Object.freeze({ source: "fixture" }),
    id,
    key: `key-${id}`,
    offset: id,
    originalByteSize: 64,
    partition: Number(id) % 3,
    payload: `{"record":"${id}"}`,
    preview: `{"record":"${id}"}`,
    ruleEvaluation: evaluated,
    timestamp: `2026-07-25T10:00:0${id}.000Z`,
    topic: "orders",
    truncated: false,
    ...overrides,
  });
}

describe("Kafka message operation state", () => {
  it("filters every reference field with exact partition and case-insensitive text", () => {
    const first = message("1", {
      key: "Order-ALPHA",
      offset: "101",
      partition: 7,
      payload: "  Payment APPROVED  ",
      preview: "  Payment APPROVED  ",
      timestamp: "2026-07-25T10:41:00.000Z",
    });
    const second = message("2", {
      key: "order-beta",
      offset: "202",
      partition: 8,
      payload: "Payment rejected",
      preview: "Payment rejected",
      timestamp: "2026-07-25T10:42:00.000Z",
    });
    const retained = Object.freeze([first, second]);

    expect(
      selectFilteredKafkaMessages(retained, {
        ...initialKafkaMessageFilters,
        timestamp: "10:41",
      }),
    ).toEqual([first]);
    expect(
      selectFilteredKafkaMessages(retained, {
        ...initialKafkaMessageFilters,
        partition: 8,
      }),
    ).toEqual([second]);
    expect(
      selectFilteredKafkaMessages(retained, {
        ...initialKafkaMessageFilters,
        offset: "01",
      }),
    ).toEqual([first]);
    expect(
      selectFilteredKafkaMessages(retained, {
        ...initialKafkaMessageFilters,
        key: "alpha",
      }),
    ).toEqual([first]);
    expect(
      selectFilteredKafkaMessages(retained, {
        ...initialKafkaMessageFilters,
        value: "approved",
      }),
    ).toEqual([first]);
    expect(retained).toEqual([first, second]);
  });

  it("composes non-empty fields and authoritative active matches with logical AND", () => {
    const active = message("1", {
      key: "orders-eu",
      partition: 4,
      payload: '{"status":"approved"}',
      preview: '{"status":"approved"}',
      ruleEvaluation: {
        ...evaluated,
        activeMatchCount: 1,
        activeMatches: [{ level: "warn", name: "Approval warning" }],
        highestActiveSeverity: "warn",
      },
    });
    const noRuleMatch = message("2", {
      key: "orders-eu",
      partition: 4,
      payload: '{"status":"approved"}',
      preview: '{"status":"approved"}',
    });
    const wrongValue = message("3", {
      key: "orders-eu",
      partition: 4,
      payload: '{"status":"rejected"}',
      preview: '{"status":"rejected"}',
      ruleEvaluation: active.ruleEvaluation,
    });

    expect(
      selectFilteredKafkaMessages([active, noRuleMatch, wrongValue], {
        ...initialKafkaMessageFilters,
        activeRuleMatchesOnly: true,
        key: "orders",
        partition: 4,
        value: "approved",
      }),
    ).toEqual([active]);
  });

  it("searches only retained preview for an unavailable complete value and excludes Kafka null", () => {
    const truncated = message("1", {
      originalByteSize: 2_000_000,
      payload: null,
      preview: "retained PREFIX only",
      truncated: true,
    });
    const kafkaNull = message("2", {
      originalByteSize: 0,
      payload: null,
      preview: "",
    });

    expect(
      selectFilteredKafkaMessages([truncated, kafkaNull], {
        ...initialKafkaMessageFilters,
        value: "prefix",
      }),
    ).toEqual([truncated]);
    expect(
      selectFilteredKafkaMessages([truncated, kafkaNull], {
        ...initialKafkaMessageFilters,
        value: "unretained suffix",
      }),
    ).toEqual([]);
    expect(
      selectFilteredKafkaMessages([kafkaNull], {
        ...initialKafkaMessageFilters,
        value: "null",
      }),
    ).toEqual([]);
  });

  it("preserves retained order, returns the retained array when neutral, and admits new matches", () => {
    const third = message("3");
    const first = message("1");
    const second = message("2");
    const retained = Object.freeze([third, first]);

    expect(selectFilteredKafkaMessages(retained, initialKafkaMessageFilters)).toBe(retained);
    expect(
      selectFilteredKafkaMessages([...retained, second], {
        ...initialKafkaMessageFilters,
        key: "key-2",
      }),
    ).toEqual([second]);
    expect(retained).toEqual([third, first]);
  });

  it("bounds reducer text, counts criteria, and clears all filters atomically", () => {
    let state = reduceKafkaUiState(initialKafkaUiState, {
      field: "key",
      type: "messages.filter.text.changed",
      value: "x".repeat(KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters + 20),
    });
    state = reduceKafkaUiState(state, {
      partition: 4,
      type: "messages.filter.partition.changed",
    });
    state = reduceKafkaUiState(state, {
      activeOnly: true,
      type: "messages.rule-filter.changed",
    });

    expect(state.messageFilters.key).toHaveLength(KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters);
    expect(countActiveKafkaMessageFilters(state.messageFilters)).toBe(3);

    const cleared = reduceKafkaUiState(state, { type: "messages.filters.cleared" });
    expect(cleared.messageFilters).toEqual(initialKafkaMessageFilters);
    expect(countActiveKafkaMessageFilters(cleared.messageFilters)).toBe(0);
    expect(cleared.messages).toBe(state.messages);
    expect(cleared.activities).toBe(state.activities);
  });

  it("rejects invalid partition criteria at the reducer boundary", () => {
    for (const partition of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(
        reduceKafkaUiState(initialKafkaUiState, {
          partition,
          type: "messages.filter.partition.changed",
        }),
      ).toBe(initialKafkaUiState);
    }
  });
});
