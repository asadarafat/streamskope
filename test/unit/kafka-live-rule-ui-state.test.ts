import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type KafkaExploredMessage,
  type KafkaLiveRuleCapability,
  type KafkaLiveRuleEvaluation,
} from "../../src/features/kafka/contracts";
import {
  highestKafkaRuleSeverity,
  initialKafkaUiState,
  reduceKafkaHostEvent,
  reduceKafkaUiState,
  selectKafkaMessageById,
  selectVisibleKafkaMessages,
} from "../../src/features/kafka/ui";

const ready: KafkaLiveRuleCapability = {
  applicableRules: 4,
  omittedRules: 0,
  state: "ready",
};

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 12,
  errorCount: 0,
  errors: [],
  evaluatedRules: 4,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(
  id: string,
  ruleEvaluation: KafkaLiveRuleEvaluation = evaluated,
): KafkaExploredMessage {
  return {
    headers: {},
    id,
    key: null,
    offset: id,
    originalByteSize: 2,
    partition: 0,
    payload: "{}",
    preview: "{}",
    ruleEvaluation,
    timestamp: "2026-07-25T22:00:00.000Z",
    topic: "orders",
    truncated: false,
  };
}

function consumption(sequence: number, ruleEvaluation: KafkaLiveRuleCapability): HostEvent {
  return {
    event: "consumption.state",
    payload: {
      droppedMessages: 0,
      receivedMessages: 0,
      request: {
        maxMessages: 1_000,
        mode: "tail",
        topic: "orders",
      },
      ruleEvaluation,
      state: "streaming",
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function batch(sequence: number, messages: readonly KafkaExploredMessage[]): HostEvent {
  return {
    event: "messages.batch",
    payload: {
      droppedMessages: 0,
      messages,
      topic: "orders",
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function notification(sequence: number, name: string): HostEvent {
  return {
    event: "rules.notification",
    payload: {
      activeMatchCount: 1,
      highestSeverity: "warn",
      matches: [{ count: 1, level: "warn", name }],
      omittedMatches: 0,
      topic: "orders",
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka live rule renderer state", () => {
  it("retains three newest notices, supports exact dismissal and clears connection context", () => {
    let state = initialKafkaUiState;
    for (let sequence = 1; sequence <= 4; sequence += 1) {
      state = reduceKafkaHostEvent(state, notification(sequence, `Rule ${String(sequence)}`));
    }

    expect(state.ruleNotifications.map((notice) => notice.matches[0]?.name)).toEqual([
      "Rule 2",
      "Rule 3",
      "Rule 4",
    ]);
    state = reduceKafkaUiState(state, {
      sequence: 3,
      type: "rules.notification.dismissed",
    });
    expect(state.ruleNotifications.map((notice) => notice.sequence)).toEqual([2, 4]);

    state = reduceKafkaHostEvent(state, {
      event: "connection.state",
      payload: { connectionName: "replacement", state: "connecting" },
      sequence: 5,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(state.ruleNotifications).toEqual([]);
  });

  it("retains honest capability and rejects late capability events", () => {
    const partial: KafkaLiveRuleCapability = {
      applicableRules: 52,
      omittedRules: 2,
      state: "partial",
    };
    let state = reduceKafkaHostEvent(initialKafkaUiState, consumption(2, partial));

    expect(state.liveRuleCapability).toEqual(partial);
    const late = reduceKafkaHostEvent(state, consumption(1, ready));
    expect(late).toBe(state);

    const unavailable: KafkaLiveRuleCapability = {
      applicableRules: 0,
      omittedRules: 0,
      recovery: "Restore rule storage and retry consumption.",
      state: "unavailable",
    };
    state = reduceKafkaHostEvent(state, consumption(3, unavailable));
    expect(state.liveRuleCapability).toEqual(unavailable);
  });

  it("filters by authoritative active-match count without mutating retained messages", () => {
    const active = message("active", {
      ...evaluated,
      activeMatchCount: 2,
      activeMatches: [
        { level: "info", name: "Informational" },
        { level: "error", name: "Critical" },
      ],
      highestActiveSeverity: "error",
    });
    const suppressed = message("suppressed", {
      ...evaluated,
      activeMatchCount: 0,
      activeMatches: [],
      suppressedMatchCount: 1,
      suppressedMatches: [{ level: "warn", name: "Cooldown" }],
    });
    const unavailable = message("unavailable", {
      activeMatchCount: 0,
      activeMatches: [],
      durationMicros: 0,
      errorCount: 0,
      errors: [],
      evaluatedRules: 0,
      omittedEvidence: 0,
      omittedRules: 0,
      reason: "payload-null",
      state: "unavailable",
      suppressedMatchCount: 0,
      suppressedMatches: [],
    });
    let state = reduceKafkaHostEvent(
      reduceKafkaHostEvent(initialKafkaUiState, consumption(1, ready)),
      batch(2, [active, suppressed, unavailable]),
    );
    const retained = state.messages;

    expect(selectVisibleKafkaMessages(state)).toBe(retained);
    state = reduceKafkaUiState(state, {
      activeOnly: true,
      type: "messages.rule-filter.changed",
    });

    expect(selectVisibleKafkaMessages(state)).toEqual([active]);
    expect(state.messages).toBe(retained);
    expect(highestKafkaRuleSeverity(active)).toBe("error");
    expect(highestKafkaRuleSeverity(suppressed)).toBeNull();
  });

  it("uses host-confirmed highest severity when higher-severity detail was omitted", () => {
    const partial = message("partial", {
      ...evaluated,
      activeMatchCount: 2,
      activeMatches: [{ level: "info", name: "Retained informational match" }],
      highestActiveSeverity: "error",
      omittedEvidence: 1,
      state: "partial",
    });

    expect(highestKafkaRuleSeverity(partial)).toBe("error");
  });

  it("preserves a visible selection and clears it when filtering or lifecycle removal hides it", () => {
    const active = message("active", {
      ...evaluated,
      activeMatchCount: 1,
      activeMatches: [{ level: "warn", name: "Match" }],
      highestActiveSeverity: "warn",
    });
    const plain = message("plain");
    let state = reduceKafkaHostEvent(
      reduceKafkaHostEvent(initialKafkaUiState, consumption(1, ready)),
      batch(2, [active, plain]),
    );

    expect(selectKafkaMessageById(selectVisibleKafkaMessages(state), "plain")).toBe(plain);
    state = reduceKafkaUiState(state, {
      activeOnly: true,
      type: "messages.rule-filter.changed",
    });
    expect(selectKafkaMessageById(selectVisibleKafkaMessages(state), "plain")).toBeNull();
    expect(selectKafkaMessageById(selectVisibleKafkaMessages(state), "active")).toBe(active);

    state = reduceKafkaHostEvent(state, {
      event: "consumption.state",
      payload: {
        droppedMessages: 0,
        receivedMessages: 0,
        request: {
          maxMessages: 1_000,
          mode: "tail",
          topic: "orders",
        },
        ruleEvaluation: ready,
        state: "loading",
      },
      sequence: 3,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(selectKafkaMessageById(selectVisibleKafkaMessages(state), "active")).toBeNull();
  });

  it("changes only renderer filter state and leaves other feature owners intact", () => {
    const state = {
      ...initialKafkaUiState,
      profiles: Object.freeze([]),
      topics: Object.freeze(["orders"]),
    };

    const changed = reduceKafkaUiState(state, {
      activeOnly: true,
      type: "messages.rule-filter.changed",
    });

    expect(changed.messageFilters.activeRuleMatchesOnly).toBe(true);
    expect(changed.profiles).toBe(state.profiles);
    expect(changed.ruleState).toBe(state.ruleState);
    expect(changed.templateSnapshot).toBe(state.templateSnapshot);
    expect(changed.topics).toBe(state.topics);
  });
});
