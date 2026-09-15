import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_LIVE_RULE_CAPABILITY_STATES,
  KAFKA_LIVE_RULE_EVALUATION_STATES,
  KAFKA_LIVE_RULE_LIMITS,
  KAFKA_LIVE_RULE_UNAVAILABLE_REASONS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  KAFKA_RULE_NOTIFICATION_LIMITS,
  HOST_EVENTS,
  KAFKA_MESSAGE_LIMITS,
  HostContractValidationError,
  kafkaMessageRetainedBytes,
  parseHostEvent,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
  type KafkaMessage,
} from "../../src/kafka/contracts";

const rawMessage: KafkaMessage = {
  headers: {},
  id: "orders:0:7",
  key: "order-7",
  offset: "7",
  originalByteSize: 35,
  partition: 0,
  payload: '{"priority":"high","total":200}',
  preview: '{"priority":"high","total":200}',
  timestamp: "2026-07-25T20:00:00.000Z",
  topic: "orders",
  truncated: false,
};

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 1,
  activeMatches: [{ level: "warn", name: "High priority" }],
  durationMicros: 125,
  errorCount: 0,
  errors: [],
  evaluatedRules: 2,
  highestActiveSeverity: "warn",
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

const evaluatedWithoutHighest = structuredClone(evaluated);
Reflect.deleteProperty(evaluatedWithoutHighest, "highestActiveSeverity");
const zeroEvaluated: KafkaLiveRuleEvaluation = {
  ...evaluatedWithoutHighest,
  activeMatchCount: 0,
  activeMatches: [],
};

function explored(ruleEvaluation: KafkaLiveRuleEvaluation = evaluated): KafkaExploredMessage {
  return {
    ...rawMessage,
    ruleEvaluation,
  };
}

function batch(message: KafkaExploredMessage): unknown {
  return {
    event: "messages.batch",
    payload: {
      droppedMessages: 0,
      messages: [message],
      topic: "orders",
    },
    sequence: 1,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka live rule contract", () => {
  it("retains the bounded live vocabulary on protocol version 15", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(KAFKA_LIVE_RULE_CAPABILITY_STATES).toEqual(["idle", "ready", "partial", "unavailable"]);
    expect(KAFKA_LIVE_RULE_EVALUATION_STATES).toEqual(["evaluated", "partial", "unavailable"]);
    expect(KAFKA_LIVE_RULE_UNAVAILABLE_REASONS).toEqual([
      "catalog-unavailable",
      "payload-null",
      "payload-truncated",
      "payload-malformed",
      "payload-limit-exceeded",
      "internal",
    ]);
    expect(KAFKA_LIVE_RULE_LIMITS).toEqual({
      applicableRules: 50,
      diagnosticBytes: 512,
      durationMicros: 3_600_000_000,
      entries: 50,
      evidenceBytes: 12_288,
      payloadBytes: 262_144,
      sampleNodes: 25_000,
    });
    expect(KAFKA_RULE_NOTIFICATION_LIMITS).toEqual({
      activeMatches:
        KAFKA_LIVE_RULE_LIMITS.applicableRules *
        KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.maximum,
      matches: 10,
      rendererQueue: 3,
    });
    expect(HOST_EVENTS).toContain("rules.notification");
    expect(KAFKA_MESSAGE_LIMITS.batchBytes).toBe(
      KAFKA_MESSAGE_LIMITS.messageBytes + KAFKA_LIVE_RULE_LIMITS.evidenceBytes,
    );
  });

  it("parses one strict bounded rule-notification aggregate", () => {
    const notification = {
      event: "rules.notification",
      payload: {
        activeMatchCount: 4,
        highestSeverity: "error",
        matches: [
          { count: 2, level: "warn", name: "High priority" },
          { count: 1, level: "error", name: "Payment failed" },
        ],
        omittedMatches: 1,
        topic: "orders",
      },
      sequence: 4,
      version: HOST_PROTOCOL_VERSION,
    } as const;

    expect(parseHostEvent(notification)).toEqual(notification);
  });

  it("accepts an honest notice whose authoritative active names were all omitted", () => {
    const notification = {
      event: "rules.notification",
      payload: {
        activeMatchCount: 2,
        highestSeverity: "error",
        matches: [],
        omittedMatches: 2,
        topic: "orders",
      },
      sequence: 5,
      version: HOST_PROTOCOL_VERSION,
    } as const;

    expect(parseHostEvent(notification)).toEqual(notification);
  });

  it.each([
    {
      label: "unknown notification knowledge",
      payload: {
        activeMatchCount: 1,
        highestSeverity: "warn",
        matches: [{ count: 1, level: "warn", name: "High priority" }],
        omittedMatches: 0,
        payload: "private",
        topic: "orders",
      },
    },
    {
      label: "inconsistent aggregate count",
      payload: {
        activeMatchCount: 2,
        highestSeverity: "warn",
        matches: [{ count: 1, level: "warn", name: "High priority" }],
        omittedMatches: 0,
        topic: "orders",
      },
    },
    {
      label: "duplicate retained identity",
      payload: {
        activeMatchCount: 2,
        highestSeverity: "warn",
        matches: [
          { count: 1, level: "warn", name: "High priority" },
          { count: 1, level: "warn", name: "High priority" },
        ],
        omittedMatches: 0,
        topic: "orders",
      },
    },
    {
      label: "more retained identities than declared",
      payload: {
        activeMatchCount: KAFKA_RULE_NOTIFICATION_LIMITS.matches + 1,
        highestSeverity: "info",
        matches: Array.from(
          { length: KAFKA_RULE_NOTIFICATION_LIMITS.matches + 1 },
          (_value, index) => ({
            count: 1,
            level: "info",
            name: `Rule ${String(index)}`,
          }),
        ),
        omittedMatches: 0,
        topic: "orders",
      },
    },
    {
      label: "highest severity below retained evidence",
      payload: {
        activeMatchCount: 1,
        highestSeverity: "info",
        matches: [{ count: 1, level: "error", name: "Payment failed" }],
        omittedMatches: 0,
        topic: "orders",
      },
    },
  ])("rejects $label", ({ payload }) => {
    expect(() =>
      parseHostEvent({
        event: "rules.notification",
        payload,
        sequence: 5,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("distinguishes raw adapter messages from explored wire messages and accounts for evidence", () => {
    const message = explored();

    expect(message).toMatchObject({
      id: rawMessage.id,
      ruleEvaluation: {
        activeMatches: [{ level: "warn", name: "High priority" }],
        state: "evaluated",
      },
    });
    expect(kafkaMessageRetainedBytes(message)).toBeGreaterThan(
      kafkaMessageRetainedBytes(rawMessage),
    );
    expect(parseHostEvent(batch(message))).toEqual(batch(message));
  });

  it("preserves authoritative highest severity when higher-severity detail is omitted", () => {
    const ruleEvaluation = {
      ...evaluated,
      activeMatchCount: 2,
      activeMatches: [{ level: "warn", name: "Retained warning" }],
      highestActiveSeverity: "error",
      omittedEvidence: 1,
      state: "partial",
    } satisfies KafkaLiveRuleEvaluation;

    expect(parseHostEvent(batch(explored(ruleEvaluation)))).toMatchObject({
      payload: {
        messages: [
          {
            ruleEvaluation: {
              activeMatchCount: 2,
              highestActiveSeverity: "error",
              omittedEvidence: 1,
            },
          },
        ],
      },
    });
  });

  it("rejects an active result without authoritative highest severity", () => {
    expect(() => parseHostEvent(batch(explored(evaluatedWithoutHighest)))).toThrow(
      HostContractValidationError,
    );
  });

  it("preserves the maximum raw-message capacity when bounded evidence is attached", () => {
    const payload = JSON.stringify("x".repeat(KAFKA_MESSAGE_LIMITS.messageBytes - 2));
    const message: KafkaExploredMessage = {
      ...explored({
        ...zeroEvaluated,
        durationMicros: 0,
        evaluatedRules: 0,
      }),
      key: null,
      originalByteSize: payload.length,
      payload,
      preview: payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes),
    };

    expect(payload.length).toBe(KAFKA_MESSAGE_LIMITS.messageBytes);
    expect(kafkaMessageRetainedBytes(message)).toBeGreaterThan(KAFKA_MESSAGE_LIMITS.messageBytes);
    expect(parseHostEvent(batch(message))).toEqual(batch(message));
  });

  it.each([
    {
      label: "complete zero-rule evaluation",
      result: {
        ...zeroEvaluated,
        durationMicros: 0,
        evaluatedRules: 0,
      },
    },
    {
      label: "partial result with omitted applicable rules",
      result: {
        ...zeroEvaluated,
        evaluatedRules: KAFKA_LIVE_RULE_LIMITS.applicableRules,
        omittedRules: 2,
        state: "partial",
      },
    },
    {
      label: "partial result with suppressed match and bounded error",
      result: {
        ...zeroEvaluated,
        errorCount: 1,
        errors: [{ diagnostic: "Predicate failed safely.", name: "Broken" }],
        state: "partial",
        suppressedMatchCount: 1,
        suppressedMatches: [{ level: "info", name: "Cooldown" }],
      },
    },
    {
      label: "catalog unavailable result",
      result: {
        activeMatchCount: 0,
        activeMatches: [],
        durationMicros: 0,
        errorCount: 0,
        errors: [],
        evaluatedRules: 0,
        omittedEvidence: 0,
        omittedRules: 0,
        reason: "catalog-unavailable",
        state: "unavailable",
        suppressedMatchCount: 0,
        suppressedMatches: [],
      },
    },
  ])("parses a $label", ({ result }) => {
    expect(parseHostEvent(batch(explored(result as KafkaLiveRuleEvaluation)))).toMatchObject({
      payload: {
        messages: [{ ruleEvaluation: result }],
      },
    });
  });

  it("parses idle, ready, partial and unavailable live capability on consumption state", () => {
    for (const ruleEvaluation of [
      {
        applicableRules: 0,
        omittedRules: 0,
        state: "idle",
      },
      {
        applicableRules: 3,
        omittedRules: 0,
        state: "ready",
      },
      {
        applicableRules: 52,
        omittedRules: 2,
        state: "partial",
      },
      {
        applicableRules: 0,
        omittedRules: 0,
        recovery: "Restore rule storage and restart StreamSkope.",
        state: "unavailable",
      },
    ] as const) {
      expect(
        parseHostEvent({
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
          sequence: 2,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toMatchObject({
        payload: { ruleEvaluation },
      });
    }
  });

  it.each([
    {
      label: "unknown result field",
      result: { ...evaluated, expression: '$.secret == "value"' },
    },
    {
      label: "reason on a complete result",
      result: { ...evaluated, reason: "internal" },
    },
    {
      label: "unavailable result containing evaluated rules",
      result: {
        ...evaluated,
        reason: "catalog-unavailable",
        state: "unavailable",
      },
    },
    {
      label: "partial result without a partial condition",
      result: { ...evaluated, state: "partial" },
    },
    {
      label: "active count below retained entries",
      result: { ...evaluated, activeMatchCount: 0 },
    },
    {
      label: "evidence omission inconsistent with counts",
      result: { ...evaluated, activeMatchCount: 2, omittedEvidence: 0 },
    },
    {
      label: "same rule in active and suppressed entries",
      result: {
        ...evaluated,
        evaluatedRules: 2,
        suppressedMatchCount: 1,
        suppressedMatches: [{ level: "warn", name: "High priority" }],
      },
    },
    {
      label: "more outcomes than evaluated rules",
      result: {
        ...evaluated,
        errorCount: 1,
        errors: [{ diagnostic: "Failure.", name: "Broken" }],
        evaluatedRules: 1,
        state: "partial",
      },
    },
    {
      label: "omitted applicable rules without evaluating live capacity",
      result: {
        ...zeroEvaluated,
        evaluatedRules: KAFKA_LIVE_RULE_LIMITS.applicableRules - 1,
        omittedRules: 1,
        state: "partial",
      },
    },
    {
      label: "oversized UTF-8 diagnostic",
      result: {
        ...zeroEvaluated,
        errorCount: 1,
        errors: [
          {
            diagnostic: "😀".repeat(KAFKA_LIVE_RULE_LIMITS.diagnosticBytes / 2),
            name: "Broken",
          },
        ],
        state: "partial",
      },
    },
    {
      label: "duration beyond bound",
      result: {
        ...evaluated,
        durationMicros: KAFKA_LIVE_RULE_LIMITS.durationMicros + 1,
      },
    },
    {
      label: "highest severity on a zero-match result",
      result: {
        ...zeroEvaluated,
        highestActiveSeverity: "info",
      },
    },
    {
      label: "highest severity below retained active evidence",
      result: {
        ...evaluated,
        highestActiveSeverity: "info",
      },
    },
    {
      label: "higher severity without omitted active evidence",
      result: {
        ...evaluated,
        highestActiveSeverity: "error",
      },
    },
  ])("rejects $label", ({ result }) => {
    expect(() =>
      parseHostEvent(batch(explored(result as unknown as KafkaLiveRuleEvaluation))),
    ).toThrow(HostContractValidationError);
  });

  it.each([
    {
      label: "ready capability with omissions",
      ruleEvaluation: {
        applicableRules: 51,
        omittedRules: 1,
        state: "ready",
      },
    },
    {
      label: "partial capability without omissions",
      ruleEvaluation: {
        applicableRules: 3,
        omittedRules: 0,
        state: "partial",
      },
    },
    {
      label: "unavailable capability without recovery",
      ruleEvaluation: {
        applicableRules: 0,
        omittedRules: 0,
        state: "unavailable",
      },
    },
    {
      label: "idle capability claiming applicable rules",
      ruleEvaluation: {
        applicableRules: 1,
        omittedRules: 0,
        state: "idle",
      },
    },
    {
      label: "ready capability beyond live capacity",
      ruleEvaluation: {
        applicableRules: KAFKA_LIVE_RULE_LIMITS.applicableRules + 1,
        omittedRules: 0,
        state: "ready",
      },
    },
    {
      label: "partial capability with an inexact omitted count",
      ruleEvaluation: {
        applicableRules: KAFKA_LIVE_RULE_LIMITS.applicableRules + 2,
        omittedRules: 1,
        state: "partial",
      },
    },
  ])("rejects $label", ({ ruleEvaluation }) => {
    expect(() =>
      parseHostEvent({
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
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });
});
