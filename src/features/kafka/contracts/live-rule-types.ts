import type { KafkaRuleSeverity } from "./rule-types";
import { KAFKA_OPERATIONAL_PREFERENCE_LIMITS } from "./operational-preference-types";

export const KAFKA_LIVE_RULE_CAPABILITY_STATES = [
  "idle",
  "ready",
  "partial",
  "unavailable",
] as const;

export const KAFKA_LIVE_RULE_EVALUATION_STATES = ["evaluated", "partial", "unavailable"] as const;

export const KAFKA_LIVE_RULE_UNAVAILABLE_REASONS = [
  "catalog-unavailable",
  "payload-null",
  "payload-truncated",
  "payload-malformed",
  "payload-limit-exceeded",
  "internal",
] as const;

export const KAFKA_LIVE_RULE_LIMITS = {
  applicableRules: 50,
  diagnosticBytes: 512,
  durationMicros: 3_600_000_000,
  entries: 50,
  evidenceBytes: 12_288,
  payloadBytes: 262_144,
  sampleNodes: 25_000,
} as const;

export const KAFKA_RULE_NOTIFICATION_LIMITS = {
  activeMatches:
    KAFKA_LIVE_RULE_LIMITS.applicableRules * KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.maximum,
  matches: 10,
  rendererQueue: 3,
} as const;

export type KafkaLiveRuleCapabilityState = (typeof KAFKA_LIVE_RULE_CAPABILITY_STATES)[number];
export type KafkaLiveRuleEvaluationState = (typeof KAFKA_LIVE_RULE_EVALUATION_STATES)[number];
export type KafkaLiveRuleUnavailableReason = (typeof KAFKA_LIVE_RULE_UNAVAILABLE_REASONS)[number];

export interface KafkaLiveRuleCapability {
  readonly applicableRules: number;
  readonly omittedRules: number;
  readonly recovery?: string;
  readonly state: KafkaLiveRuleCapabilityState;
}

export interface KafkaLiveRuleMatch {
  readonly level: KafkaRuleSeverity;
  readonly name: string;
}

export interface KafkaLiveRuleError {
  readonly diagnostic: string;
  readonly name: string;
}

export interface KafkaRuleNotificationMatch {
  readonly count: number;
  readonly level: KafkaRuleSeverity;
  readonly name: string;
}

export interface KafkaRuleNotification {
  readonly activeMatchCount: number;
  readonly highestSeverity: KafkaRuleSeverity;
  readonly matches: readonly KafkaRuleNotificationMatch[];
  readonly omittedMatches: number;
  readonly topic: string;
}

export interface KafkaLiveRuleEvaluation {
  readonly activeMatchCount: number;
  readonly activeMatches: readonly KafkaLiveRuleMatch[];
  readonly durationMicros: number;
  readonly errorCount: number;
  readonly errors: readonly KafkaLiveRuleError[];
  readonly evaluatedRules: number;
  readonly highestActiveSeverity?: KafkaRuleSeverity;
  readonly omittedEvidence: number;
  readonly omittedRules: number;
  readonly reason?: KafkaLiveRuleUnavailableReason;
  readonly state: KafkaLiveRuleEvaluationState;
  readonly suppressedMatchCount: number;
  readonly suppressedMatches: readonly KafkaLiveRuleMatch[];
}
