export const KAFKA_RULE_SEVERITIES = ["info", "warn", "error"] as const;
export const KAFKA_RULE_EVALUATION_OUTCOMES = [
  "valid",
  "invalid",
  "matched",
  "not-matched",
  "skipped",
] as const;
export const KAFKA_RULE_SKIP_REASONS = ["disabled", "topic-mismatch"] as const;
export const KAFKA_RULE_STORE_DURABILITIES = ["durable", "session"] as const;
export const KAFKA_RULE_STORE_STATES = ["ready", "unavailable"] as const;

export const KAFKA_RULE_LIMITS = {
  conditions: 64,
  cooldownMs: 86_400_000,
  descriptionCharacters: 2_048,
  diagnosticCharacters: 2_048,
  expressionCharacters: 4_096,
  groupingDepth: 16,
  nameCharacters: 128,
  pathSegments: 64,
  regexCharacters: 256,
  rules: 500,
  sampleBytes: 1_048_576,
  sampleDepth: 64,
  sampleNodes: 100_000,
  topicCharacters: 249,
} as const;

export type KafkaRuleSeverity = (typeof KAFKA_RULE_SEVERITIES)[number];
export type KafkaRuleEvaluationOutcome = (typeof KAFKA_RULE_EVALUATION_OUTCOMES)[number];
export type KafkaRuleSkipReason = (typeof KAFKA_RULE_SKIP_REASONS)[number];
export type KafkaRuleStoreDurability = (typeof KAFKA_RULE_STORE_DURABILITIES)[number];
export type KafkaRuleStoreState = (typeof KAFKA_RULE_STORE_STATES)[number];

export function maximumKafkaRuleSeverity(
  severities: Iterable<KafkaRuleSeverity>,
): KafkaRuleSeverity | null {
  let maximumIndex = -1;
  for (const severity of severities) {
    maximumIndex = Math.max(maximumIndex, KAFKA_RULE_SEVERITIES.indexOf(severity));
  }
  return maximumIndex < 0 ? null : (KAFKA_RULE_SEVERITIES[maximumIndex] ?? null);
}

export interface KafkaRuleDefinition {
  readonly cooldownMs: number;
  readonly description?: string;
  readonly enabled: boolean;
  readonly expression: string;
  readonly level: KafkaRuleSeverity;
  readonly name: string;
  readonly topic?: string;
}

export type KafkaRuleField =
  "cooldownMs" | "description" | "enabled" | "expression" | "level" | "name" | "topic";

export interface KafkaRuleIssue {
  readonly field: KafkaRuleField;
  readonly message: string;
}

export interface KafkaRuleStoreCapability {
  readonly durability: KafkaRuleStoreDurability;
  readonly recovery?: string;
  readonly state: KafkaRuleStoreState;
}

export interface KafkaRuleSnapshot {
  readonly rules: readonly KafkaRuleDefinition[];
  readonly store: KafkaRuleStoreCapability;
}

export interface KafkaRuleEvaluationResult {
  readonly diagnostic?: string;
  readonly name: string;
  readonly outcome: KafkaRuleEvaluationOutcome;
  readonly reason?: KafkaRuleSkipReason;
}

export interface KafkaRuleEvaluationReport {
  readonly kind: "evaluation" | "validation";
  readonly requestId: string;
  readonly results: readonly KafkaRuleEvaluationResult[];
}

export type KafkaRuleEvaluationInput =
  | {
      readonly rule: KafkaRuleDefinition;
      readonly sample: string;
      readonly scope: "single";
      readonly topic?: string;
    }
  | {
      readonly sample: string;
      readonly scope: "catalog";
      readonly topic?: string;
    };
