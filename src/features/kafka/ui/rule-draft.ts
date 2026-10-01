import { KAFKA_RULE_LIMITS, type KafkaRuleDefinition, type KafkaRuleSeverity } from "../contracts";

export interface KafkaRuleDraft {
  readonly cooldownMs: string;
  readonly description: string;
  readonly enabled: boolean;
  readonly expression: string;
  readonly level: KafkaRuleSeverity;
  readonly name: string;
  readonly topic: string;
}

export const emptyKafkaRuleDraft: KafkaRuleDraft = {
  cooldownMs: "0",
  description: "",
  enabled: true,
  expression: "",
  level: "info",
  name: "",
  topic: "",
};

export function kafkaRuleDraftFromDefinition(rule: KafkaRuleDefinition): KafkaRuleDraft {
  return {
    cooldownMs: String(rule.cooldownMs),
    description: rule.description ?? "",
    enabled: rule.enabled,
    expression: rule.expression,
    level: rule.level,
    name: rule.name,
    topic: rule.topic ?? "",
  };
}

export function kafkaRuleDefinitionFromDraft(draft: KafkaRuleDraft): KafkaRuleDefinition | null {
  const cooldownMs = Number(draft.cooldownMs);
  const name = draft.name.trim();
  const expression = draft.expression.trim();
  if (
    name.length === 0 ||
    expression.length === 0 ||
    !Number.isSafeInteger(cooldownMs) ||
    cooldownMs < 0 ||
    cooldownMs > KAFKA_RULE_LIMITS.cooldownMs
  ) {
    return null;
  }
  const description = draft.description.trim();
  const topic = draft.topic.trim();
  return {
    cooldownMs,
    ...(description.length === 0 ? {} : { description }),
    enabled: draft.enabled,
    expression,
    level: draft.level,
    name,
    ...(topic.length === 0 ? {} : { topic }),
  };
}
