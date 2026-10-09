import type { StructuredRecord } from "./structured-record";
import { matchesKafkaSearchFilter, type KafkaSearchFilter } from "./query-search";
import { KAFKA_LIVE_RULE_LIMITS } from "./live-rule-types";
import { KAFKA_RULE_LIMITS } from "./rule-types";
import { compileKafkaRuleExpression } from "./rule-expression-parser";
import {
  evaluateCompiledKafkaRuleExpression,
  type KafkaRuleWorkBudget,
} from "./rule-expression-evaluator";
import { parseKafkaRuleSample } from "./rule-sample";

export interface KafkaSearchMessage {
  readonly structured?: StructuredRecord;
  readonly key: string | null;
  readonly payload: string | null;
  readonly offset: string;
  readonly timestamp: string;
  readonly partition: number;
  readonly truncated?: boolean;
}

export type KafkaSearchMatch = "matched" | "not-matched" | "unavailable";

export function compileKafkaSearchFilter(
  filter: KafkaSearchFilter,
  budget?: KafkaRuleWorkBudget,
): (message: KafkaSearchMessage) => KafkaSearchMatch {
  const expression = filter.expression?.trim();
  const compiled =
    expression === undefined || expression.length === 0
      ? undefined
      : compileKafkaRuleExpression(expression);
  return (message) => {
    // Do not infer a negative match from a field that could not be decoded.
    const structured = message.structured;
    if (
      structured &&
      ((filter.key.trim().length > 0 &&
        (structured.key.state === "error" || structured.key.state === "masked")) ||
        ((filter.value.trim().length > 0 || compiled !== undefined) &&
          (structured.value.state === "error" || structured.value.state === "masked")))
    )
      return "unavailable";
    if (
      structured &&
      compiled !== undefined &&
      (structured.value.state !== "decoded" || structured.value.json === null)
    )
      return "unavailable";
    if (!matchesKafkaSearchFilter(message, filter)) return "not-matched";
    if (compiled === undefined) return "matched";
    if (budget?.exhausted === true || message.payload === null || message.truncated === true)
      return "unavailable";
    try {
      const sample = parseKafkaRuleSample(message.payload, {
        bytes: KAFKA_LIVE_RULE_LIMITS.payloadBytes,
        depth: KAFKA_RULE_LIMITS.sampleDepth,
        nodes: KAFKA_LIVE_RULE_LIMITS.sampleNodes,
      });
      return evaluateCompiledKafkaRuleExpression(compiled, sample, budget)
        ? "matched"
        : "not-matched";
    } catch {
      // Malformed, oversized or over-budget input is unknown, never a negative match.
      return "unavailable";
    }
  };
}
