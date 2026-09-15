import type { KafkaRuleEvaluator, KafkaRulePredicate, KafkaRuleSampleLimits } from "../application";

import { evaluateCompiledKafkaRuleExpression } from "./rule-expression-evaluator";
import { compileKafkaRuleExpression, validateKafkaRuleExpression } from "./rule-expression-parser";
import { parseKafkaRuleSample } from "./rule-sample";

export class StreamSkopeKafkaRuleEvaluator implements KafkaRuleEvaluator {
  compile(expression: string): KafkaRulePredicate {
    const compiled = compileKafkaRuleExpression(expression);
    return Object.freeze({
      evaluate: (sample: unknown): boolean => evaluateCompiledKafkaRuleExpression(compiled, sample),
    });
  }

  evaluate(expression: string, sample: unknown): boolean {
    return this.compile(expression).evaluate(sample);
  }

  parseSample(sample: string, limits?: KafkaRuleSampleLimits): unknown {
    return parseKafkaRuleSample(sample, limits);
  }

  validate(expression: string): {
    readonly diagnostic?: string;
    readonly valid: boolean;
  } {
    return validateKafkaRuleExpression(expression);
  }
}
