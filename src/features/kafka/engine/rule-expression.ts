import { evaluateCompiledKafkaRuleExpression } from "./rule-expression-evaluator";
import {
  compileKafkaRuleExpression,
  KafkaRuleExpressionError,
  validateKafkaRuleExpression,
} from "./rule-expression-parser";

export { KafkaRuleExpressionError, validateKafkaRuleExpression };

export function evaluateKafkaRuleExpression(expression: string, sample: unknown): boolean {
  return evaluateCompiledKafkaRuleExpression(compileKafkaRuleExpression(expression), sample);
}
