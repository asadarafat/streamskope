import { KAFKA_RULE_LIMITS } from "./rule-types";
import type {
  CompiledKafkaRuleExpression,
  RuleCondition,
  RuleOperand,
  RulePath,
} from "./rule-expression-types";

export class KafkaRuleWorkBudget {
  private remaining: number;
  constructor(
    private readonly maximum: number = KAFKA_RULE_LIMITS.evaluationWork,
    private readonly shared?: KafkaRuleWorkBudget,
  ) {
    this.remaining = maximum;
  }
  get exhausted(): boolean {
    return this.remaining < 0;
  }
  spend(units = 1): void {
    this.shared?.spend(units);
    this.remaining -= units;
    if (this.remaining < 0)
      throw new Error(`Expression exceeds ${String(this.maximum)} evaluation work units.`);
  }
}

function objectValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}

function resolvePath(
  path: RulePath,
  root: unknown,
  current: unknown,
  budget: KafkaRuleWorkBudget,
): unknown[] {
  let values: unknown[] = [path.origin === "root" ? root : current];
  for (const segment of path.segments) {
    const next: unknown[] = [];
    const append = (value: unknown): void => {
      budget.spend();
      next.push(value);
    };
    for (const value of values) {
      budget.spend();
      const object = objectValue(value);
      switch (segment.kind) {
        case "property":
          if (object !== undefined && Object.hasOwn(object, segment.name))
            append(object[segment.name]);
          break;
        case "index":
          if (Array.isArray(value) && segment.index < value.length) append(value[segment.index]);
          break;
        case "wildcard":
          if (object !== undefined) for (const child of Object.values(object)) append(child);
          break;
        case "filter":
          if (Array.isArray(value))
            for (const candidate of value as unknown[]) {
              budget.spend();
              if (evaluateCondition(segment.condition, root, candidate, budget)) append(candidate);
            }
          break;
        case "recursive-property": {
          const stack: unknown[] = [value];
          while (stack.length > 0) {
            budget.spend();
            const entry = objectValue(stack.pop());
            if (entry === undefined) continue;
            if (Object.hasOwn(entry, segment.name)) append(entry[segment.name]);
            for (const child of Object.values(entry)) {
              budget.spend();
              stack.push(child);
            }
          }
          break;
        }
      }
    }
    values = next;
    if (values.length === 0) break;
  }
  return values;
}

function scalar(operand: RuleOperand | undefined): unknown {
  return operand?.kind === "scalar" ? operand.value : undefined;
}

function evaluateCondition(
  condition: RuleCondition,
  root: unknown,
  current: unknown,
  budget: KafkaRuleWorkBudget,
): boolean {
  const values = resolvePath(condition.path, root, current, budget);
  if (condition.operator === "exists")
    return values.some((value) => value !== undefined && value !== null);
  const expected = scalar(condition.operand);
  const regex =
    condition.operand?.kind === "regex"
      ? new RegExp(condition.operand.source, condition.operand.flags)
      : null;
  const compare = (value: unknown): boolean => {
    budget.spend();
    if (typeof value === "string") budget.spend(value.length);
    switch (condition.operator) {
      case "equals":
      case "not-equals":
        return Object.is(value, expected);
      case "contains":
        return String(value).includes(String(expected));
      case "matches":
        return regex?.test(String(value)) ?? false;
      case "greater-than":
        return (
          typeof value === "number" &&
          Number.isFinite(value) &&
          typeof expected === "number" &&
          Number.isFinite(expected) &&
          value > expected
        );
      case "greater-than-or-equal":
        return (
          typeof value === "number" &&
          Number.isFinite(value) &&
          typeof expected === "number" &&
          Number.isFinite(expected) &&
          value >= expected
        );
      case "less-than":
        return (
          typeof value === "number" &&
          Number.isFinite(value) &&
          typeof expected === "number" &&
          Number.isFinite(expected) &&
          value < expected
        );
      case "less-than-or-equal":
        return (
          typeof value === "number" &&
          Number.isFinite(value) &&
          typeof expected === "number" &&
          Number.isFinite(expected) &&
          value <= expected
        );
      case "exists":
        return false;
    }
  };
  for (const value of values) {
    for (const candidate of Array.isArray(value) ? (value as unknown[]) : [value]) {
      if (compare(candidate)) return condition.operator !== "not-equals";
    }
  }
  return condition.operator === "not-equals" && values.length > 0;
}

export function evaluateCompiledKafkaRuleExpression(
  expression: CompiledKafkaRuleExpression,
  sample: unknown,
  sharedBudget?: KafkaRuleWorkBudget,
): boolean {
  const budget = new KafkaRuleWorkBudget(KAFKA_RULE_LIMITS.evaluationWork, sharedBudget);
  const evaluate = (node: CompiledKafkaRuleExpression): boolean => {
    budget.spend();
    switch (node.kind) {
      case "and":
        return evaluate(node.left) && evaluate(node.right);
      case "or":
        return evaluate(node.left) || evaluate(node.right);
      case "condition":
        return evaluateCondition(node, sample, sample, budget);
    }
  };
  return evaluate(expression);
}
