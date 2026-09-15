import type {
  CompiledKafkaRuleExpression,
  RuleCondition,
  RuleOperand,
  RulePath,
} from "./rule-expression-types";

function objectValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}

function comparisonValues(values: readonly unknown[]): readonly unknown[] {
  return values.flatMap((value) => (Array.isArray(value) ? (value as unknown[]) : [value]));
}

function scalar(operand: RuleOperand | undefined): unknown {
  return operand?.kind === "scalar" ? operand.value : undefined;
}

function numericCompare(
  values: readonly unknown[],
  operand: RuleOperand | undefined,
  compare: (left: number, right: number) => boolean,
): boolean {
  const right = Number(scalar(operand));
  return (
    Number.isFinite(right) &&
    comparisonValues(values).some((value) => {
      const left = Number(value);
      return Number.isFinite(left) && compare(left, right);
    })
  );
}

function evaluateCondition(condition: RuleCondition, root: unknown, current: unknown): boolean {
  const values = resolvePath(condition.path, root, current);
  switch (condition.operator) {
    case "exists":
      return values.some((value) => value !== undefined && value !== null);
    case "equals":
      return comparisonValues(values).some((value) => Object.is(value, scalar(condition.operand)));
    case "not-equals":
      return (
        values.length > 0 &&
        comparisonValues(values).every((value) => !Object.is(value, scalar(condition.operand)))
      );
    case "contains": {
      const expected = String(scalar(condition.operand));
      return comparisonValues(values).some((value) => String(value).includes(expected));
    }
    case "greater-than":
      return numericCompare(values, condition.operand, (left, right) => left > right);
    case "greater-than-or-equal":
      return numericCompare(values, condition.operand, (left, right) => left >= right);
    case "less-than":
      return numericCompare(values, condition.operand, (left, right) => left < right);
    case "less-than-or-equal":
      return numericCompare(values, condition.operand, (left, right) => left <= right);
    case "matches": {
      if (condition.operand?.kind !== "regex") {
        return false;
      }
      const expression = new RegExp(condition.operand.source, condition.operand.flags);
      return comparisonValues(values).some((value) => expression.test(String(value)));
    }
  }
}

function recursiveProperty(values: readonly unknown[], name: string): unknown[] {
  const matches: unknown[] = [];
  const stack = [...values];
  while (stack.length > 0) {
    const value = stack.pop();
    const object = objectValue(value);
    if (object === undefined) {
      continue;
    }
    if (Object.hasOwn(object, name)) {
      matches.push(object[name]);
    }
    if (Array.isArray(value)) {
      stack.push(...(value as unknown[]));
    } else {
      stack.push(...Object.values(object));
    }
  }
  return matches;
}

function resolvePath(path: RulePath, root: unknown, current: unknown): unknown[] {
  let values: unknown[] = [path.origin === "root" ? root : current];
  for (const segment of path.segments) {
    switch (segment.kind) {
      case "property":
        values = values.flatMap((value) => {
          const object = objectValue(value);
          return object !== undefined && Object.hasOwn(object, segment.name)
            ? [object[segment.name]]
            : [];
        });
        break;
      case "index":
        values = values.flatMap((value) =>
          Array.isArray(value) && segment.index < value.length
            ? [(value as unknown[])[segment.index]]
            : [],
        );
        break;
      case "wildcard":
        values = values.flatMap((value) => {
          if (Array.isArray(value)) {
            return value as unknown[];
          }
          const object = objectValue(value);
          return object === undefined ? [] : Object.values(object);
        });
        break;
      case "recursive-property":
        values = recursiveProperty(values, segment.name);
        break;
      case "filter":
        values = values.flatMap((value) =>
          Array.isArray(value)
            ? (value as unknown[]).filter((candidate) =>
                evaluateCondition(segment.condition, root, candidate),
              )
            : [],
        );
        break;
    }
    if (values.length === 0) {
      return values;
    }
  }
  return values;
}

export function evaluateCompiledKafkaRuleExpression(
  expression: CompiledKafkaRuleExpression,
  sample: unknown,
): boolean {
  switch (expression.kind) {
    case "and":
      return (
        evaluateCompiledKafkaRuleExpression(expression.left, sample) &&
        evaluateCompiledKafkaRuleExpression(expression.right, sample)
      );
    case "or":
      return (
        evaluateCompiledKafkaRuleExpression(expression.left, sample) ||
        evaluateCompiledKafkaRuleExpression(expression.right, sample)
      );
    case "condition":
      return evaluateCondition(expression, sample, sample);
  }
}
