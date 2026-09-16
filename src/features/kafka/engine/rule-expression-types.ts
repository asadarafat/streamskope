export type RuleScalar = string | number | boolean | null;

export type RulePathSegment =
  | { readonly kind: "property"; readonly name: string }
  | { readonly index: number; readonly kind: "index" }
  | { readonly kind: "wildcard" }
  | { readonly kind: "recursive-property"; readonly name: string }
  | { readonly condition: RuleCondition; readonly kind: "filter" };

export interface RulePath {
  readonly origin: "current" | "root";
  readonly segments: readonly RulePathSegment[];
}

export type RuleOperator =
  | "contains"
  | "equals"
  | "exists"
  | "greater-than"
  | "greater-than-or-equal"
  | "less-than"
  | "less-than-or-equal"
  | "matches"
  | "not-equals";

export type RuleOperand =
  | { readonly kind: "scalar"; readonly value: RuleScalar }
  | { readonly flags: string; readonly kind: "regex"; readonly source: string };

export interface RuleCondition {
  readonly kind: "condition";
  readonly operand?: RuleOperand;
  readonly operator: RuleOperator;
  readonly path: RulePath;
}

export type CompiledKafkaRuleExpression =
  | RuleCondition
  | {
      readonly kind: "and" | "or";
      readonly left: CompiledKafkaRuleExpression;
      readonly right: CompiledKafkaRuleExpression;
    };
