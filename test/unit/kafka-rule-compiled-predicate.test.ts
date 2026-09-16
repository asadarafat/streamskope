import { describe, expect, it } from "vitest";

import type { KafkaRulePredicate } from "../../src/features/kafka/application";
import { KafkaRuleExpressionError, StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine";

interface PredicateCase {
  readonly expression: string;
  readonly expected: boolean;
  readonly sample: unknown;
}

const cases: readonly PredicateCase[] = [
  {
    expression: '$.priority == "high"',
    expected: true,
    sample: { priority: "high" },
  },
  {
    expression: "$.metrics.latency >= 20 && $.metrics.loss < 0.1",
    expected: true,
    sample: { metrics: { latency: 34, loss: 0.07 } },
  },
  {
    expression: '$["changes"][?(@["old-value"] == "before")]',
    expected: true,
    sample: {
      changes: [{ "old-value": "other" }, { "old-value": "before" }],
    },
  },
  {
    expression: '$.name matches /^edge-(?:42|7)$/ && $.labels[*] contains "prod"',
    expected: true,
    sample: { labels: ["blue", "prod"], name: "edge-42" },
  },
  {
    expression: '$.status == "ready" || $.status == "active"',
    expected: false,
    sample: { status: "failed" },
  },
  {
    expression: "$..alarm exists",
    expected: true,
    sample: { nested: { alarm: { severity: "major" } } },
  },
];

function compiled(expression: string): KafkaRulePredicate {
  return new StreamSkopeKafkaRuleEvaluator().compile(expression);
}

describe("compiled Kafka rule predicate", () => {
  it.each(cases)(
    "preserves the declared result for $expression",
    ({ expression, expected, sample }) => {
      expect(compiled(expression).evaluate(sample)).toBe(expected);
    },
  );

  it("evaluates repeatedly without receiving or exposing the source expression or AST", () => {
    const predicate = compiled('$.priority == "high"');

    expect(predicate.evaluate({ priority: "high" })).toBe(true);
    expect(predicate.evaluate({ priority: "low" })).toBe(false);
    expect(predicate.evaluate({ priority: "high" })).toBe(true);
    expect(predicate).not.toHaveProperty("expression");
    expect(predicate).not.toHaveProperty("kind");
    expect(predicate).not.toHaveProperty("segments");
  });

  it("rejects an invalid expression before a predicate is returned", () => {
    expect(() => compiled("$.priority ==")).toThrow(KafkaRuleExpressionError);
  });

  it("does not mutate a frozen sample", () => {
    const sample = Object.freeze({
      changes: Object.freeze([Object.freeze({ "old-value": "before" })]),
    });
    const before = JSON.stringify(sample);

    expect(compiled('$["changes"][?(@["old-value"] == "before")]').evaluate(sample)).toBe(true);
    expect(JSON.stringify(sample)).toBe(before);
  });
});
