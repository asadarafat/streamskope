import { describe, expect, it } from "vitest";

import { KAFKA_RULE_LIMITS } from "../../src/features/kafka/contracts";
import {
  KafkaRuleExpressionError,
  KafkaRuleSampleError,
  evaluateKafkaRuleExpression,
  parseKafkaRuleSample,
  validateKafkaRuleExpression,
} from "../../src/features/kafka/engine";

const sample = {
  changes: [
    { name: "state", "old-value": "down", "new-value": "up" },
    { name: "latency", "old-value": 80, "new-value": 34 },
  ],
  enabled: true,
  metrics: { latency_ms: 34, loss: 0.07 },
  name: "edge-01",
  priority: "high",
  tags: ["core", "critical"],
};

describe("Kafka rule engine", () => {
  it.each([
    ['$.priority == "high"', true],
    ['$["priority"] != "low"', true],
    ["$.metrics.latency_ms > 30", true],
    ["$.metrics.latency_ms >= 34", true],
    ["$.metrics.latency_ms < 35", true],
    ["$.metrics.latency_ms <= 34", true],
    ['$.name contains "edge"', true],
    ["$.enabled == true", true],
    ["$.missing exists", false],
    ['$.tags[*] == "critical"', true],
    ["$.changes[0]['old-value'] == \"down\"", true],
  ])("evaluates supported condition %s", (expression, expected) => {
    expect(evaluateKafkaRuleExpression(expression, sample)).toBe(expected);
  });

  it("evaluates the documented bracketed array filter without executing code", () => {
    expect(evaluateKafkaRuleExpression('$["changes"][?(@["old-value"] == "down")]', sample)).toBe(
      true,
    );
    expect(evaluateKafkaRuleExpression('$.changes[?(@["old-value"] == "missing")]', sample)).toBe(
      false,
    );
  });

  it("supports recursive properties, wildcards and item filters with bounded traversal", () => {
    expect(evaluateKafkaRuleExpression('$..name == "latency"', sample)).toBe(true);
    expect(evaluateKafkaRuleExpression('$.changes[*].name == "state"', sample)).toBe(true);
    expect(evaluateKafkaRuleExpression('$.tags[?(@ == "core")]', sample)).toBe(true);
  });

  it("uses AND precedence over OR and honors explicit grouping", () => {
    expect(
      evaluateKafkaRuleExpression(
        '$.priority == "low" || $.priority == "high" && $.enabled == true',
        sample,
      ),
    ).toBe(true);
    expect(
      evaluateKafkaRuleExpression(
        '($.priority == "low" || $.priority == "high") && $.enabled == false',
        sample,
      ),
    ).toBe(false);
  });

  it("supports the bounded safe regex examples from the source workflow", () => {
    expect(evaluateKafkaRuleExpression("$.name matches /^(?:edge|core)-/i", sample)).toBe(true);
    expect(evaluateKafkaRuleExpression('$.name matches "^edge-[0-9][0-9]$"', sample)).toBe(true);
    expect(evaluateKafkaRuleExpression("$.priority matches /error|high|timeout/i", sample)).toBe(
      true,
    );
  });

  it.each([
    "",
    "$.priority ==",
    '$.priority === "high"',
    "$.changes[",
    '$.changes[?(@.name = "state")]',
    '$.name unknown "edge"',
    "process.exit()",
    '$[?(@.constructor.constructor("return process")())]',
    "$.name matches /(a+)+$/",
    "$.name matches /edge/g",
  ])("rejects malformed or executable expression %s", (expression) => {
    expect(validateKafkaRuleExpression(expression)).toMatchObject({ valid: false });
    expect(() => evaluateKafkaRuleExpression(expression, sample)).toThrow(KafkaRuleExpressionError);
  });

  it("returns a bounded diagnostic with a source position", () => {
    const result = validateKafkaRuleExpression("$.priority ==");

    expect(result.valid).toBe(false);
    expect(result.diagnostic).toMatch(/^Position \d+: /);
    expect(result.diagnostic?.length).toBeLessThanOrEqual(KAFKA_RULE_LIMITS.diagnosticCharacters);
  });

  it("rejects condition, grouping, path and regex complexity beyond contract bounds", () => {
    const tooManyConditions = Array.from(
      { length: KAFKA_RULE_LIMITS.conditions + 1 },
      () => "$.enabled == true",
    ).join(" && ");
    const tooDeep = `${"(".repeat(KAFKA_RULE_LIMITS.groupingDepth + 1)}$.enabled${")".repeat(
      KAFKA_RULE_LIMITS.groupingDepth + 1,
    )}`;
    const tooManySegments = `$${".child".repeat(KAFKA_RULE_LIMITS.pathSegments + 1)}`;
    const tooManyFilterConditions = `$${"[?(@.enabled == true)]".repeat(
      KAFKA_RULE_LIMITS.conditions,
    )}`;
    const unsafeArrayIndex = `$[${"9".repeat(400)}] exists`;
    const longRegex = `$.name matches /${"x".repeat(KAFKA_RULE_LIMITS.regexCharacters + 1)}/`;

    for (const expression of [
      tooManyConditions,
      tooDeep,
      tooManySegments,
      tooManyFilterConditions,
      unsafeArrayIndex,
      longRegex,
    ]) {
      expect(validateKafkaRuleExpression(expression)).toMatchObject({ valid: false });
    }
  });

  it("parses bounded JSON samples and rejects malformed, oversized or structurally deep data", () => {
    expect(parseKafkaRuleSample('{"ok":true}')).toEqual({ ok: true });
    expect(() => parseKafkaRuleSample("{")).toThrow(KafkaRuleSampleError);
    expect(() => parseKafkaRuleSample("😀".repeat(KAFKA_RULE_LIMITS.sampleBytes / 2))).toThrow(
      KafkaRuleSampleError,
    );

    let deep = "null";
    for (let index = 0; index <= KAFKA_RULE_LIMITS.sampleDepth; index += 1) {
      deep = `{"child":${deep}}`;
    }
    expect(() => parseKafkaRuleSample(deep)).toThrow(KafkaRuleSampleError);

    const tooManyNodes = JSON.stringify(
      Array.from({ length: KAFKA_RULE_LIMITS.sampleNodes + 1 }, () => null),
    );
    expect(() => parseKafkaRuleSample(tooManyNodes)).toThrow(KafkaRuleSampleError);
  });

  it("does not mutate the supplied sample during filters or recursive traversal", () => {
    const before = structuredClone(sample);

    evaluateKafkaRuleExpression(
      '$..name == "state" && $.changes[?(@["new-value"] == "up")]',
      sample,
    );

    expect(sample).toEqual(before);
  });
});
