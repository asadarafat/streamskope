import {
  KAFKA_RULE_EVALUATION_OUTCOMES,
  KAFKA_RULE_LIMITS,
  KAFKA_RULE_SEVERITIES,
  KAFKA_RULE_SKIP_REASONS,
  KAFKA_RULE_STORE_DURABILITIES,
  KAFKA_RULE_STORE_STATES,
  type KafkaRuleDefinition,
  type KafkaRuleEvaluationInput,
  type KafkaRuleEvaluationReport,
  type KafkaRuleEvaluationResult,
  type KafkaRuleSnapshot,
  type KafkaRuleStoreCapability,
} from "./rule-types";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  boundedUtf8Text,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  optionalText,
  record,
  text,
  truth,
} from "./validation-primitives";

export function canonicalKafkaRuleName(name: string): string {
  return name.trim();
}

function optionalBoundedText(
  value: Record<string, unknown>,
  key: "description" | "topic",
  path: string,
  maximum: number,
): string | undefined {
  if (!Object.hasOwn(value, key)) {
    return undefined;
  }
  return boundedText(value[key], `${path}.${key}`, maximum);
}

export function parseKafkaRuleDefinition(value: unknown, path: string): KafkaRuleDefinition {
  const rule = record(value, path);
  exactKeys(
    rule,
    ["cooldownMs", "description", "enabled", "expression", "level", "name", "topic"],
    path,
  );
  const cooldownMs = nonNegativeInteger(rule.cooldownMs, `${path}.cooldownMs`);
  if (cooldownMs > KAFKA_RULE_LIMITS.cooldownMs) {
    throw new HostContractValidationError(
      `${path}.cooldownMs`,
      `must be no greater than ${KAFKA_RULE_LIMITS.cooldownMs}`,
    );
  }
  const base = {
    cooldownMs,
    enabled: truth(rule.enabled, `${path}.enabled`),
    expression: text(rule.expression, `${path}.expression`, KAFKA_RULE_LIMITS.expressionCharacters),
    level: declaredValue(rule.level, KAFKA_RULE_SEVERITIES, `${path}.level`),
    name: text(rule.name, `${path}.name`, KAFKA_RULE_LIMITS.nameCharacters),
  };
  const description = optionalBoundedText(
    rule,
    "description",
    path,
    KAFKA_RULE_LIMITS.descriptionCharacters,
  );
  const topic = optionalBoundedText(rule, "topic", path, KAFKA_RULE_LIMITS.topicCharacters);
  return {
    ...base,
    ...(description === undefined ? {} : { description }),
    ...(topic === undefined ? {} : { topic }),
  };
}

function parseCanonicalRule(value: unknown, path: string): KafkaRuleDefinition {
  const rule = parseKafkaRuleDefinition(value, path);
  if (rule.name !== canonicalKafkaRuleName(rule.name)) {
    throw new HostContractValidationError(`${path}.name`, "must be canonical");
  }
  if (rule.description === "" || rule.topic === "") {
    throw new HostContractValidationError(path, "must omit empty optional fields");
  }
  return rule;
}

export function parseKafkaRuleIdentityPayload(
  value: unknown,
  path: string,
): { readonly name: string } {
  const payload = record(value, path);
  exactKeys(payload, ["name"], path);
  return { name: text(payload.name, `${path}.name`, KAFKA_RULE_LIMITS.nameCharacters) };
}

export function parseKafkaRuleCreatePayload(
  value: unknown,
  path: string,
): { readonly rule: KafkaRuleDefinition } {
  const payload = record(value, path);
  exactKeys(payload, ["rule"], path);
  return { rule: parseKafkaRuleDefinition(payload.rule, `${path}.rule`) };
}

export function parseKafkaRuleUpdatePayload(
  value: unknown,
  path: string,
): { readonly originalName: string; readonly rule: KafkaRuleDefinition } {
  const payload = record(value, path);
  exactKeys(payload, ["originalName", "rule"], path);
  return {
    originalName: text(
      payload.originalName,
      `${path}.originalName`,
      KAFKA_RULE_LIMITS.nameCharacters,
    ),
    rule: parseKafkaRuleDefinition(payload.rule, `${path}.rule`),
  };
}

export function parseKafkaRuleEvaluationInput(
  value: unknown,
  path: string,
): KafkaRuleEvaluationInput {
  const payload = record(value, path);
  const scope = declaredValue(payload.scope, ["catalog", "single"], `${path}.scope`);
  const topic = optionalText(payload, "topic", path, KAFKA_RULE_LIMITS.topicCharacters);
  if (scope === "single") {
    exactKeys(payload, ["rule", "sample", "scope", "topic"], path);
    return {
      rule: parseKafkaRuleDefinition(payload.rule, `${path}.rule`),
      sample: boundedUtf8Text(payload.sample, `${path}.sample`, KAFKA_RULE_LIMITS.sampleBytes),
      scope,
      ...(topic === undefined ? {} : { topic }),
    };
  }
  exactKeys(payload, ["sample", "scope", "topic"], path);
  return {
    sample: boundedUtf8Text(payload.sample, `${path}.sample`, KAFKA_RULE_LIMITS.sampleBytes),
    scope,
    ...(topic === undefined ? {} : { topic }),
  };
}

function parseStoreCapability(value: unknown, path: string): KafkaRuleStoreCapability {
  const store = record(value, path);
  exactKeys(store, ["durability", "recovery", "state"], path);
  const durability = declaredValue(
    store.durability,
    KAFKA_RULE_STORE_DURABILITIES,
    `${path}.durability`,
  );
  const state = declaredValue(store.state, KAFKA_RULE_STORE_STATES, `${path}.state`);
  const recovery = optionalText(store, "recovery", path, 2_048);
  if (state === "unavailable" && recovery === undefined) {
    throw new HostContractValidationError(`${path}.recovery`, "is required while unavailable");
  }
  if (state === "ready" && recovery !== undefined) {
    throw new HostContractValidationError(`${path}.recovery`, "must be omitted while ready");
  }
  return recovery === undefined ? { durability, state } : { durability, recovery, state };
}

export function parseKafkaRuleSnapshot(value: unknown, path: string): KafkaRuleSnapshot {
  const payload = record(value, path);
  exactKeys(payload, ["rules", "store"], path);
  if (!Array.isArray(payload.rules) || payload.rules.length > KAFKA_RULE_LIMITS.rules) {
    throw new HostContractValidationError(
      `${path}.rules`,
      `must contain at most ${KAFKA_RULE_LIMITS.rules} rules`,
    );
  }
  const rules = payload.rules.map((rule, index) =>
    parseCanonicalRule(rule, `${path}.rules[${index}]`),
  );
  const names = rules.map((rule) => rule.name.toLocaleLowerCase("en-US"));
  if (new Set(names).size !== names.length) {
    throw new HostContractValidationError(`${path}.rules`, "must have unique canonical names");
  }
  const store = parseStoreCapability(payload.store, `${path}.store`);
  if (store.state === "unavailable" && rules.length > 0) {
    throw new HostContractValidationError(
      `${path}.rules`,
      "must be empty while rule storage is unavailable",
    );
  }
  return { rules, store };
}

function parseEvaluationResult(value: unknown, path: string): KafkaRuleEvaluationResult {
  const result = record(value, path);
  exactKeys(result, ["diagnostic", "name", "outcome", "reason"], path);
  const outcome = declaredValue(result.outcome, KAFKA_RULE_EVALUATION_OUTCOMES, `${path}.outcome`);
  const diagnostic = optionalText(
    result,
    "diagnostic",
    path,
    KAFKA_RULE_LIMITS.diagnosticCharacters,
  );
  const reason = Object.hasOwn(result, "reason")
    ? declaredValue(result.reason, KAFKA_RULE_SKIP_REASONS, `${path}.reason`)
    : undefined;
  if (outcome === "skipped" && reason === undefined) {
    throw new HostContractValidationError(`${path}.reason`, "is required for skipped results");
  }
  if (outcome !== "skipped" && reason !== undefined) {
    throw new HostContractValidationError(`${path}.reason`, "is only valid for skipped results");
  }
  if (outcome === "invalid" && diagnostic === undefined) {
    throw new HostContractValidationError(`${path}.diagnostic`, "is required for invalid results");
  }
  return {
    name: text(result.name, `${path}.name`, KAFKA_RULE_LIMITS.nameCharacters),
    outcome,
    ...(diagnostic === undefined ? {} : { diagnostic }),
    ...(reason === undefined ? {} : { reason }),
  };
}

export function parseKafkaRuleEvaluationReport(
  value: unknown,
  path: string,
): KafkaRuleEvaluationReport {
  const payload = record(value, path);
  exactKeys(payload, ["kind", "requestId", "results"], path);
  const kind = declaredValue(payload.kind, ["evaluation", "validation"], `${path}.kind`);
  if (!Array.isArray(payload.results) || payload.results.length > KAFKA_RULE_LIMITS.rules) {
    throw new HostContractValidationError(
      `${path}.results`,
      `must contain at most ${KAFKA_RULE_LIMITS.rules} results`,
    );
  }
  if (kind === "validation" && payload.results.length !== 1) {
    throw new HostContractValidationError(
      `${path}.results`,
      "must contain exactly one validation result",
    );
  }
  return {
    kind,
    requestId: text(payload.requestId, `${path}.requestId`, 128),
    results: payload.results.map((result, index) =>
      parseEvaluationResult(result, `${path}.results[${index}]`),
    ),
  };
}
