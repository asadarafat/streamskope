import {
  KAFKA_RULE_LIMITS,
  KAFKA_RULE_SEVERITIES,
  canonicalKafkaRuleName,
  type KafkaRuleDefinition,
  type KafkaRuleEvaluationInput,
  type KafkaRuleEvaluationResult,
  type KafkaRuleField,
  type KafkaRuleIssue,
  type KafkaRuleSnapshot,
  type KafkaRuleStoreCapability,
} from "../contracts";

import {
  DuplicateKafkaRuleError,
  KafkaRuleCapacityError,
  KafkaRuleCatalogNotLoadedError,
  KafkaRuleCorruptError,
  KafkaRuleNotFoundError,
  KafkaRuleSampleValidationError,
  KafkaRuleStoreUnavailableError,
  KafkaRuleValidationError,
} from "./rule-errors";
import { cloneKafkaRuleDocument } from "./in-memory-rule-store";
import type {
  KafkaRuleDocument,
  KafkaRuleEvaluator,
  KafkaRuleStore,
  KafkaRuleStructuredError,
} from "./rule-types";

const CORRUPT_RULE_RECOVERY =
  "Preserve the rule data, correct it outside the running application, then restart StreamSkope.";

interface RuleInspection {
  readonly issues: readonly KafkaRuleIssue[];
  readonly rule?: KafkaRuleDefinition;
}

function issue(field: KafkaRuleField, message: string): KafkaRuleIssue {
  return { field, message };
}

function boundedString(
  value: unknown,
  field: KafkaRuleField,
  maximum: number,
  issues: KafkaRuleIssue[],
): string | undefined {
  if (typeof value !== "string") {
    issues.push(issue(field, "must be a string"));
    return undefined;
  }
  if (value.length > maximum) {
    issues.push(issue(field, `must be no longer than ${String(maximum)} characters`));
    return undefined;
  }
  return value;
}

function optionalCanonicalString(
  value: unknown,
  field: "description" | "topic",
  maximum: number,
  issues: KafkaRuleIssue[],
): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const text = boundedString(value, field, maximum, issues);
  if (text === undefined) {
    return undefined;
  }
  const canonical = text.trim();
  return canonical.length === 0 ? undefined : canonical;
}

function comparableName(name: string): string {
  return canonicalKafkaRuleName(name).toLocaleLowerCase("en-US");
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function isStructuredRuleError(error: unknown): error is KafkaRuleStructuredError {
  return (
    error instanceof Error &&
    "code" in error &&
    typeof error.code === "string" &&
    error.code.startsWith("RULE_") &&
    "recovery" in error &&
    typeof error.recovery === "string"
  );
}

function inspectRule(input: unknown, evaluator: KafkaRuleEvaluator): RuleInspection {
  const value =
    input !== null && typeof input === "object" && !Array.isArray(input)
      ? (input as Record<string, unknown>)
      : {};
  const issues: KafkaRuleIssue[] = [];

  const rawName = boundedString(value.name, "name", KAFKA_RULE_LIMITS.nameCharacters, issues);
  const name = rawName === undefined ? undefined : canonicalKafkaRuleName(rawName);
  if (name !== undefined && name.length === 0) {
    issues.push(issue("name", "is required"));
  }

  const rawExpression = boundedString(
    value.expression,
    "expression",
    KAFKA_RULE_LIMITS.expressionCharacters,
    issues,
  );
  const expression = rawExpression?.trim();
  if (expression !== undefined && expression.length === 0) {
    issues.push(issue("expression", "is required"));
  } else if (expression !== undefined) {
    const validation = evaluator.validate(expression);
    if (!validation.valid) {
      issues.push(issue("expression", validation.diagnostic ?? "Expression validation failed."));
    }
  }

  const cooldownMs = value.cooldownMs;
  if (
    !Number.isSafeInteger(cooldownMs) ||
    (cooldownMs as number) < 0 ||
    (cooldownMs as number) > KAFKA_RULE_LIMITS.cooldownMs
  ) {
    issues.push(
      issue(
        "cooldownMs",
        `must be a safe integer from 0 through ${String(KAFKA_RULE_LIMITS.cooldownMs)}`,
      ),
    );
  }

  const enabled = value.enabled;
  if (typeof enabled !== "boolean") {
    issues.push(issue("enabled", "must be a boolean"));
  }

  const level = value.level;
  if (
    typeof level !== "string" ||
    !KAFKA_RULE_SEVERITIES.includes(level as (typeof KAFKA_RULE_SEVERITIES)[number])
  ) {
    issues.push(issue("level", `must be one of ${KAFKA_RULE_SEVERITIES.join(", ")}`));
  }

  const description = optionalCanonicalString(
    value.description,
    "description",
    KAFKA_RULE_LIMITS.descriptionCharacters,
    issues,
  );
  const topic = optionalCanonicalString(
    value.topic,
    "topic",
    KAFKA_RULE_LIMITS.topicCharacters,
    issues,
  );

  if (
    issues.length > 0 ||
    name === undefined ||
    expression === undefined ||
    typeof enabled !== "boolean" ||
    typeof level !== "string" ||
    !KAFKA_RULE_SEVERITIES.includes(level as (typeof KAFKA_RULE_SEVERITIES)[number]) ||
    !Number.isSafeInteger(cooldownMs)
  ) {
    return { issues };
  }

  return {
    issues,
    rule: {
      cooldownMs: cooldownMs as number,
      ...(description === undefined ? {} : { description }),
      enabled,
      expression,
      level: level as (typeof KAFKA_RULE_SEVERITIES)[number],
      name,
      ...(topic === undefined ? {} : { topic }),
    },
  };
}

function sameRule(left: unknown, right: KafkaRuleDefinition): boolean {
  if (left === null || typeof left !== "object" || Array.isArray(left)) {
    return false;
  }
  const value = left as Record<string, unknown>;
  const allowedKeys = new Set([
    "cooldownMs",
    "description",
    "enabled",
    "expression",
    "level",
    "name",
    "topic",
  ]);
  return (
    Object.keys(value).every((key) => allowedKeys.has(key)) &&
    value.cooldownMs === right.cooldownMs &&
    value.description === right.description &&
    value.enabled === right.enabled &&
    value.expression === right.expression &&
    value.level === right.level &&
    value.name === right.name &&
    value.topic === right.topic
  );
}

function validDocument(document: unknown, evaluator: KafkaRuleEvaluator): boolean {
  if (document === null || typeof document !== "object" || Array.isArray(document)) {
    return false;
  }
  const documentRecord = document as Record<string, unknown>;
  if (
    Object.keys(documentRecord).some((key) => key !== "rules") ||
    !Array.isArray(documentRecord.rules) ||
    documentRecord.rules.length > KAFKA_RULE_LIMITS.rules
  ) {
    return false;
  }
  const names = new Set<string>();
  for (const untrustedCandidate of documentRecord.rules) {
    const candidate: unknown = untrustedCandidate;
    const inspected = inspectRule(candidate, evaluator);
    if (
      inspected.rule === undefined ||
      !sameRule(candidate, inspected.rule) ||
      names.has(comparableName(inspected.rule.name))
    ) {
      return false;
    }
    names.add(comparableName(inspected.rule.name));
  }
  return true;
}

function emptyDocument(): KafkaRuleDocument {
  return { rules: [] };
}

export class KafkaRuleService {
  private document: KafkaRuleDocument = emptyDocument();
  private loaded = false;
  private loadPromise: Promise<void> | undefined;
  private mutationTail: Promise<void> = Promise.resolve();
  private ruleDataUnavailable = false;

  constructor(
    private readonly store: KafkaRuleStore,
    private readonly evaluator: KafkaRuleEvaluator,
  ) {}

  create(input: KafkaRuleDefinition, signal?: AbortSignal): Promise<KafkaRuleSnapshot> {
    return this.mutate(() => this.completeCreate(input, signal), signal);
  }

  currentSnapshot(): KafkaRuleSnapshot {
    return this.snapshot();
  }

  delete(name: string, signal?: AbortSignal): Promise<KafkaRuleSnapshot> {
    return this.mutate(() => this.completeDelete(name, signal), signal);
  }

  evaluate(
    input: KafkaRuleEvaluationInput,
    signal?: AbortSignal,
  ): readonly KafkaRuleEvaluationResult[] {
    signal?.throwIfAborted();
    let sample: unknown;
    try {
      sample = this.evaluator.parseSample(input.sample);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      const message = error instanceof Error ? error.message : "The sample could not be parsed.";
      throw new KafkaRuleSampleValidationError(message, { cause: error });
    }
    if (input.scope === "catalog") {
      this.assertStoreAvailable();
      if (!this.loaded) {
        throw new KafkaRuleCatalogNotLoadedError();
      }
    }
    const rules =
      input.scope === "single" ? [input.rule] : cloneKafkaRuleDocument(this.document).rules;

    return rules.map((candidate) => {
      signal?.throwIfAborted();
      const inspected = inspectRule(candidate, this.evaluator);
      const name =
        typeof candidate.name === "string" && canonicalKafkaRuleName(candidate.name).length > 0
          ? canonicalKafkaRuleName(candidate.name)
          : "Rule draft";
      if (inspected.rule === undefined) {
        return {
          diagnostic: inspected.issues
            .map((candidateIssue) => `${candidateIssue.field}: ${candidateIssue.message}`)
            .join("\n")
            .slice(0, KAFKA_RULE_LIMITS.diagnosticCharacters),
          name,
          outcome: "invalid",
        };
      }
      const rule = inspected.rule;
      if (!rule.enabled) {
        return {
          diagnostic: "Rule is disabled.",
          name: rule.name,
          outcome: "skipped",
          reason: "disabled",
        };
      }
      if (input.topic !== undefined && rule.topic !== undefined && input.topic !== rule.topic) {
        return {
          diagnostic: `Rule applies to topic ${rule.topic}.`,
          name: rule.name,
          outcome: "skipped",
          reason: "topic-mismatch",
        };
      }
      return {
        name: rule.name,
        outcome: this.evaluator.evaluate(rule.expression, sample) ? "matched" : "not-matched",
      };
    });
  }

  async list(signal?: AbortSignal): Promise<KafkaRuleSnapshot> {
    await this.ensureLoaded(signal);
    return this.snapshot();
  }

  update(
    originalName: string,
    input: KafkaRuleDefinition,
    signal?: AbortSignal,
  ): Promise<KafkaRuleSnapshot> {
    return this.mutate(() => this.completeUpdate(originalName, input, signal), signal);
  }

  validate(input: KafkaRuleDefinition): KafkaRuleEvaluationResult {
    const inspected = inspectRule(input, this.evaluator);
    const name =
      typeof input.name === "string" && canonicalKafkaRuleName(input.name).length > 0
        ? canonicalKafkaRuleName(input.name)
        : "Rule draft";
    if (inspected.rule === undefined) {
      return {
        diagnostic: inspected.issues
          .map((candidate) => `${candidate.field}: ${candidate.message}`)
          .join("\n")
          .slice(0, KAFKA_RULE_LIMITS.diagnosticCharacters),
        name,
        outcome: "invalid",
      };
    }
    return { name: inspected.rule.name, outcome: "valid" };
  }

  private async commit(
    next: KafkaRuleDocument,
    signal: AbortSignal | undefined,
    target: string,
  ): Promise<KafkaRuleSnapshot> {
    try {
      await this.store.commit(next, signal);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw new KafkaRuleStoreUnavailableError(target, "RULE_STORE_UNAVAILABLE", {
        cause: error,
      });
    }
    this.document = cloneKafkaRuleDocument(next);
    return this.snapshot();
  }

  private async completeCreate(
    input: KafkaRuleDefinition,
    signal?: AbortSignal,
  ): Promise<KafkaRuleSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const rule = this.validRule(input);
    if (this.document.rules.length >= KAFKA_RULE_LIMITS.rules) {
      throw new KafkaRuleCapacityError(KAFKA_RULE_LIMITS.rules);
    }
    if (this.ruleIndex(rule.name) >= 0) {
      throw new DuplicateKafkaRuleError(rule.name);
    }
    return this.commit({ rules: [...this.document.rules, rule] }, signal, rule.name);
  }

  private async completeDelete(name: string, signal?: AbortSignal): Promise<KafkaRuleSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const index = this.ruleIndex(name);
    const existing = this.document.rules[index];
    if (existing === undefined) {
      throw new KafkaRuleNotFoundError(canonicalKafkaRuleName(name));
    }
    return this.commit(
      { rules: this.document.rules.filter((_rule, candidateIndex) => candidateIndex !== index) },
      signal,
      existing.name,
    );
  }

  private async completeUpdate(
    originalName: string,
    input: KafkaRuleDefinition,
    signal?: AbortSignal,
  ): Promise<KafkaRuleSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const index = this.ruleIndex(originalName);
    const existing = this.document.rules[index];
    if (existing === undefined) {
      throw new KafkaRuleNotFoundError(canonicalKafkaRuleName(originalName));
    }
    const rule = this.validRule(input);
    if (
      this.document.rules.some(
        (candidate, candidateIndex) =>
          candidateIndex !== index && comparableName(candidate.name) === comparableName(rule.name),
      )
    ) {
      throw new DuplicateKafkaRuleError(rule.name);
    }
    return this.commit(
      {
        rules: this.document.rules.map((candidate, candidateIndex) =>
          candidateIndex === index ? rule : candidate,
        ),
      },
      signal,
      existing.name,
    );
  }

  private async ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const pending = this.loadPromise ?? this.load(signal);
    this.loadPromise = pending;
    try {
      await pending;
    } catch (error) {
      if (isAbort(error) && this.loadPromise === pending) {
        this.loadPromise = undefined;
      }
      throw error;
    }
    signal?.throwIfAborted();
  }

  private assertStoreAvailable(): void {
    if (this.storeCapability().state === "unavailable") {
      throw new KafkaRuleStoreUnavailableError();
    }
  }

  private async load(signal?: AbortSignal): Promise<void> {
    try {
      const stored = await this.store.load(signal);
      if (stored === undefined) {
        this.document = emptyDocument();
        this.loaded = true;
        return;
      }
      if (!validDocument(stored, this.evaluator)) {
        throw new KafkaRuleCorruptError();
      }
      this.document = cloneKafkaRuleDocument(stored);
      this.loaded = true;
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.ruleDataUnavailable = true;
      this.document = emptyDocument();
      if (isStructuredRuleError(error)) {
        throw error;
      }
      throw new KafkaRuleStoreUnavailableError(undefined, "RULE_STORE_UNAVAILABLE", {
        cause: error,
      });
    }
  }

  private mutate<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const result = this.mutationTail.then(async () => {
      signal?.throwIfAborted();
      return operation();
    });
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private ruleIndex(name: string): number {
    const comparable = comparableName(name);
    return this.document.rules.findIndex(
      (candidate) => comparableName(candidate.name) === comparable,
    );
  }

  private snapshot(): KafkaRuleSnapshot {
    const document = cloneKafkaRuleDocument(this.document);
    return {
      rules: document.rules,
      store: this.storeCapability(),
    };
  }

  private storeCapability(): KafkaRuleStoreCapability {
    const capability = this.store.capability();
    if (!this.ruleDataUnavailable || capability.state === "unavailable") {
      return capability;
    }
    return {
      durability: capability.durability,
      recovery: CORRUPT_RULE_RECOVERY,
      state: "unavailable",
    };
  }

  private validRule(input: KafkaRuleDefinition): KafkaRuleDefinition {
    const inspected = inspectRule(input, this.evaluator);
    if (inspected.rule === undefined) {
      throw new KafkaRuleValidationError(inspected.issues);
    }
    return inspected.rule;
  }
}
