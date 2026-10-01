import {
  KAFKA_LIVE_RULE_LIMITS,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_RULE_LIMITS,
  canonicalKafkaRuleName,
  kafkaLiveRuleEvidenceBytes,
  maximumKafkaRuleSeverity,
  type KafkaLiveRuleCapability,
  type KafkaLiveRuleError,
  type KafkaLiveRuleEvaluation,
  type KafkaLiveRuleMatch,
  type KafkaLiveRuleUnavailableReason,
  type KafkaMessage,
  type KafkaRuleDefinition,
  type KafkaRuleSnapshot,
} from "../contracts";

import type { KafkaRuleEvaluator, KafkaRulePredicate } from "./rule-types";
import type { KafkaRuleService } from "./rule-service";
import { KafkaRuleSampleParseError, type KafkaRuleSampleLimits } from "./rule-sample-error";

const DEFAULT_CATALOG_RECOVERY =
  "Review the rule catalog failure in Activity, correct the catalog, then start consumption again.";
const COMPILE_FAILURE_DIAGNOSTIC = "Rule compilation failed.";
const EVALUATION_FAILURE_DIAGNOSTIC = "Rule evaluation failed.";
const LIVE_SAMPLE_LIMITS: KafkaRuleSampleLimits = Object.freeze({
  bytes: KAFKA_LIVE_RULE_LIMITS.payloadBytes,
  depth: KAFKA_RULE_LIMITS.sampleDepth,
  nodes: KAFKA_LIVE_RULE_LIMITS.sampleNodes,
});

const EMPTY_MATCHES: readonly KafkaLiveRuleMatch[] = Object.freeze([]);
const EMPTY_ERRORS: readonly KafkaLiveRuleError[] = Object.freeze([]);
const EMPTY_EVALUATION: KafkaLiveRuleEvaluation = Object.freeze({
  activeMatchCount: 0,
  activeMatches: EMPTY_MATCHES,
  durationMicros: 0,
  errorCount: 0,
  errors: EMPTY_ERRORS,
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: EMPTY_MATCHES,
});

function defaultPerformanceNow(): number {
  return performance.now();
}

interface CompiledRule {
  readonly predicate?: KafkaRulePredicate;
  readonly state: "failed" | "ready";
}

interface PreparedRule {
  readonly compiled: CompiledRule;
  readonly cooldownMs: number;
  readonly identity: string;
  readonly level: KafkaRuleDefinition["level"];
  readonly name: string;
}

export interface KafkaLiveRuleRuntimeOptions {
  readonly durationNow: () => number;
  readonly monotonicNow: () => number;
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function supersededPreparationError(): Error {
  const error = new Error("Live rule preparation was superseded.");
  error.name = "AbortError";
  return error;
}

function semanticIdentity(rule: KafkaRuleDefinition): string {
  return JSON.stringify([
    canonicalKafkaRuleName(rule.name),
    rule.expression,
    rule.topic ?? null,
    rule.enabled,
    rule.cooldownMs,
  ]);
}

function frozenCapability(capability: KafkaLiveRuleCapability): KafkaLiveRuleCapability {
  return Object.freeze(capability);
}

function unavailableCapability(recovery?: string): KafkaLiveRuleCapability {
  return frozenCapability({
    applicableRules: 0,
    omittedRules: 0,
    recovery: recovery ?? DEFAULT_CATALOG_RECOVERY,
    state: "unavailable",
  });
}

function unavailableEvaluation(reason: KafkaLiveRuleUnavailableReason): KafkaLiveRuleEvaluation {
  return Object.freeze({
    activeMatchCount: 0,
    activeMatches: EMPTY_MATCHES,
    durationMicros: 0,
    errorCount: 0,
    errors: EMPTY_ERRORS,
    evaluatedRules: 0,
    omittedEvidence: 0,
    omittedRules: 0,
    reason,
    state: "unavailable",
    suppressedMatchCount: 0,
    suppressedMatches: EMPTY_MATCHES,
  });
}

function durationMicros(started: number, finished: number): number {
  const elapsed = (finished - started) * 1_000;
  if (!Number.isFinite(elapsed) || elapsed <= 0) {
    return 0;
  }
  return Math.min(KAFKA_LIVE_RULE_LIMITS.durationMicros, Math.round(elapsed));
}

function evidenceBytes(
  activeMatches: readonly KafkaLiveRuleMatch[],
  suppressedMatches: readonly KafkaLiveRuleMatch[],
  errors: readonly KafkaLiveRuleError[],
): number {
  return kafkaLiveRuleEvidenceBytes({
    activeMatchCount: activeMatches.length,
    activeMatches,
    durationMicros: 0,
    errorCount: errors.length,
    errors,
    evaluatedRules: activeMatches.length + suppressedMatches.length + errors.length,
    omittedEvidence: 0,
    omittedRules: 0,
    state: "evaluated",
    suppressedMatchCount: suppressedMatches.length,
    suppressedMatches,
  });
}

function frozenMatch(rule: PreparedRule): KafkaLiveRuleMatch {
  return Object.freeze({ level: rule.level, name: rule.name });
}

function frozenError(rule: PreparedRule, diagnostic: string): KafkaLiveRuleError {
  return Object.freeze({ diagnostic, name: rule.name });
}

export class KafkaLiveRuleRuntime {
  private activeTopic: string | undefined;
  private compiledByIdentity = new Map<string, CompiledRule>();
  private currentCapability = frozenCapability({
    applicableRules: 0,
    omittedRules: 0,
    state: "idle",
  });
  private lastFiredByIdentity = new Map<string, number>();
  private preparationGeneration = 0;
  private preparedRules: readonly PreparedRule[] = Object.freeze([]);

  constructor(
    private readonly rules: KafkaRuleService,
    private readonly evaluator: KafkaRuleEvaluator,
    private readonly options: KafkaLiveRuleRuntimeOptions = {
      durationNow: defaultPerformanceNow,
      monotonicNow: defaultPerformanceNow,
    },
  ) {}

  capability(): KafkaLiveRuleCapability {
    return this.currentCapability;
  }

  deactivate(): KafkaLiveRuleCapability {
    this.preparationGeneration += 1;
    this.activeTopic = undefined;
    this.preparedRules = Object.freeze([]);
    this.currentCapability = frozenCapability({
      applicableRules: 0,
      omittedRules: 0,
      state: "idle",
    });
    return this.currentCapability;
  }

  evaluate(message: KafkaMessage): KafkaLiveRuleEvaluation {
    if (this.currentCapability.state === "unavailable") {
      return unavailableEvaluation("catalog-unavailable");
    }
    if (
      this.currentCapability.state === "idle" ||
      this.activeTopic === undefined ||
      message.topic !== this.activeTopic
    ) {
      return unavailableEvaluation("internal");
    }
    if (this.preparedRules.length === 0) {
      return EMPTY_EVALUATION;
    }
    if (message.payload === null) {
      return unavailableEvaluation(
        message.originalByteSize > KAFKA_MESSAGE_LIMITS.messageBytes
          ? "payload-truncated"
          : "payload-null",
      );
    }

    try {
      return this.evaluatePayload(message.payload);
    } catch {
      return unavailableEvaluation("internal");
    }
  }

  async prepare(topic: string, signal?: AbortSignal): Promise<KafkaLiveRuleCapability> {
    const generation = ++this.preparationGeneration;
    let snapshot: KafkaRuleSnapshot;
    try {
      snapshot = await this.rules.list(signal);
      signal?.throwIfAborted();
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      if (generation !== this.preparationGeneration) {
        throw supersededPreparationError();
      }
      this.activeTopic = topic;
      this.preparedRules = Object.freeze([]);
      const store = this.rules.currentSnapshot().store;
      this.currentCapability = unavailableCapability(store.recovery);
      return this.currentCapability;
    }
    if (generation !== this.preparationGeneration) {
      throw supersededPreparationError();
    }

    this.activeTopic = topic;
    return this.synchronize(snapshot);
  }

  synchronize(snapshot: KafkaRuleSnapshot): KafkaLiveRuleCapability {
    if (snapshot.store.state === "unavailable") {
      this.preparedRules = Object.freeze([]);
      this.currentCapability = unavailableCapability(snapshot.store.recovery);
      return this.currentCapability;
    }

    const currentIdentities = new Set(snapshot.rules.map(semanticIdentity));
    const nextCompiled = new Map(
      [...this.compiledByIdentity].filter(([identity]) => currentIdentities.has(identity)),
    );
    const nextLastFired = new Map(
      [...this.lastFiredByIdentity].filter(([identity]) => currentIdentities.has(identity)),
    );

    if (this.activeTopic === undefined) {
      this.compiledByIdentity = nextCompiled;
      this.lastFiredByIdentity = nextLastFired;
      this.preparedRules = Object.freeze([]);
      this.currentCapability = frozenCapability({
        applicableRules: 0,
        omittedRules: 0,
        state: "idle",
      });
      return this.currentCapability;
    }

    const applicable = snapshot.rules.filter(
      (rule) => rule.enabled && (rule.topic === undefined || rule.topic === this.activeTopic),
    );
    const selected = applicable.slice(0, KAFKA_LIVE_RULE_LIMITS.applicableRules);
    const prepared = selected.map((rule): PreparedRule => {
      const identity = semanticIdentity(rule);
      let compiled = nextCompiled.get(identity);
      if (compiled === undefined) {
        try {
          compiled = Object.freeze({
            predicate: this.evaluator.compile(rule.expression),
            state: "ready",
          });
        } catch {
          compiled = Object.freeze({ state: "failed" });
        }
        nextCompiled.set(identity, compiled);
      }
      return Object.freeze({
        compiled,
        cooldownMs: rule.cooldownMs,
        identity,
        level: rule.level,
        name: rule.name,
      });
    });
    const omittedRules = applicable.length - selected.length;

    this.compiledByIdentity = nextCompiled;
    this.lastFiredByIdentity = nextLastFired;
    this.preparedRules = Object.freeze(prepared);
    this.currentCapability = frozenCapability({
      applicableRules: applicable.length,
      omittedRules,
      state: omittedRules === 0 ? "ready" : "partial",
    });
    return this.currentCapability;
  }

  private evaluatePayload(payload: string): KafkaLiveRuleEvaluation {
    const started = this.options.durationNow();
    let sample: unknown;
    try {
      sample = this.evaluator.parseSample(payload, LIVE_SAMPLE_LIMITS);
    } catch (error) {
      return unavailableEvaluation(
        error instanceof KafkaRuleSampleParseError && error.reason === "limit-exceeded"
          ? "payload-limit-exceeded"
          : "payload-malformed",
      );
    }

    const activeMatches: KafkaLiveRuleMatch[] = [];
    const suppressedMatches: KafkaLiveRuleMatch[] = [];
    const errors: KafkaLiveRuleError[] = [];
    let activeMatchCount = 0;
    let highestActiveSeverity: KafkaRuleDefinition["level"] | null = null;
    let suppressedMatchCount = 0;
    let errorCount = 0;
    let omittedEvidence = 0;
    const monotonicNow = this.options.monotonicNow();

    const retain = (
      active: KafkaLiveRuleMatch | undefined,
      suppressed: KafkaLiveRuleMatch | undefined,
      error: KafkaLiveRuleError | undefined,
    ): void => {
      const nextActive = active === undefined ? activeMatches : [...activeMatches, active];
      const nextSuppressed =
        suppressed === undefined ? suppressedMatches : [...suppressedMatches, suppressed];
      const nextErrors = error === undefined ? errors : [...errors, error];
      const entryCount = nextActive.length + nextSuppressed.length + nextErrors.length;
      if (
        entryCount > KAFKA_LIVE_RULE_LIMITS.entries ||
        evidenceBytes(nextActive, nextSuppressed, nextErrors) > KAFKA_LIVE_RULE_LIMITS.evidenceBytes
      ) {
        omittedEvidence += 1;
        return;
      }
      if (active !== undefined) {
        activeMatches.push(active);
      } else if (suppressed !== undefined) {
        suppressedMatches.push(suppressed);
      } else if (error !== undefined) {
        errors.push(error);
      }
    };

    for (const rule of this.preparedRules) {
      if (rule.compiled.state === "failed" || rule.compiled.predicate === undefined) {
        errorCount += 1;
        retain(undefined, undefined, frozenError(rule, COMPILE_FAILURE_DIAGNOSTIC));
        continue;
      }

      let matched: boolean;
      try {
        matched = rule.compiled.predicate.evaluate(sample);
      } catch {
        errorCount += 1;
        retain(undefined, undefined, frozenError(rule, EVALUATION_FAILURE_DIAGNOSTIC));
        continue;
      }
      if (!matched) {
        continue;
      }

      const lastFired = this.lastFiredByIdentity.get(rule.identity);
      if (
        rule.cooldownMs > 0 &&
        lastFired !== undefined &&
        monotonicNow - lastFired < rule.cooldownMs
      ) {
        suppressedMatchCount += 1;
        retain(undefined, frozenMatch(rule), undefined);
        continue;
      }

      activeMatchCount += 1;
      highestActiveSeverity = maximumKafkaRuleSeverity(
        highestActiveSeverity === null ? [rule.level] : [highestActiveSeverity, rule.level],
      );
      this.lastFiredByIdentity.set(rule.identity, monotonicNow);
      retain(frozenMatch(rule), undefined, undefined);
    }

    const omittedRules = this.currentCapability.omittedRules;
    const isPartial = errorCount > 0 || omittedEvidence > 0 || omittedRules > 0;
    return Object.freeze({
      activeMatchCount,
      activeMatches: Object.freeze(activeMatches),
      durationMicros: durationMicros(started, this.options.durationNow()),
      errorCount,
      errors: Object.freeze(errors),
      evaluatedRules: this.preparedRules.length,
      ...(highestActiveSeverity === null ? {} : { highestActiveSeverity }),
      omittedEvidence,
      omittedRules,
      state: isPartial ? "partial" : "evaluated",
      suppressedMatchCount,
      suppressedMatches: Object.freeze(suppressedMatches),
    });
  }
}
