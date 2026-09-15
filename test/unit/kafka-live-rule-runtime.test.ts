import { describe, expect, it } from "vitest";

import {
  KAFKA_LIVE_RULE_LIMITS,
  kafkaMessageRetainedBytes,
  type KafkaLiveRuleEvaluation,
  type KafkaMessage,
  type KafkaRuleDefinition,
  type KafkaRuleStoreCapability,
} from "../../src/kafka/contracts";
import {
  InMemoryKafkaRuleStore,
  KafkaLiveRuleRuntime,
  KafkaRuleService,
  type KafkaRuleDocument,
  type KafkaRuleEvaluator,
  type KafkaRulePredicate,
  type KafkaRuleStore,
} from "../../src/kafka/application";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/kafka/engine";

const sessionCapability: KafkaRuleStoreCapability = {
  durability: "session",
  state: "ready",
};

function rule(
  name: string,
  expression = name,
  overrides: Partial<KafkaRuleDefinition> = {},
): KafkaRuleDefinition {
  return {
    cooldownMs: 0,
    enabled: true,
    expression,
    level: "info",
    name,
    ...overrides,
  };
}

function message(
  payload: string | null = '{"priority":"high"}',
  overrides: Partial<KafkaMessage> = {},
): KafkaMessage {
  return {
    headers: {},
    id: "orders:0:1",
    key: null,
    offset: "1",
    originalByteSize: payload?.length ?? 0,
    partition: 0,
    payload,
    preview: payload ?? "",
    timestamp: "2026-07-25T20:00:00.000Z",
    topic: "orders",
    truncated: false,
    ...overrides,
  };
}

type PredicateBehavior = (sample: unknown) => boolean;

class FakeEvaluator implements KafkaRuleEvaluator {
  readonly compileExpressions: string[] = [];
  parseCount = 0;

  constructor(private readonly behaviors: ReadonlyMap<string, PredicateBehavior> = new Map()) {}

  compile(expression: string): KafkaRulePredicate {
    this.compileExpressions.push(expression);
    if (expression === "compile-failure") {
      throw new Error("compile sentinel must be bounded");
    }
    const behavior = this.behaviors.get(expression) ?? ((): boolean => false);
    return {
      evaluate(sample: unknown): boolean {
        return behavior(sample);
      },
    };
  }

  evaluate(expression: string, sample: unknown): boolean {
    return this.compile(expression).evaluate(sample);
  }

  parseSample(sample: string): unknown {
    this.parseCount += 1;
    return JSON.parse(sample) as unknown;
  }

  validate(expression: string): { readonly diagnostic?: string; readonly valid: boolean } {
    return expression.length === 0
      ? { diagnostic: "Expression is required.", valid: false }
      : { valid: true };
  }
}

class UnavailableRuleStore implements KafkaRuleStore {
  capability(): KafkaRuleStoreCapability {
    return {
      durability: "durable",
      recovery: "Restore a known-good rule document and restart StreamSkope.",
      state: "unavailable",
    };
  }

  commit(): Promise<void> {
    return Promise.reject(new Error("unavailable"));
  }

  load(): Promise<KafkaRuleDocument | undefined> {
    return Promise.reject(new Error("unavailable"));
  }
}

class DeferredFailureRuleStore implements KafkaRuleStore {
  private rejectLoad: ((error: Error) => void) | undefined;
  private readonly pendingLoad = new Promise<KafkaRuleDocument | undefined>((_resolve, reject) => {
    this.rejectLoad = reject;
  });

  capability(): KafkaRuleStoreCapability {
    return sessionCapability;
  }

  commit(): Promise<void> {
    return Promise.reject(new Error("commit was not expected"));
  }

  failLoad(): void {
    this.rejectLoad?.(new Error("catalog load failed"));
  }

  load(): Promise<KafkaRuleDocument | undefined> {
    return this.pendingLoad;
  }
}

interface RuntimeFixture {
  readonly evaluator: FakeEvaluator;
  readonly rules: KafkaRuleService;
  readonly runtime: KafkaLiveRuleRuntime;
  readonly setMonotonic: (value: number) => void;
}

function fixture(
  definitions: readonly KafkaRuleDefinition[] = [],
  behaviors: ReadonlyMap<string, PredicateBehavior> = new Map(),
  options: {
    readonly durationNow?: () => number;
    readonly store?: KafkaRuleStore;
  } = {},
): RuntimeFixture {
  let monotonic = 0;
  const evaluator = new FakeEvaluator(behaviors);
  const rules = new KafkaRuleService(
    options.store ??
      new InMemoryKafkaRuleStore(sessionCapability, {
        rules: definitions,
      }),
    evaluator,
  );
  return {
    evaluator,
    rules,
    runtime: new KafkaLiveRuleRuntime(rules, evaluator, {
      durationNow: options.durationNow ?? ((): number => 0),
      monotonicNow: (): number => monotonic,
    }),
    setMonotonic: (value): void => {
      monotonic = value;
    },
  };
}

function evidenceBytes(result: KafkaLiveRuleEvaluation): number {
  const raw = message();
  const explored = { ...raw, ruleEvaluation: result };
  return kafkaMessageRetainedBytes(explored) - kafkaMessageRetainedBytes(raw);
}

describe("Kafka live rule runtime", () => {
  it("does not parse payloads when no enabled rule applies to the topic", async () => {
    const { runtime, evaluator } = fixture([
      rule("disabled", "true", { enabled: false }),
      rule("other-topic", "true", { topic: "elsewhere" }),
    ]);
    await runtime.prepare("orders");
    for (const payload of ["plain text", "{malformed", null]) {
      expect(runtime.evaluate(message(payload))).toMatchObject({
        state: "evaluated",
        evaluatedRules: 0,
        errorCount: 0,
        activeMatchCount: 0,
      });
    }
    expect(evaluator.parseCount).toBe(0);
  });
  it("prepares enabled topic-applicable rules in catalog order and reports capacity", async () => {
    const definitions = [
      rule("Disabled", "disabled", { enabled: false }),
      rule("Payments", "payments", { topic: "payments" }),
      ...Array.from({ length: KAFKA_LIVE_RULE_LIMITS.applicableRules + 3 }, (_value, index) =>
        rule(`Rule ${String(index + 1)}`, `expression-${String(index + 1)}`, {
          ...(index % 2 === 0 ? { topic: "orders" } : {}),
        }),
      ),
    ];
    const executionOrder: string[] = [];
    const behaviors = new Map(
      definitions.map((definition) => [
        definition.expression,
        (): boolean => {
          executionOrder.push(definition.name);
          return false;
        },
      ]),
    );
    const { evaluator, runtime } = fixture(definitions, behaviors);

    await expect(runtime.prepare("orders")).resolves.toEqual({
      applicableRules: KAFKA_LIVE_RULE_LIMITS.applicableRules + 3,
      omittedRules: 3,
      state: "partial",
    });
    const result = runtime.evaluate(message());

    expect(result).toMatchObject({
      evaluatedRules: KAFKA_LIVE_RULE_LIMITS.applicableRules,
      omittedRules: 3,
      state: "partial",
    });
    expect(executionOrder).toEqual(
      definitions
        .filter(
          (definition) =>
            definition.enabled && (definition.topic === undefined || definition.topic === "orders"),
        )
        .slice(0, KAFKA_LIVE_RULE_LIMITS.applicableRules)
        .map((definition) => definition.name),
    );
    expect(evaluator.compileExpressions).toHaveLength(KAFKA_LIVE_RULE_LIMITS.applicableRules);
    expect(evaluator.parseCount).toBe(1);

    runtime.evaluate(message('{"priority":"low"}', { id: "orders:0:2", offset: "2" }));
    expect(evaluator.compileExpressions).toHaveLength(KAFKA_LIVE_RULE_LIMITS.applicableRules);
    expect(evaluator.parseCount).toBe(2);
  });

  it("treats an absent document as a ready empty catalog", async () => {
    const evaluator = new FakeEvaluator();
    const rules = new KafkaRuleService(new InMemoryKafkaRuleStore(sessionCapability), evaluator);
    const runtime = new KafkaLiveRuleRuntime(rules, evaluator, {
      durationNow: (): number => 0,
      monotonicNow: (): number => 0,
    });

    await expect(runtime.prepare("orders")).resolves.toEqual({
      applicableRules: 0,
      omittedRules: 0,
      state: "ready",
    });
    expect(runtime.evaluate(message())).toEqual({
      activeMatchCount: 0,
      activeMatches: [],
      durationMicros: 0,
      errorCount: 0,
      errors: [],
      evaluatedRules: 0,
      omittedEvidence: 0,
      omittedRules: 0,
      state: "evaluated",
      suppressedMatchCount: 0,
      suppressedMatches: [],
    });
  });

  it("reports active and suppressed matches without advancing the cooldown on suppression", async () => {
    const { runtime, setMonotonic } = fixture(
      [
        rule("Cooldown", "match-cooldown", { cooldownMs: 1_000, level: "warn" }),
        rule("Always", "match-always", { cooldownMs: 0, level: "error" }),
      ],
      new Map([
        ["match-cooldown", (): boolean => true],
        ["match-always", (): boolean => true],
      ]),
    );
    await runtime.prepare("orders");

    setMonotonic(10_000);
    expect(runtime.evaluate(message())).toMatchObject({
      activeMatchCount: 2,
      activeMatches: [
        { level: "warn", name: "Cooldown" },
        { level: "error", name: "Always" },
      ],
      highestActiveSeverity: "error",
      state: "evaluated",
      suppressedMatchCount: 0,
    });

    setMonotonic(10_500);
    expect(runtime.evaluate(message(undefined, { id: "orders:0:2", offset: "2" }))).toMatchObject({
      activeMatchCount: 1,
      activeMatches: [{ level: "error", name: "Always" }],
      suppressedMatchCount: 1,
      suppressedMatches: [{ level: "warn", name: "Cooldown" }],
    });

    setMonotonic(10_999);
    expect(runtime.evaluate(message(undefined, { id: "orders:0:3", offset: "3" }))).toMatchObject({
      activeMatchCount: 1,
      suppressedMatchCount: 1,
    });

    setMonotonic(11_000);
    expect(runtime.evaluate(message(undefined, { id: "orders:0:4", offset: "4" }))).toMatchObject({
      activeMatchCount: 2,
      suppressedMatchCount: 0,
    });
  });

  it("does not start cooldown on a non-match or evaluation error", async () => {
    let matchCalls = 0;
    let errorCalls = 0;
    const { runtime, setMonotonic } = fixture(
      [rule("Eventually", "eventually", { cooldownMs: 1_000 }), rule("Recovers", "recovers")],
      new Map([
        [
          "eventually",
          (): boolean => {
            matchCalls += 1;
            return matchCalls > 1;
          },
        ],
        [
          "recovers",
          (): boolean => {
            errorCalls += 1;
            if (errorCalls === 1) {
              throw new Error("first evaluation failed");
            }
            return true;
          },
        ],
      ]),
    );
    await runtime.prepare("orders");

    setMonotonic(2_000);
    expect(runtime.evaluate(message())).toMatchObject({
      activeMatchCount: 0,
      errorCount: 1,
      state: "partial",
    });
    setMonotonic(2_100);
    expect(runtime.evaluate(message(undefined, { id: "orders:0:2", offset: "2" }))).toMatchObject({
      activeMatchCount: 2,
      errorCount: 0,
      suppressedMatchCount: 0,
    });
  });

  it("uses monotonic time independently of duration and wall-clock values", async () => {
    const durationSamples = [8_000, 8_000.25, 1_000, 1_000.5];
    const { runtime, setMonotonic } = fixture(
      [rule("Timed", "match", { cooldownMs: 100 })],
      new Map([["match", (): boolean => true]]),
      {
        durationNow: (): number => durationSamples.shift() ?? 0,
      },
    );
    await runtime.prepare("orders");

    setMonotonic(500);
    expect(runtime.evaluate(message())).toMatchObject({
      activeMatchCount: 1,
      durationMicros: 250,
    });
    setMonotonic(550);
    expect(runtime.evaluate(message(undefined, { id: "orders:0:2", offset: "2" }))).toMatchObject({
      activeMatchCount: 0,
      durationMicros: 500,
      suppressedMatchCount: 1,
    });
  });

  it("preserves cooldown for presentation-only edits and resets it for semantic edits", async () => {
    const { rules, runtime, setMonotonic } = fixture(
      [rule("Stable", "first", { cooldownMs: 1_000, description: "Before", level: "info" })],
      new Map([
        ["first", (): boolean => true],
        ["second", (): boolean => true],
      ]),
    );
    await runtime.prepare("orders");
    setMonotonic(10_000);
    expect(runtime.evaluate(message()).activeMatchCount).toBe(1);

    const presentation = await rules.update(
      "Stable",
      rule("Stable", "first", {
        cooldownMs: 1_000,
        description: "After",
        level: "error",
      }),
    );
    runtime.synchronize(presentation);
    setMonotonic(10_100);
    expect(runtime.evaluate(message()).suppressedMatches).toEqual([
      { level: "error", name: "Stable" },
    ]);

    const semantic = await rules.update(
      "Stable",
      rule("Stable", "second", {
        cooldownMs: 1_000,
        description: "After",
        level: "error",
      }),
    );
    runtime.synchronize(semantic);
    setMonotonic(10_200);
    expect(runtime.evaluate(message()).activeMatches).toEqual([{ level: "error", name: "Stable" }]);
  });

  it.each([
    {
      label: "Kafka null payload",
      record: message(null),
      reason: "payload-null",
    },
    {
      label: "truncated payload",
      record: message(null, {
        originalByteSize: 1_048_577,
        preview: '{"priority":"h',
        truncated: true,
      }),
      reason: "payload-truncated",
    },
    {
      label: "malformed payload",
      record: message("{"),
      reason: "payload-malformed",
    },
  ] as const)(
    "keeps $label available without evaluating invented data",
    async ({ record, reason }) => {
      const { evaluator, runtime } = fixture(
        [rule("Would match", "match")],
        new Map([["match", (): boolean => true]]),
      );
      await runtime.prepare("orders");
      const before = structuredClone(record);

      expect(runtime.evaluate(record)).toEqual({
        activeMatchCount: 0,
        activeMatches: [],
        durationMicros: 0,
        errorCount: 0,
        errors: [],
        evaluatedRules: 0,
        omittedEvidence: 0,
        omittedRules: 0,
        reason,
        state: "unavailable",
        suppressedMatchCount: 0,
        suppressedMatches: [],
      });
      expect(record).toEqual(before);
      expect(evaluator.compileExpressions).toEqual(["match"]);
    },
  );

  it("distinguishes complete JSON beyond live byte or node limits from malformed payloads", async () => {
    const evaluator = new StreamSkopeKafkaRuleEvaluator();
    const rules = new KafkaRuleService(
      new InMemoryKafkaRuleStore(sessionCapability, {
        rules: [rule("Bounded", "$..missing exists")],
      }),
      evaluator,
    );
    const runtime = new KafkaLiveRuleRuntime(rules, evaluator, {
      durationNow: (): number => 0,
      monotonicNow: (): number => 0,
    });
    await runtime.prepare("orders");

    const maximumNodes = JSON.stringify(
      Array.from({ length: KAFKA_LIVE_RULE_LIMITS.sampleNodes - 1 }, () => null),
    );
    const excessiveNodes = JSON.stringify(
      Array.from({ length: KAFKA_LIVE_RULE_LIMITS.sampleNodes }, () => null),
    );
    const excessiveBytes = JSON.stringify("x".repeat(KAFKA_LIVE_RULE_LIMITS.payloadBytes));

    expect(runtime.evaluate(message(maximumNodes))).toMatchObject({
      evaluatedRules: 1,
      state: "evaluated",
    });
    for (const payload of [excessiveNodes, excessiveBytes]) {
      expect(runtime.evaluate(message(payload))).toMatchObject({
        evaluatedRules: 0,
        reason: "payload-limit-exceeded",
        state: "unavailable",
      });
    }
  });

  it("fails open when the catalog store is unavailable", async () => {
    const { runtime } = fixture([], new Map(), {
      store: new UnavailableRuleStore(),
    });

    await expect(runtime.prepare("orders")).resolves.toEqual({
      applicableRules: 0,
      omittedRules: 0,
      recovery: "Restore a known-good rule document and restart StreamSkope.",
      state: "unavailable",
    });
    expect(runtime.evaluate(message())).toMatchObject({
      evaluatedRules: 0,
      reason: "catalog-unavailable",
      state: "unavailable",
    });
  });

  it("reports a superseded failed preparation as cancellation while the current one degrades", async () => {
    const store = new DeferredFailureRuleStore();
    const evaluator = new FakeEvaluator();
    const rules = new KafkaRuleService(store, evaluator);
    const runtime = new KafkaLiveRuleRuntime(rules, evaluator, {
      durationNow: (): number => 0,
      monotonicNow: (): number => 0,
    });

    const obsolete = runtime.prepare("orders");
    const current = runtime.prepare("payments");
    const obsoleteResult = expect(obsolete).rejects.toMatchObject({ name: "AbortError" });
    const currentResult = expect(current).resolves.toMatchObject({
      state: "unavailable",
    });
    store.failLoad();

    await Promise.all([obsoleteResult, currentResult]);
    expect(runtime.evaluate(message(undefined, { topic: "payments" }))).toMatchObject({
      reason: "catalog-unavailable",
      state: "unavailable",
    });
  });

  it("isolates compile and predicate failures while evaluating remaining rules", async () => {
    const { runtime } = fixture(
      [
        rule("Compile broken", "compile-failure"),
        rule("Evaluate broken", "evaluate-failure"),
        rule("Good", "match", { level: "warn" }),
      ],
      new Map([
        [
          "evaluate-failure",
          (): never => {
            throw new Error("evaluation sentinel");
          },
        ],
        ["match", (): boolean => true],
      ]),
    );
    await runtime.prepare("orders");

    expect(runtime.evaluate(message())).toMatchObject({
      activeMatchCount: 1,
      activeMatches: [{ level: "warn", name: "Good" }],
      errorCount: 2,
      errors: [
        { diagnostic: "Rule compilation failed.", name: "Compile broken" },
        { diagnostic: "Rule evaluation failed.", name: "Evaluate broken" },
      ],
      evaluatedRules: 3,
      state: "partial",
    });
  });

  it("bounds hostile UTF-8 error evidence and reports omitted entries", async () => {
    const longDiagnostic = "😀".repeat(KAFKA_LIVE_RULE_LIMITS.diagnosticBytes);
    const definitions = Array.from(
      { length: KAFKA_LIVE_RULE_LIMITS.applicableRules },
      (_v, index) => rule(`${"😀".repeat(60)} ${String(index)}`, `failure-${String(index)}`),
    );
    const behaviors = new Map(
      definitions.map((definition) => [
        definition.expression,
        (): boolean => {
          throw new Error(longDiagnostic);
        },
      ]),
    );
    const { runtime } = fixture(definitions, behaviors);
    await runtime.prepare("orders");

    const result = runtime.evaluate(message());

    expect(result).toMatchObject({
      errorCount: KAFKA_LIVE_RULE_LIMITS.applicableRules,
      evaluatedRules: KAFKA_LIVE_RULE_LIMITS.applicableRules,
      state: "partial",
    });
    expect(result.errors.length).toBeLessThan(result.errorCount);
    expect(result.omittedEvidence).toBe(result.errorCount - result.errors.length);
    expect(evidenceBytes(result)).toBeLessThanOrEqual(KAFKA_LIVE_RULE_LIMITS.evidenceBytes);
    for (const error of result.errors) {
      expect(error.diagnostic).toBe("Rule evaluation failed.");
      expect(new TextEncoder().encode(error.diagnostic).byteLength).toBeLessThanOrEqual(
        KAFKA_LIVE_RULE_LIMITS.diagnosticBytes,
      );
    }
  });

  it("preserves highest active severity when its detailed match is omitted", async () => {
    const definitions = Array.from(
      { length: KAFKA_LIVE_RULE_LIMITS.applicableRules },
      (_value, index) =>
        rule(`${"😀".repeat(60)} ${String(index)}`, `match-${String(index)}`, {
          level: index === KAFKA_LIVE_RULE_LIMITS.applicableRules - 1 ? "error" : "info",
        }),
    );
    const { runtime } = fixture(
      definitions,
      new Map(definitions.map((definition) => [definition.expression, (): boolean => true])),
    );
    await runtime.prepare("orders");

    const result = runtime.evaluate(message());

    expect(result.activeMatchCount).toBe(KAFKA_LIVE_RULE_LIMITS.applicableRules);
    expect(result.activeMatches.length).toBeLessThan(result.activeMatchCount);
    expect(result.activeMatches).not.toContainEqual(expect.objectContaining({ level: "error" }));
    expect(result.highestActiveSeverity).toBe("error");
    expect(result.omittedEvidence).toBe(result.activeMatchCount - result.activeMatches.length);
  });

  it("applies confirmed catalog changes prospectively without mutating prior evidence", async () => {
    const { rules, runtime } = fixture(
      [rule("Initial", "first", { level: "info" })],
      new Map([
        ["first", (): boolean => true],
        ["second", (): boolean => true],
      ]),
    );
    await runtime.prepare("orders");
    const first = runtime.evaluate(message());
    const firstCopy = structuredClone(first);
    expect(Object.isFrozen(first)).toBe(true);
    expect(Object.isFrozen(first.activeMatches)).toBe(true);
    expect(Object.isFrozen(first.activeMatches[0])).toBe(true);

    const snapshot = await rules.update("Initial", rule("Updated", "second", { level: "error" }));
    expect(runtime.synchronize(snapshot)).toEqual({
      applicableRules: 1,
      omittedRules: 0,
      state: "ready",
    });
    const second = runtime.evaluate(message(undefined, { id: "orders:0:2", offset: "2" }));

    expect(first).toEqual(firstCopy);
    expect(first.activeMatches).toEqual([{ level: "info", name: "Initial" }]);
    expect(second.activeMatches).toEqual([{ level: "error", name: "Updated" }]);
  });

  it("returns to an explicit idle state when deactivated", async () => {
    const { runtime } = fixture([rule("Rule", "match")], new Map([["match", (): boolean => true]]));
    await runtime.prepare("orders");

    expect(runtime.deactivate()).toEqual({
      applicableRules: 0,
      omittedRules: 0,
      state: "idle",
    });
    expect(runtime.capability()).toEqual({
      applicableRules: 0,
      omittedRules: 0,
      state: "idle",
    });
    expect(runtime.evaluate(message())).toMatchObject({
      reason: "internal",
      state: "unavailable",
    });
  });
});
