import { describe, expect, it } from "vitest";

import {
  KAFKA_RULE_LIMITS,
  type KafkaRuleDefinition,
  type KafkaRuleStoreCapability,
} from "../../src/kafka/contracts";
import {
  DuplicateKafkaRuleError,
  InMemoryKafkaRuleStore,
  KafkaRuleCapacityError,
  KafkaRuleCatalogNotLoadedError,
  KafkaRuleNotFoundError,
  KafkaRuleService,
  KafkaRuleStoreUnavailableError,
  KafkaRuleValidationError,
  type KafkaRuleDocument,
  type KafkaRuleStore,
} from "../../src/kafka/application";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/kafka/engine";

const sessionCapability: KafkaRuleStoreCapability = {
  durability: "session",
  state: "ready",
};

const highPriority: KafkaRuleDefinition = {
  cooldownMs: 5_000,
  description: "Detect critical orders.",
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
};

interface Deferred<Value> {
  readonly promise: Promise<Value>;
  reject(reason: Error): void;
  resolve(value: Value): void;
}

function deferred<Value>(): Deferred<Value> {
  let resolvePromise: ((value: Value) => void) | undefined;
  let rejectPromise: ((reason: Error) => void) | undefined;
  const promise = new Promise<Value>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    reject: (reason): void => {
      rejectPromise?.(reason);
    },
    resolve: (value): void => {
      resolvePromise?.(value);
    },
  };
}

class ControlledRuleStore implements KafkaRuleStore {
  readonly commitStarted = deferred<void>();
  readonly commits: KafkaRuleDocument[] = [];
  nextCommit: Promise<void> | undefined;

  constructor(private current: KafkaRuleDocument | undefined) {}

  capability(): KafkaRuleStoreCapability {
    return sessionCapability;
  }

  async commit(document: KafkaRuleDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.commits.push(structuredClone(document));
    this.commitStarted.resolve();
    await this.nextCommit;
    signal?.throwIfAborted();
    this.current = structuredClone(document);
  }

  load(): Promise<KafkaRuleDocument | undefined> {
    return Promise.resolve(this.current === undefined ? undefined : structuredClone(this.current));
  }
}

class AbortOnceLoadStore implements KafkaRuleStore {
  loadCount = 0;

  capability(): KafkaRuleStoreCapability {
    return sessionCapability;
  }

  commit(): Promise<void> {
    return Promise.resolve();
  }

  load(): Promise<KafkaRuleDocument | undefined> {
    this.loadCount += 1;
    if (this.loadCount === 1) {
      return Promise.reject(new DOMException("Cancelled", "AbortError"));
    }
    return Promise.resolve({ rules: [highPriority] });
  }
}

class CountingLoadStore implements KafkaRuleStore {
  loadCount = 0;

  capability(): KafkaRuleStoreCapability {
    return sessionCapability;
  }

  commit(): Promise<void> {
    return Promise.resolve();
  }

  load(): Promise<KafkaRuleDocument | undefined> {
    this.loadCount += 1;
    return Promise.resolve({ rules: [highPriority] });
  }
}

function service(
  store: KafkaRuleStore = new InMemoryKafkaRuleStore(sessionCapability),
): KafkaRuleService {
  return new KafkaRuleService(store, new StreamSkopeKafkaRuleEvaluator());
}

describe("Kafka rule application", () => {
  it("loads a missing document as an explicitly session-scoped empty catalog", async () => {
    await expect(service().list()).resolves.toEqual({
      rules: [],
      store: sessionCapability,
    });
  });

  it("creates a canonical rule at the end and commits once", async () => {
    const store = new InMemoryKafkaRuleStore(sessionCapability);
    const rules = service(store);

    const snapshot = await rules.create({
      ...highPriority,
      description: "  Detect critical orders.  ",
      name: "  High priority  ",
      topic: "  orders  ",
    });

    expect(snapshot.rules).toEqual([highPriority]);
    expect(store.commitCount).toBe(1);
  });

  it("rejects canonical duplicates and invalid expressions without committing", async () => {
    const store = new InMemoryKafkaRuleStore(sessionCapability, { rules: [highPriority] });
    const rules = service(store);

    await expect(rules.create({ ...highPriority, name: " HIGH PRIORITY " })).rejects.toBeInstanceOf(
      DuplicateKafkaRuleError,
    );
    await expect(
      rules.create({ ...highPriority, expression: "$.priority ==", name: "Broken" }),
    ).rejects.toBeInstanceOf(KafkaRuleValidationError);
    expect(store.commitCount).toBe(0);
    expect(rules.currentSnapshot().rules).toEqual([highPriority]);
  });

  it("reports every invalid field and enforces the bounded catalog", async () => {
    const invalid = {
      cooldownMs: -1,
      description: "x".repeat(KAFKA_RULE_LIMITS.descriptionCharacters + 1),
      enabled: "yes",
      expression: " ",
      level: "critical",
      name: " ",
      topic: "x".repeat(KAFKA_RULE_LIMITS.topicCharacters + 1),
    } as unknown as KafkaRuleDefinition;
    const invalidStore = new InMemoryKafkaRuleStore(sessionCapability);
    const invalidService = service(invalidStore);

    let failure: unknown;
    try {
      await invalidService.create(invalid);
    } catch (error: unknown) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(KafkaRuleValidationError);
    if (!(failure instanceof KafkaRuleValidationError)) {
      throw new Error("Expected KafkaRuleValidationError.");
    }
    expect(failure.issues.map((candidate) => candidate.field)).toEqual(
      expect.arrayContaining([
        "cooldownMs",
        "description",
        "enabled",
        "expression",
        "level",
        "name",
        "topic",
      ]),
    );
    expect(invalidStore.commitCount).toBe(0);

    const fullCatalog = Array.from({ length: KAFKA_RULE_LIMITS.rules }, (_value, index) => ({
      ...highPriority,
      name: `Rule ${String(index + 1)}`,
    }));
    const fullStore = new InMemoryKafkaRuleStore(sessionCapability, {
      rules: fullCatalog,
    });
    const fullService = service(fullStore);

    await expect(
      fullService.create({ ...highPriority, name: "One too many" }),
    ).rejects.toBeInstanceOf(KafkaRuleCapacityError);
    expect(fullStore.commitCount).toBe(0);
    expect(fullService.currentSnapshot().rules).toHaveLength(KAFKA_RULE_LIMITS.rules);
  });

  it("renames in place, changes enablement and deletes only the exact rule", async () => {
    const second: KafkaRuleDefinition = {
      cooldownMs: highPriority.cooldownMs,
      ...(highPriority.description === undefined ? {} : { description: highPriority.description }),
      enabled: highPriority.enabled,
      expression: highPriority.expression,
      level: highPriority.level,
      name: "Second",
    };
    const store = new InMemoryKafkaRuleStore(sessionCapability, {
      rules: [highPriority, second],
    });
    const rules = service(store);

    const renamed = await rules.update("High priority", {
      ...highPriority,
      enabled: false,
      name: "Important orders",
    });
    expect(renamed.rules.map((rule) => [rule.name, rule.enabled])).toEqual([
      ["Important orders", false],
      ["Second", true],
    ]);

    const deleted = await rules.delete("Second");
    expect(deleted.rules.map((rule) => rule.name)).toEqual(["Important orders"]);
    await expect(rules.delete("Second")).rejects.toBeInstanceOf(KafkaRuleNotFoundError);
    expect(rules.currentSnapshot()).toEqual(deleted);
  });

  it("serializes overlapping mutations against the last committed catalog", async () => {
    const gate = deferred<void>();
    const store = new ControlledRuleStore({ rules: [] });
    store.nextCommit = gate.promise;
    const rules = service(store);

    const first = rules.create(highPriority);
    const second = rules.create({ ...highPriority, name: "Second" });
    await store.commitStarted.promise;
    expect(store.commits).toHaveLength(1);

    gate.resolve();
    await first;
    await second;

    expect(rules.currentSnapshot().rules.map((rule) => rule.name)).toEqual([
      "High priority",
      "Second",
    ]);
    expect(store.commits).toHaveLength(2);
  });

  it("retains the prior catalog when commit fails or is cancelled", async () => {
    const failure = deferred<void>();
    const failureStore = new ControlledRuleStore({ rules: [highPriority] });
    failureStore.nextCommit = failure.promise;
    const failureService = service(failureStore);
    await failureService.list();

    const creation = failureService.create({ ...highPriority, name: "Not committed" });
    await failureStore.commitStarted.promise;
    failure.reject(new Error("disk full"));
    await expect(creation).rejects.toBeInstanceOf(KafkaRuleStoreUnavailableError);
    expect(failureService.currentSnapshot().rules).toEqual([highPriority]);

    const cancellation = deferred<void>();
    const cancellationStore = new ControlledRuleStore({ rules: [highPriority] });
    cancellationStore.nextCommit = cancellation.promise;
    const cancellationService = service(cancellationStore);
    await cancellationService.list();
    const controller = new AbortController();
    const deletion = cancellationService.delete("High priority", controller.signal);
    await cancellationStore.commitStarted.promise;
    controller.abort();
    cancellation.resolve();
    await expect(deletion).rejects.toMatchObject({ name: "AbortError" });
    expect(cancellationService.currentSnapshot().rules).toEqual([highPriority]);
  });

  it("validates a draft without saving it", () => {
    const store = new InMemoryKafkaRuleStore(sessionCapability);
    const rules = service(store);

    expect(rules.validate(highPriority)).toEqual({
      name: "High priority",
      outcome: "valid",
    });
    const invalid = rules.validate({ ...highPriority, expression: "$.priority ==" });
    expect(invalid).toMatchObject({
      name: "High priority",
      outcome: "invalid",
    });
    expect(invalid.diagnostic).toContain("Position");
    expect(store.commitCount).toBe(0);
  });

  it("evaluates one draft without Kafka or catalog mutation", () => {
    const store = new InMemoryKafkaRuleStore(sessionCapability);
    const rules = service(store);

    expect(
      rules.evaluate({
        rule: highPriority,
        sample: '{"priority":"high"}',
        scope: "single",
        topic: "orders",
      }),
    ).toEqual([{ name: "High priority", outcome: "matched" }]);
    expect(rules.currentSnapshot().rules).toEqual([]);
    expect(store.commitCount).toBe(0);
  });

  it("reports catalog results in order with disabled and topic-mismatch skips", async () => {
    const store = new InMemoryKafkaRuleStore(sessionCapability, {
      rules: [
        highPriority,
        { ...highPriority, enabled: false, name: "Disabled" },
        { ...highPriority, name: "Other topic", topic: "payments" },
        { ...highPriority, expression: '$.priority == "low"', name: "No match" },
      ],
    });
    const rules = service(store);
    await rules.list();

    expect(
      rules.evaluate({
        sample: '{"priority":"high"}',
        scope: "catalog",
        topic: "orders",
      }),
    ).toEqual([
      { name: "High priority", outcome: "matched" },
      {
        diagnostic: "Rule is disabled.",
        name: "Disabled",
        outcome: "skipped",
        reason: "disabled",
      },
      {
        diagnostic: "Rule applies to topic payments.",
        name: "Other topic",
        outcome: "skipped",
        reason: "topic-mismatch",
      },
      { name: "No match", outcome: "not-matched" },
    ]);
  });

  it("rejects catalog evaluation before load without reading storage", async () => {
    const store = new CountingLoadStore();
    const rules = service(store);

    await expect(
      Promise.resolve().then(() =>
        rules.evaluate({
          sample: '{"priority":"high"}',
          scope: "catalog",
          topic: "orders",
        }),
      ),
    ).rejects.toBeInstanceOf(KafkaRuleCatalogNotLoadedError);
    expect(store.loadCount).toBe(0);
  });

  it("allows a clean load retry after the initial load is cancelled", async () => {
    const store = new AbortOnceLoadStore();
    const rules = service(store);

    await expect(rules.list()).rejects.toMatchObject({ name: "AbortError" });
    await expect(rules.list()).resolves.toEqual({
      rules: [highPriority],
      store: sessionCapability,
    });
    expect(store.loadCount).toBe(2);
  });

  it("rejects malformed samples and fails closed on an invalid loaded document", async () => {
    const rules = service(
      new InMemoryKafkaRuleStore(sessionCapability, {
        rules: [highPriority, { ...highPriority, name: " high priority " }],
      }),
    );

    await expect(rules.list()).rejects.toBeInstanceOf(KafkaRuleStoreUnavailableError);
    expect(rules.currentSnapshot()).toEqual({
      rules: [],
      store: {
        durability: "session",
        recovery:
          "Preserve the rule data, correct it outside the running application, then restart StreamSkope.",
        state: "unavailable",
      },
    });

    expect(() => service().evaluate({ sample: "{", scope: "catalog" })).toThrow(
      KafkaRuleValidationError,
    );
  });
});
