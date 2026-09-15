import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaRuleDefinition,
  type KafkaRuleStoreCapability,
} from "../../src/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  InMemoryKafkaRuleStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaRuleDocument,
  type KafkaRuleStore,
} from "../../src/kafka/application";
import { KafkaBackendFacade } from "../../src/kafka/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/kafka/engine";

const capability: KafkaRuleStoreCapability = {
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

class UnusedConnectionPort implements KafkaConnectionPort {
  openConnection(): Promise<KafkaActiveConnection> {
    return Promise.reject(new Error("Kafka connection was not expected in rule facade tests."));
  }

  testConnection(): Promise<KafkaConnectionTestResult> {
    return Promise.reject(new Error("Kafka connection test was not expected."));
  }
}

function ruleCommand(
  command:
    | "rules.create"
    | "rules.delete"
    | "rules.evaluate"
    | "rules.list"
    | "rules.update"
    | "rules.validate",
  payload: HostCommand["payload"],
  id: string = command,
): HostCommand {
  return {
    command,
    id,
    payload,
    version: HOST_PROTOCOL_VERSION,
  } as HostCommand;
}

function setup(
  store: KafkaRuleStore = new InMemoryKafkaRuleStore(capability, {
    rules: [highPriority],
  }),
): {
  readonly events: HostEvent[];
  readonly facade: KafkaBackendFacade;
  readonly store: KafkaRuleStore;
} {
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore({
      durability: "session",
      protection: "memory",
      state: "ready",
    }),
    {
      decode: (): Promise<never> => Promise.reject(new Error("Profile decoding was not expected.")),
    },
  );
  const templates = new KafkaConnectionTemplateService(
    new InMemoryKafkaConnectionTemplateStore({
      durability: "session",
      state: "ready",
    }),
  );
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(store, evaluator);
  let correlation = 0;
  const session = new KafkaApplicationSession(new UnusedConnectionPort());
  const facade = new KafkaBackendFacade(
    session,
    profiles,
    templates,
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    ),
    {
      createCorrelationId: (): string => `rule-correlation-${String(++correlation)}`,
      now: (): Date => new Date("2026-07-25T21:00:00.000Z"),
    },
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(event);
  });
  return { events, facade, store };
}

function ruleSnapshots(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "rules.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "rules.changed" }> =>
      event.event === "rules.changed",
  );
}

function evaluations(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "rules.evaluation" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "rules.evaluation" }> =>
      event.event === "rules.evaluation",
  );
}

describe("Kafka rule facade", () => {
  it("routes every rule command and publishes confirmed state with correlated evaluation", async () => {
    const { events, facade } = setup();
    const second: KafkaRuleDefinition = {
      cooldownMs: highPriority.cooldownMs,
      ...(highPriority.description === undefined ? {} : { description: highPriority.description }),
      enabled: highPriority.enabled,
      expression: '$.marker == "expression-must-not-enter-activity"',
      level: highPriority.level,
      name: "Second",
    };

    await expect(facade.execute(ruleCommand("rules.list", {}))).resolves.toMatchObject({
      ok: true,
    });
    await expect(
      facade.execute(ruleCommand("rules.create", { rule: second })),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        ruleCommand("rules.update", {
          originalName: "Second",
          rule: { ...second, enabled: false, name: "Second renamed" },
        }),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        ruleCommand(
          "rules.validate",
          { rule: { ...second, expression: "$.marker ==" } },
          "validate-draft",
        ),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(
        ruleCommand(
          "rules.evaluate",
          {
            rule: highPriority,
            sample: '{"priority":"high","secret":"fixture-sample-secret"}',
            scope: "single",
            topic: "orders",
          },
          "evaluate-draft",
        ),
      ),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      facade.execute(ruleCommand("rules.delete", { name: "Second renamed" })),
    ).resolves.toMatchObject({ ok: true });

    expect(
      ruleSnapshots(events).map((event) => event.payload.rules.map((rule) => rule.name)),
    ).toEqual([
      ["High priority"],
      ["High priority", "Second"],
      ["High priority", "Second renamed"],
      ["High priority"],
    ]);
    const reports = evaluations(events).map((event) => event.payload);
    expect(reports).toMatchObject([
      {
        kind: "validation",
        requestId: "validate-draft",
        results: [
          {
            name: "Second",
            outcome: "invalid",
          },
        ],
      },
      {
        kind: "evaluation",
        requestId: "evaluate-draft",
        results: [{ name: "High priority", outcome: "matched" }],
      },
    ]);
    expect(reports[0]?.results[0]?.diagnostic).toContain("Position");
    expect(
      events
        .filter((event) => event.event === "activity.recorded")
        .map((event) => [event.payload.operation, event.payload.object, event.payload.outcome]),
    ).toEqual([
      ["Load rules", "Kafka rules", "succeeded"],
      ["Create rule", "Second", "succeeded"],
      ["Update rule", "Second renamed", "succeeded"],
      ["Validate rule", "Second", "succeeded"],
      ["Evaluate rule", "High priority", "succeeded"],
      ["Delete rule", "Second renamed", "succeeded"],
    ]);
    const activity = JSON.stringify(events.filter((event) => event.event === "activity.recorded"));
    expect(activity).not.toMatch(
      /expression-must-not-enter-activity|fixture-sample-secret|\$\.marker/,
    );
  });

  it.each([
    {
      command: "rules.create" as const,
      expectedCode: "RULE_DUPLICATE",
      payload: { rule: { ...highPriority, name: " HIGH PRIORITY " } },
    },
    {
      command: "rules.create" as const,
      expectedCode: "RULE_VALIDATION",
      payload: {
        rule: { ...highPriority, expression: "$.priority ==", name: "Broken" },
      },
    },
    {
      command: "rules.delete" as const,
      expectedCode: "RULE_NOT_FOUND",
      payload: { name: "Missing" },
    },
    {
      command: "rules.evaluate" as const,
      expectedCode: "RULE_SAMPLE",
      payload: {
        sample: '{"secret":"failure-sample-must-not-enter-activity"',
        scope: "catalog" as const,
      },
    },
  ])(
    "translates $expectedCode and republishes the last confirmed catalog",
    async ({ command, expectedCode, payload }) => {
      const { events, facade } = setup();
      await facade.execute(ruleCommand("rules.list", {}));
      const before = ruleSnapshots(events).at(-1)?.payload;

      const response = await facade.execute(ruleCommand(command, payload));

      expect(response).toMatchObject({
        error: {
          activeStateChanged: false,
          code: expectedCode,
          stage: "rule",
        },
        ok: false,
      });
      expect(ruleSnapshots(events).at(-1)?.payload).toEqual(before);
      expect(events.filter((event) => event.event === "activity.recorded").at(-1)).toMatchObject({
        payload: {
          outcome: "failed",
          severity: "error",
        },
      });
      expect(
        JSON.stringify(events.filter((event) => event.event === "activity.recorded")),
      ).not.toContain("failure-sample-must-not-enter-activity");
    },
  );

  it("does not publish an unconfirmed catalog for a draft evaluation failure", async () => {
    const { events, facade } = setup();

    const response = await facade.execute(
      ruleCommand("rules.evaluate", {
        rule: highPriority,
        sample: "{",
        scope: "single",
      }),
    );

    expect(response).toMatchObject({
      error: { code: "RULE_SAMPLE" },
      ok: false,
    });
    expect(ruleSnapshots(events)).toEqual([]);
  });

  it("publishes isolated unavailable rule state for a corrupt catalog", async () => {
    const invalidDocument: KafkaRuleDocument = {
      rules: [highPriority, { ...highPriority, name: " high PRIORITY " }],
    };
    const { events, facade } = setup(new InMemoryKafkaRuleStore(capability, invalidDocument));

    const response = await facade.execute(ruleCommand("rules.list", {}));

    expect(response).toMatchObject({
      error: {
        activeStateChanged: false,
        code: "RULE_CORRUPT",
        stage: "storage",
      },
      ok: false,
    });
    expect(ruleSnapshots(events).at(-1)).toMatchObject({
      payload: {
        rules: [],
        store: {
          durability: "session",
          state: "unavailable",
        },
      },
    });
  });

  it("rejects rule work honestly after backend shutdown", async () => {
    const { events, facade } = setup();
    await facade.shutdown();

    const response = await facade.execute(ruleCommand("rules.list", {}));

    expect(response).toMatchObject({
      error: {
        activeStateChanged: false,
        code: "BACKEND_UNAVAILABLE",
        stage: "backend",
      },
      ok: false,
    });
    expect(ruleSnapshots(events)).toEqual([]);
  });
});
