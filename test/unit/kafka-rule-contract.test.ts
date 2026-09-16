import { describe, expect, it } from "vitest";

import {
  HOST_COMMANDS,
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  HOST_EVENTS,
  HOST_PROTOCOL_VERSION,
  KAFKA_RULE_EVALUATION_OUTCOMES,
  KAFKA_RULE_LIMITS,
  KAFKA_RULE_SEVERITIES,
  KAFKA_RULE_STORE_DURABILITIES,
  KAFKA_RULE_STORE_STATES,
  HostContractValidationError,
  parseHostCommand,
  parseHostEvent,
} from "../../src/features/kafka/contracts";

const rule = {
  cooldownMs: 5_000,
  description: "Detect critical orders.",
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
} as const;

const sessionSnapshot = {
  rules: [rule],
  store: {
    durability: "session",
    state: "ready",
  },
} as const;

describe("Kafka rule contract", () => {
  it("retains the bounded rule vocabulary on protocol version 15", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(17);
    expect(KAFKA_RULE_SEVERITIES).toEqual(["info", "warn", "error"]);
    expect(KAFKA_RULE_EVALUATION_OUTCOMES).toEqual([
      "valid",
      "invalid",
      "matched",
      "not-matched",
      "skipped",
    ]);
    expect(KAFKA_RULE_STORE_DURABILITIES).toEqual(["durable", "session"]);
    expect(KAFKA_RULE_STORE_STATES).toEqual(["ready", "unavailable"]);
    expect(KAFKA_RULE_LIMITS).toEqual({
      conditions: 64,
      cooldownMs: 86_400_000,
      descriptionCharacters: 2_048,
      diagnosticCharacters: 2_048,
      expressionCharacters: 4_096,
      groupingDepth: 16,
      nameCharacters: 128,
      pathSegments: 64,
      regexCharacters: 256,
      rules: 500,
      sampleBytes: 1_048_576,
      sampleDepth: 64,
      sampleNodes: 100_000,
      topicCharacters: 249,
    });
    expect(HOST_COMMANDS).toEqual(
      expect.arrayContaining([
        "rules.list",
        "rules.create",
        "rules.update",
        "rules.delete",
        "rules.validate",
        "rules.evaluate",
      ]),
    );
    expect(HOST_EVENTS).toEqual(expect.arrayContaining(["rules.changed", "rules.evaluation"]));
    expect(HOST_ERROR_CODES).toEqual(
      expect.arrayContaining([
        "RULE_VALIDATION",
        "RULE_DUPLICATE",
        "RULE_NOT_FOUND",
        "RULE_CAPACITY",
        "RULE_SAMPLE",
        "RULE_STORE_UNAVAILABLE",
        "RULE_CORRUPT",
      ]),
    );
    expect(HOST_ERROR_STAGES).toContain("rule");
  });

  it("parses exact list, create, update, delete and validate commands", () => {
    expect(
      parseHostCommand({
        command: "rules.list",
        id: "rules-list",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "rules.list", payload: {} });
    expect(
      parseHostCommand({
        command: "rules.create",
        id: "rules-create",
        payload: { rule },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "rules.create", payload: { rule } });
    expect(
      parseHostCommand({
        command: "rules.update",
        id: "rules-update",
        payload: { originalName: "Old name", rule },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "rules.update",
      payload: { originalName: "Old name", rule },
    });
    expect(
      parseHostCommand({
        command: "rules.delete",
        id: "rules-delete",
        payload: { name: rule.name },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "rules.delete", payload: { name: rule.name } });
    expect(
      parseHostCommand({
        command: "rules.validate",
        id: "rules-validate",
        payload: { rule },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({ command: "rules.validate", payload: { rule } });
  });

  it("parses single-draft and catalog sample evaluation commands", () => {
    expect(
      parseHostCommand({
        command: "rules.evaluate",
        id: "evaluate-one",
        payload: {
          rule,
          sample: '{"priority":"high"}',
          scope: "single",
          topic: "orders",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "rules.evaluate",
      payload: {
        rule,
        sample: '{"priority":"high"}',
        scope: "single",
        topic: "orders",
      },
    });
    expect(
      parseHostCommand({
        command: "rules.evaluate",
        id: "evaluate-catalog",
        payload: {
          sample: '{"priority":"high"}',
          scope: "catalog",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "rules.evaluate",
      payload: {
        sample: '{"priority":"high"}',
        scope: "catalog",
      },
    });
  });

  it("parses safe rule snapshot and request-correlated evaluation events", () => {
    expect(
      parseHostEvent({
        event: "rules.changed",
        payload: sessionSnapshot,
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toEqual({
      event: "rules.changed",
      payload: sessionSnapshot,
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(
      parseHostEvent({
        event: "rules.evaluation",
        payload: {
          kind: "evaluation",
          requestId: "evaluate-catalog",
          results: [
            { name: "High priority", outcome: "matched" },
            {
              diagnostic: "Rule is disabled.",
              name: "Disabled",
              outcome: "skipped",
              reason: "disabled",
            },
          ],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "rules.evaluation",
      payload: {
        requestId: "evaluate-catalog",
        results: [{ outcome: "matched" }, { outcome: "skipped", reason: "disabled" }],
      },
    });
  });

  it("parses unavailable storage only with an empty catalog and recovery", () => {
    expect(
      parseHostEvent({
        event: "rules.changed",
        payload: {
          rules: [],
          store: {
            durability: "durable",
            recovery: "Restore a known-good rule file and restart StreamSkope.",
            state: "unavailable",
          },
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: { rules: [], store: { state: "unavailable" } },
    });

    expect(() =>
      parseHostEvent({
        event: "rules.changed",
        payload: {
          ...sessionSnapshot,
          store: {
            durability: "durable",
            recovery: "Restore it.",
            state: "unavailable",
          },
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it.each([
    {
      label: "undeclared rule field",
      value: {
        command: "rules.create",
        id: "unknown-field",
        payload: { rule: { ...rule, script: "process.exit()" } },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "oversized expression",
      value: {
        command: "rules.create",
        id: "large-expression",
        payload: {
          rule: { ...rule, expression: "x".repeat(KAFKA_RULE_LIMITS.expressionCharacters + 1) },
        },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "negative cooldown",
      value: {
        command: "rules.create",
        id: "negative-cooldown",
        payload: { rule: { ...rule, cooldownMs: -1 } },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "single evaluation without a rule",
      value: {
        command: "rules.evaluate",
        id: "missing-rule",
        payload: { sample: "{}", scope: "single" },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "catalog evaluation with a draft rule",
      value: {
        command: "rules.evaluate",
        id: "unexpected-rule",
        payload: { rule, sample: "{}", scope: "catalog" },
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "oversized sample",
      value: {
        command: "rules.evaluate",
        id: "large-sample",
        payload: { sample: "😀".repeat(KAFKA_RULE_LIMITS.sampleBytes / 2), scope: "catalog" },
        version: HOST_PROTOCOL_VERSION,
      },
    },
  ])("rejects $label", ({ value }) => {
    expect(() => parseHostCommand(value)).toThrow(HostContractValidationError);
  });

  it("rejects activity-leaking or malformed evaluation event fields", () => {
    for (const extra of [
      { expression: '$.secret == "value"' },
      { sample: '{"secret":"value"}' },
      { extracted: { secret: "value" } },
    ]) {
      expect(() =>
        parseHostEvent({
          event: "rules.evaluation",
          payload: {
            kind: "evaluation",
            requestId: "evaluate-one",
            results: [{ name: rule.name, outcome: "matched", ...extra }],
          },
          sequence: 5,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).toThrow(HostContractValidationError);
    }
  });
});
