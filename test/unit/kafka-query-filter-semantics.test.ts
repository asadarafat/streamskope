import { describe, expect, it } from "vitest";

import {
  compileKafkaSearchFilter,
  parseKafkaSearchFilter,
  type KafkaMessage,
  type KafkaRuleDefinition,
} from "../../src/features/kafka/contracts";
import { InMemoryKafkaRuleStore } from "../../src/features/kafka/application/in-memory-rule-store";
import { KafkaRuleService } from "../../src/features/kafka/application/rule-service";
import { KafkaLiveRuleRuntime } from "../../src/features/kafka/application/live-rule-runtime";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/features/kafka/engine/rule-evaluator";
import { KafkaReadTracker } from "../../src/features/kafka/engine/read-coverage";
import {
  initialKafkaMessageFilters,
  selectKafkaQueryMessages,
} from "../../src/features/kafka/ui/message-operations";

function message(payload: string | null): KafkaMessage {
  return {
    headers: {},
    id: "orders:0:0",
    key: null,
    offset: "0",
    originalByteSize: payload?.length ?? 0,
    partition: 0,
    payload,
    preview: payload ?? "",
    timestamp: "2026-10-02T00:00:00.000Z",
    topic: "orders",
    truncated: false,
  };
}

async function live(expression: string): Promise<KafkaLiveRuleRuntime> {
  const definition: KafkaRuleDefinition = {
    name: "Filter",
    expression,
    cooldownMs: 0,
    level: "info",
    enabled: true,
  };
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const runtime = new KafkaLiveRuleRuntime(
    new KafkaRuleService(
      new InMemoryKafkaRuleStore(
        { durability: "session", state: "ready" },
        { rules: [definition] },
      ),
      evaluator,
    ),
    evaluator,
  );
  await runtime.prepare("orders");
  return runtime;
}

describe("shared preview, live and query filter semantics", () => {
  it.each([
    ["$.a == null", { a: null }, true],
    ["$.a == null", {}, false],
    ["$.a exists", { a: null }, false],
    ["$.a exists", { a: false }, true],
    ["$.a != null", {}, false],
    ["$.a > 2", { a: 3 }, true],
    ["$.a > 2", { a: "3" }, false],
    ["$.a >= 0", { a: null }, false],
    ["$.a >= 0", { a: false }, false],
    ["$.a >= 0", { a: "" }, false],
    [
      "$.nested.items[*].n > 2 && $.active == true",
      { nested: { items: [{ n: 3 }] }, active: true },
      true,
    ],
    ["$.name matches /^edge-/i", { name: "EDGE-01" }, true],
  ] as const)("applies %s to %j consistently", async (expression, sample, expected) => {
    const payload = JSON.stringify(sample);
    const filters = { ...initialKafkaMessageFilters, expression };
    const record = message(payload);
    const runtime = await live(expression);
    const ruleEvaluation = runtime.evaluate(record);
    expect(new StreamSkopeKafkaRuleEvaluator().evaluate(expression, sample)).toBe(expected);
    expect(ruleEvaluation.activeMatchCount).toBe(Number(expected));
    expect(compileKafkaSearchFilter(filters)(record)).toBe(expected ? "matched" : "not-matched");
    expect(
      selectKafkaQueryMessages([{ ...record, ruleEvaluation }], filters).messages,
    ).toHaveLength(Number(expected));
    const request = {
      mode: "earliest" as const,
      maxMessages: 10,
      topic: "orders",
      search: parseKafkaSearchFilter(
        { key: "", value: "", offset: "", timestamp: "", partition: null, expression },
        "search",
      ),
    };
    const tracker = new KafkaReadTracker({
      continuous: false,
      maxMessages: 10,
      startOffsets: new Map([[0, 0n]]),
      endOffsets: new Map([[0, 1n]]),
      request,
    });
    expect(
      tracker.accept({
        topic: "orders",
        headers: new Map(),
        offset: 0n,
        partition: 0,
        timestamp: 1000n,
        value: Buffer.from(payload),
      }),
    ).toBe(expected);
    expect(tracker.snapshot()).toMatchObject({
      reason: "range-complete",
      matchedRecords: Number(expected),
      unavailableRecords: 0,
    });
  });

  it("stops costly traversal explicitly and reports unevaluable records rather than false negatives", async () => {
    const payload = JSON.stringify({ items: Array.from({ length: 10_000 }, () => ({ n: 0 })) });
    const expression = Array.from({ length: 8 }, () => "$.items[*].n == 1").join(" || ");
    const record = message(payload);
    expect(() =>
      new StreamSkopeKafkaRuleEvaluator().evaluate(expression, JSON.parse(payload)),
    ).toThrow("evaluation work units");
    const evaluation = (await live(expression)).evaluate(record);
    expect(evaluation).toMatchObject({ activeMatchCount: 0, errorCount: 1 });
    const filters = { ...initialKafkaMessageFilters, expression };
    expect(compileKafkaSearchFilter(filters)(record)).toBe("unavailable");
    expect(selectKafkaQueryMessages([{ ...record, ruleEvaluation: evaluation }], filters)).toEqual({
      messages: [],
      unavailable: 1,
    });
  });

  it.each([null, "not JSON", '{"a":'])(
    "does not treat malformed or absent JSON as a negative match",
    (payload) => {
      expect(
        compileKafkaSearchFilter({ ...initialKafkaMessageFilters, expression: "$.a == null" })(
          message(payload),
        ),
      ).toBe("unavailable");
    },
  );

  it("rejects executable and over-budget expressions before a broker read", () => {
    for (const expression of ["process.exit()", "$.name matches /(a+)+$/", "x".repeat(4097)]) {
      expect(() =>
        parseKafkaSearchFilter(
          { key: "", value: "", offset: "", timestamp: "", partition: null, expression },
          "search",
        ),
      ).toThrow();
    }
  });
});

it("bounds an entire loaded selection and exposes every unevaluated remainder", async () => {
  const payload = JSON.stringify({ text: "x".repeat(120_000) });
  const expression = '$.text contains "x"';
  const record = message(payload);
  const ruleEvaluation = (await live(expression)).evaluate(record);
  const records = Array.from({ length: 10 }, (_unused, index) => ({
    ...record,
    id: String(index),
    ruleEvaluation,
  }));
  const selected = selectKafkaQueryMessages(records, { ...initialKafkaMessageFilters, expression });
  expect(selected.messages).toHaveLength(4);
  expect(selected.unavailable).toBe(6);
});
