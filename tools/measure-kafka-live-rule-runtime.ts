import { performance } from "node:perf_hooks";

import {
  KAFKA_LIVE_RULE_LIMITS,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
  type KafkaMessage,
  type KafkaRuleDefinition,
  type KafkaRuleStoreCapability,
} from "../src/kafka/contracts";
import {
  InMemoryKafkaRuleStore,
  KafkaLiveRuleRuntime,
  KafkaRuleService,
} from "../src/kafka/application";
import { aggregateFacadeRuleOutputs } from "../src/kafka/facade/rule-output";
import type { QueuedFacadeMessage } from "../src/kafka/facade/facade-support";
import { StreamSkopeKafkaRuleEvaluator } from "../src/kafka/engine";

interface Measurement {
  readonly maximumMs: number;
  readonly name: string;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly samples: number;
  readonly thresholdMs: number;
}

interface RuntimeCase {
  readonly message: KafkaMessage;
  readonly runtime: KafkaLiveRuleRuntime;
}

const READY_SESSION_STORE: KafkaRuleStoreCapability = {
  durability: "session",
  state: "ready",
};

let checksum = 0;

function rule(name: string, expression: string): KafkaRuleDefinition {
  return {
    cooldownMs: 0,
    enabled: true,
    expression,
    level: "info",
    name,
  };
}

function message(payload: string | null, originalByteSize?: number): KafkaMessage {
  return {
    headers: {},
    id: "performance:0:1",
    key: null,
    offset: "1",
    originalByteSize: originalByteSize ?? payload?.length ?? 0,
    partition: 0,
    payload,
    preview: payload?.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes) ?? "",
    timestamp: "2026-07-25T20:00:00.000Z",
    topic: "performance",
    truncated: payload === null && (originalByteSize ?? 0) > KAFKA_MESSAGE_LIMITS.messageBytes,
  };
}

async function runtimeCase(
  definitions: readonly KafkaRuleDefinition[],
  payload: string | null,
  originalByteSize?: number,
): Promise<RuntimeCase> {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const service = new KafkaRuleService(
    new InMemoryKafkaRuleStore(READY_SESSION_STORE, { rules: definitions }),
    evaluator,
  );
  const runtime = new KafkaLiveRuleRuntime(service, evaluator, {
    durationNow: (): number => performance.now(),
    monotonicNow: (): number => performance.now(),
  });
  await runtime.prepare("performance");
  return { message: message(payload, originalByteSize), runtime };
}

function percentile(sorted: readonly number[], proportion: number): number {
  const index = Math.max(0, Math.ceil(sorted.length * proportion) - 1);
  return sorted[index] ?? 0;
}

function elapsedThreadCpuMilliseconds(started: NodeJS.CpuUsage): number {
  const elapsed = process.threadCpuUsage(started);
  return (elapsed.system + elapsed.user) / 1_000;
}

function measure(
  name: string,
  thresholdMs: number,
  samples: number,
  warmups: number,
  operation: () => number,
): Measurement {
  for (let index = 0; index < warmups; index += 1) {
    checksum += operation();
  }
  const durations: number[] = [];
  for (let index = 0; index < samples; index += 1) {
    const started = process.threadCpuUsage();
    checksum += operation();
    durations.push(elapsedThreadCpuMilliseconds(started));
  }
  durations.sort((left, right) => left - right);
  return {
    maximumMs: durations.at(-1) ?? 0,
    name,
    p50Ms: percentile(durations, 0.5),
    p95Ms: percentile(durations, 0.95),
    samples,
    thresholdMs,
  };
}

function assertState(
  runtime: KafkaLiveRuleRuntime,
  input: KafkaMessage,
  expected: KafkaLiveRuleEvaluation["state"],
): void {
  const result = runtime.evaluate(input);
  if (result.state !== expected) {
    throw new Error(`${input.id} produced ${result.state}; expected ${expected}.`);
  }
}

async function main(): Promise<void> {
  if (process.versions.node.split(".")[0] !== "24") {
    throw new Error(`Performance gate requires Node 24; received ${process.version}.`);
  }

  const simpleRules = Array.from({ length: 50 }, (_value, index) =>
    rule(`Representative ${String(index + 1)}`, "$.metrics.latency >= 20"),
  );
  const representative = await runtimeCase(
    simpleRules,
    JSON.stringify({
      metrics: { latency: 34, loss: 0.07 },
      source: "streamskope-fixture",
      tags: ["core", "critical"],
    }),
  );

  const maximumNodePayload = JSON.stringify(
    Array.from({ length: KAFKA_LIVE_RULE_LIMITS.sampleNodes - 1 }, () => null),
  );
  const maximumNode = await runtimeCase(
    Array.from({ length: 50 }, (_value, index) =>
      rule(`Maximum node ${String(index + 1)}`, "$..missing exists"),
    ),
    maximumNodePayload,
  );

  const malformed = await runtimeCase(
    simpleRules,
    `[${"0,".repeat(Math.floor((KAFKA_LIVE_RULE_LIMITS.payloadBytes - 1) / 2))}`,
  );
  const limitedPayload = await runtimeCase(
    simpleRules,
    JSON.stringify("x".repeat(KAFKA_LIVE_RULE_LIMITS.payloadBytes)),
  );
  const nullPayload = await runtimeCase(simpleRules, null);
  const truncated = await runtimeCase(simpleRules, null, KAFKA_MESSAGE_LIMITS.messageBytes + 1);
  const maximumEvidence = await runtimeCase(
    Array.from({ length: 50 }, (_value, index) =>
      rule(`${"😀".repeat(60)} ${String(index)}`, "$.match == true"),
    ),
    '{"match":true}',
  );

  assertState(representative.runtime, representative.message, "evaluated");
  assertState(maximumNode.runtime, maximumNode.message, "evaluated");
  assertState(malformed.runtime, malformed.message, "unavailable");
  assertState(limitedPayload.runtime, limitedPayload.message, "unavailable");
  assertState(nullPayload.runtime, nullPayload.message, "unavailable");
  assertState(truncated.runtime, truncated.message, "unavailable");
  assertState(maximumEvidence.runtime, maximumEvidence.message, "partial");

  const maximumNotificationMessage: KafkaExploredMessage = {
    ...representative.message,
    ruleEvaluation: representative.runtime.evaluate(representative.message),
  };
  const maximumNotificationQueue: readonly QueuedFacadeMessage[] = Array.from(
    { length: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.maximum },
    () => ({
      message: maximumNotificationMessage,
      ruleOutput: {
        logLevel: "info",
        loggingEnabled: true,
        notificationsEnabled: true,
      },
    }),
  );
  const maximumNotificationOutput = aggregateFacadeRuleOutputs(
    maximumNotificationMessage.topic,
    maximumNotificationQueue,
  );
  if (
    maximumNotificationOutput.notification?.activeMatchCount !==
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.maximum *
        KAFKA_LIVE_RULE_LIMITS.applicableRules ||
    maximumNotificationOutput.notification.matches.length !== 10 ||
    maximumNotificationOutput.activity === undefined
  ) {
    throw new Error("Maximum rule-output fixture does not exercise the declared bounds.");
  }

  const measurements = [
    measure(
      "representative-50-rules",
      10,
      100,
      20,
      (): number => representative.runtime.evaluate(representative.message).evaluatedRules,
    ),
    measure(
      "maximum-nodes-path-heavy-50-rules",
      100,
      20,
      5,
      (): number => maximumNode.runtime.evaluate(maximumNode.message).evaluatedRules,
    ),
    measure(
      "malformed-maximum-bytes",
      10,
      100,
      20,
      (): number => malformed.runtime.evaluate(malformed.message).evaluatedRules,
    ),
    measure(
      "live-limit-exceeded",
      5,
      100,
      20,
      (): number => limitedPayload.runtime.evaluate(limitedPayload.message).evaluatedRules,
    ),
    measure(
      "null-payload",
      5,
      1_000,
      100,
      (): number => nullPayload.runtime.evaluate(nullPayload.message).evaluatedRules,
    ),
    measure(
      "truncated-payload",
      5,
      1_000,
      100,
      (): number => truncated.runtime.evaluate(truncated.message).evaluatedRules,
    ),
    measure(
      "maximum-evidence",
      10,
      100,
      20,
      (): number => maximumEvidence.runtime.evaluate(maximumEvidence.message).evaluatedRules,
    ),
    measure("maximum-notice-and-activity-aggregation", 20, 20, 5, (): number => {
      const output = aggregateFacadeRuleOutputs(
        maximumNotificationMessage.topic,
        maximumNotificationQueue,
      );
      return output.notification?.activeMatchCount ?? 0;
    }),
  ];

  process.stdout.write(
    `${JSON.stringify(
      {
        checksum,
        clock: "process.threadCpuUsage milliseconds",
        measurements,
        node: process.version,
      },
      undefined,
      2,
    )}\n`,
  );

  const failures = measurements.filter(
    (measurement) => measurement.p95Ms > measurement.thresholdMs,
  );
  if (failures.length > 0) {
    throw new Error(
      `Kafka live-rule performance gate failed: ${failures
        .map(
          (failure) =>
            `${failure.name} p95 ${failure.p95Ms.toFixed(3)} ms > ${failure.thresholdMs.toFixed(
              3,
            )} ms`,
        )
        .join("; ")}`,
    );
  }
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Kafka live-rule performance measurement failed: ${detail}\n`);
  process.exitCode = 1;
});
