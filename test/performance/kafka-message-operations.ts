import {
  KAFKA_MESSAGE_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  kafkaMessageRetainedBytes,
  utf8ByteLength,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
} from "../../src/kafka/contracts";
import {
  KAFKA_MESSAGE_OPERATION_LIMITS,
  createKafkaMessageExportDocument,
  initialKafkaMessageFilters,
  selectFilteredKafkaMessages,
  type KafkaMessageFilters,
} from "../../src/kafka/ui/message-operations";

interface Measurement {
  readonly maximumMs: number;
  readonly name: string;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly samples: number;
  readonly thresholdMs: number;
}

const activeEvaluation: KafkaLiveRuleEvaluation = Object.freeze({
  activeMatchCount: 1,
  activeMatches: Object.freeze([{ level: "warn" as const, name: "Performance fixture" }]),
  durationMicros: 0,
  errorCount: 0,
  errors: Object.freeze([]),
  evaluatedRules: 1,
  highestActiveSeverity: "warn",
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: Object.freeze([]),
});

const inactiveEvaluation: KafkaLiveRuleEvaluation = Object.freeze({
  activeMatchCount: 0,
  activeMatches: Object.freeze([]),
  durationMicros: 0,
  errorCount: 0,
  errors: Object.freeze([]),
  evaluatedRules: 1,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: Object.freeze([]),
});

let checksum = 0;

function fixtureMessage(index: number): KafkaExploredMessage {
  const group = index % 10;
  const prefix = `APPROVED|group-${String(group).padStart(2, "0")}|record-${String(index).padStart(
    4,
    "0",
  )}|`;
  const payload = `${prefix}${"x".repeat(64_000 - prefix.length)}`;
  return Object.freeze({
    headers: Object.freeze({ source: "performance-fixture" }),
    id: `performance:${String(index)}`,
    key: `group-${String(group).padStart(2, "0")}-record-${String(index).padStart(4, "0")}`,
    offset: String(index),
    originalByteSize: utf8ByteLength(payload),
    partition: group,
    payload,
    preview: payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes),
    ruleEvaluation: index % 2 === 0 ? activeEvaluation : inactiveEvaluation,
    timestamp: `2026-07-25T10:${String(index % 60).padStart(2, "0")}:00.000Z`,
    topic: "performance",
    truncated: false,
  });
}

function percentile(sorted: readonly number[], proportion: number): number {
  const index = Math.max(0, Math.ceil(sorted.length * proportion) - 1);
  return sorted[index] ?? 0;
}

function elapsedCpuMilliseconds(started: NodeJS.CpuUsage): number {
  const elapsed = process.cpuUsage(started);
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
    const started = process.cpuUsage();
    checksum += operation();
    durations.push(elapsedCpuMilliseconds(started));
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

function assertWithinBounds(
  retained: readonly KafkaExploredMessage[],
  retainedBytes: number,
): void {
  if (retained.length !== KAFKA_MESSAGE_LIMITS.retainedMessages) {
    throw new Error(
      `Expected ${String(KAFKA_MESSAGE_LIMITS.retainedMessages)} retained records; received ${String(retained.length)}.`,
    );
  }
  if (retainedBytes > KAFKA_MESSAGE_LIMITS.retainedBytes) {
    throw new Error(
      `Performance fixture exceeds the retained byte bound: ${String(retainedBytes)} > ${String(
        KAFKA_MESSAGE_LIMITS.retainedBytes,
      )}.`,
    );
  }
}

function filtered(
  retained: readonly KafkaExploredMessage[],
  filters: KafkaMessageFilters,
): readonly KafkaExploredMessage[] {
  return selectFilteredKafkaMessages(retained, filters);
}

function main(): void {
  if (process.versions.node.split(".")[0] !== "24") {
    throw new Error(`Performance gate requires Node 24; received ${process.version}.`);
  }

  const retained = Object.freeze(
    Array.from({ length: KAFKA_MESSAGE_LIMITS.retainedMessages }, (_value, index) =>
      fixtureMessage(index),
    ),
  );
  if (retained.length !== KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages.maximum) {
    throw new Error("Maximum retained fixture does not match the maximum fetch preference.");
  }
  const minimumRetained = retained.slice(
    0,
    KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages.minimum,
  );
  const retainedBytes = retained.reduce(
    (bytes, message) => bytes + kafkaMessageRetainedBytes(message),
    0,
  );
  assertWithinBounds(retained, retainedBytes);
  if (selectFilteredKafkaMessages(retained, initialKafkaMessageFilters) !== retained) {
    throw new Error("A neutral filter must preserve the authoritative retained array.");
  }

  const keyFilters: KafkaMessageFilters = {
    ...initialKafkaMessageFilters,
    key: "GROUP-06",
  };
  const missingValueFilters: KafkaMessageFilters = {
    ...initialKafkaMessageFilters,
    value: "not-present-in-any-retained-payload",
  };
  const combinedFilters: KafkaMessageFilters = {
    ...initialKafkaMessageFilters,
    activeRuleMatchesOnly: true,
    key: "group-06",
    partition: 6,
    value: "approved",
  };
  const minimumFilters: KafkaMessageFilters = {
    ...initialKafkaMessageFilters,
    activeRuleMatchesOnly: true,
    key: "group-00",
    partition: 0,
    value: "approved",
  };
  if (filtered(minimumRetained, minimumFilters).length !== 1) {
    throw new Error("Minimum fetch fixture does not exercise one matching message.");
  }
  const exportMessages = filtered(retained, combinedFilters);
  if (exportMessages.length !== 100) {
    throw new Error(`Expected 100 export records; received ${String(exportMessages.length)}.`);
  }

  const measurements = [
    measure("minimum-retained-combined-filter", 5, 100, 20, () => {
      return filtered(minimumRetained, minimumFilters).length;
    }),
    measure("maximum-retained-key-filter", 20, 30, 5, () => {
      return filtered(retained, keyFilters).length;
    }),
    measure("maximum-retained-value-miss", 100, 20, 3, () => {
      return filtered(retained, missingValueFilters).length;
    }),
    measure("maximum-retained-combined-filter", 40, 20, 3, () => {
      return filtered(retained, combinedFilters).length;
    }),
    measure("bounded-100-record-export", 250, 5, 1, () => {
      return createKafkaMessageExportDocument({
        filters: combinedFilters,
        messages: exportMessages,
        retainedMessageCount: retained.length,
        stale: false,
        topic: "performance",
      }).byteSize;
    }),
  ];

  const exportDocument = createKafkaMessageExportDocument({
    filters: combinedFilters,
    messages: exportMessages,
    retainedMessageCount: retained.length,
    stale: false,
    topic: "performance",
  });
  if (
    exportDocument.byteSize !== utf8ByteLength(exportDocument.content) ||
    exportDocument.byteSize > KAFKA_MESSAGE_OPERATION_LIMITS.exportBytes
  ) {
    throw new Error("The measured export violates its declared UTF-8 document bound.");
  }

  process.stdout.write(
    `${JSON.stringify(
      {
        checksum,
        clock: "process.cpuUsage milliseconds",
        exportBytes: exportDocument.byteSize,
        exportRecords: exportMessages.length,
        measurements,
        node: process.version,
        retainedBytes,
        retainedRecords: retained.length,
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
      `Kafka message-operation performance gate failed: ${failures
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

try {
  main();
} catch (error) {
  const detail = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Kafka message-operation performance measurement failed: ${detail}\n`);
  process.exitCode = 1;
}
