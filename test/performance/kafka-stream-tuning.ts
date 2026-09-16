import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
  type KafkaStreamPreferences,
} from "../../src/features/kafka/contracts";
import {
  type ActiveFacadeConsumption,
  type QueuedFacadeMessage,
} from "../../src/features/kafka/facade/facade-support";
import { appendFacadeMessage, takeFacadeMessageBatch } from "../../src/features/kafka/facade/message-queue";
import {
  createStreamMonitoring,
  streamMetricsEvent,
} from "../../src/features/kafka/facade/stream-monitor-facade";
import { initialKafkaUiState, reduceKafkaUiState } from "../../src/features/kafka/ui/state";

interface Measurement {
  readonly maximumMs: number;
  readonly name: string;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly samples: number;
  readonly thresholdMs: number;
}

const inactiveEvaluation: KafkaLiveRuleEvaluation = Object.freeze({
  activeMatchCount: 0,
  activeMatches: Object.freeze([]),
  durationMicros: 0,
  errorCount: 0,
  errors: Object.freeze([]),
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: Object.freeze([]),
});

const message: KafkaExploredMessage = Object.freeze({
  headers: Object.freeze({ source: "stream-tuning-performance" }),
  id: "performance:0:0",
  key: "performance",
  offset: "0",
  originalByteSize: 5,
  partition: 0,
  payload: "value",
  preview: "value",
  ruleEvaluation: inactiveEvaluation,
  timestamp: "2026-07-25T00:00:00.000Z",
  topic: "performance",
  truncated: false,
});

const queuedMessage: QueuedFacadeMessage = Object.freeze({
  message,
  ruleOutput: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.rules,
});

let checksum = 0;

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

function preferences(bound: "maximum" | "minimum"): KafkaStreamPreferences {
  return {
    batchSize: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize[bound],
    historySamples: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.historySamples[bound],
    intervalMs: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.intervalMs[bound],
    queueDepth: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth[bound],
  };
}

function consumption(stream: KafkaStreamPreferences): ActiveFacadeConsumption {
  return {
    cancelScheduledFlush: undefined,
    correlationId: "stream-tuning-performance",
    droppedMessages: 0,
    flushScheduled: false,
    messages: [],
    queuedBytes: 0,
    receivedMessages: 0,
    request: {
      maxMessages: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages.maximum,
      mode: "tail",
      topic: "performance",
    },
    ruleFailureRecorded: false,
    state: "streaming",
    streamMonitoring: createStreamMonitoring(0),
    streamTuning: { ...stream, source: "confirmed" },
  };
}

function queueCycle(stream: KafkaStreamPreferences): number {
  const active = consumption(stream);
  for (let index = 0; index < stream.queueDepth * 2; index += 1) {
    appendFacadeMessage(active, queuedMessage);
  }
  if (
    active.messages.length !== stream.queueDepth ||
    active.droppedMessages !== stream.queueDepth
  ) {
    throw new Error("Stream queue did not retain and drop at its effective message bound.");
  }

  let batches = 0;
  let delivered = 0;
  while (active.messages.length > 0) {
    const batch = takeFacadeMessageBatch(active);
    if (batch.length === 0 || batch.length > stream.batchSize) {
      throw new Error("Stream queue produced an invalid effective batch.");
    }
    batches += 1;
    delivered += batch.length;
  }
  const evidence = streamMetricsEvent(
    active,
    "streaming",
    "Performance fixture",
    "2026-07-25T00:00:00.000Z",
    1,
  );
  if (
    active.queuedBytes !== 0 ||
    delivered !== stream.queueDepth ||
    evidence?.payload.queue?.capacityMessages !== stream.queueDepth ||
    evidence.payload.delivery?.batchSize !== stream.batchSize ||
    evidence.payload.delivery.intervalMs !== stream.intervalMs ||
    evidence.payload.delivery.historySamples !== stream.historySamples
  ) {
    throw new Error("Stream queue evidence does not report its effective preference bounds.");
  }
  return delivered + active.droppedMessages + batches;
}

function historyCycle(stream: KafkaStreamPreferences): number {
  const active = consumption(stream);
  let state = initialKafkaUiState;
  for (let sequence = 1; sequence <= stream.historySamples * 2; sequence += 1) {
    active.receivedMessages = sequence;
    active.streamMonitoring.deliveredMessages = sequence;
    const event = streamMetricsEvent(
      active,
      "streaming",
      "Performance fixture",
      "2026-07-25T00:00:00.000Z",
      sequence,
    );
    if (event === null) {
      throw new Error("Stream monitor did not produce bounded history evidence.");
    }
    state = reduceKafkaUiState(state, { event, type: "host.event" });
  }
  if (
    state.streamMonitor.history.length !== stream.historySamples ||
    state.streamMonitor.current.delivery?.historySamples !== stream.historySamples
  ) {
    throw new Error("Renderer history did not retain its exact effective preference bound.");
  }
  return state.streamMonitor.history.length + state.lastSequence;
}

function main(): void {
  if (process.versions.node.split(".")[0] !== "24") {
    throw new Error(`Performance gate requires Node 24; received ${process.version}.`);
  }
  const minimum = preferences("minimum");
  const maximum = preferences("maximum");
  const measurements = [
    measure("minimum-stream-queue-cycle", 10, 100, 20, () => queueCycle(minimum)),
    measure("maximum-stream-queue-cycle", 50, 30, 5, () => queueCycle(maximum)),
    measure("minimum-monitor-history-retention", 10, 100, 20, () => historyCycle(minimum)),
    measure("maximum-monitor-history-retention", 50, 30, 5, () => historyCycle(maximum)),
  ];

  process.stdout.write(
    `${JSON.stringify(
      {
        checksum,
        clock: "process.cpuUsage milliseconds",
        measurements,
        node: process.version,
        preferenceBounds: { maximum, minimum },
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
      `Kafka stream-tuning performance gate failed: ${failures
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
  process.stderr.write(`Kafka stream-tuning performance measurement failed: ${detail}\n`);
  process.exitCode = 1;
}
