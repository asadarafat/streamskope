import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type KafkaExploredMessage,
  type KafkaLiveRuleEvaluation,
} from "../../src/kafka/contracts";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/kafka/ui/state";
import { ELECTRON_RUNTIME_EFFICIENCY_POLICY } from "../../tools/electron-runtime-efficiency-policy";

export interface RendererMessageRetentionMeasurementOptions {
  readonly collectHeapBytes: () => number;
  readonly deliveredMessages?: number;
  readonly forceGarbageCollection: () => void;
  readonly heapThresholdBytes?: number;
}

export interface RendererMessageRetentionEvidence {
  readonly deliveredMessages: number;
  readonly elapsedCpuMs: number;
  readonly heapDeltaBytes: number;
  readonly heapThresholdBytes: number;
  readonly rendererEvictions: number;
  readonly retainedBytes: number;
  readonly retainedByteLimit: number;
  readonly retainedMessageLimit: number;
  readonly retainedMessages: number;
}

const noRuleMatches: KafkaLiveRuleEvaluation = Object.freeze({
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

function fixtureMessage(index: number): KafkaExploredMessage {
  const payload = `{"index":${String(index)},"state":"retention-stress","value":"${"x".repeat(96)}"}`;
  return {
    headers: Object.freeze({ source: "renderer-retention-stress" }),
    id: `retention:0:${String(index)}`,
    key: `key-${String(index)}`,
    offset: String(index),
    originalByteSize: Buffer.byteLength(payload),
    partition: 0,
    payload,
    preview: payload,
    ruleEvaluation: noRuleMatches,
    timestamp: new Date(Date.UTC(2026, 6, 29, 0, 0, 0, index)).toISOString(),
    topic: "retention.performance",
    truncated: false,
  };
}

function elapsedCpuMilliseconds(started: NodeJS.CpuUsage): number {
  const elapsed = process.cpuUsage(started);
  return (elapsed.system + elapsed.user) / 1_000;
}

export function measureRendererMessageRetention(
  options: RendererMessageRetentionMeasurementOptions,
): RendererMessageRetentionEvidence {
  options.forceGarbageCollection();
  const heapBefore = options.collectHeapBytes();
  const started = process.cpuUsage();
  const deliveredMessages =
    options.deliveredMessages ?? ELECTRON_RUNTIME_EFFICIENCY_POLICY.messageStress.messages;
  let state = initialKafkaUiState;
  let sequence = 0;
  for (
    let batchStart = 0;
    batchStart < deliveredMessages;
    batchStart += KAFKA_MESSAGE_LIMITS.batchMessages
  ) {
    const batchSize = Math.min(KAFKA_MESSAGE_LIMITS.batchMessages, deliveredMessages - batchStart);
    const messages = Array.from({ length: batchSize }, (_value, index) =>
      fixtureMessage(batchStart + index),
    );
    sequence += 1;
    state = reduceKafkaHostEvent(state, {
      event: "messages.batch",
      payload: {
        droppedMessages: 0,
        messages,
        topic: "retention.performance",
      },
      sequence,
      version: HOST_PROTOCOL_VERSION,
    });
  }
  const elapsedCpuMs = elapsedCpuMilliseconds(started);
  options.forceGarbageCollection();
  const heapDeltaBytes = Math.max(0, options.collectHeapBytes() - heapBefore);
  return Object.freeze({
    deliveredMessages,
    elapsedCpuMs,
    heapDeltaBytes,
    heapThresholdBytes:
      options.heapThresholdBytes ?? ELECTRON_RUNTIME_EFFICIENCY_POLICY.messageStress.heapDeltaBytes,
    rendererEvictions: state.rendererDroppedMessages,
    retainedBytes: state.retainedMessageBytes,
    retainedByteLimit: KAFKA_MESSAGE_LIMITS.retainedBytes,
    retainedMessageLimit: KAFKA_MESSAGE_LIMITS.retainedMessages,
    retainedMessages: state.messages.length,
  });
}

export function assertRendererMessageRetentionEvidence(
  evidence: RendererMessageRetentionEvidence,
): void {
  const expectedEvictions = evidence.deliveredMessages - evidence.retainedMessageLimit;
  if (
    evidence.retainedMessages !== evidence.retainedMessageLimit ||
    evidence.rendererEvictions !== expectedEvictions ||
    evidence.retainedBytes > evidence.retainedByteLimit
  ) {
    throw new Error(
      `Renderer retention violated its contract: retained=${String(evidence.retainedMessages)}, evicted=${String(evidence.rendererEvictions)}, bytes=${String(evidence.retainedBytes)}.`,
    );
  }
  if (evidence.heapDeltaBytes > evidence.heapThresholdBytes) {
    throw new Error(
      `Renderer retention heap delta ${String(evidence.heapDeltaBytes)} exceeds ${String(evidence.heapThresholdBytes)} bytes.`,
    );
  }
}
