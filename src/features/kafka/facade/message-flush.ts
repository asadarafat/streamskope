import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  type HostEvent,
} from "../contracts";

import type { ActiveFacadeConsumption, ActivityInput, QueuedFacadeMessage } from "./facade-support";
import {
  discardFacadeMessages,
  facadeMessageBatchEnvelopeBytes,
  takeFacadeMessageBatch,
} from "./message-queue";
import { aggregateFacadeRuleOutputs, ruleNotificationEvent } from "./rule-output";
import { recordStreamFlush } from "./stream-monitor-facade";

const MAX_PUBLICATION_BYTES_PER_TURN = 4 * 1_024 * 1_024;
const MAX_TERMINAL_BATCHES =
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth.maximum /
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize.minimum;

interface FacadeMessageFlushBindings {
  readonly monotonicNow: () => number;
  readonly now: () => Date;
  readonly isPresentationPaused: () => boolean;
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
}

export interface FacadeMessageFlushResult {
  readonly completedAtMs: number;
  readonly publishedMessages: number;
}

function publishRuleOutputs(
  consumption: ActiveFacadeConsumption,
  delivered: readonly QueuedFacadeMessage[],
  bindings: FacadeMessageFlushBindings,
): void {
  const output = aggregateFacadeRuleOutputs(consumption.request.topic, delivered);
  if (output.notification !== undefined) {
    bindings.publish(ruleNotificationEvent(output.notification, bindings.nextSequence()));
  }
  if (output.activity !== undefined) {
    bindings.recordActivity({
      correlationId: consumption.correlationId,
      detail: output.activity.detail,
      object: consumption.request.topic,
      operation: "Match live rules",
      outcome: "succeeded",
      severity: output.activity.severity,
    });
  }
}

export function flushFacadeMessages(
  consumption: ActiveFacadeConsumption,
  drainAll: boolean,
  bindings: FacadeMessageFlushBindings,
): FacadeMessageFlushResult {
  const startedAtMs = bindings.monotonicNow();
  let publishedMessages = 0;
  let batchCount = 0;
  let lastBatchMessages = 0;
  let oldestPublishedAtMs: number | null = null;
  let serializedBytesRemaining = MAX_PUBLICATION_BYTES_PER_TURN;
  const envelopeBytes = facadeMessageBatchEnvelopeBytes(consumption);
  const delivered: QueuedFacadeMessage[] = [];
  // A terminal operation has a finite synchronous budget; it never waits for an ACK.
  // Pressure is rechecked after every publish because transport callbacks are synchronous.
  while (
    consumption.messages.length > 0 &&
    !bindings.isPresentationPaused() &&
    batchCount < (drainAll ? MAX_TERMINAL_BATCHES : 4) &&
    (drainAll || publishedMessages < consumption.streamTuning.batchSize)
  ) {
    const batch = takeFacadeMessageBatch(
      consumption,
      drainAll
        ? consumption.streamTuning.batchSize
        : consumption.streamTuning.batchSize - publishedMessages,
      Math.min(KAFKA_MESSAGE_LIMITS.batchBytes, serializedBytesRemaining),
    );
    if (batch.length === 0) break;
    oldestPublishedAtMs ??= batch[0]?.enqueuedAtMs ?? null;
    delivered.push(...batch);
    publishedMessages += batch.length;
    batchCount += 1;
    lastBatchMessages = batch.length;
    serializedBytesRemaining -=
      envelopeBytes + batch.reduce((bytes, queued) => bytes + queued.serializedBytes, 0);
    bindings.publish({
      event: "messages.batch",
      payload: {
        droppedMessages: consumption.droppedMessages,
        messages: batch.map((queued) => queued.message),
        topic: consumption.request.topic,
      },
      sequence: bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }
  if (drainAll) discardFacadeMessages(consumption);
  publishRuleOutputs(consumption, delivered, bindings);
  const completedAtMs = bindings.monotonicNow();
  recordStreamFlush(consumption, {
    batchCount,
    publishedMessages,
    lastBatchMessages,
    publicationDurationMs: Math.max(0, completedAtMs - startedAtMs),
    queueWaitMs:
      oldestPublishedAtMs === null ? null : Math.max(0, startedAtMs - oldestPublishedAtMs),
    sampledAt: bindings.now().toISOString(),
  });
  return { completedAtMs, publishedMessages };
}
