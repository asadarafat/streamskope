import { HOST_PROTOCOL_VERSION, type HostEvent } from "../contracts";

import type { ActiveFacadeConsumption, ActivityInput, QueuedFacadeMessage } from "./facade-support";
import { takeFacadeMessageBatch } from "./message-queue";
import { aggregateFacadeRuleOutputs, ruleNotificationEvent } from "./rule-output";
import {
  completeStreamMeasurement,
  recordStreamFlush,
  streamQueueWaitMs,
} from "./stream-monitor-facade";

interface FacadeMessageFlushBindings {
  readonly monotonicNow: () => number;
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
}

export interface FacadeMessageFlushResult {
  readonly completedAtMs: number;
  readonly droppedSincePrevious: number;
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
  const queueWaitMs = streamQueueWaitMs(consumption, startedAtMs);
  let deliveredMessages = 0;
  let batchCount = 0;
  let lastBatchMessages = 0;
  const delivered: QueuedFacadeMessage[] = [];
  do {
    const batch = takeFacadeMessageBatch(consumption);
    if (batch.length > 0) {
      delivered.push(...batch);
      deliveredMessages += batch.length;
      batchCount += 1;
      lastBatchMessages = batch.length;
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
  } while (drainAll && consumption.messages.length > 0);
  publishRuleOutputs(consumption, delivered, bindings);
  const completedAtMs = bindings.monotonicNow();
  const droppedSincePrevious = recordStreamFlush(consumption, {
    batchCount,
    completedAtMs,
    deliveredMessages,
    lastBatchMessages,
    publicationDurationMs: Math.max(0, completedAtMs - startedAtMs),
    queueWaitMs,
  });
  completeStreamMeasurement(consumption, completedAtMs);
  return { completedAtMs, droppedSincePrevious };
}
