import { KAFKA_MESSAGE_LIMITS, kafkaMessageRetainedBytes } from "../contracts";

import type { ActiveFacadeConsumption, QueuedFacadeMessage } from "./facade-support";

export function appendFacadeMessage(
  consumption: ActiveFacadeConsumption,
  queued: QueuedFacadeMessage,
): void {
  const messageBytes = kafkaMessageRetainedBytes(queued.message);
  consumption.messages.push(queued);
  consumption.queuedBytes += messageBytes;
  while (
    consumption.messages.length > consumption.streamTuning.queueDepth ||
    consumption.queuedBytes > KAFKA_MESSAGE_LIMITS.queuedBytes
  ) {
    const dropped = consumption.messages.shift();
    if (dropped === undefined) {
      return;
    }
    consumption.queuedBytes -= kafkaMessageRetainedBytes(dropped.message);
    consumption.droppedMessages += 1;
  }
}

export function takeFacadeMessageBatch(
  consumption: ActiveFacadeConsumption,
): readonly QueuedFacadeMessage[] {
  const batch: QueuedFacadeMessage[] = [];
  let batchBytes = 0;
  while (batch.length < consumption.streamTuning.batchSize && consumption.messages.length > 0) {
    const next = consumption.messages[0];
    if (next === undefined) {
      break;
    }
    const nextBytes = kafkaMessageRetainedBytes(next.message);
    if (nextBytes > KAFKA_MESSAGE_LIMITS.batchBytes) {
      consumption.messages.shift();
      consumption.queuedBytes -= nextBytes;
      consumption.droppedMessages += 1;
      continue;
    }
    if (batch.length > 0 && batchBytes + nextBytes > KAFKA_MESSAGE_LIMITS.batchBytes) {
      break;
    }
    consumption.messages.shift();
    consumption.queuedBytes -= nextBytes;
    batch.push(next);
    batchBytes += nextBytes;
  }
  return batch;
}
