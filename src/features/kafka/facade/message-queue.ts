import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  kafkaMessageRetainedBytes,
  utf8ByteLength,
} from "../contracts";

import type { ActiveFacadeConsumption, QueuedFacadeMessage } from "./facade-support";

// Reserve the actual topic plus the largest supported counters in each event envelope.
export function facadeMessageBatchEnvelopeBytes(consumption: ActiveFacadeConsumption): number {
  return utf8ByteLength(
    JSON.stringify({
      event: "messages.batch",
      payload: {
        droppedMessages: Number.MAX_SAFE_INTEGER,
        messages: [],
        topic: consumption.request.topic,
      },
      sequence: Number.MAX_SAFE_INTEGER,
      version: HOST_PROTOCOL_VERSION,
    }),
  );
}

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
    // Attribute each omission once; count capacity takes precedence if both are exceeded.
    const reason =
      consumption.messages.length > consumption.streamTuning.queueDepth
        ? "countCapacity"
        : "byteCapacity";
    const dropped = consumption.messages.shift();
    if (dropped === undefined) return;
    consumption.queuedBytes -= kafkaMessageRetainedBytes(dropped.message);
    consumption.droppedMessages += 1;
    consumption.streamMonitoring.dropReasons[reason] += 1;
  }
}

/** Finish a bounded terminal drain without retaining an orphaned display queue. */
export function discardFacadeMessages(consumption: ActiveFacadeConsumption): void {
  consumption.droppedMessages += consumption.messages.length;
  consumption.streamMonitoring.dropReasons.terminalDiscarded += consumption.messages.length;
  consumption.messages.length = 0;
  consumption.queuedBytes = 0;
}

export function takeFacadeMessageBatch(
  consumption: ActiveFacadeConsumption,
  maximumMessages = consumption.streamTuning.batchSize,
  maximumSerializedBytes: number = KAFKA_MESSAGE_LIMITS.batchBytes,
): readonly QueuedFacadeMessage[] {
  const batch: QueuedFacadeMessage[] = [];
  let batchBytes = 0;
  const envelopeBytes = facadeMessageBatchEnvelopeBytes(consumption);
  let serializedBytes = envelopeBytes;
  while (batch.length < maximumMessages && consumption.messages.length > 0) {
    const next = consumption.messages[0];
    if (next === undefined) break;
    const nextBytes = kafkaMessageRetainedBytes(next.message);
    if (
      nextBytes > KAFKA_MESSAGE_LIMITS.batchBytes ||
      next.serializedBytes + envelopeBytes > KAFKA_MESSAGE_LIMITS.batchBytes
    ) {
      consumption.messages.shift();
      consumption.queuedBytes -= nextBytes;
      consumption.droppedMessages += 1;
      consumption.streamMonitoring.dropReasons.oversized += 1;
      continue;
    }
    if (
      batchBytes + nextBytes > KAFKA_MESSAGE_LIMITS.batchBytes ||
      serializedBytes + next.serializedBytes > maximumSerializedBytes
    )
      break;
    consumption.messages.shift();
    consumption.queuedBytes -= nextBytes;
    batch.push(next);
    batchBytes += nextBytes;
    serializedBytes += next.serializedBytes;
  }
  return batch;
}
