import {
  kafkaMessageRetainedBytes,
  type HostEvent,
  type KafkaExploredMessage,
} from "../../features/kafka/contracts";

import type { ProviderDeliveryPolicy } from "./accounted-provider-event-queue";

type KafkaDeliveryPolicy = ProviderDeliveryPolicy<HostEvent, KafkaExploredMessage>;

const recordProjection: Pick<
  KafkaDeliveryPolicy,
  "records" | "retainedRecordBytes" | "withRecords"
> = {
  records: (event): readonly KafkaExploredMessage[] | undefined =>
    event.event === "messages.batch" ? event.payload.messages : undefined,
  retainedRecordBytes: kafkaMessageRetainedBytes,
  withRecords: (event, messages): HostEvent =>
    event.event === "messages.batch"
      ? { ...event, payload: { ...event.payload, messages } }
      : event,
};

/** Existing IPC ordering and unsent observation replacement; overflow never omits records. */
export function createKafkaIpcDeliveryPolicy(): KafkaDeliveryPolicy {
  return {
    ...recordProjection,
    startsGeneration: (): boolean => false,
    decorateDrops: (event): HostEvent => event,
    decorationReserveBytes: (): number => 0,
    replacementKey: (event): string | undefined =>
      event.event === "streamMetrics.changed" && event.payload.state !== "loading"
        ? JSON.stringify(event.payload.operationId)
        : undefined,
  };
}

/** Per-client newest-record retention with numeric loss evidence owned by each generation. */
export function createKafkaSseDeliveryPolicy(): KafkaDeliveryPolicy {
  return {
    ...recordProjection,
    startsGeneration: (event): boolean =>
      event.event === "consumption.state" && event.payload.state === "loading",
    decorateDrops: (event, dropped): HostEvent => {
      if (event.event !== "messages.batch" && event.event !== "consumption.state") return event;
      const total = event.payload.droppedMessages + dropped;
      if (!Number.isSafeInteger(total) || total < 0)
        throw new RangeError("Transport omission evidence exceeded the safe counter limit.");
      if (event.event === "messages.batch")
        return { ...event, payload: { ...event.payload, droppedMessages: total } };
      return { ...event, payload: { ...event.payload, droppedMessages: total } };
    },
    decorationReserveBytes: (event): number =>
      event.event === "messages.batch" || event.event === "consumption.state"
        ? 16 - String(event.payload.droppedMessages).length
        : 0,
    replacementKey: (): undefined => undefined,
  };
}
