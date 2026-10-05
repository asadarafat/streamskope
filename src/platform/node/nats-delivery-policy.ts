import {
  natsRecordRetainedBytes,
  type NatsEvent,
  type NatsRecord,
} from "../../features/nats/contracts";

import type { ProviderDeliveryPolicy } from "./accounted-provider-event-queue";

/** Core NATS has no replay: each client receives explicit evidence of omitted live records. */
export function createNatsDeliveryPolicy(): ProviderDeliveryPolicy<NatsEvent, NatsRecord> {
  return {
    records: (event) => (event.event === "records.batch" ? event.payload.records : undefined),
    retainedRecordBytes: natsRecordRetainedBytes,
    withRecords: (event, records) =>
      event.event === "records.batch"
        ? { ...event, payload: { ...event.payload, records } }
        : event,
    startsGeneration: (event) =>
      event.event === "subscription.changed" && event.payload.state === "loading",
    decorateDrops: (event, dropped): NatsEvent => {
      if (event.event !== "records.batch" && event.event !== "subscription.changed") return event;
      const total = event.payload.counters.transportOmittedRecords + dropped;
      if (!Number.isSafeInteger(total) || total < 0)
        throw new RangeError("NATS transport omission counter exceeded its safe limit.");
      if (dropped === 0) return event;
      if (event.event === "records.batch")
        return {
          ...event,
          payload: {
            ...event.payload,
            counters: { ...event.payload.counters, transportOmittedRecords: total },
          },
        };
      return {
        ...event,
        payload: {
          ...event.payload,
          counters: { ...event.payload.counters, transportOmittedRecords: total },
        },
      };
    },
    decorationReserveBytes: (event) =>
      event.event === "records.batch" || event.event === "subscription.changed"
        ? 16 - String(event.payload.counters.transportOmittedRecords).length
        : 0,
    replacementKey: () => undefined,
  };
}
