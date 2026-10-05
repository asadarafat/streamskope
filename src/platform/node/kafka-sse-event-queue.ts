import type { HostEvent } from "../../features/kafka/contracts";

import {
  AccountedProviderEventQueue,
  type DeliveryFailure,
  type ProviderDeliveryBegin,
  type ProviderDeliveryCosts,
  type ProviderDeliveryQueue,
} from "./accounted-provider-event-queue";
import { createKafkaSseDeliveryPolicy } from "./kafka-delivery-policy";

export interface SseClientEventQueueOptions {
  readonly maxEvents: number;
  readonly maxMessageBytes: number;
  readonly maxMessages: number;
  readonly maxSerializedBytes?: number;
}

/** Kafka's retained record bounds remain separate from its serialized event budget. */
export class SseClientEventQueue implements ProviderDeliveryQueue<HostEvent> {
  private readonly queue: ProviderDeliveryQueue<HostEvent>;

  constructor(options: SseClientEventQueueOptions) {
    this.queue = new AccountedProviderEventQueue({
      limits: {
        maxEvents: options.maxEvents,
        maxSerializedBytes: options.maxSerializedBytes ?? 8 * 1024 * 1024,
        maxRecords: options.maxMessages,
        maxRecordBytes: options.maxMessageBytes,
      },
      overflow: "evict-oldest-pending-records",
      policy: createKafkaSseDeliveryPolicy(),
    });
  }

  get costs(): ProviderDeliveryCosts {
    return this.queue.costs;
  }

  enqueue(event: HostEvent): DeliveryFailure | undefined {
    return this.queue.enqueue(event);
  }

  begin(): ProviderDeliveryBegin<HostEvent> {
    return this.queue.begin();
  }

  close(): void {
    this.queue.close();
  }
}
