import { KAFKA_MESSAGE_LIMITS, type HostEvent } from "../../../features/kafka/contracts";
import { AccountedProviderEventQueue } from "../../node/accounted-provider-event-queue";
import { createKafkaIpcDeliveryPolicy } from "../../node/kafka-delivery-policy";

import {
  ElectronAcknowledgedEventDelivery,
  type ElectronDeliveryFailure,
} from "./provider-delivery";

/** Kafka's native driver preserves its established ACK, pressure and coalescing policy. */
export class ElectronEventDelivery extends ElectronAcknowledgedEventDelivery<HostEvent> {
  constructor(
    send: (event: HostEvent) => void,
    onFailure: (reason: ElectronDeliveryFailure) => void,
    onPressure: (paused: boolean) => void = () => undefined,
  ) {
    const limits = {
      maxEvents: 64,
      maxSerializedBytes: 8 * 1024 * 1024,
      maxRecords: KAFKA_MESSAGE_LIMITS.queuedMessages,
    };
    super({
      queue: new AccountedProviderEventQueue({
        limits,
        overflow: "reject",
        policy: createKafkaIpcDeliveryPolicy(),
      }),
      limits,
      send,
      onFailure,
      onPressure,
    });
  }
}
