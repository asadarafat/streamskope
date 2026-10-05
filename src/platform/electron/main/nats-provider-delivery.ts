import { parseNatsEvent } from "../../../features/nats/contracts";
import { AccountedProviderEventQueue } from "../../node/accounted-provider-event-queue";
import { createNatsDeliveryPolicy } from "../../node/nats-delivery-policy";

import {
  ElectronAcknowledgedEventDelivery,
  type ElectronProviderDelivery,
  type ElectronProviderDeliveryBinding,
} from "./provider-delivery";

export function createNatsElectronDeliveryBinding(): ElectronProviderDeliveryBinding {
  return {
    id: "nats",
    create: (send, fail): ElectronProviderDelivery => {
      const limits = {
        maxEvents: 64,
        maxSerializedBytes: 8 * 1024 * 1024,
        maxRecords: 1000,
        maxRecordBytes: 8 * 1024 * 1024,
      };
      const delivery = new ElectronAcknowledgedEventDelivery({
        queue: new AccountedProviderEventQueue({
          limits,
          overflow: "reject",
          policy: createNatsDeliveryPolicy(),
        }),
        limits,
        send,
        onFailure: fail,
      });
      return {
        enqueue: (event) => delivery.enqueue(parseNatsEvent(event)),
        acknowledge: (sequence) => delivery.acknowledge(sequence),
        close: () => delivery.close(),
      };
    },
    recoveryInstruction: (confirmed) =>
      confirmed
        ? "Renderer delivery failed. The NATS subscription stopped; live records may be missing. Reload and start a new subscription. Core NATS cannot replay them."
        : "Renderer delivery failed and NATS subscription cleanup could not be confirmed. Restart StreamSkope before subscribing again.",
  };
}
