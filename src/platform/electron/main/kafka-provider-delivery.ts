import { parseHostEvent } from "../../../features/kafka/contracts";

import { ElectronEventDelivery } from "./electron-event-delivery";
import type {
  ElectronProviderDelivery,
  ElectronProviderDeliveryBinding,
} from "./provider-delivery";

/** Kafka's typed native adapter owns presentation pressure, not a second stop operation. */
export function createKafkaElectronDeliveryBinding(backend: {
  setMessagePresentationPaused?(paused: boolean): void;
}): ElectronProviderDeliveryBinding {
  return {
    id: "kafka",
    create: (send, fail): ElectronProviderDelivery => {
      const delivery = new ElectronEventDelivery(send, fail, (paused) =>
        backend.setMessagePresentationPaused?.(paused),
      );
      return {
        enqueue: (event): void => delivery.enqueue(parseHostEvent(event)),
        acknowledge: (sequence): void => delivery.acknowledge(sequence),
        close: (): void => delivery.close(),
      };
    },
    recoveryInstruction: (confirmed, reason): string =>
      confirmed
        ? `Renderer delivery failed (${reason}). Consumption stopped; records may be missing from this view. Reload the workbench and restart consumption.`
        : "Renderer delivery failed and consumption stop could not be confirmed. Restart StreamSkope before consuming again.",
  };
}
