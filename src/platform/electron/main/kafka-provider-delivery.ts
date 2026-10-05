import { randomUUID } from "node:crypto";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type StreamSkopeBackend,
} from "../../../features/kafka/contracts";

import { ElectronEventDelivery } from "./electron-event-delivery";
import type {
  ElectronProviderDelivery,
  ElectronProviderDeliveryBinding,
} from "./provider-delivery";

/** Kafka's established delivery and stop policy remains behind its typed native adapter. */
export function createKafkaElectronDeliveryBinding(
  backend: StreamSkopeBackend & { setMessagePresentationPaused?(paused: boolean): void },
): ElectronProviderDeliveryBinding {
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
    failureRecovery: async (reason): Promise<string> => {
      let stopped = false;
      try {
        const response = await backend.execute({
          command: "messages.stop",
          id: randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        stopped = response.ok;
      } catch {
        /* An uncertain stop has its own recovery instruction. */
      }
      return stopped
        ? `Renderer delivery failed (${reason}). Consumption stopped; records may be missing from this view. Reload the workbench and restart consumption.`
        : "Renderer delivery failed and consumption stop could not be confirmed. Restart StreamSkope before consuming again.";
    },
  };
}
