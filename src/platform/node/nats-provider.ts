import {
  NATS_PROVIDER_EVENT_CODEC,
  NATS_PROTOCOL_VERSION,
  parseNatsCommand,
  parseCorrelatedNatsResponse,
  parseNatsEvent,
  type NatsHost,
} from "../../features/nats/contracts";

import { AccountedProviderEventQueue } from "./accounted-provider-event-queue";
import { createNatsDeliveryPolicy } from "./nats-delivery-policy";
import { createProviderEndpoint, type ProviderWireEndpoint } from "./provider-host";

export function createNatsProviderEndpoint(
  backend: NatsHost & { stopStream(): Promise<void>; shutdown(): Promise<void> },
): ProviderWireEndpoint {
  return createProviderEndpoint({
    id: "nats",
    version: NATS_PROTOCOL_VERSION,
    parseCommand: parseNatsCommand,
    commandErrorSummary: () => "NATS command is invalid. Check the profile or subscription input.",
    execute: (command) => backend.execute(command),
    correlateResponse: parseCorrelatedNatsResponse,
    parseEvent: parseNatsEvent,
    subscribe: (listener) => backend.subscribe(listener),
    availability: NATS_PROVIDER_EVENT_CODEC.availability,
    createEventQueue: (options) =>
      new AccountedProviderEventQueue({
        limits: {
          maxEvents: options.maxEvents,
          maxSerializedBytes: options.maxSerializedBytes ?? 8 * 1024 * 1024,
          maxRecords: options.maxMessages,
          maxRecordBytes: options.maxMessageBytes,
        },
        overflow: "evict-oldest-pending-records",
        policy: createNatsDeliveryPolicy(),
      }),
    stopStream: () => backend.stopStream(),
    shutdown: () => backend.shutdown(),
  });
}
