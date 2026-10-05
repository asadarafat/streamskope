import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  KAFKA_PROVIDER_EVENT_CODEC,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostEvent,
  type StreamSkopeBackend,
} from "../../features/kafka/contracts";

import { SseClientEventQueue } from "./kafka-sse-event-queue";
import { createProviderEndpoint, type ProviderWireEndpoint } from "./provider-host";

/** Kafka retains its codec and record queue policy behind the host's sealed route. */
export function createKafkaProviderEndpoint(
  backend: StreamSkopeBackend & { shutdown?(): Promise<void>; stopStream?(): Promise<void> },
): ProviderWireEndpoint {
  const stopStream = backend.stopStream?.bind(backend);
  return createProviderEndpoint({
    id: "kafka",
    version: HOST_PROTOCOL_VERSION,
    parseCommand: parseHostCommand,
    commandErrorSummary: (error): string =>
      error instanceof HostContractValidationError ? error.message : "Provider command is invalid.",
    execute: (command) => backend.execute(command),
    correlateResponse: (wire, command) => parseCorrelatedHostResponse(wire, command),
    parseEvent: parseHostEvent,
    subscribe: (listener) => backend.subscribe(listener),
    createEventQueue: (options) => new SseClientEventQueue(options),
    availability: KAFKA_PROVIDER_EVENT_CODEC.availability,
    ...(stopStream === undefined ? {} : { stopStream }),
    shutdown: (): Promise<void> => backend.shutdown?.() ?? Promise.resolve(),
  });
}
