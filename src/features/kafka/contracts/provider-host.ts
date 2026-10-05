import type { ProviderEventCodec } from "../../../platform/providers/host";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "./types";
import { parseHostEvent } from "./validation";

export const KAFKA_PROVIDER_EVENT_CODEC: ProviderEventCodec<HostEvent> = {
  version: HOST_PROTOCOL_VERSION,
  parseEvent: parseHostEvent,
  isAvailability: (event): boolean => event.event === "backend.availability",
  availability: (sequence, state, recovery): HostEvent => ({
    event: "backend.availability",
    payload:
      state === "ready"
        ? { state }
        : { state, recovery: recovery ?? "Restart StreamSkope and reconnect to its host." },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  }),
};
