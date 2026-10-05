import type { ProviderEventCodec } from "../../../platform/providers/host";

import { NATS_PROTOCOL_VERSION, type NatsEvent } from "./types";
import { parseNatsEvent } from "./validation";

export const NATS_PROVIDER_EVENT_CODEC: ProviderEventCodec<NatsEvent> = {
  version: NATS_PROTOCOL_VERSION,
  parseEvent: parseNatsEvent,
  isAvailability: (event): boolean => event.event === "backend.availability",
  availability: (sequence, state, recovery): NatsEvent => ({
    version: NATS_PROTOCOL_VERSION,
    sequence,
    event: "backend.availability",
    payload:
      state === "ready"
        ? { state }
        : { state, recovery: recovery ?? "Restart StreamSkope and reconnect to its NATS host." },
  }),
};
