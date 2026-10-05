import {
  NATS_PROVIDER_EVENT_CODEC,
  parseNatsCommand,
  parseCorrelatedNatsResponse,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsHost,
} from "../../../features/nats/contracts";

import { providerIpcChannels } from "./channels";
import { createPreloadProviderWire, type PreloadIpcRenderer } from "./provider-wire";

export function createNatsPreloadHost(ipcRenderer: PreloadIpcRenderer): NatsHost {
  const wire = createPreloadProviderWire(
    ipcRenderer,
    providerIpcChannels("nats"),
    NATS_PROVIDER_EVENT_CODEC,
  );
  return {
    execute: async <Command extends NatsCommand>(
      value: Command,
    ): Promise<NatsCommandResponse<Command["command"]>> => {
      const submitted = { ...value };
      const command = parseNatsCommand(submitted);
      return parseCorrelatedNatsResponse(await wire.invoke(command), submitted);
    },
    subscribe: wire.subscribe,
  };
}
