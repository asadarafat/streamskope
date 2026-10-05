import {
  HOST_PROTOCOL_VERSION,
  KAFKA_PROVIDER_EVENT_CODEC,
  parseExternalUrlOpenRequest,
  parseExternalUrlOpenResult,
  parseCorrelatedHostResponse,
  parseHostCommand,
  type ExternalUrlOpenResult,
  type HostCommandResponse,
  type HostCommand,
  type StreamSkopeHost,
} from "../../../features/kafka/contracts";

import { EXTERNAL_URL_OPEN_CHANNEL, providerIpcChannels } from "./channels";
import { createPreloadProviderWire, type PreloadIpcRenderer } from "./provider-wire";

export type { PreloadIpcRenderer } from "./provider-wire";

export interface PreloadContextBridge {
  exposeInMainWorld(name: string, value: unknown): void;
}

export function createStreamSkopePreloadHost(ipcRenderer: PreloadIpcRenderer): StreamSkopeHost {
  const wire = createPreloadProviderWire(
    ipcRenderer,
    providerIpcChannels("kafka"),
    KAFKA_PROVIDER_EVENT_CODEC,
  );
  return {
    execute: async <Command extends HostCommand>(
      value: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      const submitted = { ...value };
      const command = parseHostCommand(submitted);
      const response = await wire.invoke(command);
      return parseCorrelatedHostResponse(response, submitted);
    },
    openExternalUrl: async (url): Promise<ExternalUrlOpenResult> => {
      const request = parseExternalUrlOpenRequest({
        url,
        version: HOST_PROTOCOL_VERSION,
      });
      return parseExternalUrlOpenResult(
        await ipcRenderer.invoke(EXTERNAL_URL_OPEN_CHANNEL, request),
      );
    },
    subscribe: wire.subscribe,
  };
}

export function exposeStreamSkopeHost(
  contextBridge: PreloadContextBridge,
  ipcRenderer: PreloadIpcRenderer,
): void {
  const kafka = createStreamSkopePreloadHost(ipcRenderer);
  contextBridge.exposeInMainWorld("streamSkopeHost", kafka);
  contextBridge.exposeInMainWorld("streamSkopeProviders", Object.freeze({ kafka }));
}
