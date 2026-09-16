import {
  HOST_PROTOCOL_VERSION,
  parseExternalUrlOpenRequest,
  parseExternalUrlOpenResult,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostEvent,
  type ExternalUrlOpenResult,
  type HostCommandResponse,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../../features/kafka/contracts";

import {
  EXTERNAL_URL_OPEN_CHANNEL,
  HOST_COMMAND_CHANNEL,
  HOST_EVENT_CHANNEL,
  HOST_EVENT_ACK_CHANNEL,
  HOST_SUBSCRIBE_CHANNEL,
} from "./channels";

type PreloadEventListener = (event: unknown, value: unknown) => void;

export interface PreloadIpcRenderer {
  invoke(channel: string, value: unknown): Promise<unknown>;
  on(channel: string, listener: PreloadEventListener): void;
  removeListener(channel: string, listener: PreloadEventListener): void;
}

export interface PreloadContextBridge {
  exposeInMainWorld(name: string, value: unknown): void;
}

export function createStreamSkopePreloadHost(ipcRenderer: PreloadIpcRenderer): StreamSkopeHost {
  return {
    execute: async (value): Promise<HostCommandResponse> => {
      const command = parseHostCommand(value);
      const response = await ipcRenderer.invoke(HOST_COMMAND_CHANNEL, command);
      return parseCorrelatedHostResponse(response, command);
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
    subscribe: (listener: HostEventListener): (() => void) => {
      let subscribed = true;
      let lastSequence = -1;
      const handleEvent: PreloadEventListener = (_event, value): void => {
        const event = parseHostEvent(value);
        lastSequence = Math.max(lastSequence, event.sequence);
        listener(event);
        void ipcRenderer.invoke(HOST_EVENT_ACK_CHANNEL, event.sequence).catch(() => {
          if (!subscribed) return;
          listener({
            event: "backend.availability",
            payload: {
              state: "unavailable",
              recovery: "Desktop event acknowledgement failed. Reload the workbench to reconnect.",
            },
            sequence: lastSequence + 1,
            version: HOST_PROTOCOL_VERSION,
          });
        });
      };
      ipcRenderer.on(HOST_EVENT_CHANNEL, handleEvent);
      void ipcRenderer
        .invoke(HOST_SUBSCRIBE_CHANNEL, HOST_PROTOCOL_VERSION)
        .then((version) => {
          if (version !== HOST_PROTOCOL_VERSION) {
            throw new Error("Unsupported desktop host subscription acknowledgement.");
          }
        })
        .catch(() => {
          if (!subscribed) return;
          listener({
            event: "backend.availability",
            payload: {
              state: "unavailable",
              recovery:
                "Reload the workbench or restart StreamSkope to reconnect to its desktop host.",
            },
            sequence: lastSequence + 1,
            version: HOST_PROTOCOL_VERSION,
          });
        });
      return (): void => {
        if (!subscribed) {
          return;
        }
        subscribed = false;
        ipcRenderer.removeListener(HOST_EVENT_CHANNEL, handleEvent);
      };
    },
  };
}

export function exposeStreamSkopeHost(
  contextBridge: PreloadContextBridge,
  ipcRenderer: PreloadIpcRenderer,
): void {
  contextBridge.exposeInMainWorld("streamSkopeHost", createStreamSkopePreloadHost(ipcRenderer));
}
