import {
  HOST_PROTOCOL_VERSION,
  parseExternalUrlOpenRequest,
  parseExternalUrlOpenResult,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostEvent,
  type ExternalUrlOpenResult,
  type HostCommandResponse,
  type HostCommand,
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
  // One upstream subscription and acknowledgement stream per renderer. Additional
  // workbench/plugin consumers must not reset the host's pending delivery queue.
  const listeners = new Set<HostEventListener>();
  let lastSequence = -1;
  let generation = 0;
  let handleEvent: PreloadEventListener | undefined;
  let availability:
    Extract<Parameters<HostEventListener>[0], { event: "backend.availability" }> | undefined;
  const publish = (event: Parameters<HostEventListener>[0]): void => {
    if (event.event === "backend.availability") availability = event;
    for (const listener of [...listeners]) {
      if (!listeners.has(listener)) continue;
      try {
        listener(event);
      } catch {
        /* Isolate a failing view from other consumers. */
      }
    }
  };
  const unavailable = (recovery: string, expectedGeneration: number): void => {
    if (generation !== expectedGeneration || listeners.size === 0) return;
    publish({
      event: "backend.availability",
      payload: { state: "unavailable", recovery },
      sequence: ++lastSequence,
      version: HOST_PROTOCOL_VERSION,
    });
  };
  const startEvents = (): void => {
    const current = ++generation;
    handleEvent = (_event, value): void => {
      const event = parseHostEvent(value);
      lastSequence = Math.max(lastSequence, event.sequence);
      publish(event);
      void ipcRenderer.invoke(HOST_EVENT_ACK_CHANNEL, event.sequence).catch(() => {
        unavailable(
          "Desktop event acknowledgement failed. Reload the workbench to reconnect.",
          current,
        );
      });
    };
    ipcRenderer.on(HOST_EVENT_CHANNEL, handleEvent);
    void ipcRenderer
      .invoke(HOST_SUBSCRIBE_CHANNEL, HOST_PROTOCOL_VERSION)
      .then((version) => {
        if (version !== HOST_PROTOCOL_VERSION)
          throw new Error("Unsupported desktop host subscription acknowledgement.");
      })
      .catch(() => {
        unavailable(
          "Reload the workbench or restart StreamSkope to reconnect to its desktop host.",
          current,
        );
      });
  };
  return {
    execute: async <Command extends HostCommand>(
      value: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      const submitted = { ...value };
      const command = parseHostCommand(submitted);
      const response = await ipcRenderer.invoke(HOST_COMMAND_CHANNEL, command);
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
    subscribe: (listener: HostEventListener): (() => void) => {
      // A separate wrapper allows the same callback to hold independent subscriptions.
      const subscriber: HostEventListener = (event) => listener(event);
      listeners.add(subscriber);
      if (listeners.size === 1) startEvents();
      else if (availability !== undefined) {
        try {
          subscriber(availability);
        } catch {
          /* A failing view must not interrupt subscription management. */
        }
      }
      let subscribed = true;
      return (): void => {
        if (!subscribed) return;
        subscribed = false;
        listeners.delete(subscriber);
        if (listeners.size === 0 && handleEvent !== undefined) {
          generation += 1;
          ipcRenderer.removeListener(HOST_EVENT_CHANNEL, handleEvent);
          handleEvent = undefined;
          availability = undefined;
        }
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
