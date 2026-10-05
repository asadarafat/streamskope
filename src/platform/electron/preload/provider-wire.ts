import type { ProviderEventCodec, ProviderWireEvent } from "../../providers/host";

import type { ProviderIpcChannels } from "./channels";

type PreloadEventListener = (event: unknown, value: unknown) => void;

export interface PreloadIpcRenderer {
  invoke(channel: string, value: unknown): Promise<unknown>;
  on(channel: string, listener: PreloadEventListener): void;
  removeListener(channel: string, listener: PreloadEventListener): void;
}

/** Private serialization transport. Only typed provider adapters expose renderer operations. */
export interface PreloadProviderWire<Event extends ProviderWireEvent> {
  readonly invoke: (value: unknown) => Promise<unknown>;
  readonly subscribe: (listener: (event: Event) => void) => () => void;
}

export function createPreloadProviderWire<Event extends ProviderWireEvent>(
  ipc: PreloadIpcRenderer,
  channels: ProviderIpcChannels,
  codec: ProviderEventCodec<Event>,
): PreloadProviderWire<Event> {
  const listeners = new Set<(event: Event) => void>();
  let lastSequence = -1;
  let generation = 0;
  let handleEvent: PreloadEventListener | undefined;
  let availability: Event | undefined;
  let unavailable = false;
  const publish = (event: Event): void => {
    if (codec.isAvailability(event)) availability = event;
    for (const listener of [...listeners]) {
      if (!listeners.has(listener)) continue;
      try {
        listener(event);
      } catch {
        /* Isolate a failing view from other consumers. */
      }
    }
  };
  const fail = (recovery: string, expectedGeneration: number): void => {
    if (generation !== expectedGeneration || listeners.size === 0 || unavailable) return;
    unavailable = true;
    if (handleEvent !== undefined) {
      ipc.removeListener(channels.event, handleEvent);
      handleEvent = undefined;
    }
    publish(codec.availability(++lastSequence, "unavailable", recovery));
  };
  const startEvents = (): void => {
    const current = ++generation;
    unavailable = false;
    handleEvent = (_event, value): void => {
      if (generation !== current || unavailable || listeners.size === 0) return;
      let event: Event;
      try {
        event = codec.parseEvent(value);
      } catch {
        fail("Desktop event validation failed. Reload the workbench to reconnect.", current);
        return;
      }
      lastSequence = Math.max(lastSequence, event.sequence);
      publish(event);
      if (generation !== current || unavailable || listeners.size === 0) return;
      void ipc.invoke(channels.acknowledge, event.sequence).catch(() => {
        fail("Desktop event acknowledgement failed. Reload the workbench to reconnect.", current);
      });
    };
    ipc.on(channels.event, handleEvent);
    void ipc
      .invoke(channels.subscribe, codec.version)
      .then((version) => {
        if (version !== codec.version)
          throw new Error("Unsupported desktop host subscription acknowledgement.");
      })
      .catch(() => {
        fail(
          "Reload the workbench or restart StreamSkope to reconnect to its desktop host.",
          current,
        );
      });
  };
  return {
    invoke: (value): Promise<unknown> =>
      unavailable
        ? Promise.reject(
            new Error(
              "Desktop provider event stream is unavailable. Reload before submitting commands.",
            ),
          )
        : ipc.invoke(channels.command, value),
    subscribe: (listener): (() => void) => {
      const subscriber = (event: Event): void => listener(event);
      listeners.add(subscriber);
      if (listeners.size === 1) startEvents();
      else if (availability !== undefined) {
        try {
          subscriber(availability);
        } catch {
          /* Keep subscription ownership intact. */
        }
      }
      let subscribed = true;
      return (): void => {
        if (!subscribed) return;
        subscribed = false;
        listeners.delete(subscriber);
        if (listeners.size === 0) {
          generation += 1;
          if (handleEvent !== undefined) ipc.removeListener(channels.event, handleEvent);
          handleEvent = undefined;
          availability = undefined;
          unavailable = false;
        }
      };
    },
  };
}
