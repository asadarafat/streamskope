import type { ProviderEventCodec, ProviderWireEvent } from "../../providers/host";
import { BROWSER_DEVELOPMENT_GATEWAY_PATH } from "../../providers/browser-development";

export class BrowserDevelopmentHostError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "BrowserDevelopmentHostError";
  }
}

export interface BrowserProviderTransport<Event extends ProviderWireEvent> {
  readonly invoke: (wire: unknown) => Promise<unknown>;
  readonly subscribe: (listener: (event: Event) => void) => () => void;
}

interface BrowserProviderTransportOptions<Event extends ProviderWireEvent> {
  readonly codec: ProviderEventCodec<Event>;
  /** Omit only for the compatibility Kafka route. IDs come from trusted composition. */
  readonly providerId?: string;
}

type BrowserTransportWindow = Partial<Pick<Window, "location">>;
type BrowserEventStreamState = "connecting" | "ready" | "unavailable";

interface BrowserEventStreamReadiness {
  state: BrowserEventStreamState;
  readonly settled: Promise<void>;
  transition(state: Exclude<BrowserEventStreamState, "connecting">): void;
}

function rendererOrigin(browserWindow: BrowserTransportWindow): string {
  const origin = browserWindow.location?.origin;
  if (origin === undefined) {
    throw new BrowserDevelopmentHostError("Browser renderer origin is unavailable.");
  }
  let parsed: URL;
  try {
    parsed = new URL(origin);
  } catch (error) {
    throw new BrowserDevelopmentHostError("Browser renderer origin must be an absolute URL.", {
      cause: error,
    });
  }
  if (
    parsed.origin !== origin ||
    !["http:", "https:"].includes(parsed.protocol) ||
    parsed.hostname.length === 0
  ) {
    throw new BrowserDevelopmentHostError(
      "Browser renderer origin must be one exact HTTP or HTTPS origin.",
    );
  }
  return origin;
}

function createReadiness(): BrowserEventStreamReadiness {
  let settle = (): void => undefined;
  const readiness: BrowserEventStreamReadiness = {
    state: "connecting",
    settled: new Promise<void>((resolve) => {
      settle = resolve;
    }),
    transition: (state): void => {
      if (readiness.state === "unavailable") return;
      const wasConnecting = readiness.state === "connecting";
      readiness.state = state;
      if (wasConnecting) settle();
    },
  };
  return readiness;
}

function eventData(block: string): string | null {
  const data = block
    .split("\n")
    .filter((line) => line.startsWith("data: "))
    .map((line) => line.slice(6))
    .join("\n");
  return data.length === 0 ? null : data;
}

function eventStreamBody(response: Response): ReadableStream<Uint8Array> {
  const body = response.body;
  if (body === null || !response.headers.get("content-type")?.startsWith("text/event-stream")) {
    throw new BrowserDevelopmentHostError("StreamSkope host did not provide an event stream.");
  }
  return body;
}

async function consumeEventStream<Event extends ProviderWireEvent>(
  response: Response,
  codec: ProviderEventCodec<Event>,
  listener: (event: Event) => void,
): Promise<void> {
  const reader = eventStreamBody(response).getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) return;
      buffer += decoder.decode(result.value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary >= 0) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const data = eventData(block);
        if (data !== null) listener(codec.parseEvent(JSON.parse(data) as unknown));
        boundary = buffer.indexOf("\n\n");
      }
    }
  } finally {
    reader.releaseLock();
  }
}

/** Host adapters validate their command/response contract around this private wire transport. */
export function createBrowserProviderTransport<Event extends ProviderWireEvent>(
  browserWindow: BrowserTransportWindow,
  options: BrowserProviderTransportOptions<Event>,
): BrowserProviderTransport<Event> {
  const origin = rendererOrigin(browserWindow);
  if (options.providerId !== undefined && !/^[a-z][a-z0-9-]{0,31}$/u.test(options.providerId)) {
    throw new BrowserDevelopmentHostError(
      "Provider route must use a registered provider identifier.",
    );
  }
  const prefix = options.providerId === undefined ? "" : `/providers/${options.providerId}`;
  const gatewayUrl = (action: "commands" | "events"): string =>
    `${origin}${BROWSER_DEVELOPMENT_GATEWAY_PATH}${prefix}/${action}`;
  const { codec } = options;
  let readiness: BrowserEventStreamReadiness | undefined;
  const listeners = new Set<(event: Event) => void>();
  const deliver = (listener: (event: Event) => void, event: Event): void => {
    try {
      listener(event);
    } catch {
      // A failed view callback must not change transport availability or sibling delivery.
    }
  };
  let stopEventStream = (): void => undefined;
  let nextSequence = 0;
  const availability = (state: "ready" | "unavailable"): Event =>
    codec.availability(
      nextSequence,
      state,
      state === "unavailable"
        ? "Check the StreamSkope host, unlock its vault if needed, and reload."
        : undefined,
    );

  const startEventStream = (): void => {
    const controller = new AbortController();
    const currentReadiness = createReadiness();
    readiness = currentReadiness;
    let active = true;
    const emit = (event: Event): void => {
      if (!active) return;
      const sequenced = { ...event, sequence: nextSequence++ };
      for (const listener of [...listeners]) {
        if (listeners.has(listener)) deliver(listener, sequenced);
      }
    };
    const run = async (): Promise<void> => {
      try {
        const response = await fetch(gatewayUrl("events"), {
          cache: "no-store",
          credentials: "same-origin",
          headers: { accept: "text/event-stream" },
          mode: "same-origin",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new BrowserDevelopmentHostError(
            `StreamSkope gateway event stream failed with HTTP ${response.status}.`,
          );
        }
        eventStreamBody(response);
        if (!active) return;
        currentReadiness.transition("ready");
        emit(availability("ready"));
        await consumeEventStream(response, codec, emit);
      } catch {
        // Failed or unexpectedly completed streams make only this provider unavailable.
      } finally {
        controller.abort();
        if (active) {
          currentReadiness.transition("unavailable");
          emit(availability("unavailable"));
        }
      }
    };
    stopEventStream = (): void => {
      active = false;
      currentReadiness.transition("unavailable");
      controller.abort();
    };
    void run();
  };

  return {
    invoke: async (wire): Promise<unknown> => {
      const currentReadiness = readiness;
      if (currentReadiness !== undefined) {
        await currentReadiness.settled;
        if (currentReadiness.state !== "ready") {
          throw new BrowserDevelopmentHostError(
            "StreamSkope host event stream is unavailable. Reload before submitting commands.",
          );
        }
      }
      const response = await fetch(gatewayUrl("commands"), {
        body: JSON.stringify(wire),
        cache: "no-store",
        credentials: "same-origin",
        headers: { "content-type": "application/json" },
        method: "POST",
        mode: "same-origin",
      });
      if (!response.ok) {
        throw new BrowserDevelopmentHostError(
          `StreamSkope gateway rejected the command with HTTP ${response.status}.`,
        );
      }
      return (await response.json()) as unknown;
    },
    subscribe: (listener): (() => void) => {
      const subscription = (event: Event): void => listener(event);
      const first = listeners.size === 0;
      listeners.add(subscription);
      if (first) startEventStream();
      else if (readiness !== undefined && readiness.state !== "connecting") {
        const event = availability(readiness.state);
        nextSequence += 1;
        deliver(subscription, event);
      }
      return (): void => {
        if (listeners.delete(subscription) && listeners.size === 0) stopEventStream();
      };
    },
  };
}
