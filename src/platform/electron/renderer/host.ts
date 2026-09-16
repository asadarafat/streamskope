import {
  BROWSER_DEVELOPMENT_GATEWAY_PATH,
  HOST_PROTOCOL_VERSION,
  parseCorrelatedHostResponse,
  parseExternalUrlOpenRequest,
  parseHostCommand,
  parseHostEvent,
  type ExternalUrlOpenRequest,
  type ExternalUrlOpenResult,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../../features/kafka/contracts";

declare global {
  interface Window {
    streamSkopeHost?: StreamSkopeHost;
  }
}

export class BrowserDevelopmentHostError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "BrowserDevelopmentHostError";
  }
}

type BrowserDevelopmentWindow = Pick<Window, "open"> & Partial<Pick<Window, "location">>;

function browserRendererOrigin(browserWindow: BrowserDevelopmentWindow): string {
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
  if (parsed.origin !== origin || parsed.protocol !== "http:" || parsed.hostname.length === 0) {
    throw new BrowserDevelopmentHostError(
      "Browser renderer origin must be one exact HTTP development origin.",
    );
  }
  return origin;
}

function gatewayUrl(rendererOrigin: string, path: "/commands" | "/events"): string {
  return `${rendererOrigin}${BROWSER_DEVELOPMENT_GATEWAY_PATH}${path}`;
}

function resequence(event: HostEvent, sequence: number): HostEvent {
  return { ...event, sequence };
}

function availabilityEvent(state: "ready" | "unavailable", sequence: number): HostEvent {
  return {
    event: "backend.availability",
    payload:
      state === "ready"
        ? { state }
        : {
            recovery: "Restart the local StreamSkope development host and reload.",
            state,
          },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function eventData(block: string): string | null {
  const data = block
    .split("\n")
    .filter((line) => line.startsWith("data: "))
    .map((line) => line.slice(6))
    .join("\n");
  return data.length === 0 ? null : data;
}

type BrowserEventStreamState = "connecting" | "ready" | "unavailable";

interface BrowserEventStreamReadiness {
  state: BrowserEventStreamState;
  readonly settled: Promise<void>;
  transition(state: Exclude<BrowserEventStreamState, "connecting">): void;
}

function createEventStreamReadiness(): BrowserEventStreamReadiness {
  let settle = (): void => undefined;
  const readiness: BrowserEventStreamReadiness = {
    state: "connecting",
    settled: new Promise<void>((resolve) => {
      settle = resolve;
    }),
    transition: (state): void => {
      if (readiness.state === "unavailable") {
        return;
      }
      const wasConnecting = readiness.state === "connecting";
      readiness.state = state;
      if (wasConnecting) {
        settle();
      }
    },
  };
  return readiness;
}

function eventStreamBody(response: Response): ReadableStream<Uint8Array> {
  const body = response.body;
  if (body === null || !response.headers.get("content-type")?.startsWith("text/event-stream")) {
    throw new BrowserDevelopmentHostError("Development host did not provide an event stream.");
  }
  return body;
}

async function consumeEventStream(
  response: Response,
  listener: (event: HostEvent) => void,
): Promise<void> {
  const reader = eventStreamBody(response).getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) {
        return;
      }
      buffer += decoder.decode(result.value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary >= 0) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const data = eventData(block);
        if (data !== null) {
          listener(parseHostEvent(JSON.parse(data) as unknown));
        }
        boundary = buffer.indexOf("\n\n");
      }
    }
  } finally {
    reader.releaseLock();
  }
}

export function createBrowserDevelopmentHost(
  browserWindow: BrowserDevelopmentWindow = window,
): StreamSkopeHost {
  const rendererOrigin = browserRendererOrigin(browserWindow);
  let eventStreamReadiness: BrowserEventStreamReadiness | undefined;
  const listeners = new Set<HostEventListener>();
  let stopEventStream = (): void => undefined;
  let nextSequence = 0;
  const subscribe = (listener: HostEventListener): (() => void) => {
    const subscription: HostEventListener = (event) => listener(event);
    const first = listeners.size === 0;
    listeners.add(subscription);
    if (first) {
      startEventStream();
    } else if (
      eventStreamReadiness?.state !== undefined &&
      eventStreamReadiness.state !== "connecting"
    ) {
      subscription(availabilityEvent(eventStreamReadiness.state, nextSequence++));
    }
    return (): void => {
      if (listeners.delete(subscription) && listeners.size === 0) {
        stopEventStream();
      }
    };
  };
  const startEventStream = (): void => {
    const controller = new AbortController();
    const readiness = createEventStreamReadiness();
    eventStreamReadiness = readiness;
    let active = true;
    const emit = (event: HostEvent): void => {
      if (!active) return;
      const sequenced = resequence(event, nextSequence++);
      for (const listener of listeners) listener(sequenced);
    };
    const run = async (): Promise<void> => {
      try {
        const response = await fetch(gatewayUrl(rendererOrigin, "/events"), {
          cache: "no-store",
          credentials: "same-origin",
          headers: { accept: "text/event-stream" },
          mode: "same-origin",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new BrowserDevelopmentHostError(
            `Development gateway event stream failed with HTTP ${response.status}.`,
          );
        }
        eventStreamBody(response);
        if (!active) return;
        readiness.transition("ready");
        emit(availabilityEvent("ready", nextSequence));
        await consumeEventStream(response, emit);
      } catch {
        // Stream failure and unexpected completion both make commands unavailable.
      } finally {
        if (active) {
          readiness.transition("unavailable");
          emit(availabilityEvent("unavailable", nextSequence));
        }
      }
    };
    stopEventStream = (): void => {
      active = false;
      readiness.transition("unavailable");
      controller.abort();
    };
    void run();
  };
  return {
    execute: async (value): Promise<HostCommandResponse> => {
      const command = parseHostCommand(value);
      const readiness = eventStreamReadiness;
      if (readiness !== undefined) {
        await readiness.settled;
        if (readiness.state !== "ready") {
          throw new BrowserDevelopmentHostError(
            "Development-host event stream is unavailable. Reload before submitting commands.",
          );
        }
      }
      const response = await fetch(gatewayUrl(rendererOrigin, "/commands"), {
        body: JSON.stringify(command),
        cache: "no-store",
        credentials: "same-origin",
        headers: {
          "content-type": "application/json",
        },
        method: "POST",
        mode: "same-origin",
      });
      if (!response.ok) {
        throw new BrowserDevelopmentHostError(
          `Development gateway rejected the command with HTTP ${response.status}.`,
        );
      }
      return parseCorrelatedHostResponse((await response.json()) as unknown, command);
    },
    openExternalUrl: (url): Promise<ExternalUrlOpenResult> => {
      let request: ExternalUrlOpenRequest;
      try {
        request = parseExternalUrlOpenRequest({
          url,
          version: HOST_PROTOCOL_VERSION,
        });
      } catch (error) {
        return Promise.reject(
          error instanceof Error
            ? error
            : new BrowserDevelopmentHostError("External runbook request is invalid."),
        );
      }
      try {
        browserWindow.open(request.url, "_blank", "noopener,noreferrer");
      } catch (error) {
        return Promise.reject(
          new BrowserDevelopmentHostError("Browser rejected the external runbook request.", {
            cause: error,
          }),
        );
      }
      return Promise.resolve({
        state: "accepted",
        version: HOST_PROTOCOL_VERSION,
      });
    },
    subscribe,
  };
}

export function resolveStreamSkopeHost(browserWindow: Window): StreamSkopeHost {
  if (browserWindow.streamSkopeHost !== undefined) {
    return browserWindow.streamSkopeHost;
  }
  if (browserWindow.location.hash.length > 0 || browserWindow.location.search.length > 0) {
    browserWindow.history.replaceState(
      {},
      browserWindow.document.title,
      browserWindow.location.pathname,
    );
  }
  return createBrowserDevelopmentHost(browserWindow);
}
