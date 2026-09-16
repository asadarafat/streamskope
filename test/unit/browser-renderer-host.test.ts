// @vitest-environment jsdom
// @vitest-environment-options {"url":"http://127.0.0.1/"}

import { cleanup } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  createBrowserDevelopmentHost,
  resolveStreamSkopeHost,
} from "../../src/platform/electron/renderer/host";

const RUNBOOK_URL = "https://runbooks.example.test/kafka/latency?cluster=local#recovery";
const GATEWAY_ORIGIN = "http://127.0.0.1/__streamskope_host";

function fetchInputUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") {
    return input;
  }
  return input instanceof URL ? input.href : input.url;
}

class FakeElectronHost implements StreamSkopeHost {
  execute(_command: HostCommand): Promise<HostCommandResponse> {
    throw new Error("Not used.");
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(_listener: HostEventListener): () => void {
    return (): void => undefined;
  }
}

function successfulDisconnectResponse(): HostCommandResponse {
  return {
    command: "connection.disconnect",
    id: "request-1",
    ok: true,
    result: { correlationId: "correlation-request-1" },
    version: HOST_PROTOCOL_VERSION,
  };
}

function sseResponse(event: HostEvent): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller): void {
      controller.enqueue(encoder.encode(`: ready\n\ndata: ${JSON.stringify(event)}\n\n`));
      controller.close();
    },
  });
  return new Response(body, {
    headers: { "content-type": "text/event-stream" },
    status: 200,
  });
}

function controlledSseResponse(): {
  readonly close: () => void;
  readonly response: Response;
} {
  let streamController: ReadableStreamDefaultController<Uint8Array> | undefined;
  const response = new Response(
    new ReadableStream<Uint8Array>({
      start(controller): void {
        streamController = controller;
        controller.enqueue(new TextEncoder().encode(": ready\n\n"));
      },
    }),
    {
      headers: { "content-type": "text/event-stream" },
      status: 200,
    },
  );
  return {
    close: (): void => {
      streamController?.close();
    },
    response,
  };
}

async function waitFor(predicate: () => boolean, milliseconds = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > milliseconds) {
      throw new Error("Timed out waiting for browser-host events.");
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  window.history.replaceState({}, "", "/");
  window.sessionStorage.clear();
  delete window.streamSkopeHost;
});

describe("browser renderer host", () => {
  it("keeps one live event stream when a transient profile subscriber closes", async () => {
    const stream = controlledSseResponse();
    const fetchMock = vi.fn<typeof fetch>((input) =>
      Promise.resolve(
        fetchInputUrl(input).endsWith("/events")
          ? stream.response
          : new Response(JSON.stringify(successfulDisconnectResponse()), { status: 200 }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    const events: HostEvent[] = [];
    const stopRoot = host.subscribe((event) => events.push(event));
    await waitFor(() => events.length > 0);
    const transientEvents: HostEvent[] = [];
    const stopEditor = host.subscribe((event) => transientEvents.push(event));
    await waitFor(() => transientEvents.length > 0);
    stopEditor();
    await expect(
      host.execute({
        command: "connection.disconnect",
        id: "request-1",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toEqual(successfulDisconnectResponse());
    expect(
      fetchMock.mock.calls.filter(([input]) => fetchInputUrl(input).endsWith("/events")),
    ).toHaveLength(1);
    expect(transientEvents[0]).toMatchObject({
      event: "backend.availability",
      payload: { state: "ready" },
    });
    const signal = fetchMock.mock.calls[0]?.[1]?.signal;
    expect(signal?.aborted).toBe(false);
    stream.close();
    await waitFor(() => events.at(-1)?.event === "backend.availability" && events.length === 2);
    expect(events.at(-1)).toMatchObject({ payload: { state: "unavailable" } });
    expect(transientEvents).toHaveLength(1);
    stopRoot();
    expect(signal?.aborted).toBe(true);
  });

  it("uses the clean renderer origin without exposing a browser host credential", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify(successfulDisconnectResponse()), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState({}, "", "/");

    const host = resolveStreamSkopeHost(window);
    await expect(
      host.execute({
        command: "connection.disconnect",
        id: "request-1",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toEqual(successfulDisconnectResponse());

    expect(fetchMock).toHaveBeenCalledOnce();
    const [url, init] = fetchMock.mock.calls[0] ?? [];
    expect(url).toBe("http://127.0.0.1/__streamskope_host/commands");
    expect(init).toMatchObject({
      headers: { "content-type": "application/json" },
      method: "POST",
      mode: "same-origin",
    });
    expect(init?.headers).not.toHaveProperty("x-streamskope-token");
    expect(window.location.hash).toBe("");
    expect(window.sessionStorage).toHaveLength(0);
  });

  it("sends a same-origin gateway command and correlates its response", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify(successfulDisconnectResponse()), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    const command = {
      command: "connection.disconnect",
      id: "request-1",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    } as const;

    await expect(host.execute(command)).resolves.toEqual(successfulDisconnectResponse());
    expect(fetchMock).toHaveBeenCalledOnce();
    const [url, init] = fetchMock.mock.calls[0] ?? [];
    expect(url).toBe("http://127.0.0.1/__streamskope_host/commands");
    expect(init).toMatchObject({
      credentials: "same-origin",
      headers: {
        "content-type": "application/json",
      },
      method: "POST",
      mode: "same-origin",
    });
    expect(init?.body).toBeTypeOf("string");
    if (typeof init?.body !== "string") {
      throw new Error("Expected a serialized command body.");
    }
    expect(init.body).not.toContain("undefined");
  });

  it("requests only an exact HTTPS runbook in an isolated browser context", async () => {
    const open = vi.fn<Window["open"]>(() => null);
    const host = createBrowserDevelopmentHost({ location: window.location, open });
    const rendererLocation = window.location.href;

    await expect(host.openExternalUrl(RUNBOOK_URL)).resolves.toEqual({
      state: "accepted",
      version: HOST_PROTOCOL_VERSION,
    });
    expect(open).toHaveBeenCalledExactlyOnceWith(RUNBOOK_URL, "_blank", "noopener,noreferrer");
    expect(window.location.href).toBe(rendererLocation);
  });

  it("reports a synchronous browser rejection and rejects unsafe URLs without navigating", async () => {
    const open = vi.fn<Window["open"]>(() => {
      throw new Error("browser rejected request");
    });
    const host = createBrowserDevelopmentHost({ location: window.location, open });
    const rendererLocation = window.location.href;

    await expect(host.openExternalUrl(RUNBOOK_URL)).rejects.toThrow(
      "Browser rejected the external runbook request.",
    );
    await expect(host.openExternalUrl("file:///tmp/private-runbook.html")).rejects.toBeInstanceOf(
      Error,
    );
    await expect(
      host.openExternalUrl("https://operator:private@runbooks.example.test/kafka"),
    ).rejects.toBeInstanceOf(Error);
    expect(open).toHaveBeenCalledOnce();
    expect(window.location.href).toBe(rendererLocation);
  });

  it("waits for the browser event stream before submitting an evidence-producing command", async () => {
    const stream = controlledSseResponse();
    let resolveEventResponse: ((response: Response) => void) | undefined;
    const eventResponse = new Promise<Response>((resolveResponse) => {
      resolveEventResponse = resolveResponse;
    });
    const fetchMock = vi.fn<typeof fetch>((input) => {
      return fetchInputUrl(input).endsWith("/events")
        ? eventResponse
        : Promise.resolve(
            new Response(JSON.stringify(successfulDisconnectResponse()), {
              headers: { "content-type": "application/json" },
              status: 200,
            }),
          );
    });
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    const unsubscribe = host.subscribe(() => undefined);
    const execution = host.execute({
      command: "connection.disconnect",
      id: "request-1",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    await waitFor(() => fetchMock.mock.calls.length > 0);
    expect(fetchMock.mock.calls.map(([input]) => fetchInputUrl(input))).toEqual([
      `${GATEWAY_ORIGIN}/events`,
    ]);

    resolveEventResponse?.(stream.response);
    await expect(execution).resolves.toEqual(successfulDisconnectResponse());
    expect(fetchMock.mock.calls.map(([input]) => fetchInputUrl(input))).toEqual([
      `${GATEWAY_ORIGIN}/events`,
      `${GATEWAY_ORIGIN}/commands`,
    ]);

    unsubscribe();
    stream.close();
  });

  it("does not submit a command when the browser event stream cannot start", async () => {
    const fetchMock = vi.fn<typeof fetch>((input) => {
      return Promise.resolve(
        fetchInputUrl(input).endsWith("/events")
          ? new Response(null, { status: 503 })
          : new Response(JSON.stringify(successfulDisconnectResponse()), {
              headers: { "content-type": "application/json" },
              status: 200,
            }),
      );
    });
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    const events: HostEvent[] = [];
    host.subscribe((event) => {
      events.push(event);
    });

    await expect(
      host.execute({
        command: "connection.disconnect",
        id: "request-1",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).rejects.toThrow("event stream is unavailable");
    expect(fetchMock.mock.calls.map(([input]) => fetchInputUrl(input))).toEqual([
      `${GATEWAY_ORIGIN}/events`,
    ]);
    expect(events.at(-1)).toMatchObject({
      event: "backend.availability",
      payload: { state: "unavailable" },
    });
  });

  it("reports SSE readiness, validates events, and reports an unexpected stream end", async () => {
    const backendEvent: HostEvent = {
      event: "connection.state",
      payload: {
        connectionName: null,
        state: "disconnected",
      },
      sequence: 9,
      version: HOST_PROTOCOL_VERSION,
    };
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(sseResponse(backendEvent)));
    const host = createBrowserDevelopmentHost(window);
    const events: HostEvent[] = [];

    host.subscribe((event) => {
      events.push(event);
    });
    await waitFor(() =>
      events.some(
        (event) => event.event === "backend.availability" && event.payload.state === "unavailable",
      ),
    );

    expect(events.map((event) => event.event)).toEqual([
      "backend.availability",
      "connection.state",
      "backend.availability",
    ]);
    expect(events[0]).toMatchObject({
      payload: { state: "ready" },
      sequence: 0,
    });
    expect(events[1]).toMatchObject({
      payload: { state: "disconnected" },
      sequence: 1,
    });
    expect(events[2]).toMatchObject({
      payload: { state: "unavailable" },
      sequence: 2,
    });
  });

  it("uses the preload host in Electron and does not inspect a browser fragment", () => {
    const electronHost = new FakeElectronHost();
    window.streamSkopeHost = electronHost;
    window.history.replaceState({}, "", "/#untouched");

    expect(resolveStreamSkopeHost(window)).toBe(electronHost);
    expect(window.location.hash).toBe("#untouched");
  });
});
