// @vitest-environment jsdom
// @vitest-environment-options {"url":"http://127.0.0.1/"}

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  BrowserDevelopmentHostError,
  createBrowserProviderTransport,
} from "../../src/platform/electron/renderer/browser-provider-transport";
import { createProviderFixture, type FixtureEvent } from "../support/provider-fixture";

const cleanups: (() => void)[] = [];

afterEach(() => {
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
  vi.unstubAllGlobals();
});

function inputUrl(input: RequestInfo | URL): string {
  return typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
}

function commandBody(init: RequestInit | undefined): unknown {
  if (typeof init?.body !== "string") throw new Error("Expected a JSON command body.");
  return JSON.parse(init.body) as unknown;
}

function controlledStream(): {
  readonly response: Response;
  readonly emit: (wire: unknown) => void;
  readonly close: () => void;
} {
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  let closed = false;
  const response = new Response(
    new ReadableStream<Uint8Array>({
      start(streamController): void {
        controller = streamController;
        streamController.enqueue(new TextEncoder().encode(": ready\n\n"));
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
  const close = (): void => {
    if (!closed) {
      closed = true;
      controller?.close();
    }
  };
  cleanups.push(close);
  return {
    response,
    emit: (wire): void => {
      controller?.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(wire)}\n\n`));
    },
    close,
  };
}

describe("browser provider wire isolation", () => {
  it("uses exact named same-origin routes and keeps readiness and sequences independent", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    const streams = { alpha: controlledStream(), beta: controlledStream() };
    const fetchMock = vi.fn<typeof fetch>((input, init) => {
      const url = new URL(inputUrl(input));
      const fixture = url.pathname.includes("/providers/alpha/") ? alpha : beta;
      if (url.pathname.endsWith("/events")) {
        return Promise.resolve(fixture === alpha ? streams.alpha.response : streams.beta.response);
      }
      const command = fixture.parseCommand(commandBody(init));
      return Promise.resolve(
        new Response(JSON.stringify(fixture.response(command)), {
          headers: { "content-type": "application/json" },
        }),
      );
    });
    vi.stubGlobal("fetch", fetchMock);
    const alphaTransport = createBrowserProviderTransport(window, {
      providerId: "alpha",
      codec: alpha.codec,
    });
    const betaTransport = createBrowserProviderTransport(window, {
      providerId: "beta",
      codec: beta.codec,
    });
    const alphaEvents: FixtureEvent[] = [];
    const betaEvents: FixtureEvent[] = [];
    cleanups.push(alphaTransport.subscribe((event) => alphaEvents.push(event)));
    cleanups.push(betaTransport.subscribe((event) => betaEvents.push(event)));
    await vi.waitFor(() => {
      expect(alphaEvents).toHaveLength(1);
      expect(betaEvents).toHaveLength(1);
    });
    streams.alpha.emit(alpha.event("alpha-value", 99));
    streams.beta.emit(beta.event("beta-value", 999));
    await vi.waitFor(() => {
      expect(alphaEvents).toHaveLength(2);
      expect(betaEvents).toHaveLength(2);
    });
    expect(alphaEvents[1]).toEqual({ ...alpha.event("alpha-value"), sequence: 1 });
    expect(betaEvents[1]).toEqual({ ...beta.event("beta-value"), sequence: 1 });
    streams.alpha.emit(beta.event("foreign-provider"));
    await vi.waitFor(() =>
      expect(alphaEvents.at(-1)).toMatchObject({
        type: "fixture.availability",
        state: "unavailable",
        sequence: 2,
      }),
    );
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
    expect(fetchMock.mock.calls[1]?.[1]?.signal?.aborted).toBe(false);
    await expect(alphaTransport.invoke(alpha.command())).rejects.toBeInstanceOf(
      BrowserDevelopmentHostError,
    );
    const betaCommand = beta.parseCommand(beta.command());
    expect(beta.correlateResponse(await betaTransport.invoke(betaCommand), betaCommand)).toEqual(
      beta.response(betaCommand),
    );
    streams.beta.emit(beta.event("beta-still-live", 1));
    await vi.waitFor(() =>
      expect(betaEvents.at(-1)).toEqual({ ...beta.event("beta-still-live"), sequence: 2 }),
    );
    expect(fetchMock.mock.calls.map(([input]) => inputUrl(input))).toEqual([
      "http://127.0.0.1/__streamskope_host/providers/alpha/events",
      "http://127.0.0.1/__streamskope_host/providers/beta/events",
      "http://127.0.0.1/__streamskope_host/providers/beta/commands",
    ]);
    for (const [, init] of fetchMock.mock.calls) {
      expect(init).toMatchObject({ credentials: "same-origin", mode: "same-origin" });
      expect(new Headers(init?.headers).has("x-streamskope-token")).toBe(false);
    }
  });

  it("waits for only its own stream, rejects failed readiness and never invokes that provider", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    const betaStream = controlledStream();
    let failAlpha = (_response: Response): void => undefined;
    const alphaResponse = new Promise<Response>((resolve) => {
      failAlpha = resolve;
    });
    const fetchMock = vi.fn<typeof fetch>((input, init) => {
      const url = inputUrl(input);
      if (url.endsWith("/alpha/events")) return alphaResponse;
      if (url.endsWith("/beta/events")) return Promise.resolve(betaStream.response);
      const command = beta.parseCommand(commandBody(init));
      return Promise.resolve(new Response(JSON.stringify(beta.response(command))));
    });
    vi.stubGlobal("fetch", fetchMock);
    const alphaTransport = createBrowserProviderTransport(window, {
      providerId: "alpha",
      codec: alpha.codec,
    });
    const betaTransport = createBrowserProviderTransport(window, {
      providerId: "beta",
      codec: beta.codec,
    });
    cleanups.push(alphaTransport.subscribe(() => undefined));
    cleanups.push(betaTransport.subscribe(() => undefined));
    const pendingAlpha = alphaTransport.invoke(alpha.command());
    const rejectedAlpha = expect(pendingAlpha).rejects.toThrow("event stream is unavailable");
    const betaCommand = beta.command();
    expect(beta.correlateResponse(await betaTransport.invoke(betaCommand), betaCommand)).toEqual(
      beta.response(betaCommand),
    );
    expect(fetchMock).toHaveBeenCalledTimes(3);
    failAlpha(new Response(null, { status: 503 }));
    await rejectedAlpha;
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("isolates failed view callbacks from sibling listeners and provider availability", async () => {
    const fixture = createProviderFixture({ id: "alpha", version: 7 });
    const stream = controlledStream();
    const fetchMock = vi.fn<typeof fetch>((input, init) => {
      if (inputUrl(input).endsWith("/events")) return Promise.resolve(stream.response);
      const command = fixture.parseCommand(commandBody(init));
      return Promise.resolve(new Response(JSON.stringify(fixture.response(command))));
    });
    vi.stubGlobal("fetch", fetchMock);
    const transport = createBrowserProviderTransport(window, {
      providerId: "alpha",
      codec: fixture.codec,
    });
    const events: FixtureEvent[] = [];
    cleanups.push(
      transport.subscribe(() => {
        throw new Error("failed view");
      }),
    );
    cleanups.push(transport.subscribe((event) => events.push(event)));
    await vi.waitFor(() => expect(events).toHaveLength(1));
    // A callback added after readiness cannot throw through subscribe either.
    cleanups.push(
      transport.subscribe(() => {
        throw new Error("failed late view");
      }),
    );
    stream.emit(fixture.event("visible"));
    await vi.waitFor(() =>
      expect(events.at(-1)).toMatchObject({
        type: "fixture.value",
        value: "visible",
      }),
    );
    const command = fixture.command();
    expect(fixture.correlateResponse(await transport.invoke(command), command)).toEqual(
      fixture.response(command),
    );
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(false);
  });

  it("refuses unsafe IDs and unavailable origins before opening any route", () => {
    const fixture = createProviderFixture({ id: "alpha", version: 7 });
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    for (const providerId of ["../kafka", "alpha/commands", "Alpha", ""]) {
      expect(() =>
        createBrowserProviderTransport(window, { providerId, codec: fixture.codec }),
      ).toThrow("registered provider identifier");
    }
    expect(() =>
      createBrowserProviderTransport({}, { providerId: "alpha", codec: fixture.codec }),
    ).toThrow("origin is unavailable");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
