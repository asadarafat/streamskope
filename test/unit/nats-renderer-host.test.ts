// @vitest-environment jsdom
// @vitest-environment-options {"url":"http://127.0.0.1/"}

import { waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  NATS_PROTOCOL_VERSION,
  type NatsEvent,
  type NatsHost,
} from "../../src/features/nats/contracts";
import {
  createBrowserNatsHost,
  resolveNatsWorkspaceSource,
} from "../../src/platform/electron/renderer/nats-host";
import { createBrowserDevelopmentHost } from "../../src/platform/electron/renderer/host";

const command = {
  command: "subscription.stop",
  id: "stop-one",
  payload: {},
  version: NATS_PROTOCOL_VERSION,
} as const;
const receipt = {
  command: command.command,
  id: command.id,
  version: NATS_PROTOCOL_VERSION,
  ok: true,
  result: {
    correlationId: "stop-receipt",
    subscription: {
      revision: 0,
      state: "idle",
      generation: null,
      subject: null,
      counters: {
        receivedRecords: 0,
        applicationOmittedRecords: 0,
        publishedRecords: 0,
        queuedRecords: 0,
        queuedBytes: 0,
        transportOmittedRecords: 0,
      },
    },
  },
};

afterEach(() => {
  vi.unstubAllGlobals();
  delete window.streamSkopeProviders;
  delete window.streamSkopeHost;
  delete window.streamSkopeDesktop;
});

describe("Core NATS renderer host", () => {
  it("uses the named route and rejects wrong correlation, Kafka protocol and extra response fields", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response(JSON.stringify(receipt)));
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserNatsHost(window);
    await expect(host.execute(command)).resolves.toEqual(receipt);
    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1/__streamskope_host/providers/nats/commands",
      expect.objectContaining({
        body: expect.any(String),
        credentials: "same-origin",
        mode: "same-origin",
      }),
    );
    const submitted: unknown = JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body));
    expect(submitted).toEqual(command);
    for (const invalid of [
      { ...receipt, id: "another-request" },
      { ...receipt, version: 49 },
      { ...receipt, kafka: true },
    ]) {
      fetchMock.mockResolvedValueOnce(new Response(JSON.stringify(invalid)));
      await expect(host.execute(command)).rejects.toThrow();
    }
    const beforeInvalidCommand = fetchMock.mock.calls.length;
    await expect(
      host.execute({ ...command, payload: { unexpected: true } } as typeof command),
    ).rejects.toThrow();
    expect(fetchMock).toHaveBeenCalledTimes(beforeInvalidCommand);
  });

  it("validates NATS SSE strictly and marks this host unavailable before refusing new commands", async () => {
    const invalid = { event: "connection.state", version: 49, sequence: 0, payload: {} };
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller): void {
            controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(invalid)}\n\n`));
            controller.close();
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserNatsHost(window);
    const events: NatsEvent[] = [];
    const unsubscribe = host.subscribe((event) => events.push(event));
    await waitFor(() => expect(events.at(-1)).toMatchObject({ payload: { state: "unavailable" } }));
    expect(events).toEqual([
      expect.objectContaining({
        version: NATS_PROTOCOL_VERSION,
        event: "backend.availability",
        payload: { state: "ready" },
      }),
      expect.objectContaining({
        version: NATS_PROTOCOL_VERSION,
        event: "backend.availability",
        payload: { state: "unavailable", recovery: expect.any(String) },
      }),
    ]);
    await expect(host.execute(command)).rejects.toThrow("event stream is unavailable");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe(
      "http://127.0.0.1/__streamskope_host/providers/nats/events",
    );
    unsubscribe();
  });

  it("takes a named native port and never starts HTTP when a native host lacks NATS", () => {
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    const native: NatsHost = {
      execute: () => Promise.reject(new Error("Native dispatch was not expected.")),
      subscribe: () => (): void => undefined,
    };
    const kafka = createBrowserDevelopmentHost(window);
    window.streamSkopeProviders = { kafka, nats: native };
    expect(resolveNatsWorkspaceSource(window)).toEqual({ state: "ready", host: native });
    window.streamSkopeProviders = { kafka };
    expect(resolveNatsWorkspaceSource(window)).toMatchObject({ state: "unavailable" });
    expect(fetchMock).not.toHaveBeenCalled();
    delete window.streamSkopeProviders;
    window.streamSkopeHost = kafka;
    expect(resolveNatsWorkspaceSource(window)).toMatchObject({ state: "unavailable" });
    expect(fetchMock).not.toHaveBeenCalled();
    delete window.streamSkopeHost;
    const source = resolveNatsWorkspaceSource(window);
    expect(source.state).toBe("ready");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
