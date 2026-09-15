import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostCommand,
  type HostEvent,
  type SecureConnectionInput,
} from "../../src/kafka/contracts";
import { createKafkaBackend } from "../../src/main";
import { startDevelopmentHost } from "../../src/platform/dev-host";
import { loadFixtureConfig, loadFixtureConnection } from "../support/kafka-fixture";

const RENDERER_ORIGIN = "http://127.0.0.1:4173";
const INVOCATION_TOKEN = "0123456789abcdef0123456789abcdef";

async function readEventsUntil(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  predicate: (events: readonly HostEvent[]) => boolean,
): Promise<readonly HostEvent[]> {
  const events: HostEvent[] = [];
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const result = await reader.read();
    if (result.done) {
      throw new Error("Development-host event stream ended before expected events arrived.");
    }
    buffer += decoder.decode(result.value, { stream: true });
    let boundary = buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = block
        .split("\n")
        .filter((line) => line.startsWith("data: "))
        .map((line) => line.slice(6))
        .join("\n");
      if (data.length > 0) {
        events.push(parseHostEvent(JSON.parse(data) as unknown));
        if (predicate(events)) {
          return events;
        }
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
}

describe("real browser-development Kafka path", () => {
  it("connects through the authenticated HTTP contract and emits safe operational activity", async () => {
    const config = await loadFixtureConfig();
    const fixture = await loadFixtureConnection();
    const connection: SecureConnectionInput = {
      brokers: [fixture.kafkaEndpoint],
      name: "Local aio",
      oauth: {
        clientId: config.oauthClientId,
        clientSecret: config.oauthClientSecret,
        scope: config.oauthScope,
        tokenEndpoint: fixture.oauthEndpoint,
      },
      tls: {
        caPem: await readFile(fixture.caPath, "utf8"),
        enabled: true,
      },
    };
    const backend = createKafkaBackend();
    const host = await startDevelopmentHost({
      backend,
      port: 0,
      rendererOrigin: RENDERER_ORIGIN,
      token: INVOCATION_TOKEN,
    });
    const eventController = new AbortController();

    try {
      const eventResponse = await fetch(`${host.origin}/events`, {
        headers: {
          accept: "text/event-stream",
          origin: RENDERER_ORIGIN,
          "x-streamskope-token": INVOCATION_TOKEN,
        },
        signal: eventController.signal,
      });
      if (eventResponse.body === null) {
        throw new Error("Development host returned no event body.");
      }
      const reader = eventResponse.body.getReader();
      const command: HostCommand = {
        command: "connection.connect",
        id: "real-browser-connect",
        payload: connection,
        version: HOST_PROTOCOL_VERSION,
      };
      const commandResponse = await fetch(`${host.origin}/commands`, {
        body: JSON.stringify(command),
        headers: {
          "content-type": "application/json",
          origin: RENDERER_ORIGIN,
          "x-streamskope-token": INVOCATION_TOKEN,
        },
        method: "POST",
      });

      expect(commandResponse.status).toBe(200);
      await expect(commandResponse.json()).resolves.toMatchObject({
        command: "connection.connect",
        id: "real-browser-connect",
        ok: true,
      });
      const events = await readEventsUntil(
        reader,
        (received) =>
          received.some(
            (event) => event.event === "connection.state" && event.payload.state === "connected",
          ) && received.some((event) => event.event === "activity.recorded"),
      );
      expect(events).toContainEqual(
        expect.objectContaining({
          event: "connection.state",
          payload: {
            connectionName: "Local aio",
            state: "connecting",
          },
        }),
      );
      expect(events).toContainEqual(
        expect.objectContaining({
          event: "connection.state",
          payload: {
            connectionName: "Local aio",
            state: "connected",
          },
        }),
      );
      expect(events.find((event) => event.event === "activity.recorded")).toMatchObject({
        event: "activity.recorded",
        payload: {
          object: "Local aio",
          operation: "Connect",
          outcome: "succeeded",
        },
      });
      expect(JSON.stringify(events)).not.toContain(config.oauthClientSecret);
      expect(JSON.stringify(events)).not.toContain("access_token");
    } finally {
      eventController.abort();
      await host.close();
    }
  }, 20_000);
});
