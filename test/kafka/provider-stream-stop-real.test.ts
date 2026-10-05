import { Producer } from "@platformatic/kafka";
import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostCommand,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import { startDevelopmentHost, type RunningDevelopmentHost } from "../../src/platform/dev-host";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { readKafkaEventsUntil } from "../support/kafka-sse-events";

it("stops a real Kafka reader after the final HTTP client leaves and retains the usable connection", async () => {
  const fixture = await startAuthorizationFixture();
  const topic = "provider-stream-stop";
  const backend = createKafkaBackend();
  const events: HostEvent[] = [];
  const unsubscribe = backend.subscribe((event) => events.push(parseHostEvent(event)));
  const producer = new Producer({
    bootstrapBrokers: [...fixture.connection.brokers],
    clientId: "streamskope-provider-stop-proof",
    autocreateTopics: false,
    retries: 0,
  });
  const rendererOrigin = "http://127.0.0.1:4173";
  const token = "0123456789abcdef0123456789abcdef";
  const eventController = new AbortController();
  let host: RunningDevelopmentHost | undefined;
  const headers = { origin: rendererOrigin, "x-streamskope-token": token };
  const command = (value: HostCommand): Promise<Response> => {
    if (host === undefined) throw new Error("The owned host has not started.");
    return fetch(`${host.origin}/commands`, {
      method: "POST",
      headers: { ...headers, "content-type": "application/json" },
      body: JSON.stringify(value),
    });
  };
  // A failed observation cannot leave a test-owned HTTP reader alive indefinitely.
  const eventDeadline = setTimeout(() => eventController.abort(), 25_000);
  try {
    host = await startDevelopmentHost({ backend, port: 0, rendererOrigin, token });
    await fixture.admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    const response = await fetch(`${host.origin}/events`, {
      headers: { ...headers, accept: "text/event-stream" },
      signal: eventController.signal,
    });
    expect(response.status).toBe(200);
    if (response.body === null) throw new Error("The real event stream has no body.");
    const reader = response.body.getReader();
    const connected = await command({
      command: "connection.connect",
      id: "owned-connect",
      payload: fixture.connection,
      version: HOST_PROTOCOL_VERSION,
    });
    expect(connected.status).toBe(200);
    await expect(connected.json()).resolves.toMatchObject({ ok: true });
    const started = await command({
      command: "messages.start",
      id: "owned-tail",
      payload: { topic, mode: "tail", maxMessages: 1_000 },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(started.status).toBe(200);
    await expect(started.json()).resolves.toMatchObject({ ok: true });
    await producer.send({
      messages: [{ topic, key: Buffer.from("proof"), value: Buffer.from("owned-stop-record") }],
    });
    const observed = await readKafkaEventsUntil(reader, (received) =>
      received.some(
        (event) =>
          event.event === "messages.batch" &&
          event.payload.messages.some((message) => message.payload === "owned-stop-record"),
      ),
    );
    expect(observed.some((event) => event.event === "messages.batch")).toBe(true);
    clearTimeout(eventDeadline);
    eventController.abort();
    await expect
      .poll(
        () =>
          events.some(
            (event) =>
              event.event === "consumption.state" &&
              event.payload.state === "stopped" &&
              event.payload.request?.topic === topic,
          ),
        { timeout: 15_000, interval: 50 },
      )
      .toBe(true);
    expect(backend.connectionSnapshot().state).toBe("connected");
    // The stopped terminal is observed independently of the socket, and recovery must
    // also permit real broker metadata work through the same authenticated route.
    await expect
      .poll(
        async () => {
          const listed = await command({
            command: "topics.list",
            id: "after-confirmed-stop",
            payload: {},
            version: HOST_PROTOCOL_VERSION,
          });
          return listed.status === 200 && ((await listed.json()) as { ok: boolean }).ok;
        },
        { timeout: 10_000, interval: 50 },
      )
      .toBe(true);
    expect(
      events.some(
        (event) =>
          event.event === "topics.changed" &&
          event.payload.state === "ready" &&
          event.payload.topics.includes(topic),
      ),
    ).toBe(true);
  } finally {
    clearTimeout(eventDeadline);
    eventController.abort();
    unsubscribe();
    const cleanup = await Promise.allSettled([
      host?.close() ?? backend.shutdown(),
      producer.close(),
    ]);
    const fixtureCleanup = await Promise.allSettled([fixture.dispose()]);
    const failures = [...cleanup, ...fixtureCleanup].flatMap((result) =>
      result.status === "rejected" ? [result.reason as unknown] : [],
    );
    if (failures.length > 0)
      throw new AggregateError(failures, "Owned Kafka stop fixture cleanup failed.");
  }
}, 120_000);
