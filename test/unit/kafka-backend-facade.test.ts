import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type HostEvent,
  type KafkaFetchRequest,
} from "../../src/features/kafka/contracts";
import { type KafkaActiveConnection, type SchemaRegistryPort } from "../../src/features/kafka/application";
import { KafkaEngineFailure } from "../../src/features/kafka/engine";
import {
  command,
  ControlledMessageStream,
  createFacade,
  fetchCommand,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
  tailRequest,
} from "../support/kafka-backend-facade-fixture";

afterEach(() => {
  vi.useRealTimers();
});

describe("Kafka backend facade", () => {
  it("retains messages while transport presentation is paused and resumes in order", async () => {
    const stream = new ControlledMessageStream();
    const connection = new RecordingActiveConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const flushes: Array<() => void> = [];
    const facade = createFacade(port, (flush) => {
      flushes.push(flush);
    });
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    facade.setMessagePresentationPaused(true);
    stream.push(message("1"));
    await settleAsyncIteration();
    stream.push(message("2"));
    await settleAsyncIteration();
    expect(flushes).toHaveLength(0);
    expect(events.filter((event) => event.event === "messages.batch")).toHaveLength(0);
    facade.setMessagePresentationPaused(false);
    expect(flushes).toHaveLength(1);
    flushes.shift()?.();
    const batches = events.filter((event) => event.event === "messages.batch");
    expect(
      batches.flatMap((event) => event.payload.messages.map((record) => record.offset)),
    ).toEqual(["1", "2"]);
    await facade.execute(command("messages.stop", "stop"));
  });
  it("routes Schema Registry commands through the configured service port", async () => {
    const activeConnection = new RecordingActiveConnection();
    activeConnection.clusterServiceContexts.schemaRegistry = {
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
      baseUrl: "http://schema.local:8081",
      caPem: "fixture-ca",
    };
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const listSubjects = vi.fn<SchemaRegistryPort["listSubjects"]>(() =>
      Promise.resolve({ omittedSubjects: 0, subjects: ["test-value"] }),
    );
    const schemaRegistry: SchemaRegistryPort = {
      checkCompatibility: () => Promise.reject(new Error("not expected")),
      delete: () => Promise.reject(new Error("not expected")),
      listSubjects,
      loadLatestSubject: () => Promise.reject(new Error("not expected")),
      loadSubject: () => Promise.reject(new Error("not expected")),
      register: () => Promise.reject(new Error("not expected")),
    };
    const facade = createFacade(port, undefined, undefined, undefined, undefined, schemaRegistry);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));

    await expect(
      facade.execute({
        command: "schemas.list",
        id: "request-schemas",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });

    expect(listSubjects).toHaveBeenCalledOnce();
    expect(events.filter((event) => event.event === "schemas.changed").at(-1)).toMatchObject({
      payload: {
        endpoint: "http://schema.local:8081",
        state: "ready",
        subjects: ["test-value"],
      },
    });
  });

  it("tests a connection and publishes confirmed, correlated activity without changing active state", async () => {
    const port = new RecordingConnectionPort();
    port.testOperations.push(() =>
      Promise.resolve({
        checks: ["oauth", "tls", "kafka-authentication", "metadata"],
        topicCount: 1,
      }),
    );
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });

    await expect(facade.execute(command("connection.test", "request-test"))).resolves.toEqual({
      command: "connection.test",
      id: "request-test",
      ok: true,
      result: { correlationId: "correlation-1" },
      version: HOST_PROTOCOL_VERSION,
    });

    expect(events.map((event) => event.event)).toEqual([
      "backend.availability",
      "activity.recorded",
    ]);
    expect(events[1]).toMatchObject({
      event: "activity.recorded",
      payload: {
        correlationId: "correlation-1",
        object: "Local aio",
        operation: "Connection test",
        outcome: "succeeded",
        severity: "info",
        timestamp: "2026-07-25T13:00:00.000Z",
      },
    });
    expect(facade.connectionSnapshot()).toEqual({
      connectionName: null,
      state: "disconnected",
    });
  });

  it("publishes honest connecting and connected states around confirmed metadata", async () => {
    let resolveOpen: ((connection: KafkaActiveConnection) => void) | undefined;
    const opened = new Promise<KafkaActiveConnection>((resolve) => {
      resolveOpen = resolve;
    });
    const activeConnection = new RecordingActiveConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => opened);
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });

    const connecting = facade.execute(command("connection.connect", "request-connect"));

    expect(events.at(-1)).toMatchObject({
      event: "connection.state",
      payload: {
        connectionName: "Local aio",
        state: "connecting",
      },
    });
    resolveOpen?.(activeConnection);
    await expect(connecting).resolves.toMatchObject({
      ok: true,
      result: { correlationId: "correlation-1" },
    });
    expect(events.map((event) => event.event)).toEqual([
      "backend.availability",
      "latency.changed",
      "latency.history.changed",
      "consumerGroups.changed",
      "consumerGroup.changed",
      "schemas.changed",
      "schema.changed",
      "acls.changed",
      "transforms.changed",
      "transform.changed",
      "transformLogs.changed",
      "connection.state",
      "connection.state",
      "latency.changed",
      "activity.recorded",
    ]);
    expect(events[1]).toMatchObject({
      event: "latency.changed",
      payload: { evidence: null, request: null, state: "unavailable" },
    });
    expect(events[2]).toMatchObject({
      event: "latency.history.changed",
      payload: { connectionName: null, entries: [] },
    });
    expect(events[12]).toMatchObject({
      event: "connection.state",
      payload: {
        connectionName: "Local aio",
        state: "connected",
      },
    });
    expect(events[13]).toMatchObject({
      event: "latency.changed",
      payload: { evidence: null, request: null, state: "idle" },
    });
  });

  it("redacts a rejected secret from the command response and activity detail", async () => {
    const port = new RecordingConnectionPort();
    port.testOperations.push(() =>
      Promise.reject(
        new KafkaEngineFailure({
          cause: new Error("upstream echoed fixture-secret and client_secret=invalid-secret"),
          code: "OAUTH_REJECTED",
          recovery: "Check the OAuth credentials.",
          retryable: false,
          stage: "oauth",
          summary: "OAuth credentials were rejected.",
          target: "http://localhost:15000/token?client_secret=invalid-secret",
        }),
      ),
    );
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });

    const response = await facade.execute(command("connection.test", "request-failed"));
    const visible = JSON.stringify({ events, response });

    expect(response).toMatchObject({
      command: "connection.test",
      error: {
        activeStateChanged: false,
        code: "OAUTH_REJECTED",
        correlationId: "correlation-1",
        stage: "oauth",
      },
      id: "request-failed",
      ok: false,
    });
    expect(events.at(-1)).toMatchObject({
      event: "activity.recorded",
      payload: {
        outcome: "failed",
        severity: "error",
      },
    });
    expect(visible).not.toContain("fixture-secret");
    expect(visible).not.toContain("invalid-secret");
    expect(visible).not.toContain("upstream echoed");
    expect(visible).toContain("[REDACTED]");
  });

  it("disconnects and shuts down the application session through explicit lifecycle methods", async () => {
    const activeConnection = new RecordingActiveConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));

    await expect(
      facade.execute(command("connection.disconnect", "request-disconnect")),
    ).resolves.toMatchObject({ ok: true });
    expect(activeConnection.closeCalls).toBe(1);
    expect(events.filter((event) => event.event === "connection.state").at(-2)).toMatchObject({
      payload: { connectionName: "Local aio", state: "disconnecting" },
    });
    expect(events.filter((event) => event.event === "connection.state").at(-1)).toMatchObject({
      payload: { connectionName: null, state: "disconnected" },
    });

    await facade.shutdown();
    expect(events.at(-1)).toMatchObject({
      event: "backend.availability",
      payload: {
        recovery: "Restart StreamSkope to create a new application session.",
        state: "unavailable",
      },
    });
  });

  it("publishes loading and timestamped ready topic events from the active connection", async () => {
    const activeConnection = new RecordingActiveConnection();
    activeConnection.listOperations.push(() => Promise.resolve(["test", "audit.events"]));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));

    await expect(facade.execute(command("topics.list", "request-topics"))).resolves.toMatchObject({
      command: "topics.list",
      id: "request-topics",
      ok: true,
      result: { correlationId: "correlation-2" },
    });

    const topicEvents = events.filter((event) => event.event === "topics.changed");
    expect(topicEvents.map((event) => typeof event.sequence)).toEqual(["number", "number"]);
    expect(topicEvents.map(({ sequence: _sequence, ...event }) => event)).toEqual([
      {
        event: "topics.changed",
        payload: {
          refreshedAt: null,
          state: "loading",
          topics: [],
        },
        version: HOST_PROTOCOL_VERSION,
      },
      {
        event: "topics.changed",
        payload: {
          refreshedAt: "2026-07-25T13:00:00.000Z",
          state: "ready",
          topics: ["test", "audit.events"],
        },
        version: HOST_PROTOCOL_VERSION,
      },
    ]);
  });

  it("reports topic authorization denial without presenting an empty cluster", async () => {
    const activeConnection = new RecordingActiveConnection();
    activeConnection.listOperations.push(() =>
      Promise.reject(
        new KafkaEngineFailure({
          code: "AUTHORIZATION_DENIED",
          recovery: "Request permission for broker metadata access and retry.",
          retryable: false,
          stage: "authorization",
          summary: "Kafka denied metadata access.",
          target: "localhost:19093",
        }),
      ),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));

    const response = await facade.execute(command("topics.list", "request-topics"));

    expect(response).toMatchObject({
      command: "topics.list",
      error: {
        activeStateChanged: false,
        code: "AUTHORIZATION_DENIED",
        correlationId: "correlation-2",
        stage: "authorization",
      },
      id: "request-topics",
      ok: false,
    });
    expect(events.filter((event) => event.event === "topics.changed").at(-1)).toMatchObject({
      event: "topics.changed",
      payload: {
        error: {
          code: "AUTHORIZATION_DENIED",
          correlationId: "correlation-2",
        },
        refreshedAt: null,
        state: "denied",
        topics: [],
      },
    });
  });

  it("publishes start, batched delivery and acknowledged stop without late records", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const scheduledFlushes: Array<() => void> = [];
    const facade = createFacade(port, (flush) => {
      scheduledFlushes.push(flush);
    });
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));

    await expect(facade.execute(command("messages.start", "request-start"))).resolves.toMatchObject(
      {
        command: "messages.start",
        id: "request-start",
        ok: true,
      },
    );
    expect(
      events
        .filter((event) => event.event === "consumption.state")
        .map((event) => event.payload.state),
    ).toEqual(["loading", "streaming"]);

    stream.push(message("1"));
    await settleAsyncIteration();
    expect(scheduledFlushes).toHaveLength(1);
    scheduledFlushes.shift()?.();
    expect(events.filter((event) => event.event === "messages.batch")).toMatchObject([
      {
        event: "messages.batch",
        payload: {
          droppedMessages: 0,
          messages: [
            {
              ...message("1"),
              ruleEvaluation: {
                evaluatedRules: 0,
                state: "evaluated",
              },
            },
          ],
          topic: "test",
        },
      },
    ]);

    await expect(facade.execute(command("messages.stop", "request-stop"))).resolves.toMatchObject({
      command: "messages.stop",
      id: "request-stop",
      ok: true,
    });
    expect(stream.closeCalls).toBe(1);
    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: {
        droppedMessages: 0,
        receivedMessages: 1,
        request: tailRequest(),
        state: "stopped",
      },
    });

    stream.push(message("late"));
    await settleAsyncIteration();
    expect(events.filter((event) => event.event === "messages.batch")).toHaveLength(1);
  });

  it("publishes one coalesced aggregate sample for a scheduled multi-record flush", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const scheduledFlushes: Array<() => void> = [];
    const monotonicNow = vi
      .fn<() => number>()
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(10)
      .mockReturnValueOnce(20)
      .mockReturnValueOnce(25)
      .mockReturnValue(25);
    const facade = createFacade(
      port,
      (flush) => {
        scheduledFlushes.push(flush);
      },
      monotonicNow,
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    stream.push(message("1", "first-private-payload"));
    stream.push(message("2", "second-private-payload"));
    stream.push(message("3", "third-private-payload"));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(3);
    });
    expect(scheduledFlushes).toHaveLength(1);
    scheduledFlushes.shift()?.();

    const monitorEvents = events.filter((event) => event.event === "streamMetrics.changed");
    const delivered = monitorEvents.find(
      (event) => event.payload.delivery?.deliveredMessages === 3,
    );
    expect(delivered).toMatchObject({
      payload: {
        connectionName: "Local aio",
        delivery: {
          batchCount: 1,
          deliveredMessages: 3,
          lastBatchMessages: 3,
          messagesPerSecond: 120,
          publicationDurationMs: 5,
          queueWaitMs: 10,
          receivedMessages: 3,
        },
        queue: {
          capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
          capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
          currentBytes: 0,
          currentMessages: 0,
          droppedMessages: 0,
          droppedSincePrevious: 0,
          peakMessages: 3,
        },
        request: tailRequest(),
        sampledAt: "2026-07-25T13:00:00.000Z",
        state: "streaming",
        status: "nominal",
      },
    });
    expect(delivered?.payload.queue?.peakBytes).toBeGreaterThan(0);
    expect(monitorEvents.length).toBeLessThanOrEqual(4);
    expect(JSON.stringify(monitorEvents)).not.toContain("private-payload");
    expect(JSON.stringify(monitorEvents)).not.toContain("fixture-secret");

    await facade.execute(command("messages.stop", "request-stop"));
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        delivery: { deliveredMessages: 3 },
        state: "stopped",
        status: "nominal",
      },
    });
  });

  it("flushes a finite snapshot before publishing correlated completion evidence", async () => {
    const request: KafkaFetchRequest = {
      maxMessages: 2,
      mode: "newest",
      topic: "test",
    };
    const stream = new ControlledMessageStream();
    let forwardedRequest: KafkaFetchRequest | undefined;
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push((candidate) => {
      forwardedRequest = candidate;
      return Promise.resolve(stream);
    });
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const scheduledFlushes: Array<() => void> = [];
    const facade = createFacade(port, (flush) => {
      scheduledFlushes.push(flush);
    });
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));

    await expect(facade.execute(fetchCommand(request, "request-newest"))).resolves.toMatchObject({
      ok: true,
    });
    expect(forwardedRequest).toEqual(request);
    expect(
      events
        .filter((event) => event.event === "consumption.state")
        .map((event) => event.payload.state),
    ).toEqual(["loading", "fetching"]);

    stream.push(message("snapshot-1", "private-payload-marker"));
    await settleAsyncIteration();
    stream.end();
    await vi.waitFor(() => {
      expect(
        events.filter((event) => event.event === "consumption.state").at(-1)?.payload.state,
      ).toBe("complete");
    });

    const batchIndex = events.findIndex((event) => event.event === "messages.batch");
    const completeIndex = events.findIndex(
      (event) => event.event === "consumption.state" && event.payload.state === "complete",
    );
    expect(batchIndex).toBeGreaterThan(-1);
    expect(completeIndex).toBeGreaterThan(batchIndex);
    expect(events[completeIndex]).toMatchObject({
      payload: {
        droppedMessages: 0,
        receivedMessages: 1,
        request,
        state: "complete",
      },
    });
    const operationActivity = events.filter(
      (event) =>
        event.event === "activity.recorded" && event.payload.correlationId === "correlation-2",
    );
    const activityPayloads = operationActivity.map((event) => {
      if (event.event !== "activity.recorded") {
        throw new Error("Expected only activity events.");
      }
      return event.payload;
    });
    expect(activityPayloads).toMatchObject([
      {
        object: "test",
        operation: "Consume messages",
        outcome: "started",
      },
      {
        object: "test",
        operation: "Consume messages",
        outcome: "succeeded",
      },
    ]);
    expect(activityPayloads[0]?.detail).toContain("Newest N");
    expect(activityPayloads[1]?.detail).toContain("1");
    expect(JSON.stringify(operationActivity)).not.toContain("private-payload-marker");
    expect(stream.closeCalls).toBe(1);
  });

  it("publishes an honest empty result when a finite snapshot ends without records", async () => {
    const request: KafkaFetchRequest = {
      maxMessages: 10,
      mode: "earliest",
      topic: "test",
    };
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(fetchCommand(request, "request-first"));

    stream.end();
    await vi.waitFor(() => {
      expect(
        events.filter((event) => event.event === "consumption.state").at(-1)?.payload.state,
      ).toBe("empty");
    });

    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: {
        receivedMessages: 0,
        request,
        state: "empty",
      },
    });
  });

  it("suppresses completion from a finite snapshot superseded by a Tail request", async () => {
    const finiteStream = new ControlledMessageStream();
    const tailStream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(
      () => Promise.resolve(finiteStream),
      () => Promise.resolve(tailStream),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(
      fetchCommand(
        {
          maxMessages: 10,
          mode: "newest",
          topic: "test",
        },
        "request-newest",
      ),
    );
    await facade.execute(command("messages.start", "request-tail"));

    finiteStream.end();
    await settleAsyncIteration();
    expect(
      events.some(
        (event) => event.event === "consumption.state" && event.payload.state === "complete",
      ),
    ).toBe(false);
    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: {
        request: tailRequest(),
        state: "streaming",
      },
    });
    expect(
      events.some(
        (event) =>
          event.event === "streamMetrics.changed" &&
          event.payload.request?.mode === "newest" &&
          event.payload.state === "complete",
      ),
    ).toBe(false);
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        request: tailRequest(),
        state: "streaming",
      },
    });
    await facade.execute(command("messages.stop", "request-stop"));
  });

  it("reports an accessible topic as empty after the bounded observation period", async () => {
    vi.useFakeTimers();
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    await vi.advanceTimersByTimeAsync(KAFKA_MESSAGE_LIMITS.emptyObservationMs);

    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: {
        droppedMessages: 0,
        receivedMessages: 0,
        request: tailRequest(),
        state: "empty",
      },
    });
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        delivery: {
          deliveredMessages: 0,
          receivedMessages: 0,
        },
        state: "empty",
        status: "idle",
      },
    });
    await facade.execute(command("messages.stop", "request-stop"));
  });

  it("reports a structured stream failure with correlated recovery activity", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    stream.fail(
      new KafkaEngineFailure({
        code: "AUTHORIZATION_DENIED",
        recovery: "Request permission to consume this topic and retry.",
        retryable: false,
        stage: "authorization",
        summary: "Kafka denied message consumption.",
        target: "test",
      }),
    );
    await settleAsyncIteration();

    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: {
        droppedMessages: 0,
        error: {
          code: "AUTHORIZATION_DENIED",
          correlationId: "correlation-2",
          recovery: "Request permission to consume this topic and retry.",
        },
        receivedMessages: 0,
        request: tailRequest(),
        state: "failed",
      },
    });
    expect(events.at(-1)).toMatchObject({
      event: "activity.recorded",
      payload: {
        correlationId: "correlation-2",
        object: "test",
        operation: "Consume messages",
        outcome: "failed",
      },
    });
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        state: "failed",
        status: "degraded",
      },
    });
    await facade.execute(command("messages.stop", "request-stop"));
  });

  it("drops the oldest queued record and splits host batches at the canonical count bound", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    for (let index = 0; index <= KAFKA_MESSAGE_LIMITS.queuedMessages; index += 1) {
      stream.push(message(String(index)));
    }
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(KAFKA_MESSAGE_LIMITS.queuedMessages + 1);
    });
    await facade.execute(command("messages.stop", "request-stop"));

    const batches = events.filter((event) => event.event === "messages.batch");
    expect(batches).toHaveLength(
      KAFKA_MESSAGE_LIMITS.queuedMessages / KAFKA_MESSAGE_LIMITS.batchMessages,
    );
    expect(batches[0]).toMatchObject({
      payload: {
        droppedMessages: 1,
        topic: "test",
      },
    });
    expect(batches[0]?.payload.messages[0]).toMatchObject({ id: "1" });
    expect(batches.every((event) => event.payload.messages.length <= 200)).toBe(true);
    expect(batches.reduce((count, event) => count + event.payload.messages.length, 0)).toBe(
      KAFKA_MESSAGE_LIMITS.queuedMessages,
    );
    const pressure = events
      .filter((event) => event.event === "streamMetrics.changed")
      .find((event) => event.payload.queue?.droppedSincePrevious === 1);
    expect(pressure).toMatchObject({
      payload: {
        delivery: {
          batchCount: KAFKA_MESSAGE_LIMITS.queuedMessages / KAFKA_MESSAGE_LIMITS.batchMessages,
          deliveredMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
          receivedMessages: KAFKA_MESSAGE_LIMITS.queuedMessages + 1,
        },
        queue: {
          droppedMessages: 1,
          peakMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
        },
        status: "backpressure",
      },
    });
  });

  it("splits host batches before their retained bytes exceed the canonical byte bound", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    const payload = "x".repeat(600 * 1_024);
    stream.push(message("1", payload));
    stream.push(message("2", payload));
    stream.push(message("3", payload));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(3);
    });
    await facade.execute(command("messages.stop", "request-stop"));

    const batches = events.filter((event) => event.event === "messages.batch");
    expect(batches).toHaveLength(3);
    expect(batches.every((event) => event.payload.messages.length === 1)).toBe(true);
  });

  it("drops the oldest records before the canonical queued-byte bound is exceeded", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    const payload = "x".repeat(600 * 1_024);
    for (let index = 0; index < 28; index += 1) {
      stream.push(message(String(index), payload));
    }
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(28);
    });
    await facade.execute(command("messages.stop", "request-stop"));

    const batches = events.filter((event) => event.event === "messages.batch");
    expect(batches).toHaveLength(27);
    expect(batches[0]).toMatchObject({
      payload: {
        droppedMessages: 1,
        topic: "test",
      },
    });
    expect(batches[0]?.payload.messages[0]).toMatchObject({ id: "1" });
  });

  it("reports an oversize batch omission as backpressure without exposing content", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "request-connect"));
    await facade.execute(command("messages.start", "request-start"));

    const privatePayload = `private-${"x".repeat(KAFKA_MESSAGE_LIMITS.batchBytes)}`;
    stream.push(message("oversize", privatePayload));
    await vi.waitFor(() => {
      expect(stream.deliveredMessages).toBe(1);
    });
    await facade.execute(command("messages.stop", "request-stop"));

    expect(events.filter((event) => event.event === "messages.batch")).toHaveLength(0);
    const pressure = events
      .filter((event) => event.event === "streamMetrics.changed")
      .find((event) => event.payload.queue?.droppedSincePrevious === 1);
    expect(pressure).toMatchObject({
      payload: {
        delivery: {
          batchCount: 0,
          deliveredMessages: 0,
          lastBatchMessages: 0,
          receivedMessages: 1,
        },
        queue: {
          currentBytes: 0,
          currentMessages: 0,
          droppedMessages: 1,
          peakMessages: 1,
        },
        status: "backpressure",
      },
    });
    expect(JSON.stringify(pressure)).not.toContain("private-");
  });
});
