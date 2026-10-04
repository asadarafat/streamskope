import { afterEach, describe, expect, it, vi } from "vitest";

import {
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceService,
} from "../../src/features/kafka/application";
import type { HostEvent } from "../../src/features/kafka/contracts";
import {
  command,
  ControlledMessageStream,
  createFacade,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

function deferred<Value>(): { readonly promise: Promise<Value>; resolve(value: Value): void } {
  let resolve!: (value: Value) => void;
  const promise = new Promise<Value>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Kafka consumption facade lifecycle", () => {
  it.each([true, false])(
    "retains the failed operation for subsequent Stop when automatic cleanup rejects=%s",
    async (cleanupRejects) => {
      const stream = new ControlledMessageStream();
      const cleanupFailure = new Error("automatic cleanup failed");
      if (cleanupRejects) vi.spyOn(stream, "close").mockRejectedValue(cleanupFailure);
      const connection = new RecordingActiveConnection();
      const openStream = vi.spyOn(connection, "openMessageStream");
      connection.messageStreamOperations.push(() => Promise.resolve(stream));
      const port = new RecordingConnectionPort();
      port.openOperations.push(() => Promise.resolve(connection));
      const facade = createFacade(port, () => undefined);
      const events: HostEvent[] = [];
      const failed = deferred<void>();
      facade.subscribe((event) => {
        events.push(event);
        if (event.event === "consumption.state" && event.payload.state === "failed")
          failed.resolve();
      });
      try {
        await facade.execute(command("connection.connect", "connect"));
        await facade.execute(command("messages.start", "original-operation"));
        stream.push(message("queued"));
        await settleAsyncIteration();
        stream.fail(new Error("record iteration failed"));
        await failed.promise;
        expect(await facade.execute(command("messages.stop", "stop"))).toMatchObject({
          ok: !cleanupRejects,
        });
        expect(
          events.filter((event) => event.event === "streamMetrics.changed").at(-1),
        ).toMatchObject({
          payload: {
            connectionName: "Local aio",
            state: cleanupRejects ? "failed" : "stopped",
            request: { topic: "test" },
            delivery: { receivedMessages: 1, publishedMessages: 1 },
          },
        });
        expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
          payload: { receivedMessages: 1, droppedMessages: 0, request: { topic: "test" } },
        });
        expect(events.filter((event) => event.event === "messages.batch")).toHaveLength(1);
        if (cleanupRejects) {
          expect(await facade.execute(command("messages.start", "replacement"))).toMatchObject({
            ok: false,
          });
          expect(openStream).toHaveBeenCalledOnce();
          expect(
            events.some(
              (event) => event.event === "consumption.state" && event.payload.state === "stopped",
            ),
          ).toBe(false);
        }
      } finally {
        await facade.shutdown().catch(() => undefined);
      }
    },
  );

  it("coalesces concurrent Stops and publishes one terminal result for the original queue", async () => {
    const closing = deferred<void>();
    const stream = new ControlledMessageStream();
    const close = vi.spyOn(stream, "close").mockReturnValue(closing.promise);
    const connection = new RecordingActiveConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    stream.push(message("queued"));
    await settleAsyncIteration();
    const firstStop = facade.execute(command("messages.stop", "stop-1"));
    const secondStop = facade.execute(command("messages.stop", "stop-2"));
    expect(close).toHaveBeenCalledOnce();
    stream.end();
    closing.resolve();
    expect(await firstStop).toMatchObject({ id: "stop-1", ok: true });
    expect(await secondStop).toMatchObject({ id: "stop-2", ok: true });
    const terminal = events.filter(
      (event) => event.event === "consumption.state" && event.payload.state === "stopped",
    );
    expect(terminal).toHaveLength(1);
    expect(terminal[0]).toMatchObject({
      payload: { receivedMessages: 1, droppedMessages: 0, request: { topic: "test" } },
    });
    expect(
      events
        .filter((event) => event.event === "messages.batch")
        .flatMap((event) => event.payload.messages.map((record) => record.id)),
    ).toEqual(["queued"]);
  });

  it("retains the stopped operation through a cleanup deadline and publishes its counters on retry", async () => {
    vi.useFakeTimers();
    const closing = deferred<void>();
    const stream = new ControlledMessageStream();
    vi.spyOn(stream, "close").mockReturnValue(closing.promise);
    const connection = new RecordingActiveConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    stream.push(message("queued"));
    await settleAsyncIteration();
    const stopping = facade.execute(command("messages.stop", "stop"));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(await stopping).toMatchObject({
      ok: false,
      error: {
        code: "TIMEOUT",
        retryable: true,
        target: "kafka-consumption-cleanup",
        summary: "Kafka message stream cleanup did not finish within five seconds.",
        recovery: "Cleanup continues. Wait for it to finish, then retry Stop.",
      },
    });
    stream.end();
    closing.resolve();
    expect(await facade.execute(command("messages.stop", "retry-stop"))).toMatchObject({
      ok: true,
    });
    const terminal = events.filter((event) => event.event === "consumption.state").at(-1);
    expect(terminal).toMatchObject({
      payload: {
        state: "stopped",
        receivedMessages: 1,
        droppedMessages: 0,
        request: { topic: "test" },
      },
    });
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        state: "stopped",
        delivery: { receivedMessages: 1, publishedMessages: 1 },
        request: { topic: "test" },
      },
    });
  });

  it("lets a newer Start supersede the retained Stop queue without publishing old records", async () => {
    const closing = deferred<void>();
    const firstStream = new ControlledMessageStream();
    const nextStream = new ControlledMessageStream();
    vi.spyOn(firstStream, "close").mockReturnValue(closing.promise);
    const connection = new RecordingActiveConnection();
    connection.messageStreamOperations.push(
      () => Promise.resolve(firstStream),
      () => Promise.resolve(nextStream),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(port, () => undefined);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    firstStream.push(message("old"));
    await settleAsyncIteration();
    const stopping = facade.execute(command("messages.stop", "stop"));
    const starting = facade.execute(command("messages.start", "replacement"));
    await settleAsyncIteration();
    firstStream.end();
    closing.resolve();
    expect(await stopping).toMatchObject({ ok: true });
    expect(await starting).toMatchObject({ ok: true });
    nextStream.push(message("current"));
    await settleAsyncIteration();
    await facade.execute(command("messages.stop", "stop-replacement"));
    expect(
      events
        .filter((event) => event.event === "messages.batch")
        .flatMap((event) => event.payload.messages.map((record) => record.id)),
    ).toEqual(["current"]);
    expect(
      events.filter(
        (event) => event.event === "consumption.state" && event.payload.state === "stopped",
      ),
    ).toHaveLength(1);
  });

  it("drains the owned queue on shutdown before publishing its terminal evidence", async () => {
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
    stream.push(message("queued"));
    await settleAsyncIteration();
    await facade.shutdown();
    flushes.forEach((flush) => {
      flush();
    });
    expect(
      events
        .filter((event) => event.event === "messages.batch")
        .flatMap((event) => event.payload.messages.map((record) => record.id)),
    ).toEqual(["queued"]);
    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: {
        state: "stopped",
        receivedMessages: 1,
        droppedMessages: 0,
        request: { topic: "test" },
      },
    });
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        state: "stopped",
        connectionName: "Local aio",
        delivery: { receivedMessages: 1, publishedMessages: 1 },
      },
    });
  });

  it.each(["Second cluster", "Local aio"])(
    "invalidates queued records and scheduled callbacks when switching to %s",
    async (connectionName) => {
      const oldStream = new ControlledMessageStream();
      const newStream = new ControlledMessageStream();
      const oldConnection = new RecordingActiveConnection();
      const newConnection = new RecordingActiveConnection();
      oldConnection.messageStreamOperations.push(() => Promise.resolve(oldStream));
      newConnection.messageStreamOperations.push(() => Promise.resolve(newStream));
      const port = new RecordingConnectionPort();
      port.openOperations.push(
        () => Promise.resolve(oldConnection),
        () => Promise.resolve(newConnection),
      );
      const flushes: Array<() => void> = [];
      const cancel = vi.fn();
      const facade = createFacade(port, (flush) => {
        flushes.push(flush);
        return cancel;
      });
      const events: HostEvent[] = [];
      facade.subscribe((event) => {
        events.push(event);
      });
      await facade.execute(command("connection.connect", "connect-a"));
      await facade.execute(command("messages.start", "start-a"));
      oldStream.push(message("old"));
      await settleAsyncIteration();
      expect(flushes).toHaveLength(1);
      const staleFlush = flushes.shift()!;
      const switchCommand = command("connection.connect", "connect-b");
      if (switchCommand.command !== "connection.connect")
        throw new Error("Expected connect command.");
      events.length = 0;
      expect(
        await facade.execute({
          ...switchCommand,
          payload: { ...switchCommand.payload, name: connectionName },
        }),
      ).toMatchObject({ ok: true });
      expect(cancel).toHaveBeenCalledOnce();
      staleFlush();
      expect(
        events.filter(
          (event) => event.event === "messages.batch" || event.event === "streamMetrics.changed",
        ),
      ).toEqual([]);
      expect(oldStream.closeCalls).toBe(1);
      await facade.execute(command("messages.start", "start-b"));
      newStream.push(message("new"));
      await settleAsyncIteration();
      staleFlush();
      flushes.shift()?.();
      expect(
        events
          .filter((event) => event.event === "messages.batch")
          .flatMap((event) => event.payload.messages.map((record) => record.id)),
      ).toEqual(["new"]);
      expect(
        events
          .filter((event) => event.event === "streamMetrics.changed")
          .every((event) => event.payload.connectionName === connectionName),
      ).toBe(true);
      await facade.execute(command("messages.stop", "stop-b"));
    },
  );

  it("discards queued records on disconnect even if a cancelled callback later runs", async () => {
    const stream = new ControlledMessageStream();
    const connection = new RecordingActiveConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const flushes: Array<() => void> = [];
    const cancel = vi.fn();
    const facade = createFacade(port, (flush) => {
      flushes.push(flush);
      return cancel;
    });
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    stream.push(message("old"));
    await settleAsyncIteration();
    expect(flushes).toHaveLength(1);
    events.length = 0;
    expect(await facade.execute(command("connection.disconnect", "disconnect"))).toMatchObject({
      ok: true,
    });
    flushes.shift()?.();
    facade.setMessagePresentationPaused(false);
    expect(cancel).toHaveBeenCalledOnce();
    expect(
      events.filter(
        (event) => event.event === "messages.batch" || event.event === "streamMetrics.changed",
      ),
    ).toEqual([]);
    expect(stream.closeCalls).toBe(1);
  });

  it("cancels a start whose preference preparation completes after Stop", async () => {
    const preferences = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
    );
    const port = new RecordingConnectionPort();
    const connection = new RecordingActiveConnection();
    const open = vi.spyOn(connection, "openMessageStream");
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(port, undefined, undefined, undefined, preferences);
    await facade.execute(command("connection.connect", "connect"));
    const loading = deferred<Awaited<ReturnType<typeof preferences.get>>>();
    vi.spyOn(preferences, "get").mockReturnValueOnce(loading.promise);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    const starting = facade.execute(command("messages.start", "start"));
    expect(await facade.execute(command("messages.stop", "stop"))).toMatchObject({ ok: true });
    loading.resolve(preferences.currentSnapshot());
    expect(await starting).toMatchObject({ ok: false, error: { code: "CANCELLED" } });
    expect(open).not.toHaveBeenCalled();
    expect(
      events
        .filter((event) => event.event === "consumption.state")
        .map((event) => event.payload.state),
    ).toEqual(["stopped"]);
  });

  it("stops a pending stream open without publishing its late records or a false start failure", async () => {
    const opening = deferred<ControlledMessageStream>();
    const stream = new ControlledMessageStream();
    const connection = new RecordingActiveConnection();
    let signal: AbortSignal | undefined;
    connection.messageStreamOperations.push((_request, openingSignal) => {
      signal = openingSignal;
      return opening.promise;
    });
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    const starting = facade.execute(command("messages.start", "start"));
    await vi.waitFor(() => {
      expect(signal).toBeDefined();
    });
    const stopping = facade.execute(command("messages.stop", "stop"));
    expect(signal?.aborted).toBe(true);
    stream.push(message("late"));
    opening.resolve(stream);
    expect(await starting).toMatchObject({ ok: false, error: { code: "CANCELLED" } });
    expect(await stopping).toMatchObject({ ok: true });
    expect(stream.closeCalls).toBe(1);
    expect(stream.deliveredMessages).toBe(0);
    expect(events.filter((event) => event.event === "messages.batch")).toEqual([]);
    expect(
      events
        .filter((event) => event.event === "consumption.state")
        .map((event) => event.payload.state),
    ).toEqual(["loading", "stopped"]);
  });

  it("reports incomplete Stop cleanup and surfaces a later close failure", async () => {
    const opening = deferred<ControlledMessageStream>();
    const stream = new ControlledMessageStream();
    const cleanup = new Error("stream failed to close after Stop deadline");
    vi.spyOn(stream, "close").mockRejectedValue(cleanup);
    const connection = new RecordingActiveConnection();
    const open = vi.spyOn(connection, "openMessageStream");
    connection.messageStreamOperations.push(() => opening.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(port);
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    const starting = facade.execute(command("messages.start", "start"));
    await vi.waitFor(() => {
      expect(open).toHaveBeenCalledOnce();
    });
    vi.useFakeTimers();
    const stopping = facade.execute(command("messages.stop", "stop"));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(await stopping).toMatchObject({
      ok: false,
      error: { code: "TIMEOUT", retryable: true, target: "kafka-consumption-cleanup" },
    });
    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: { state: "failed" },
    });
    opening.resolve(stream);
    expect(await starting).toMatchObject({ ok: false, error: { code: "INTERNAL" } });
    expect(await facade.execute(command("messages.stop", "retry-stop"))).toMatchObject({
      ok: false,
      error: { code: "INTERNAL" },
    });
    expect(
      events
        .filter((event) => event.event === "consumption.state")
        .some((event) => event.payload.state === "stopped"),
    ).toBe(false);
  });
});
