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
    expect(await stopping).toMatchObject({ ok: false, error: { code: "INTERNAL" } });
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
