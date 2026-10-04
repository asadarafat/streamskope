import { afterEach, describe, expect, it, vi } from "vitest";

import type { KafkaFetchRequest, SecureConnectionInput } from "../../src/features/kafka/contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaApplicationSession,
} from "../../src/features/kafka/application/session";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";
import {
  ControlledMessageStream,
  RecordingActiveConnection,
  RecordingConnectionPort,
  message as fixtureMessage,
  tailRequest,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

const firstConnection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "First cluster",
  tls: { enabled: false },
};
const secondConnection: SecureConnectionInput = { ...firstConnection, name: "Second cluster" };

function deferred<Value>(): {
  readonly promise: Promise<Value>;
  resolve(value: Value): void;
  reject(error: Error): void;
} {
  let resolve!: (value: Value) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<Value>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

class RecordingConnection extends RecordingActiveConnection {
  readonly messageStreamCalls: Array<{
    readonly request: KafkaFetchRequest;
    readonly signal: AbortSignal;
  }> = [];
  override openMessageStream(
    request: KafkaFetchRequest,
    signal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    this.messageStreamCalls.push({ request, signal });
    return super.openMessageStream(request, signal);
  }
}

function message(id: string, topic = "test"): ReturnType<typeof fixtureMessage> {
  return { ...fixtureMessage(id), topic };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Kafka application consumption lifecycle", () => {
  it.each(["reconnect", "disconnect", "shutdown"] as const)(
    "bounds %s when connection closure awaits a stalled active stream and retains teardown ownership",
    async (action) => {
      vi.useFakeTimers();
      const closing = deferred<void>();
      const stream = new ControlledMessageStream();
      const closeStream = vi.spyOn(stream, "close").mockReturnValue(closing.promise);
      const connection = new RecordingConnection();
      const closeConnection = vi.spyOn(connection, "close").mockReturnValue(closing.promise);
      connection.messageStreamOperations.push(() => Promise.resolve(stream));
      const port = new RecordingConnectionPort();
      const openConnection = vi.spyOn(port, "openConnection");
      port.openOperations.push(
        () => Promise.resolve(connection),
        () => Promise.resolve(new RecordingConnection()),
      );
      const session = new KafkaApplicationSession(port);
      await session.connect(firstConnection);
      const observer = {
        onComplete: vi.fn(),
        onEmpty: vi.fn(),
        onFailure: vi.fn(),
        onMessage: vi.fn(),
      };
      await session.startConsumption(tailRequest(), observer);
      const stopping =
        action === "reconnect" ? session.connect(secondConnection) : session[action]();
      const rejected = expect(stopping).rejects.toBeInstanceOf(Error);
      await vi.advanceTimersByTimeAsync(5_000);
      await rejected;
      expect(session.snapshot().state).toBe("failed");
      expect(closeConnection).toHaveBeenCalledOnce();
      expect(closeStream).toHaveBeenCalledOnce();
      expect(openConnection).toHaveBeenCalledOnce();
      if (action !== "shutdown") {
        const retry = expect(session.connect(secondConnection)).rejects.toBeInstanceOf(Error);
        await vi.advanceTimersByTimeAsync(5_000);
        await retry;
        expect(closeConnection).toHaveBeenCalledOnce();
        expect(openConnection).toHaveBeenCalledOnce();
      }
      stream.end();
      closing.resolve();
      await settleAsyncIteration();
      if (action !== "shutdown") {
        await session.connect(secondConnection);
        expect(openConnection).toHaveBeenCalledTimes(2);
      }
    },
  );

  it.each(["stop", "reconnect", "disconnect", "shutdown"] as const)(
    "aborts a pending open on %s and closes its late result before acknowledging cleanup",
    async (action) => {
      const opening = deferred<ControlledMessageStream>();
      const stream = new ControlledMessageStream();
      const iterate = vi.spyOn(stream, Symbol.asyncIterator);
      const activeConnection = new RecordingConnection();
      activeConnection.messageStreamOperations.push(() => opening.promise);
      const port = new RecordingConnectionPort();
      port.openOperations.push(
        () => Promise.resolve(activeConnection),
        () => Promise.resolve(new RecordingConnection()),
      );
      const session = new KafkaApplicationSession(port);
      const observer = {
        onComplete: vi.fn(),
        onCoverage: vi.fn(),
        onEmpty: vi.fn(),
        onFailure: vi.fn(),
        onMessage: vi.fn(),
      };
      await session.connect(firstConnection);
      const starting = session.startConsumption(tailRequest(), observer);
      const rejectedStart = expect(starting).rejects.toBeInstanceOf(
        ConnectionAttemptSupersededError,
      );
      await settleAsyncIteration();
      expect(activeConnection.messageStreamCalls).toHaveLength(1);
      const stopping =
        action === "stop"
          ? session.stopConsumption()
          : action === "reconnect"
            ? session.connect(secondConnection)
            : session[action]();
      let acknowledged = false;
      void stopping.then(() => {
        acknowledged = true;
      });
      expect(activeConnection.messageStreamCalls[0]?.signal.aborted).toBe(true);
      await settleAsyncIteration();
      expect(acknowledged).toBe(false);
      stream.push(message("late"));
      opening.resolve(stream);
      await rejectedStart;
      await stopping;
      expect(stream.closeCalls).toBe(1);
      expect(iterate).not.toHaveBeenCalled();
      for (const callback of Object.values(observer)) expect(callback).not.toHaveBeenCalled();
    },
  );

  it("invalidates a start stopped before its first asynchronous step", async () => {
    const connection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const callback = vi.fn();
    const starting = session.startConsumption(tailRequest(), {
      onComplete: callback,
      onEmpty: callback,
      onFailure: callback,
      onMessage: callback,
    });
    const rejectedStart = expect(starting).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await session.stopConsumption();
    await rejectedStart;
    expect(connection.messageStreamCalls).toHaveLength(0);
    expect(callback).not.toHaveBeenCalled();
  });

  it.each(["test", "other.topic"])(
    "supersedes an unresolved start with a later start for %s",
    async (topic) => {
      const opening = deferred<ControlledMessageStream>();
      const firstStream = new ControlledMessageStream();
      const secondStream = new ControlledMessageStream();
      const connection = new RecordingConnection();
      connection.messageStreamOperations.push(
        () => opening.promise,
        () => Promise.resolve(secondStream),
      );
      const port = new RecordingConnectionPort();
      port.openOperations.push(() => Promise.resolve(connection));
      const session = new KafkaApplicationSession(port);
      await session.connect(firstConnection);
      const oldObserver = vi.fn();
      const currentObserver = vi.fn();
      const firstStart = session.startConsumption(tailRequest(), {
        onComplete: oldObserver,
        onEmpty: oldObserver,
        onFailure: oldObserver,
        onMessage: oldObserver,
      });
      const rejectedStart = expect(firstStart).rejects.toBeInstanceOf(
        ConnectionAttemptSupersededError,
      );
      await settleAsyncIteration();
      const secondStart = session.startConsumption(tailRequest(topic), {
        onComplete: vi.fn(),
        onEmpty: vi.fn(),
        onFailure: vi.fn(),
        onMessage: currentObserver,
      });
      expect(connection.messageStreamCalls[0]?.signal.aborted).toBe(true);
      firstStream.push(message("old"));
      opening.resolve(firstStream);
      await rejectedStart;
      await secondStart;
      secondStream.push(message("new", topic));
      await settleAsyncIteration();
      expect(firstStream.closeCalls).toBe(1);
      expect(oldObserver).not.toHaveBeenCalled();
      expect(currentObserver).toHaveBeenCalledExactlyOnceWith(message("new", topic));
      await session.stopConsumption();
    },
  );

  it("allows only the newest concurrent start to open a stream", async () => {
    const stream = new ControlledMessageStream();
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const firstStart = session.startConsumption(tailRequest("first"), observer);
    const rejectedStart = expect(firstStart).rejects.toBeInstanceOf(
      ConnectionAttemptSupersededError,
    );
    await session.startConsumption(tailRequest("last"), observer);
    await rejectedStart;
    expect(connection.messageStreamCalls).toHaveLength(1);
    expect(connection.messageStreamCalls[0]?.request.topic).toBe("last");
    await session.stopConsumption();
  });

  it("rejects every concurrent stop when late stream cleanup fails", async () => {
    const opening = deferred<ControlledMessageStream>();
    const stream = new ControlledMessageStream();
    const cleanup = new Error("late stream close failed");
    const close = vi.spyOn(stream, "close").mockRejectedValue(cleanup);
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => opening.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const starting = session.startConsumption(tailRequest(), observer);
    const rejectedStart = expect(starting).rejects.toMatchObject({ cleanupFailure: cleanup });
    await settleAsyncIteration();
    const firstStop = expect(session.stopConsumption()).rejects.toBe(cleanup);
    const secondStop = expect(session.stopConsumption()).rejects.toBe(cleanup);
    opening.resolve(stream);
    await Promise.all([rejectedStart, firstStop, secondStop]);
    expect(close).toHaveBeenCalledOnce();
    expect(observer.onFailure).not.toHaveBeenCalled();
  });

  it("bounds cleanup acknowledgement while owning the late stream and blocking replacement starts", async () => {
    vi.useFakeTimers();
    const opening = deferred<ControlledMessageStream>();
    const lateStream = new ControlledMessageStream();
    const nextStream = new ControlledMessageStream();
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(
      () => opening.promise,
      () => Promise.resolve(nextStream),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const starting = session.startConsumption(tailRequest(), observer);
    const rejectedStart = expect(starting).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await settleAsyncIteration();
    const stopFailure = expect(session.stopConsumption()).rejects.toThrow(
      "cleanup did not finish within five seconds",
    );
    await vi.advanceTimersByTimeAsync(5_000);
    await stopFailure;
    const replacementFailure = expect(
      session.startConsumption(tailRequest("replacement"), observer),
    ).rejects.toThrow("cleanup did not finish within five seconds");
    await vi.advanceTimersByTimeAsync(5_000);
    await replacementFailure;
    expect(connection.messageStreamCalls).toHaveLength(1);
    opening.resolve(lateStream);
    await rejectedStart;
    await session.stopConsumption();
    expect(lateStream.closeCalls).toBe(1);
    await session.startConsumption(tailRequest("replacement"), observer);
    expect(connection.messageStreamCalls).toHaveLength(2);
    await session.stopConsumption();
  });

  it("bounds reconnect while retaining a pending open and allows retry after its late close", async () => {
    vi.useFakeTimers();
    const opening = deferred<ControlledMessageStream>();
    const stream = new ControlledMessageStream();
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => opening.promise);
    const port = new RecordingConnectionPort();
    const openConnection = vi.spyOn(port, "openConnection");
    port.openOperations.push(
      () => Promise.resolve(connection),
      () => Promise.resolve(new RecordingConnection()),
    );
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const starting = session.startConsumption(tailRequest(), observer);
    const rejectedStart = expect(starting).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await settleAsyncIteration();
    const reconnectFailure = expect(session.connect(secondConnection)).rejects.toThrow(
      "cleanup did not finish within five seconds",
    );
    await vi.advanceTimersByTimeAsync(5_000);
    await reconnectFailure;
    expect(connection.closeCalls).toBe(1);
    expect(openConnection).toHaveBeenCalledOnce();
    opening.resolve(stream);
    await rejectedStart;
    await session.connect(secondConnection);
    expect(stream.closeCalls).toBe(1);
    expect(openConnection).toHaveBeenCalledTimes(2);
    expect(session.snapshot()).toMatchObject({
      state: "connected",
      connectionName: "Second cluster",
    });
  });

  it("does not retain a preparation timeout as a stream cleanup failure", async () => {
    vi.useFakeTimers();
    const opening = deferred<ControlledMessageStream>();
    const stream = new ControlledMessageStream();
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => opening.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const starting = session.startConsumption(tailRequest(), observer);
    const rejectedStart = expect(starting).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await settleAsyncIteration();
    const replacementFailure = expect(
      session.startConsumption(tailRequest("replacement"), observer),
    ).rejects.toThrow("cleanup did not finish within five seconds");
    const stopFailure = expect(session.stopConsumption()).rejects.toThrow(
      "cleanup did not finish within five seconds",
    );
    await vi.advanceTimersByTimeAsync(5_000);
    await Promise.all([replacementFailure, stopFailure]);
    opening.resolve(stream);
    await rejectedStart;
    await session.stopConsumption();
    expect(stream.closeCalls).toBe(1);
    expect(connection.messageStreamCalls).toHaveLength(1);
  });

  it("retains a late cleanup failure after a Stop deadline for subsequent lifecycle calls", async () => {
    vi.useFakeTimers();
    const opening = deferred<ControlledMessageStream>();
    const lateStream = new ControlledMessageStream();
    const cleanup = new Error("late cleanup failed after deadline");
    vi.spyOn(lateStream, "close").mockRejectedValue(cleanup);
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => opening.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const starting = session.startConsumption(tailRequest(), observer);
    const rejectedStart = expect(starting).rejects.toMatchObject({ cleanupFailure: cleanup });
    await settleAsyncIteration();
    const stopFailure = expect(session.stopConsumption()).rejects.toThrow(
      "cleanup did not finish within five seconds",
    );
    await vi.advanceTimersByTimeAsync(5_000);
    await stopFailure;
    opening.resolve(lateStream);
    await rejectedStart;
    await expect(session.stopConsumption()).rejects.toBe(cleanup);
    await expect(session.connect(secondConnection)).rejects.toBe(cleanup);
  });

  it("recovers after failed stream cleanup once disconnect closes the owning connection", async () => {
    const stream = new ControlledMessageStream();
    const cleanup = new Error("stream cleanup failed");
    vi.spyOn(stream, "close").mockImplementation(() => {
      stream.end();
      return Promise.reject(cleanup);
    });
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const replacement = new RecordingConnection();
    const replacementStream = new ControlledMessageStream();
    replacement.messageStreamOperations.push(() => Promise.resolve(replacementStream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(
      () => Promise.resolve(connection),
      () => Promise.resolve(replacement),
    );
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    await session.startConsumption(tailRequest(), observer);
    await expect(session.stopConsumption()).rejects.toBe(cleanup);
    await expect(session.disconnect()).rejects.toThrow("did not close cleanly");
    expect(connection.closeCalls).toBe(1);
    await session.disconnect();
    await session.connect(secondConnection);
    await session.startConsumption(tailRequest(), observer);
    expect(replacement.messageStreamCalls).toHaveLength(1);
    await session.stopConsumption();
  });

  it("preserves adapter-owned cleanup failures when an aborted open rejects", async () => {
    const opening = deferred<ControlledMessageStream>();
    const cleanup = new Error("adapter cleanup failed");
    const connection = new RecordingConnection();
    connection.messageStreamOperations.push(() => opening.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const observer = {
      onComplete: vi.fn(),
      onEmpty: vi.fn(),
      onFailure: vi.fn(),
      onMessage: vi.fn(),
    };
    const starting = session.startConsumption(tailRequest(), observer);
    const rejectedStart = expect(starting).rejects.toMatchObject({ cleanupFailure: cleanup });
    await settleAsyncIteration();
    const stopped = expect(session.stopConsumption()).rejects.toBe(cleanup);
    opening.reject(Object.assign(new Error("cancelled"), { cleanupCause: cleanup }));
    await Promise.all([rejectedStart, stopped]);
  });
});
