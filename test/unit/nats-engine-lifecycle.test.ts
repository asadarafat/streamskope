import { setImmediate as nextTurn } from "node:timers/promises";

import {
  AuthorizationError,
  PermissionViolationError,
  type NodeConnectionOptions,
  type Status,
} from "@nats-io/transport-node";
import { describe, expect, it } from "vitest";

import type { NatsMessageReceipt } from "../../src/features/nats/application/engine-port";
import type { NatsConnectionInput } from "../../src/features/nats/application/profile-types";
import type { NatsSafeFailure } from "../../src/features/nats/contracts";
import { StreamSkopeNatsEngine } from "../../src/features/nats/engine/engine";
import {
  normalizeNatsEngineFailure,
  safeNatsFailure,
} from "../../src/features/nats/engine/failure";
import type { NatsSdkConnection } from "../../src/features/nats/engine/sdk-types";
import { NatsConnectionFixture, natsDeferred, natsMessage } from "../support/nats-engine-fixture";

const connectionInput: NatsConnectionInput = {
  servers: ["nats://127.0.0.1:4222"],
  authentication: { mode: "none" },
  tls: { mode: "plaintext" },
};
const noLoss = { onConnectionLoss: (): void => undefined };
const noMessages = { onMessage: (): void => undefined, onFailure: (): void => undefined };

async function until(check: () => boolean): Promise<void> {
  for (let index = 0; index < 100; index += 1) {
    if (check()) return;
    await nextTurn();
  }
  throw new Error("Expected owned lifecycle transition did not occur.");
}

describe("Core NATS engine actual work ownership", () => {
  it("whitelists explicit token/TLS options and keeps reconnect disabled", async () => {
    const connection = new NatsConnectionFixture();
    let options: NodeConnectionOptions | undefined;
    const engine = new StreamSkopeNatsEngine({
      connect: (input): Promise<NatsSdkConnection> => {
        options = input;
        return Promise.resolve(connection);
      },
    });
    try {
      await engine.connect(
        {
          ...connectionInput,
          authentication: { mode: "token", token: "private-token" },
          tls: { mode: "tls" },
        },
        noLoss,
      );
      expect(options).toEqual({
        servers: ["nats://127.0.0.1:4222"],
        timeout: 5_000,
        reconnect: false,
        waitOnFirstConnect: false,
        ignoreClusterUpdates: true,
        noRandomize: true,
        debug: false,
        tls: { rejectUnauthorized: true },
        token: "private-token",
      });
    } finally {
      await engine.shutdown();
    }
    expect(connection.closeCalls).toBe(1);
    expect(connection.statusEnded).toBe(true);
  });

  it("rejects invalid credentials in URLs and pre-aborted input before SDK invocation", async () => {
    let calls = 0;
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => {
        calls += 1;
        return Promise.resolve(new NatsConnectionFixture());
      },
    });
    await expect(
      engine.connect(
        { ...connectionInput, servers: ["nats://hidden:credential@127.0.0.1:4222"] },
        noLoss,
      ),
    ).rejects.toMatchObject({ failure: { code: "validation" } });
    await expect(
      engine.connect(connectionInput, {
        ...noLoss,
        signal: AbortSignal.abort(new Error("private cancellation")),
      }),
    ).rejects.toMatchObject({ failure: { code: "cancelled" } });
    expect(calls).toBe(0);
    await engine.shutdown();
  });

  it("rejects and closes a server-required TLS upgrade for a declared plaintext profile", async () => {
    const connection = new NatsConnectionFixture();
    connection.info = { tls_required: true };
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await expect(engine.connect(connectionInput, noLoss)).rejects.toMatchObject({
      failure: { code: "validation" },
    });
    expect(connection.closeCalls).toBe(1);
    await expect(engine.startSubscription("qualification.*", noMessages)).rejects.toMatchObject({
      failure: { code: "not-connected" },
    });
    await engine.shutdown();
    expect(connection.closeCalls).toBe(1);
  });

  it("does not create a new unending status observer for an already-closed SDK return", async () => {
    const connection = new NatsConnectionFixture();
    connection.finishClose();
    const releaseStatus = natsDeferred<void>();
    let statusCalls = 0;
    // The real public SDK allocates a new iterator even after its prior close already ran.
    connection.status = async function* (): AsyncGenerator<Status> {
      statusCalls += 1;
      await releaseStatus.promise;
      yield* [];
    };
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    const result = engine
      .connect(connectionInput, noLoss)
      .catch((error: unknown) => safeNatsFailure(error));
    try {
      await until(() => connection.closeCalls === 1);
      expect(statusCalls).toBe(0);
      expect(await result).toMatchObject({ code: "connection" });
    } finally {
      // A failing regression still retires its deliberately blocked supported-API fixture.
      releaseStatus.resolve();
      await result;
      await engine.shutdown();
    }
    expect(connection.closeCalls).toBe(1);
  });

  it("holds shutdown for the actual cancelled connect and closes its late returned connection once", async () => {
    const opening = natsDeferred<NatsSdkConnection>();
    const closing = natsDeferred<void>();
    const connection = new NatsConnectionFixture();
    connection.closeOperation = (): Promise<void> => closing.promise;
    const controller = new AbortController();
    let calls = 0;
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => {
        calls += 1;
        return opening.promise;
      },
    });
    const connecting = engine.connect(connectionInput, { ...noLoss, signal: controller.signal });
    const result = connecting.catch((error: unknown) => safeNatsFailure(error));
    await until(() => calls === 1);
    controller.abort();
    expect(await result).toMatchObject({ code: "cancelled" });
    const shutdown = engine.shutdown();
    expect(engine.shutdown()).toBe(shutdown);
    let finished = false;
    void shutdown.then(() => {
      finished = true;
    });
    await nextTurn();
    expect(finished).toBe(false);
    opening.resolve(connection);
    await until(() => connection.closeCalls === 1);
    expect(finished).toBe(false);
    closing.resolve();
    await shutdown;
    expect(connection.closeCalls).toBe(1);
    expect(connection.statusEnded).toBe(true);
    await expect(engine.connect(connectionInput, noLoss)).rejects.toMatchObject({
      failure: { code: "unavailable" },
    });
  });

  it("serializes replacement behind actual pending connect cleanup", async () => {
    const firstOpen = natsDeferred<NatsSdkConnection>();
    const firstClose = natsDeferred<void>();
    const first = new NatsConnectionFixture();
    const second = new NatsConnectionFixture();
    first.closeOperation = (): Promise<void> => firstClose.promise;
    let calls = 0;
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => {
        calls += 1;
        return calls === 1 ? firstOpen.promise : Promise.resolve(second);
      },
    });
    const one = engine
      .connect(connectionInput, noLoss)
      .catch((error: unknown) => safeNatsFailure(error));
    await until(() => calls === 1);
    const two = engine.connect(connectionInput, noLoss);
    expect(await one).toMatchObject({ code: "cancelled" });
    firstOpen.resolve(first);
    await until(() => first.closeCalls === 1);
    expect(calls).toBe(1);
    firstClose.resolve();
    await two;
    expect(calls).toBe(2);
    await engine.shutdown();
    expect(first.closeCalls).toBe(1);
    expect(second.closeCalls).toBe(1);
  });

  it("cleans up a synchronous abort during connection adoption", async () => {
    const controller = new AbortController();
    const connection = new NatsConnectionFixture();
    connection.onIsClosed = (): void => controller.abort();
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await expect(
      engine.connect(connectionInput, { ...noLoss, signal: controller.signal }),
    ).rejects.toMatchObject({ failure: { code: "cancelled" } });
    await engine.shutdown();
    expect(connection.closeCalls).toBe(1);
  });

  it("retains a late cancelled connection's failed close in the shutdown barrier", async () => {
    const opening = natsDeferred<NatsSdkConnection>();
    const controller = new AbortController();
    const connection = new NatsConnectionFixture();
    connection.closeOperation = (): Promise<void> => {
      connection.finishClose();
      return Promise.reject(new Error("private late close failure"));
    };
    let calls = 0;
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => {
        calls += 1;
        return opening.promise;
      },
    });
    const connecting = engine
      .connect(connectionInput, { ...noLoss, signal: controller.signal })
      .catch((error: unknown) => safeNatsFailure(error));
    await until(() => calls === 1);
    controller.abort();
    expect(await connecting).toMatchObject({ code: "cancelled" });
    const shutdown = engine.shutdown();
    const outcome = shutdown.catch((error: unknown) => safeNatsFailure(error));
    opening.resolve(connection);
    expect(await outcome).toMatchObject({ code: "cleanup" });
    expect(connection.closeCalls).toBe(1);
    expect(engine.shutdown()).toBe(shutdown);
  });

  it("includes a new queued reconnect after an earlier disconnect barrier already completed", async () => {
    const connection = new NatsConnectionFixture();
    let calls = 0;
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => {
        calls += 1;
        return Promise.resolve(connection);
      },
    });
    await engine.connect(connectionInput, noLoss);
    await engine.disconnect();
    const reconnecting = engine
      .connect(connectionInput, noLoss)
      .catch((error: unknown) => safeNatsFailure(error));
    await engine.shutdown();
    expect(await reconnecting).toMatchObject({ code: "cancelled" });
    expect(calls).toBe(1);
    expect(connection.closeCalls).toBe(1);
  });

  it("confirms start only after SUB flush and stop only after UNSUB flush", async () => {
    const startFlush = natsDeferred<void>();
    const stopFlush = natsDeferred<void>();
    const connection = new NatsConnectionFixture();
    connection.flushOperation = (): Promise<void> =>
      connection.flushCalls === 1 ? startFlush.promise : stopFlush.promise;
    const records: NatsMessageReceipt[] = [];
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    let started = false;
    const start = engine.startSubscription("qualification.*", {
      ...noMessages,
      onMessage: (receipt): void => {
        records.push(receipt);
      },
    });
    void start.then(() => {
      started = true;
    });
    await until(() => connection.flushCalls === 1);
    connection.subscriptions[0]?.emit(natsMessage("before confirmation"));
    expect(records).toHaveLength(1);
    expect(started).toBe(false);
    startFlush.resolve();
    await start;
    const stop = engine.stopSubscription();
    expect(engine.stopSubscription()).toBe(stop);
    let stopped = false;
    void stop.then(() => {
      stopped = true;
    });
    connection.subscriptions[0]?.emit(natsMessage("stale callback"));
    await until(() => connection.flushCalls === 2);
    expect(connection.subscriptions[0]?.isClosed()).toBe(true);
    expect(stopped).toBe(false);
    expect(records).toHaveLength(1);
    stopFlush.resolve();
    await stop;
    expect(connection.closeCalls).toBe(0);
    await engine.shutdown();
  });

  it("stops deferred subscription setup without admitting late records or success", async () => {
    const flush = natsDeferred<void>();
    const connection = new NatsConnectionFixture();
    connection.flushOperation = (): Promise<void> =>
      connection.flushCalls === 1 ? flush.promise : Promise.resolve();
    const records: NatsMessageReceipt[] = [];
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    const starting = engine
      .startSubscription("qualification.*", {
        ...noMessages,
        onMessage: (receipt): void => {
          records.push(receipt);
        },
      })
      .catch((error: unknown) => safeNatsFailure(error));
    await until(() => connection.flushCalls === 1);
    await engine.stopSubscription();
    expect(await starting).toMatchObject({ code: "cancelled" });
    connection.subscriptions[0]?.emit(natsMessage("after stop"));
    flush.resolve();
    expect(records).toHaveLength(0);
    expect(connection.subscriptions[0]?.unsubscribeCalls).toBe(1);
    expect(connection.closeCalls).toBe(0);
    await engine.shutdown();
  });

  it("does not let an old same-subject callback join the replacement generation", async () => {
    const connection = new NatsConnectionFixture();
    const first: NatsMessageReceipt[] = [];
    const second: NatsMessageReceipt[] = [];
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    await engine.startSubscription("qualification.*", {
      ...noMessages,
      onMessage: (receipt): void => {
        first.push(receipt);
      },
    });
    await engine.startSubscription("qualification.*", {
      ...noMessages,
      onMessage: (receipt): void => {
        second.push(receipt);
      },
    });
    connection.subscriptions[0]?.emit(natsMessage("old"));
    connection.subscriptions[1]?.emit(natsMessage("new"));
    expect(first).toHaveLength(0);
    expect(second).toHaveLength(1);
    await engine.shutdown();
  });

  it("rejects asynchronous permission denial even when initial PONG succeeds and reports once", async () => {
    const flush = natsDeferred<void>();
    const connection = new NatsConnectionFixture();
    const failures: NatsSafeFailure[] = [];
    const records: NatsMessageReceipt[] = [];
    connection.flushOperation = (): Promise<void> =>
      connection.flushCalls === 1 ? flush.promise : Promise.resolve();
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    const starting = engine
      .startSubscription("qualification.*", {
        onMessage: (receipt): void => {
          records.push(receipt);
        },
        onFailure: (failure): void => {
          failures.push(failure);
        },
      })
      .catch((error: unknown) => safeNatsFailure(error));
    await until(() => connection.flushCalls === 1);
    const permission = new PermissionViolationError(
      "private server diagnostic",
      "subscription",
      "qualification.*",
    );
    connection.subscriptions[0]?.fail(permission, {
      get subject(): string {
        throw new Error("Must not decode permission placeholder");
      },
      reply: "",
      data: new Uint8Array(),
    });
    connection.pushStatus({ type: "error", error: permission });
    flush.resolve();
    expect(await starting).toMatchObject({ code: "permission" });
    await engine.stopSubscription();
    expect(failures).toHaveLength(1);
    expect(failures[0]?.summary).not.toContain("private");
    expect(records).toHaveLength(0);
    expect(connection.closeCalls).toBe(0);
    await engine.shutdown();
  });

  it("contains observer exceptions and still confirms owned subscription cleanup", async () => {
    const connection = new NatsConnectionFixture();
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    await engine.startSubscription("qualification.*", {
      onMessage: (): never => {
        throw new Error("private observer error");
      },
      onFailure: (): never => {
        throw new Error("private failure observer error");
      },
    });
    expect(() => connection.subscriptions[0]?.emit(natsMessage())).not.toThrow();
    await engine.stopSubscription();
    expect(connection.subscriptions[0]?.unsubscribeCalls).toBe(1);
    await engine.shutdown();
  });

  it("closes the actual connection when UNSUB confirmation fails, then confirms stop", async () => {
    const connection = new NatsConnectionFixture();
    const losses: NatsSafeFailure[] = [];
    connection.flushOperation = (): Promise<void> =>
      connection.flushCalls > 1
        ? Promise.reject(new Error("private no PONG failure"))
        : Promise.resolve();
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, {
      onConnectionLoss: (failure): void => {
        losses.push(failure);
      },
    });
    await engine.startSubscription("qualification.*", noMessages);
    await engine.stopSubscription();
    expect(connection.closeCalls).toBe(1);
    expect(losses).toHaveLength(1);
    expect(JSON.stringify(losses)).not.toContain("private");
    await engine.shutdown();
    expect(connection.closeCalls).toBe(1);
  });

  it("reports unconfirmed cleanup without hiding a failed actual connection close", async () => {
    const connection = new NatsConnectionFixture();
    connection.flushOperation = (): Promise<void> =>
      connection.flushCalls > 1 ? Promise.reject(new Error("flush failed")) : Promise.resolve();
    connection.closeOperation = (): Promise<void> => {
      connection.finishClose();
      return Promise.reject(new Error("close failed"));
    };
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    await engine.startSubscription("qualification.*", noMessages);
    const stopping = engine.stopSubscription();
    await expect(stopping).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(engine.stopSubscription()).toBe(stopping);
    await expect(engine.disconnect()).rejects.toMatchObject({ failure: { code: "cleanup" } });
    await expect(engine.shutdown()).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(connection.closeCalls).toBe(1);
  });

  it("does not report an empty replacement as stopped when its prior cleanup failed", async () => {
    const connection = new NatsConnectionFixture();
    connection.flushOperation = (): Promise<void> =>
      connection.flushCalls > 1 ? Promise.reject(new Error("flush failed")) : Promise.resolve();
    connection.closeOperation = (): Promise<void> => {
      connection.finishClose();
      return Promise.reject(new Error("close failed"));
    };
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, noLoss);
    await engine.startSubscription("qualification.first", noMessages);
    await expect(
      engine.startSubscription("qualification.second", noMessages),
    ).rejects.toMatchObject({ failure: { code: "cleanup" } });
    await expect(engine.stopSubscription()).rejects.toMatchObject({ failure: { code: "cleanup" } });
    await expect(engine.shutdown()).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(connection.subscriptions).toHaveLength(1);
    expect(connection.closeCalls).toBe(1);
  });

  it("deduplicates network loss across status and closed while fencing the reader", async () => {
    const connection = new NatsConnectionFixture();
    const losses: NatsSafeFailure[] = [];
    const failures: NatsSafeFailure[] = [];
    const records: NatsMessageReceipt[] = [];
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => Promise.resolve(connection),
    });
    await engine.connect(connectionInput, {
      onConnectionLoss: (failure): void => {
        losses.push(failure);
      },
    });
    await engine.startSubscription("qualification.*", {
      onMessage: (receipt): void => {
        records.push(receipt);
      },
      onFailure: (failure): void => {
        failures.push(failure);
      },
    });
    connection.pushStatus({ type: "disconnect", server: "private-server-name" });
    connection.finishClose(new Error("private network failure"));
    await until(() => losses.length === 1);
    connection.subscriptions[0]?.emit(natsMessage("late"));
    await engine.disconnect();
    expect(losses).toHaveLength(1);
    expect(failures).toHaveLength(1);
    expect(records).toHaveLength(0);
    expect(connection.statusEnded).toBe(true);
    expect(JSON.stringify({ losses, failures })).not.toContain("private");
    await engine.shutdown();
  });

  it("permits explicit reconnection after successful disconnect without reusing old callbacks", async () => {
    const one = new NatsConnectionFixture();
    const two = new NatsConnectionFixture();
    let calls = 0;
    const engine = new StreamSkopeNatsEngine({
      connect: (): Promise<NatsSdkConnection> => {
        calls += 1;
        return Promise.resolve(calls === 1 ? one : two);
      },
    });
    await engine.connect(connectionInput, noLoss);
    const disconnect = engine.disconnect();
    expect(engine.disconnect()).toBe(disconnect);
    await disconnect;
    await engine.connect(connectionInput, noLoss);
    await engine.startSubscription("qualification.*", noMessages);
    await engine.shutdown();
    expect(one.closeCalls).toBe(1);
    expect(two.closeCalls).toBe(1);
    expect(calls).toBe(2);
  });

  it("preserves safe authentication and TLS classifications without exposing cause text", () => {
    const auth = safeNatsFailure(new AuthorizationError("private token diagnostic"));
    const tls = safeNatsFailure(
      new Error("private certificate", { cause: { code: "ERR_TLS_CERT_ALTNAME_INVALID" } }),
    );
    const generic = safeNatsFailure("private unknown failure");
    expect(auth.code).toBe("authentication");
    expect(tls.code).toBe("tls");
    expect(generic.code).toBe("connection");
    expect(JSON.stringify({ auth, tls, generic })).not.toContain("private");
    expect(normalizeNatsEngineFailure(null)).toBeInstanceOf(Error);
    const hostile = Object.defineProperty(new Error("private external failure"), "cause", {
      get: (): never => {
        throw new Error("private getter failure");
      },
    });
    expect(safeNatsFailure(hostile).code).toBe("connection");
  });
});
