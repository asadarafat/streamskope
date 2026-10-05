import { afterEach, describe, expect, it, vi } from "vitest";

import {
  NatsApplicationSession,
  type NatsSessionChange,
} from "../../src/features/nats/application/session";
import {
  NatsOperationError,
  natsCancelled,
  natsCleanupFailure,
} from "../../src/features/nats/application/failure";
import { parseNatsSubscriptionSnapshot } from "../../src/features/nats/contracts/state-validation";
import {
  NatsApplicationEngineFixture,
  natsContext,
  natsProfile,
  copiedNatsReceipt,
} from "../support/nats-application-fixture";
import { natsDeferred } from "../support/nats-engine-fixture";

async function connected(): Promise<{
  engine: NatsApplicationEngineFixture;
  session: NatsApplicationSession;
}> {
  const engine = new NatsApplicationEngineFixture();
  const session = new NatsApplicationSession(engine);
  await session.connect("profile-1", () => Promise.resolve(natsProfile()), natsContext);
  return { engine, session };
}
afterEach(() => vi.useRealTimers());
describe("Core NATS application lifecycle authority", () => {
  it("reserves the pending profile before asynchronous lookup", async () => {
    const engine = new NatsApplicationEngineFixture();
    const session = new NatsApplicationSession(engine);
    const lookup = natsDeferred<ReturnType<typeof natsProfile>>();
    const pending = session.connect("profile-1", () => lookup.promise, natsContext);
    expect(session.isProfileInUse("profile-1")).toBe(true);
    lookup.resolve(natsProfile());
    await pending;
    await session.shutdown();
  });
  it("retains the old profile until replacement actually closes its broker", async () => {
    const { engine, session } = await connected();
    const close = natsDeferred<void>();
    engine.disconnectOperation = (): Promise<void> => close.promise;
    const replacement = session.connect(
      "profile-2",
      () => Promise.resolve(natsProfile("profile-2")),
      natsContext,
    );
    expect(session.isProfileInUse("profile-1")).toBe(true);
    expect(session.isProfileInUse("profile-2")).toBe(true);
    await vi.waitFor(() => expect(engine.disconnectCalls).toBe(2));
    close.resolve();
    await replacement;
    expect(session.isProfileInUse("profile-1")).toBe(false);
    expect(session.isProfileInUse("profile-2")).toBe(true);
    await session.shutdown();
  });
  it("reports stopping until actual unsubscribe and pending setup finish", async () => {
    const { engine, session } = await connected();
    await session.start("qualification.>", natsContext);
    const close = natsDeferred<void>();
    engine.stopOperation = (): Promise<void> => close.promise;
    let complete = false;
    const stop = session.stop(natsContext).then((result) => {
      complete = true;
      return result;
    });
    expect(session.snapshot().subscription.state).toBe("stopping");
    await Promise.resolve();
    expect(complete).toBe(false);
    close.resolve();
    expect((await stop).state).toBe("stopped");
    await session.shutdown();
  });
  it("keeps copied setup records invisible until SUB confirmation and discards them on cancellation", async () => {
    vi.useFakeTimers();
    const { engine, session } = await connected();
    const ready = natsDeferred<void>();
    engine.startOperation = (): Promise<void> => ready.promise;
    const batches: unknown[] = [];
    session.subscribe((change) => {
      if (change.event === "records.batch") batches.push(change.payload);
    });
    const start = session.start("qualification.>", natsContext);
    const rejected = expect(start).rejects.toMatchObject({ failure: { code: "cancelled" } });
    await vi.waitFor(() => expect(engine.subscriptionOptions).toBeDefined());
    engine.subscriptionOptions!.onMessage(copiedNatsReceipt());
    await vi.advanceTimersByTimeAsync(100);
    expect(batches).toEqual([]);
    const stop = session.stop(natsContext);
    ready.resolve();
    await rejected;
    expect((await stop).counters).toMatchObject({
      receivedRecords: 1,
      applicationOmittedRecords: 1,
      publishedRecords: 0,
    });
    await vi.advanceTimersByTimeAsync(100);
    expect(batches).toEqual([]);
    await session.shutdown();
  });
  it("preserves permission failure when cancellation completes a failed setup", async () => {
    const { engine, session } = await connected();
    const ready = natsDeferred<void>();
    engine.startOperation = (): Promise<void> => ready.promise;
    const start = session.start("restricted.>", natsContext);
    const rejected = expect(start).rejects.toMatchObject({ failure: { code: "permission" } });
    await vi.waitFor(() => expect(engine.subscriptionOptions).toBeDefined());
    engine.subscriptionOptions!.onFailure({
      code: "permission",
      summary: "Subscription is denied.",
    });
    ready.reject(natsCancelled());
    await rejected;
    expect(session.snapshot().subscription.failure?.code).toBe("permission");
    await session.shutdown();
  });
  it("lets unconfirmed cleanup outrank an earlier permission failure", async () => {
    const { engine, session } = await connected();
    const ready = natsDeferred<void>();
    engine.startOperation = (): Promise<void> => ready.promise;
    const start = session.start("restricted.>", natsContext);
    const rejected = expect(start).rejects.toMatchObject({ failure: { code: "cleanup" } });
    await vi.waitFor(() => expect(engine.subscriptionOptions).toBeDefined());
    engine.subscriptionOptions!.onFailure({
      code: "permission",
      summary: "Subscription is denied.",
    });
    ready.reject(natsCleanupFailure());
    await rejected;
    expect(() => session.start("qualification.>", natsContext)).toThrow(NatsOperationError);
    await session.shutdown();
  });
  it("preserves failed generation and profile reservation until connection-loss cleanup confirms", async () => {
    const { engine, session } = await connected();
    await session.start("qualification.>", natsContext);
    const generation = session.snapshot().subscription.generation;
    const close = natsDeferred<void>();
    engine.disconnectOperation = (): Promise<void> => close.promise;
    engine.connectionOptions!.onConnectionLoss({
      code: "connection",
      summary: "The broker disconnected.",
    });
    expect(session.snapshot().subscription).toMatchObject({ state: "failed", generation });
    expect(session.isProfileInUse("profile-1")).toBe(true);
    close.resolve();
    await vi.waitFor(() => expect(session.isProfileInUse("profile-1")).toBe(false));
    await session.shutdown();
  });
  it("keeps cleanup failure latched and refuses replacement until explicit confirmed disconnect", async () => {
    const { engine, session } = await connected();
    await session.start("qualification.>", natsContext);
    engine.stopOperation = (): Promise<void> => Promise.reject(new Error("secret driver detail"));
    await expect(session.stop(natsContext)).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(() => session.start("qualification.>", natsContext)).toThrow(NatsOperationError);
    await session.disconnect(natsContext);
    expect(session.isProfileInUse("profile-1")).toBe(false);
    engine.stopOperation = (): Promise<void> => Promise.resolve();
    await session.connect("profile-1", () => Promise.resolve(natsProfile()), natsContext);
    await session.start("qualification.>", natsContext);
    await session.shutdown();
  });
  it("retains a confirmed stopped generation when the broker later disconnects", async () => {
    const { engine, session } = await connected();
    await session.start("qualification.>", natsContext);
    const stopped = await session.stop(natsContext);
    engine.connectionOptions!.onConnectionLoss({
      code: "connection",
      summary: "The broker disconnected.",
    });
    expect(session.snapshot().subscription).toEqual(stopped);
    expect(session.snapshot().connection.state).toBe("failed");
    await session.shutdown();
  });
  it("does not let stale disconnect receipts borrow a replacement generation", async () => {
    const { engine, session } = await connected();
    await session.start("qualification.old", natsContext);
    const oldGeneration = session.snapshot().subscription.generation;
    const close = natsDeferred<void>();
    engine.disconnectOperation = (): Promise<void> => close.promise;
    const oldDisconnect = session.disconnect(natsContext);
    const closing = session.snapshot();
    const replacement = session.connect(
      "profile-2",
      () => Promise.resolve(natsProfile("profile-2")),
      natsContext,
    );
    close.resolve();
    await replacement;
    await session.start("qualification.new", natsContext);
    const confirmedOld = await oldDisconnect;
    expect(confirmedOld.subscription.generation).toBe(oldGeneration);
    expect(confirmedOld.connection.revision).toBeGreaterThan(closing.connection.revision);
    expect(confirmedOld.subscription.revision).toBeGreaterThan(closing.subscription.revision);
    expect(confirmedOld.connection.revision).toBeLessThan(session.snapshot().connection.revision);
    expect(confirmedOld.subscription.revision).toBeLessThan(
      session.snapshot().subscription.revision,
    );
    expect(session.snapshot().connection.profile?.id).toBe("profile-2");
    expect(session.snapshot().subscription.state).toBe("streaming");
    await session.shutdown();
  });
  it("returns its confirmed connection receipt when an observer immediately disconnects", async () => {
    const engine = new NatsApplicationEngineFixture();
    const session = new NatsApplicationSession(engine);
    let disconnect: ReturnType<NatsApplicationSession["disconnect"]> | undefined;
    session.subscribe((change) => {
      if (change.event === "connection.state" && change.payload.state === "connected")
        disconnect = session.disconnect(natsContext);
    });
    const result = await session.connect(
      "profile-1",
      () => Promise.resolve(natsProfile()),
      natsContext,
    );
    expect(result).toMatchObject({ state: "connected", profile: { id: "profile-1" } });
    expect(disconnect).toBeDefined();
    await disconnect;
    expect(session.snapshot().connection.state).toBe("disconnected");
    expect(session.snapshot().connection.revision).toBeGreaterThan(result.revision);
    await session.shutdown();
  });
  it("returns its confirmed generation when an observer starts replacement interest", async () => {
    const { session } = await connected();
    let confirmedGeneration: string | null = null;
    let replacement: ReturnType<NatsApplicationSession["start"]> | undefined;
    session.subscribe((change) => {
      if (
        change.event === "subscription.changed" &&
        change.payload.state === "streaming" &&
        change.payload.subject === "qualification.old"
      ) {
        confirmedGeneration = change.payload.generation;
        replacement = session.start("qualification.new", natsContext);
      }
    });
    const result = await session.start("qualification.old", natsContext);
    expect(result).toMatchObject({
      state: "streaming",
      generation: confirmedGeneration,
      subject: "qualification.old",
    });
    expect(replacement).toBeDefined();
    const next = await replacement;
    expect(next?.generation).not.toBe(result.generation);
    expect(next?.revision).toBeGreaterThan(result.revision);
    expect(session.snapshot().subscription).toMatchObject({
      state: "streaming",
      subject: "qualification.new",
    });
    await session.shutdown();
  });
  it("captures paired disconnect events and receipts before a reentrant replacement changes state", async () => {
    const { session } = await connected();
    await session.start("qualification.old", natsContext);
    const cleanupContext = { ...natsContext, correlationId: "disconnect-owner" };
    const replacementContext = { ...natsContext, correlationId: "replacement-owner" };
    const changes: NatsSessionChange[] = [];
    let replacement:
      Promise<ReturnType<NatsApplicationSession["snapshot"]>["connection"]> | undefined;
    session.subscribe((change) => {
      changes.push(change);
      if (
        change.event === "subscription.changed" &&
        change.payload.state === "stopped" &&
        change.context.correlationId === cleanupContext.correlationId
      )
        replacement = session.connect(
          "profile-2",
          () => Promise.resolve(natsProfile("profile-2")),
          replacementContext,
        );
    });
    const receipt = await session.disconnect(cleanupContext);
    expect(replacement).toBeDefined();
    const current = await replacement;
    const announced = changes.find(
      (change) =>
        change.event === "connection.state" &&
        change.context.correlationId === cleanupContext.correlationId &&
        change.payload.state === "disconnected",
    );
    expect(announced?.payload).toEqual(receipt.connection);
    expect(current?.revision).toBeGreaterThan(receipt.connection.revision);
    expect(receipt.subscription.revision).toBeLessThanOrEqual(
      session.snapshot().subscription.revision,
    );
    expect(session.snapshot().connection.profile?.id).toBe("profile-2");
    await session.shutdown();
  });
  it("keeps a confirmed stop receipt older than replacement interest started by its observer", async () => {
    const { session } = await connected();
    const original = await session.start("qualification.old", natsContext);
    let replacement:
      Promise<ReturnType<NatsApplicationSession["snapshot"]>["subscription"]> | undefined;
    session.subscribe((change) => {
      if (
        change.event === "subscription.changed" &&
        change.payload.state === "stopped" &&
        change.payload.generation === original.generation
      )
        replacement = session.start("qualification.new", natsContext);
    });
    const stopped = await session.stop(natsContext);
    expect(stopped.generation).toBe(original.generation);
    expect(stopped.revision).toBeGreaterThan(original.revision);
    expect(replacement).toBeDefined();
    const current = await replacement;
    expect(current?.revision).toBeGreaterThan(stopped.revision);
    expect(current?.generation).not.toBe(stopped.generation);
    await session.shutdown();
  });
  it("gives superseded stop confirmation distinct authority between stopping and replacement", async () => {
    const { engine, session } = await connected();
    const original = await session.start("qualification.old", natsContext);
    const close = natsDeferred<void>();
    engine.stopOperation = (): Promise<void> => close.promise;
    const pending = session.stop(natsContext);
    const stopping = session.snapshot().subscription;
    await vi.waitFor(() => expect(engine.stopCalls).toBe(2));
    engine.stopOperation = (): Promise<void> => Promise.resolve();
    const replacement = await session.start("qualification.new", natsContext);
    close.resolve();
    const confirmed = await pending;
    expect(confirmed.generation).toBe(original.generation);
    expect(confirmed.state).toBe("stopped");
    expect(confirmed.revision).toBeGreaterThan(stopping.revision);
    expect(confirmed.revision).toBeLessThan(replacement.revision);
    expect(session.snapshot().subscription).toEqual(replacement);
    await session.shutdown();
  });
  it.each([false, true])(
    "keeps a later stop's authority when an earlier disconnect confirms (stop failure: %s)",
    async (failed) => {
      const { engine, session } = await connected();
      await session.start("qualification.old", natsContext);
      const close = natsDeferred<void>();
      engine.disconnectOperation = (): Promise<void> => close.promise;
      const disconnect = session.disconnect(natsContext);
      await vi.waitFor(() => expect(engine.disconnectCalls).toBe(2));
      engine.stopOperation = failed
        ? (): Promise<void> => Promise.reject(natsCleanupFailure())
        : (): Promise<void> => Promise.resolve();
      const stop = session.stop(natsContext);
      if (failed) await expect(stop).rejects.toMatchObject({ failure: { code: "cleanup" } });
      else await stop;
      const newer = session.snapshot().subscription;
      close.resolve();
      const earlier = await disconnect;
      expect(earlier.subscription.revision).toBeLessThan(newer.revision);
      expect(session.snapshot().subscription).toEqual(newer);
      expect(session.snapshot().connection.state).toBe("disconnected");
      if (failed) {
        expect(() =>
          session.connect(
            "profile-2",
            () => Promise.resolve(natsProfile("profile-2")),
            natsContext,
          ),
        ).toThrow(NatsOperationError);
        await session.disconnect(natsContext);
        await session.connect(
          "profile-2",
          () => Promise.resolve(natsProfile("profile-2")),
          natsContext,
        );
      }
      await session.shutdown();
    },
  );
  it("advances record counters without changing control-state snapshot authority", async () => {
    vi.useFakeTimers();
    const { engine, session } = await connected();
    const started = await session.start("qualification.*", natsContext);
    engine.subscriptionOptions!.onMessage(copiedNatsReceipt());
    await vi.advanceTimersByTimeAsync(100);
    const afterRecord = session.snapshot().subscription;
    expect(afterRecord.counters.receivedRecords).toBe(1);
    expect(afterRecord.revision).toBe(started.revision);
    const stopped = await session.stop(natsContext);
    expect(stopped.revision).toBeGreaterThan(afterRecord.revision);
    await session.shutdown();
  });
  it("owns shutdown before a reentrant observer tries to admit more work", async () => {
    const engine = new NatsApplicationEngineFixture();
    const session = new NatsApplicationSession(engine);
    let shutdown: Promise<void> | undefined;
    session.subscribe(() => {
      shutdown ??= session.shutdown();
    });
    const connect = session.connect("profile-1", () => Promise.resolve(natsProfile()), natsContext);
    await expect(connect).rejects.toMatchObject({ failure: { code: "cancelled" } });
    await shutdown;
    expect(engine.shutdownCalls).toBe(1);
    expect(() => session.start("qualification.>", natsContext)).toThrow();
  });
  it("validates every terminal snapshot without carrying stale failures into stopped state", async () => {
    const { engine, session } = await connected();
    await session.start("qualification.>", natsContext);
    engine.subscriptionOptions!.onFailure({
      code: "permission",
      summary: "Subscription is denied.",
    });
    const stop = await session.stop(natsContext);
    expect(stop).not.toHaveProperty("failure");
    expect(parseNatsSubscriptionSnapshot(stop)).toEqual(stop);
    await session.shutdown();
  });
});
