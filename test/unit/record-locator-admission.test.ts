import { describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostCommand } from "../../src/features/kafka/contracts";
import { KafkaCommandAdmission } from "../../src/features/kafka/facade/command-admission";
import { failureResponse } from "../../src/features/kafka/facade/facade-support";

const locator = {
  schemaVersion: 1,
  clusterId: "cluster-a",
  topicId: "27c1c482-b9e0-43f2-abd0-ae257fd6a6df",
  topic: "orders",
  partition: 0,
  offset: "1",
  leaderEpoch: 0,
} as const;
const load = (requestId = "05e2ed75-45e2-40b1-bf51-dde810e83090"): HostCommand => ({
  command: "records.locator.load",
  version: HOST_PROTOCOL_VERSION,
  id: "load",
  payload: { requestId, locator },
});
const cancel = (requestId = "05e2ed75-45e2-40b1-bf51-dde810e83090"): HostCommand => ({
  command: "records.locator.cancel",
  version: HOST_PROTOCOL_VERSION,
  id: "cancel",
  payload: { requestId },
});
type Bindings = Parameters<KafkaCommandAdmission["dispatch"]>[1];
function deferred<T = void>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function suppression(binding: Bindings): Promise<boolean | undefined> {
  return vi.mocked(binding.dispatch).mock.calls[0]?.[0]?.suppressedLocatorLoad;
}
function bindings(command: HostCommand): Bindings {
  const reply = (summary: string): ReturnType<typeof failureResponse> =>
    failureResponse(command, {
      code: "CANCELLED",
      stage: "query",
      correlationId: "test",
      activeStateChanged: false,
      retryable: true,
      summary,
      recovery: "Retry with a fresh request.",
    });
  return {
    correlationId: "test",
    internal: false,
    protection: { execute: (_command, _correlation, dispatch) => dispatch() },
    unavailable: () => reply("closed"),
    superseded: () => reply("superseded"),
    dispatch: vi.fn<Bindings["dispatch"]>(async (context) => {
      await context?.suppressedLocatorLoad;
      return reply("dispatched");
    }),
  };
}
function delayed(command: HostCommand): { binding: Bindings; release: () => void } {
  const gate = deferred();
  return {
    binding: {
      ...bindings(command),
      protection: {
        execute: async (_command, _correlation, dispatch): ReturnType<Bindings["dispatch"]> => {
          await gate.promise;
          return dispatch();
        },
      },
    },
    release: () => gate.resolve(),
  };
}

describe("record reload command admission", () => {
  it("retains bounded proof for a denied request so a later exact Stop can finish", async () => {
    const admission = new KafkaCommandAdmission();
    const request = load();
    const denied: Bindings = {
      ...bindings(request),
      protection: {
        execute: () => Promise.resolve(bindings(request).unavailable()),
      },
    };
    expect(await admission.dispatch(request, denied)).toMatchObject({ ok: false });
    expect(denied.dispatch).not.toHaveBeenCalled();
    const stop = bindings(cancel());
    await admission.dispatch(cancel(), stop);
    expect(await suppression(stop)).toBe(true);
    const other = bindings(cancel("another-request"));
    await admission.dispatch(cancel("another-request"), other);
    expect(await suppression(other)).toBe(false);
    // A fresh handoff for the same ID replaces that old no-reader proof.
    await admission.dispatch(load(), bindings(load()));
    const later = bindings(cancel());
    await admission.dispatch(cancel(), later);
    expect(await suppression(later)).toBe(false);
  });
  it("cancels a load held by asynchronous protection before it can open a reader", async () => {
    const admission = new KafkaCommandAdmission();
    const pending = delayed(load());
    const read = admission.dispatch(load(), pending.binding);
    const stop = bindings(cancel());
    const cancelled = admission.dispatch(cancel(), stop);
    await Promise.resolve();
    expect(stop.dispatch).toHaveBeenCalledOnce();
    pending.release();
    expect(await read).toMatchObject({ ok: false, error: { code: "CANCELLED" } });
    await cancelled;
    expect(pending.binding.dispatch).not.toHaveBeenCalled();
    expect(await suppression(stop)).toBe(true);
  });
  it("fences duplicate loads while a matching cancellation is awaiting admission settlement", async () => {
    const admission = new KafkaCommandAdmission();
    const pending = delayed(load());
    const read = admission.dispatch(load(), pending.binding);
    const stop = admission.dispatch(cancel(), bindings(cancel()));
    const duplicate = bindings(load());
    expect(await admission.dispatch(load(), duplicate)).toMatchObject({
      ok: false,
      error: { code: "VALIDATION" },
    });
    expect(duplicate.dispatch).not.toHaveBeenCalled();
    pending.release();
    await Promise.all([read, stop]);
  });
  it("never calls a handed-off reader suppressed and does not join its result before cancelling", async () => {
    const admission = new KafkaCommandAdmission();
    const gate = deferred<Awaited<ReturnType<Bindings["dispatch"]>>>();
    const current: Bindings = { ...bindings(load()), dispatch: vi.fn(() => gate.promise) };
    const read = admission.dispatch(load(), current);
    const stop = bindings(cancel());
    const stopped = admission.dispatch(cancel(), stop);
    expect(stop.dispatch).toHaveBeenCalledOnce();
    gate.resolve(await bindings(load()).dispatch());
    await Promise.all([read, stopped]);
    expect(await suppression(stop)).toBe(false);
  });
  it("does not cancel a different request or claim an unknown request stopped", async () => {
    const admission = new KafkaCommandAdmission();
    const pending = delayed(load());
    const read = admission.dispatch(load(), pending.binding);
    const other = cancel("ef7341c3-2fef-4aed-9d4d-340e627a5547");
    const stop = bindings(other);
    await admission.dispatch(other, stop);
    expect(await suppression(stop)).toBe(false);
    pending.release();
    await read;
    expect(pending.binding.dispatch).toHaveBeenCalledOnce();
  });
  it("dispatches stop immediately when an active reader has a duplicate awaiting protection", async () => {
    const admission = new KafkaCommandAdmission();
    const activeGate = deferred<Awaited<ReturnType<Bindings["dispatch"]>>>();
    const active = admission.dispatch(load(), {
      ...bindings(load()),
      dispatch: () => activeGate.promise,
    });
    const duplicate = delayed(load());
    const queued = admission.dispatch(load(), duplicate.binding);
    const stop = bindings(cancel());
    let confirmed = false;
    const stopped = admission.dispatch(cancel(), stop).then(() => {
      confirmed = true;
    });
    await Promise.resolve();
    expect(stop.dispatch).toHaveBeenCalledOnce();
    expect(confirmed).toBe(false);
    duplicate.release();
    await queued;
    activeGate.resolve(await bindings(load()).dispatch());
    await stopped;
    expect(await suppression(stop)).toBe(false);
    expect(duplicate.binding.dispatch).not.toHaveBeenCalled();
    await active;
  });
  it.each(["preferences.reset", "connection.disconnect"] as const)(
    "revokes a pending reload before %s proceeds",
    async (command) => {
      const admission = new KafkaCommandAdmission();
      const pending = delayed(load());
      const read = admission.dispatch(load(), pending.binding);
      const change: HostCommand = {
        command,
        payload: {},
        id: "change",
        version: HOST_PROTOCOL_VERSION,
      };
      await admission.dispatch(change, bindings(change));
      pending.release();
      await read;
      expect(pending.binding.dispatch).not.toHaveBeenCalled();
    },
  );
  it("refuses reentrant reloads until connection cleanup settles", async () => {
    const admission = new KafkaCommandAdmission();
    const disconnect: HostCommand = {
      command: "connection.disconnect",
      id: "disconnect",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    };
    const pending = delayed(disconnect);
    const change = admission.dispatch(disconnect, pending.binding);
    const next = bindings(load());
    expect(await admission.dispatch(load(), next)).toMatchObject({
      ok: false,
      error: { code: "VALIDATION" },
    });
    expect(next.dispatch).not.toHaveBeenCalled();
    pending.release();
    await change;
  });
  it("settles denied/throwing protection without leaking its admission or deadlocking cancel", async () => {
    const admission = new KafkaCommandAdmission();
    const gate = deferred();
    const current: Bindings = {
      ...bindings(load()),
      protection: {
        execute: async () => {
          await gate.promise;
          throw new Error("denied");
        },
      },
    };
    const read = admission.dispatch(load(), current).catch((error: unknown) => error);
    const stop = bindings(cancel());
    const stopped = admission.dispatch(cancel(), stop);
    gate.resolve();
    await Promise.all([read, stopped]);
    expect(await suppression(stop)).toBe(true);
    const fresh = bindings(load());
    await admission.dispatch(load(), fresh);
    expect(fresh.dispatch).toHaveBeenCalledOnce();
  });
  it("shutdown suppresses queued readers while still settling their command responses", async () => {
    const admission = new KafkaCommandAdmission();
    const pending = delayed(load());
    const read = admission.track(() => admission.dispatch(load(), pending.binding));
    admission.close();
    pending.release();
    expect(await read).toMatchObject({ ok: false, error: { summary: "closed" } });
    await admission.idle();
    expect(pending.binding.dispatch).not.toHaveBeenCalled();
  });
});
