import { afterEach, describe, expect, it, vi } from "vitest";

import { NatsOperationError } from "../../src/features/nats/application/failure";
import { NatsApplicationSession } from "../../src/features/nats/application/session";
import {
  InMemoryNatsProfileStore,
  NatsProfileService,
} from "../../src/features/nats/application/profile-service";
import type { NatsProfileStore } from "../../src/features/nats/application/profile-types";
import {
  parseCorrelatedNatsResponse,
  parseNatsEvent,
  NATS_PROVIDER_EVENT_CODEC,
  NATS_PROTOCOL_VERSION,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsProfileCreateInput,
  type NatsProfilesSnapshot,
} from "../../src/features/nats/contracts";
import type { NatsBackendFacade } from "../../src/features/nats/facade";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import {
  NatsApplicationEngineFixture,
  copiedNatsReceipt,
} from "../support/nats-application-fixture";
import { natsDeferred } from "../support/nats-engine-fixture";

const backends: NatsBackendFacade[] = [];
const releaseFixtures: (() => void)[] = [];
const draft: NatsProfileCreateInput = {
  name: "Facade NATS",
  servers: ["nats://localhost:4222"],
  authentication: { mode: "token", token: { mode: "replace", value: "facade-private-token" } },
  tls: { mode: "plaintext" },
};
function composed(
  profileStore?: NatsProfileStore,
  createCorrelationId?: () => string,
): { readonly backend: NatsBackendFacade; readonly engine: NatsApplicationEngineFixture } {
  const engine = new NatsApplicationEngineFixture();
  let id = 0;
  const backend = createNatsBackend({
    engine,
    ...(profileStore === undefined ? {} : { profileStore }),
    profileServiceOptions: {
      createId: () => `profile-${++id}`,
      now: () => new Date("2026-10-05T13:00:00.000Z"),
    },
    ...(createCorrelationId === undefined ? {} : { facadeOptions: { createCorrelationId } }),
  });
  backends.push(backend);
  return { backend, engine };
}
function request<Name extends NatsCommand["command"]>(
  command: Name,
  payload: Extract<NatsCommand, { command: Name }>["payload"],
): Extract<NatsCommand, { command: Name }> {
  // Test requests use the same strict discriminator/payload pairing as feature callers.
  return { version: NATS_PROTOCOL_VERSION, id: crypto.randomUUID(), command, payload } as Extract<
    NatsCommand,
    { command: Name }
  >;
}
async function connect(backend: NatsBackendFacade): Promise<void> {
  expect((await backend.execute(request("profiles.create", { profile: draft }))).ok).toBe(true);
  expect(
    (
      await backend.execute(
        request("profiles.connect", { profileId: "profile-1", expectedRevision: 1 }),
      )
    ).ok,
  ).toBe(true);
}
afterEach(async () => {
  for (const release of releaseFixtures.splice(0)) release();
  await Promise.allSettled(backends.splice(0).map((backend) => backend.shutdown()));
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("Core NATS facade through production composition", () => {
  it("keeps a delayed read snapshot older than a later published profile mutation", async () => {
    const { backend } = composed();
    const created = await backend.execute(request("profiles.create", { profile: draft }));
    if (!created.ok) throw new Error("Expected initial profile commit.");
    const captured = natsDeferred<void>();
    const release = natsDeferred<void>();
    releaseFixtures.push(() => release.resolve());
    vi.spyOn(NatsProfileService.prototype, "list").mockImplementationOnce(async function (
      this: NatsProfileService,
      signal?: AbortSignal,
    ): Promise<NatsProfilesSnapshot> {
      // The one-time override has been consumed, so this bound call delegates to the real list.
      const snapshot = await this.list(signal);
      captured.resolve();
      await release.promise;
      return snapshot;
    });
    const staleRead = backend.execute(request("profiles.list", {}));
    await captured.promise;
    const current = await backend.execute(
      request("profiles.update", {
        profileId: "profile-1",
        expectedRevision: 1,
        profile: {
          ...draft,
          name: "Newer committed name",
          authentication: { mode: "token", token: { mode: "retain" } },
        },
      }),
    );
    if (!current.ok) throw new Error("Expected updated profile commit.");
    release.resolve();
    const old = await staleRead;
    if (!old.ok) throw new Error("Expected captured profile read.");
    expect(old.result.profiles.revision).toBe(created.result.profiles.revision);
    expect(old.result.profiles.revision).toBeLessThan(current.result.profiles.revision);
    expect(old.result.profiles.profiles[0]?.name).toBe("Facade NATS");
    expect(current.result.profiles.profiles[0]?.name).toBe("Newer committed name");
  });
  it("preserves profile commit authority when an event observer immediately admits another write", async () => {
    const { backend } = composed();
    const events: NatsEvent[] = [];
    let replacement: Promise<NatsCommandResponse<"profiles.update">> | undefined;
    backend.subscribe((event) => {
      events.push(event);
      if (event.event === "profiles.changed" && event.payload.profiles[0]?.revision === 1)
        replacement = backend.execute(
          request("profiles.update", {
            profileId: "profile-1",
            expectedRevision: 1,
            profile: {
              ...draft,
              name: "Observer replacement",
              authentication: { mode: "token", token: { mode: "retain" } },
            },
          }),
        );
    });
    const committed = await backend.execute(request("profiles.create", { profile: draft }));
    if (!committed.ok) throw new Error("Expected initial profile commit.");
    expect(replacement).toBeDefined();
    const replaced = await replacement;
    if (replaced === undefined || !replaced.ok)
      throw new Error("Expected observer profile update.");
    const firstEvent = events.find(
      (event) => event.event === "profiles.changed" && event.payload.profiles[0]?.revision === 1,
    );
    expect(firstEvent?.payload).toEqual(committed.result.profiles);
    expect(replaced.result.profiles.revision).toBeGreaterThan(committed.result.profiles.revision);
    expect(committed.result.profiles.profiles[0]?.name).toBe("Facade NATS");
  });
  it("completes the declared eight-command workflow with strictly correlated receipts", async () => {
    const { backend } = composed();
    const creation = request("profiles.create", { profile: draft });
    const created = await backend.execute(creation);
    expect(parseCorrelatedNatsResponse(created, creation)).toEqual(created);
    expect(created.ok).toBe(true);
    const renamed = await backend.execute(
      request("profiles.update", {
        profileId: "profile-1",
        expectedRevision: 1,
        profile: {
          ...draft,
          name: "Renamed",
          authentication: { mode: "token", token: { mode: "retain" } },
        },
      }),
    );
    expect(renamed.ok).toBe(true);
    expect(
      (
        await backend.execute(
          request("profiles.connect", { profileId: "profile-1", expectedRevision: 2 }),
        )
      ).ok,
    ).toBe(true);
    const subscription = await backend.execute(
      request("subscription.start", { subject: "qualification.*" }),
    );
    expect(subscription).toMatchObject({
      ok: true,
      result: { subscription: { state: "streaming", subject: "qualification.*" } },
    });
    expect(await backend.execute(request("profiles.list", {}))).toMatchObject({
      ok: true,
      result: {
        profiles: { profiles: [{ name: "Renamed", revision: 2 }] },
        connection: { state: "connected" },
        subscription: { state: "streaming" },
      },
    });
    expect(await backend.execute(request("subscription.stop", {}))).toMatchObject({
      ok: true,
      result: { subscription: { state: "stopped" } },
    });
    expect(await backend.execute(request("connection.disconnect", {}))).toMatchObject({
      ok: true,
      result: { connection: { state: "disconnected" }, subscription: { state: "stopped" } },
    });
    expect(
      await backend.execute(
        request("profiles.delete", { profileId: "profile-1", expectedRevision: 2 }),
      ),
    ).toMatchObject({ ok: true, result: { profiles: { profiles: [] } } });
  });

  it("publishes a safe committed catalog with independent sequence/correlation ownership", async () => {
    const { backend } = composed(undefined, () => "correlation-1");
    const events: NatsEvent[] = [];
    backend.subscribe((event) => events.push(event));
    const creation = await backend.execute(request("profiles.create", { profile: draft }));
    expect(events[0]).toMatchObject({ event: "backend.availability", payload: { state: "ready" } });
    expect(events[1]).toMatchObject({
      event: "profiles.changed",
      operation: "profiles.create",
      correlationId: "correlation-1",
    });
    expect(events.map((event) => event.sequence)).toEqual([1, 2]);
    for (const event of events) expect(parseNatsEvent(event)).toEqual(event);
    expect(JSON.stringify([creation, events])).not.toContain("facade-private-token");
  });

  it("reserves a pending connection profile before a concurrent edit can enter its FIFO", async () => {
    const { backend, engine } = composed();
    expect((await backend.execute(request("profiles.create", { profile: draft }))).ok).toBe(true);
    const pending = natsDeferred<void>();
    releaseFixtures.push(() => pending.resolve());
    engine.disconnectOperation = (): Promise<void> => pending.promise;
    const connecting = backend.execute(
      request("profiles.connect", { profileId: "profile-1", expectedRevision: 1 }),
    );
    const edit = await backend.execute(
      request("profiles.update", {
        profileId: "profile-1",
        expectedRevision: 1,
        profile: { ...draft, name: "Blocked edit" },
      }),
    );
    expect(edit).toMatchObject({ ok: false, error: { code: "profile-in-use" } });
    pending.resolve();
    expect((await connecting).ok).toBe(true);
  });

  it("rehydrates current state after the asynchronous catalog read completes", async () => {
    const memory = new InMemoryNatsProfileStore();
    const gate = natsDeferred<void>();
    const started = natsDeferred<void>();
    releaseFixtures.push(() => gate.resolve());
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: async (signal) => {
        started.resolve();
        await gate.promise;
        return memory.load(signal);
      },
      save: (records, signal) => memory.save(records, signal),
    };
    const { backend, engine } = composed(store);
    const listing = backend.execute(request("profiles.list", {}));
    await started.promise;
    const closeGate = natsDeferred<void>();
    releaseFixtures.push(() => closeGate.resolve());
    engine.disconnectOperation = (): Promise<void> => closeGate.promise;
    const closing = backend.execute(request("connection.disconnect", {}));
    gate.resolve();
    expect(await listing).toMatchObject({
      ok: true,
      result: {
        connection: { state: "disconnecting", profile: null },
        subscription: { state: "idle" },
      },
    });
    closeGate.resolve();
    await closing;
  });

  it("keeps stale revisions from reaching the actual connection engine", async () => {
    const { backend, engine } = composed();
    await backend.execute(request("profiles.create", { profile: draft }));
    expect(
      await backend.execute(
        request("profiles.connect", { profileId: "profile-1", expectedRevision: 2 }),
      ),
    ).toMatchObject({
      ok: false,
      error: { code: "revision-conflict", operation: "profiles.connect" },
    });
    expect(engine.connectionOptions).toBeUndefined();
  });

  it("returns safe structured failures without copying unknown SDK diagnostic text", async () => {
    const { backend, engine } = composed();
    await backend.execute(request("profiles.create", { profile: draft }));
    engine.connectOperation = (): Promise<void> =>
      Promise.reject(
        new NatsOperationError(
          { code: "authentication", summary: "NATS authentication was rejected." },
          { cause: new Error("facade-private-token") },
        ),
      );
    const response = await backend.execute(
      request("profiles.connect", { profileId: "profile-1", expectedRevision: 1 }),
    );
    expect(response).toMatchObject({
      ok: false,
      error: { code: "authentication", stage: "connection", operation: "profiles.connect" },
    });
    expect(JSON.stringify(response)).not.toContain("facade-private-token");
    expect(
      parseCorrelatedNatsResponse(response, {
        id: response.id,
        version: NATS_PROTOCOL_VERSION,
        command: "profiles.connect",
        payload: { profileId: "profile-1", expectedRevision: 1 },
      }),
    ).toEqual(response);
  });

  it("isolates a failing presentation observer while preserving events for another consumer", async () => {
    const { backend } = composed();
    backend.subscribe(() => {
      throw new Error("view failed");
    });
    const seen: NatsEvent[] = [];
    backend.subscribe((event) => seen.push(event));
    expect((await backend.execute(request("profiles.create", { profile: draft }))).ok).toBe(true);
    expect(seen.some((event) => event.event === "profiles.changed")).toBe(true);
  });

  it("waits for actual selected subscription stop and keeps the connection available", async () => {
    const { backend, engine } = composed();
    await connect(backend);
    await backend.execute(request("subscription.start", { subject: "qualification.>" }));
    const gate = natsDeferred<void>();
    releaseFixtures.push(() => gate.resolve());
    engine.stopOperation = (): Promise<void> => gate.promise;
    let confirmed = false;
    const cleanup = backend.stopStream().then(() => {
      confirmed = true;
    });
    await Promise.resolve();
    expect(confirmed).toBe(false);
    expect(backend.snapshot().subscription.state).toBe("stopping");
    gate.resolve();
    await cleanup;
    expect(backend.snapshot()).toMatchObject({
      connection: { state: "connected" },
      subscription: { state: "stopped" },
    });
  });

  it("rejects unconfirmed selected cleanup instead of returning an acknowledgement", async () => {
    const { backend, engine } = composed();
    await connect(backend);
    await backend.execute(request("subscription.start", { subject: "qualification.>" }));
    engine.stopOperation = (): Promise<void> => Promise.reject(new Error("unknown cleanup detail"));
    await expect(backend.stopStream()).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(
      await backend.execute(request("subscription.start", { subject: "qualification.>" })),
    ).toMatchObject({ ok: false, error: { code: "cleanup" } });
  });

  it("owns shutdown before a reentrant availability observer requests selected cleanup", async () => {
    const { backend, engine } = composed();
    let reentrant: Promise<void> | undefined;
    backend.subscribe((event) => {
      if (event.event === "backend.availability" && event.payload.state === "unavailable")
        reentrant = backend.stopStream();
    });
    const shutdown = backend.shutdown();
    expect(reentrant).toBe(shutdown);
    await shutdown;
    expect(engine.shutdownCalls).toBe(1);
    expect(backend.shutdown()).toBe(shutdown);
  });

  it("still closes real owners and rejects its cached barrier when final availability encoding fails", async () => {
    const { backend, engine } = composed();
    vi.spyOn(NATS_PROVIDER_EVENT_CODEC, "availability").mockImplementationOnce(() => {
      throw new Error("Final availability could not be encoded.");
    });
    const shutdown = backend.shutdown();
    expect(backend.stopStream()).toBe(shutdown);
    await expect(shutdown).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(engine.shutdownCalls).toBe(1);
    expect(backend.shutdown()).toBe(shutdown);
  });

  it("retires actual owners if initial availability cannot be encoded", async () => {
    const { backend, engine } = composed();
    vi.spyOn(NATS_PROVIDER_EVENT_CODEC, "availability").mockImplementationOnce(() => {
      throw new Error("private availability detail");
    });
    const listener = vi.fn();
    expect(() => backend.subscribe(listener)).toThrow(
      "NATS input does not match the supported contract.",
    );
    expect(listener).not.toHaveBeenCalled();
    await backend.shutdown();
    expect(engine.shutdownCalls).toBe(1);
    expect(await backend.execute(request("profiles.list", {}))).toMatchObject({
      ok: false,
      error: { code: "unavailable" },
    });
  });

  it("returns a safe failure and joins actual cleanup after an invalid internal response", async () => {
    const { backend, engine } = composed();
    vi.spyOn(NatsApplicationSession.prototype, "snapshot").mockImplementationOnce(() => ({
      connection: { revision: 0, state: "connected", profile: null },
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
    }));
    const response = await backend.execute(request("profiles.list", {}));
    expect(response).toMatchObject({
      ok: false,
      error: { code: "unavailable", stage: "lifecycle" },
    });
    await backend.shutdown();
    expect(engine.shutdownCalls).toBe(1);
  });

  it("retains an admitted real commit receipt while shutdown waits for the profile owner", async () => {
    const memory = new InMemoryNatsProfileStore();
    const gate = natsDeferred<void>();
    const started = natsDeferred<void>();
    releaseFixtures.push(() => gate.resolve());
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        started.resolve();
        await gate.promise;
        await memory.save(records, signal);
      },
    };
    const { backend } = composed(store);
    const creation = backend.execute(request("profiles.create", { profile: draft }));
    await started.promise;
    let closed = false;
    const shutdown = backend.shutdown().then(() => {
      closed = true;
    });
    await Promise.resolve();
    expect(closed).toBe(false);
    expect(await backend.execute(request("profiles.list", {}))).toMatchObject({
      ok: false,
      error: { code: "unavailable" },
    });
    gate.resolve();
    const response = await creation;
    expect(response).toMatchObject({
      ok: true,
      result: { profiles: { profiles: [{ revision: 1 }] } },
    });
    expect((await memory.load())[0]!.revision).toBe(1);
    await shutdown;
  });

  it("preserves a genuine profile commit receipt when event sequence authority is exhausted", async () => {
    const memory = new InMemoryNatsProfileStore();
    const { backend, engine } = composed(memory);
    // Exhaust the facade's private authority to exercise a real publication failure after save.
    Reflect.set(backend, "sequence", Number.MAX_SAFE_INTEGER);
    const creation = await backend.execute(request("profiles.create", { profile: draft }));
    expect(creation).toMatchObject({
      ok: true,
      result: { profiles: { profiles: [{ id: "profile-1", revision: 1 }] } },
    });
    expect((await memory.load())[0]!.revision).toBe(1);
    await expect(backend.shutdown()).rejects.toMatchObject({ failure: { code: "cleanup" } });
    expect(engine.shutdownCalls).toBe(1);
    expect(await backend.execute(request("profiles.list", {}))).toMatchObject({
      ok: false,
      error: { code: "unavailable" },
    });
  });

  it("retains the admitted request identity while the caller reuses its mutable input object", async () => {
    const memory = new InMemoryNatsProfileStore();
    const gate = natsDeferred<void>();
    const started = natsDeferred<void>();
    releaseFixtures.push(() => gate.resolve());
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        started.resolve();
        await gate.promise;
        await memory.save(records, signal);
      },
    };
    const { backend } = composed(store);
    const creation = {
      version: NATS_PROTOCOL_VERSION,
      command: "profiles.create" as const,
      id: "original-request",
      payload: { profile: draft },
    };
    const pending = backend.execute(creation);
    await started.promise;
    creation.id = "later-request";
    gate.resolve();
    expect(await pending).toMatchObject({
      id: "original-request",
      command: "profiles.create",
      ok: true,
    });
  });

  it("joins admitted profile work even when actual engine shutdown fails", async () => {
    const memory = new InMemoryNatsProfileStore();
    const gate = natsDeferred<void>();
    const started = natsDeferred<void>();
    releaseFixtures.push(() => gate.resolve());
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        started.resolve();
        await gate.promise;
        await memory.save(records, signal);
      },
    };
    const { backend, engine } = composed(store);
    engine.shutdownOperation = (): Promise<void> =>
      Promise.reject(new Error("engine cleanup detail"));
    const creation = backend.execute(request("profiles.create", { profile: draft }));
    await started.promise;
    let settled = false;
    const shutdown = backend.shutdown();
    const rejected = expect(shutdown)
      .rejects.toMatchObject({ failure: { code: "cleanup" } })
      .then(() => {
        settled = true;
      });
    await Promise.resolve();
    expect(settled).toBe(false);
    gate.resolve();
    expect((await creation).ok).toBe(true);
    await rejected;
  });

  it("retires the actual provider after an invalid internal record event instead of silently capturing", async () => {
    vi.useFakeTimers();
    const { backend, engine } = composed();
    await connect(backend);
    await backend.execute(request("subscription.start", { subject: "qualification.>" }));
    const receipt = copiedNatsReceipt();
    if (receipt.kind !== "record") throw new Error("Expected retained fixture record.");
    engine.subscriptionOptions!.onMessage({
      kind: "record",
      record: { ...receipt.record, subject: "invalid subject" },
    });
    await vi.advanceTimersByTimeAsync(30);
    await backend.shutdown();
    expect(engine.shutdownCalls).toBe(1);
    expect(await backend.execute(request("profiles.list", {}))).toMatchObject({
      ok: false,
      error: { code: "unavailable" },
    });
  });

  it("rejects invalid correlation authority before admitting a backend operation", async () => {
    const { backend, engine } = composed(undefined, () => "bad\nidentifier");
    await expect(backend.execute(request("profiles.list", {}))).rejects.toThrow();
    expect(engine.connectionOptions).toBeUndefined();
  });
});
