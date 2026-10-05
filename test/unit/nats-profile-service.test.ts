import { describe, expect, it } from "vitest";

import {
  InMemoryNatsProfileStore,
  NatsProfileService,
  UnavailableNatsProfileStore,
} from "../../src/features/nats/application/profile-service";
import type {
  NatsProfileRecord,
  NatsProfileStore,
} from "../../src/features/nats/application/profile-types";
import type {
  NatsProfileCreateInput,
  NatsProfileStoreCapability,
  NatsProfileUpdateInput,
} from "../../src/features/nats/contracts";

const pem = "-----BEGIN CERTIFICATE-----\nZGVy\n-----END CERTIFICATE-----\n";
const draft: NatsProfileCreateInput = {
  name: "Private NATS",
  servers: ["nats://localhost:4222"],
  authentication: { mode: "token", token: { mode: "replace", value: "private-token-value" } },
  tls: { mode: "tls", caPem: { mode: "replace", value: pem } },
};
const retained: NatsProfileUpdateInput = {
  ...draft,
  authentication: { mode: "token", token: { mode: "retain" } },
  tls: { mode: "tls", caPem: { mode: "retain" } },
};
function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}
function service(
  store: NatsProfileStore,
  isProfileInUse: (id: string) => boolean = () => false,
): NatsProfileService {
  let next = 0;
  return new NatsProfileService(store, {
    createId: () => `profile-${++next}`,
    now: () => new Date("2026-10-05T13:00:00.000Z"),
    isProfileInUse,
  });
}

describe("Core NATS profile application authority", () => {
  it("keeps catalog authority monotonic when protection changes during a pending commit", async () => {
    const memory = new InMemoryNatsProfileStore();
    const saving = deferred<void>();
    const finish = deferred<void>();
    let ready = true;
    let pause = false;
    const store: NatsProfileStore = {
      get capability(): NatsProfileStoreCapability {
        return ready
          ? memory.capability
          : { durability: "session", protection: "unavailable", state: "unavailable" };
      },
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        if (pause) {
          saving.resolve();
          await finish.promise;
        }
        await memory.save(records, signal);
      },
    };
    const profiles = service(store);
    const original = await profiles.create(draft);
    pause = true;
    const update = profiles.update("profile-1", 1, {
      ...retained,
      name: "After protection change",
    });
    await saving.promise;
    ready = false;
    const unavailable = await profiles.list();
    expect(unavailable.revision).toBeGreaterThan(original.revision);
    ready = true;
    const recovered = await profiles.list();
    expect(recovered.revision).toBeGreaterThan(unavailable.revision);
    finish.resolve();
    const committed = await update;
    expect(committed.revision).toBeGreaterThan(recovered.revision);
    expect(committed.capability.state).toBe("ready");
    expect(unavailable.capability.state).toBe("unavailable");
  });
  it("captures committed catalog authority while concurrent reads still expose the prior commit", async () => {
    const memory = new InMemoryNatsProfileStore();
    const saving = deferred<void>();
    const finish = deferred<void>();
    let pause = false;
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        if (pause) {
          saving.resolve();
          await finish.promise;
        }
        await memory.save(records, signal);
      },
    };
    const profiles = service(store);
    const original = await profiles.create(draft);
    pause = true;
    const update = profiles.update("profile-1", 1, { ...retained, name: "Committed update" });
    await saving.promise;
    const concurrentRead = await profiles.list();
    expect(concurrentRead.revision).toBe(original.revision);
    expect(concurrentRead.profiles[0]?.name).toBe("Private NATS");
    finish.resolve();
    const committed = await update;
    expect(committed.revision).toBeGreaterThan(concurrentRead.revision);
    expect((await profiles.list()).revision).toBe(committed.revision);
    pause = false;
    const removed = await profiles.delete("profile-1", 2);
    expect(removed.revision).toBeGreaterThan(committed.revision);
    expect(committed.profiles[0]?.name).toBe("Committed update");
    expect(committed.profiles[0]?.revision).toBe(2);
  });
  it("retains credentials for host connection while exposing only presence flags", async () => {
    const store = new InMemoryNatsProfileStore();
    const profiles = service(store);
    const snapshot = await profiles.create(draft);
    const exposed = JSON.stringify(snapshot);
    expect(exposed).not.toContain("private-token-value");
    expect(exposed).not.toContain("BEGIN CERTIFICATE");
    expect(snapshot.profiles[0]!.authentication).toEqual({ mode: "token", tokenPresent: true });
    expect(snapshot.capability).toEqual({
      durability: "session",
      protection: "memory",
      state: "ready",
    });
    const restarted = service(store);
    expect((await restarted.resolve("profile-1", 1)).connection).toEqual({
      servers: draft.servers,
      authentication: { mode: "token", token: "private-token-value" },
      tls: { mode: "tls", caPem: pem },
    });
  });

  it("edits with retain then explicitly replaces/removes protected fields", async () => {
    const store = new InMemoryNatsProfileStore();
    const profiles = service(store);
    await profiles.create(draft);
    await profiles.update("profile-1", 1, { ...retained, name: "Renamed" });
    expect((await profiles.resolve("profile-1", 2)).connection.authentication).toEqual({
      mode: "token",
      token: "private-token-value",
    });
    await profiles.update("profile-1", 2, {
      ...retained,
      authentication: { mode: "token", token: { mode: "replace", value: "replacement-token" } },
      tls: { mode: "tls", caPem: { mode: "clear" } },
    });
    expect((await service(store).resolve("profile-1", 3)).connection).toMatchObject({
      authentication: { mode: "token", token: "replacement-token" },
      tls: { mode: "tls" },
    });
    expect((await profiles.resolve("profile-1", 3)).connection.tls).not.toHaveProperty("caPem");
    await profiles.update("profile-1", 3, {
      ...retained,
      authentication: { mode: "none" },
      tls: { mode: "plaintext" },
    });
    expect((await service(store).resolve("profile-1", 4)).connection).toEqual({
      servers: draft.servers,
      authentication: { mode: "none" },
      tls: { mode: "plaintext" },
    });
  });

  it("rejects stale edits/deletion/connection resolution without changing stored credentials", async () => {
    const store = new InMemoryNatsProfileStore();
    const profiles = service(store);
    await profiles.create(draft);
    await profiles.update("profile-1", 1, { ...retained, name: "Current revision" });
    const before = await store.load();
    await expect(profiles.update("profile-1", 1, retained)).rejects.toMatchObject({
      code: "revision-conflict",
    });
    await expect(profiles.delete("profile-1", 1)).rejects.toMatchObject({
      code: "revision-conflict",
    });
    await expect(profiles.resolve("profile-1", 1)).rejects.toMatchObject({
      code: "revision-conflict",
    });
    expect(await store.load()).toEqual(before);
  });

  it("prevents edits and deletion while a pending/active profile belongs to the session", async () => {
    let active = false;
    const store = new InMemoryNatsProfileStore();
    const profiles = service(store, () => active);
    await profiles.create(draft);
    active = true;
    await expect(profiles.update("profile-1", 1, retained)).rejects.toMatchObject({
      code: "profile-in-use",
    });
    await expect(profiles.delete("profile-1", 1)).rejects.toMatchObject({ code: "profile-in-use" });
    active = false;
    expect((await profiles.delete("profile-1", 1)).profiles).toEqual([]);
  });

  it("waits for an earlier commit before resolving a connection revision", async () => {
    const memory = new InMemoryNatsProfileStore();
    const gate = deferred<void>();
    const started = deferred<void>();
    let pause = false;
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        if (pause) {
          started.resolve();
          await gate.promise;
        }
        await memory.save(records, signal);
      },
    };
    const profiles = service(store);
    await profiles.create(draft);
    pause = true;
    const update = profiles.update("profile-1", 1, { ...retained, name: "Committed name" });
    await started.promise;
    let resolved = false;
    const resolve = profiles.resolve("profile-1", 2).then((result) => {
      resolved = true;
      return result;
    });
    await Promise.resolve();
    expect(resolved).toBe(false);
    expect((await profiles.list()).profiles[0]!.revision).toBe(1);
    gate.resolve();
    await update;
    expect((await resolve).identity).toEqual({
      id: "profile-1",
      revision: 2,
      name: "Committed name",
    });
  });

  it("rejects an already cancelled queued mutation with its custom reason and heals the FIFO", async () => {
    const memory = new InMemoryNatsProfileStore();
    const profiles = service(memory);
    await profiles.create(draft);
    const cancellation = new AbortController();
    const reason = new Error("custom cancellation");
    cancellation.abort(reason);
    await expect(profiles.update("profile-1", 1, retained, cancellation.signal)).rejects.toBe(
      reason,
    );
    await expect(
      profiles.update("profile-1", 1, { ...retained, name: "Still writable" }),
    ).resolves.toMatchObject({ profiles: [{ revision: 2 }] });
  });

  it("does not publish uncommitted memory on protection/save failure and blocks later destructive writes", async () => {
    const memory = new InMemoryNatsProfileStore();
    let fail = false;
    let saves = 0;
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        saves++;
        if (fail) throw new Error("private-token-value");
        await memory.save(records, signal);
      },
    };
    const profiles = service(store);
    await profiles.create(draft);
    fail = true;
    const update = profiles.update("profile-1", 1, { ...retained, name: "Uncommitted" });
    await expect(update).rejects.toMatchObject({
      code: "storage-unavailable",
      summary: "Protected NATS profile storage is unavailable.",
    });
    const snapshot = await profiles.list();
    expect(snapshot.profiles[0]!.name).toBe("Private NATS");
    expect(snapshot.profiles[0]!.revision).toBe(1);
    expect(snapshot.capability.state).toBe("unavailable");
    await expect(profiles.delete("profile-1", 1)).rejects.toMatchObject({
      code: "storage-unavailable",
    });
    expect(saves).toBe(2);
    expect((await memory.load())[0]!.name).toBe("Private NATS");
  });

  it("does not turn a corrupt load into a writable empty catalog", async () => {
    let saves = 0;
    const store: NatsProfileStore = {
      capability: { durability: "durable", protection: "os-protected", state: "ready" },
      load: (): Promise<readonly NatsProfileRecord[]> =>
        Promise.reject(Object.assign(new Error("decryption failed"), { code: "ENOENT" })),
      save: (): Promise<void> => {
        saves++;
        return Promise.resolve();
      },
    };
    const profiles = service(store);
    expect(await profiles.list()).toMatchObject({
      capability: { state: "unavailable", protection: "unavailable" },
      profiles: [],
    });
    await expect(profiles.create(draft)).rejects.toMatchObject({ code: "storage-unavailable" });
    expect(saves).toBe(0);
  });

  it("exposes unavailable storage distinctly and never substitutes ready memory", async () => {
    const capability = {
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
      recovery: "Unlock OS storage.",
    } as const;
    const profiles = service(new UnavailableNatsProfileStore(capability));
    expect(await profiles.list()).toEqual({ revision: 0, capability, profiles: [] });
    await expect(profiles.create(draft)).rejects.toMatchObject({ code: "storage-unavailable" });
  });

  it("preserves a genuine commit receipt when cancellation arrives after replacement", async () => {
    const memory = new InMemoryNatsProfileStore();
    const cancellation = new AbortController();
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        await memory.save(records, signal);
        cancellation.abort(new Error("after commit"));
      },
    };
    const profiles = service(store);
    expect((await profiles.create(draft, cancellation.signal)).profiles[0]!.revision).toBe(1);
    expect((await service(memory).resolve("profile-1", 1)).connection.authentication).toEqual({
      mode: "token",
      token: "private-token-value",
    });
  });

  it("cancels pending protection work before commit with the actual custom reason and permits retry", async () => {
    const memory = new InMemoryNatsProfileStore();
    const started = deferred<void>();
    const finish = deferred<void>();
    let pause = false;
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (signal) => memory.load(signal),
      save: async (records, signal): Promise<void> => {
        if (pause) {
          started.resolve();
          await finish.promise;
        }
        await memory.save(records, signal);
      },
    };
    const profiles = service(store);
    await profiles.create(draft);
    const cancellation = new AbortController();
    const reason = new Error("custom protection cancellation");
    pause = true;
    const update = profiles.update(
      "profile-1",
      1,
      { ...retained, name: "Cancelled edit" },
      cancellation.signal,
    );
    const rejected = expect(update).rejects.toBe(reason);
    await started.promise;
    cancellation.abort(reason);
    finish.resolve();
    await rejected;
    expect((await profiles.list()).profiles[0]!.revision).toBe(1);
    expect((await memory.load())[0]!.name).toBe("Private NATS");
    pause = false;
    await expect(profiles.update("profile-1", 1, retained)).resolves.toMatchObject({
      profiles: [{ revision: 2 }],
      capability: { state: "ready" },
    });
  });

  it("does not publish a valid prefix from a partially invalid loaded catalog", async () => {
    const memory = new InMemoryNatsProfileStore();
    await service(memory).create(draft);
    const valid = (await memory.load())[0]!;
    let saves = 0;
    const store: NatsProfileStore = {
      capability: memory.capability,
      load: (): Promise<readonly NatsProfileRecord[]> =>
        Promise.resolve([valid, { ...valid, id: "profile-2", name: " private nats " }]),
      save: (): Promise<void> => {
        saves++;
        return Promise.resolve();
      },
    };
    const profiles = service(store);
    expect(await profiles.list()).toMatchObject({
      profiles: [],
      capability: { state: "unavailable" },
    });
    await expect(profiles.create({ ...draft, name: "Another" })).rejects.toMatchObject({
      code: "storage-unavailable",
    });
    expect(saves).toBe(0);
  });

  it("does not allow a retained token when the prior profile had none", async () => {
    const memory = new InMemoryNatsProfileStore();
    const profiles = service(memory);
    await profiles.create({ ...draft, authentication: { mode: "none" } });
    await expect(profiles.update("profile-1", 1, retained)).rejects.toMatchObject({
      code: "validation",
    });
    expect((await profiles.resolve("profile-1", 1)).connection.authentication).toEqual({
      mode: "none",
    });
  });

  it("rejects duplicate names and retains independent store instances", async () => {
    const memory = new InMemoryNatsProfileStore();
    const profiles = service(memory);
    await profiles.create(draft);
    await expect(profiles.create({ ...draft, name: " private nats " })).rejects.toMatchObject({
      code: "validation",
    });
    await service(new InMemoryNatsProfileStore()).create(draft);
    expect((await profiles.list()).profiles).toHaveLength(1);
  });
});
