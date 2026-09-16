import { describe, expect, it } from "vitest";

import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type KafkaOperationalPreferenceStoreCapability,
  type KafkaOperationalPreferences,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceCorruptError,
  KafkaOperationalPreferenceService,
  KafkaOperationalPreferenceStoreUnavailableError,
  type KafkaOperationalPreferenceStore,
} from "../../src/features/kafka/application";

function changedPreferences(
  overrides: Partial<KafkaOperationalPreferences> = {},
): KafkaOperationalPreferences {
  return {
    fetch: { maxMessages: 100, mode: "newest" },
    latency: {
      acknowledgements: -1,
      messageCount: 50,
      runbookUrl: "https://runbooks.example.test/kafka/latency",
      timeoutMs: 20_000,
    },
    rules: {
      logLevel: "warn",
      loggingEnabled: false,
      notificationsEnabled: false,
    },
    stream: {
      batchSize: 100,
      historySamples: 120,
      intervalMs: 50,
      queueDepth: 500,
    },
    ...overrides,
  };
}

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly reject: (reason?: unknown) => void;
  readonly resolve: (value: T | PromiseLike<T>) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

class ControlledPreferenceStore implements KafkaOperationalPreferenceStore {
  readonly commits: KafkaOperationalPreferences[] = [];
  commitGate: Promise<void> | undefined;
  commitFailure: Error | undefined;
  loadFailure: Error | undefined;
  private stored: KafkaOperationalPreferences | undefined;
  private storeCapability: KafkaOperationalPreferenceStoreCapability;

  constructor(
    capability: KafkaOperationalPreferenceStoreCapability = {
      durability: "session",
      state: "ready",
    },
    stored?: KafkaOperationalPreferences,
  ) {
    this.storeCapability = capability;
    this.stored = stored;
  }

  capability(): KafkaOperationalPreferenceStoreCapability {
    return { ...this.storeCapability };
  }

  async commit(preferences: KafkaOperationalPreferences, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const gate = this.commitGate;
    this.commitGate = undefined;
    if (gate !== undefined) {
      await gate;
    }
    signal?.throwIfAborted();
    if (this.commitFailure !== undefined) {
      const failure = this.commitFailure;
      this.commitFailure = undefined;
      throw failure;
    }
    this.commits.push(structuredClone(preferences));
    this.stored = structuredClone(preferences);
    this.storeCapability = {
      durability: this.storeCapability.durability,
      state: "ready",
    };
  }

  load(signal?: AbortSignal): Promise<KafkaOperationalPreferences | undefined> {
    return Promise.resolve().then(() => {
      signal?.throwIfAborted();
      if (this.loadFailure !== undefined) {
        throw this.loadFailure;
      }
      return this.stored === undefined ? undefined : structuredClone(this.stored);
    });
  }
}

describe("Kafka operational-preference application", () => {
  it("returns factory defaults without eagerly creating missing session storage", async () => {
    const store = new InMemoryKafkaOperationalPreferenceStore({
      durability: "session",
      state: "ready",
    });
    const service = new KafkaOperationalPreferenceService(store);

    await expect(service.get()).resolves.toEqual({
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      store: { durability: "session", state: "ready" },
    });
    expect(store.commitCount).toBe(0);
  });

  it("loads confirmed values and returns snapshots isolated from caller mutation", async () => {
    const initial = changedPreferences();
    const store = new InMemoryKafkaOperationalPreferenceStore(
      { durability: "durable", state: "ready" },
      initial,
    );
    const service = new KafkaOperationalPreferenceService(store);

    const snapshot = await service.get();
    expect(snapshot.preferences).toEqual(initial);
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.preferences.stream)).toBe(true);
    expect(() => {
      (snapshot.preferences.stream as { queueDepth: number }).queueDepth = 100;
    }).toThrow(TypeError);
    expect(service.currentSnapshot().preferences.stream.queueDepth).toBe(500);
  });

  it("validates and atomically merges a partial group into one complete commit", async () => {
    const initial = changedPreferences();
    const store = new ControlledPreferenceStore({ durability: "durable", state: "ready" }, initial);
    const service = new KafkaOperationalPreferenceService(store);

    const snapshot = await service.update({
      fetch: { maxMessages: 500 },
      rules: { loggingEnabled: true },
    });

    expect(snapshot.preferences).toEqual({
      ...initial,
      fetch: { ...initial.fetch, maxMessages: 500 },
      rules: { ...initial.rules, loggingEnabled: true },
    });
    expect(store.commits).toEqual([snapshot.preferences]);
  });

  it("rejects an invalid whole update before storage and retains prior authority", async () => {
    const initial = changedPreferences();
    const store = new ControlledPreferenceStore({ durability: "session", state: "ready" }, initial);
    const service = new KafkaOperationalPreferenceService(store);

    await expect(
      service.update({
        stream: { queueDepth: 99 },
      }),
    ).rejects.toMatchObject({
      code: "VALIDATION",
      stage: "validation",
    });
    expect(store.commits).toHaveLength(0);
    expect(service.currentSnapshot().preferences).toEqual(initial);
  });

  it("serializes overlapping patches so each operation observes the prior commit", async () => {
    const firstCommit = deferred<void>();
    const store = new ControlledPreferenceStore();
    store.commitGate = firstCommit.promise;
    const service = new KafkaOperationalPreferenceService(store);

    const first = service.update({ fetch: { maxMessages: 10 } });
    await Promise.resolve();
    const second = service.update({ stream: { batchSize: 10 } });
    await Promise.resolve();
    expect(store.commits).toHaveLength(0);

    firstCommit.resolve();
    await expect(first).resolves.toMatchObject({
      preferences: { fetch: { maxMessages: 10 } },
    });
    await expect(second).resolves.toMatchObject({
      preferences: {
        fetch: { maxMessages: 10 },
        stream: { batchSize: 10 },
      },
    });
    expect(store.commits).toHaveLength(2);
  });

  it("does not start a queued mutation after its request is cancelled", async () => {
    const firstCommit = deferred<void>();
    const store = new ControlledPreferenceStore();
    store.commitGate = firstCommit.promise;
    const service = new KafkaOperationalPreferenceService(store);
    const first = service.update({ fetch: { maxMessages: 10 } });
    await Promise.resolve();

    const controller = new AbortController();
    const cancelled = service.update({ fetch: { maxMessages: 20 } }, controller.signal);
    controller.abort();
    firstCommit.resolve();

    await first;
    await expect(cancelled).rejects.toMatchObject({ name: "AbortError" });
    expect(store.commits).toHaveLength(1);
    expect(service.currentSnapshot().preferences.fetch.maxMessages).toBe(10);
  });

  it("retains the prior snapshot when an atomic commit fails", async () => {
    const initial = changedPreferences();
    const store = new ControlledPreferenceStore({ durability: "durable", state: "ready" }, initial);
    store.commitFailure = new Error("disk full: /private/secret/path");
    const service = new KafkaOperationalPreferenceService(store);
    await service.get();

    await expect(service.update({ stream: { queueDepth: 600 } })).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceStoreUnavailableError,
    );
    expect(service.currentSnapshot().preferences).toEqual(initial);
  });

  it("isolates corrupt storage behind factory fallback until explicit reset succeeds", async () => {
    const store = new ControlledPreferenceStore({
      durability: "durable",
      recovery: "Reset operational preferences to replace the unreadable file.",
      state: "unavailable",
    });
    store.loadFailure = new KafkaOperationalPreferenceCorruptError();
    const service = new KafkaOperationalPreferenceService(store);

    await expect(service.get()).resolves.toEqual({
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      store: {
        durability: "durable",
        recovery: "Reset operational preferences to replace the unreadable file.",
        state: "unavailable",
      },
    });
    await expect(service.update({ fetch: { maxMessages: 10 } })).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceStoreUnavailableError,
    );

    store.loadFailure = undefined;
    await expect(service.reset()).resolves.toEqual({
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      store: { durability: "durable", state: "ready" },
    });
    expect(store.commits).toEqual([KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS]);
  });

  it("defensively isolates a structurally invalid loaded preference document", async () => {
    const store = new ControlledPreferenceStore(
      { durability: "durable", state: "ready" },
      changedPreferences(),
    );
    const invalid = changedPreferences({
      stream: {
        ...changedPreferences().stream,
        queueDepth: 10_000,
      },
    });
    Object.assign(store, {
      load: (): Promise<KafkaOperationalPreferences> => Promise.resolve(invalid),
    });
    const service = new KafkaOperationalPreferenceService(store);

    await expect(service.get()).resolves.toMatchObject({
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      store: { durability: "durable", state: "unavailable" },
    });
  });
});
