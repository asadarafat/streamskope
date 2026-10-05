import { describe, expect, it, vi } from "vitest";

import {
  InMemoryKafkaOperationalPreferenceStore,
  InMemoryKafkaProfileStore,
  KafkaOperationalPreferenceService,
  type KafkaActiveConnection,
  type KafkaClusterMetadata,
} from "../../src/features/kafka/application";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import type { PluginHostBindings, PluginRuntimePort } from "../../src/plugins/api";
import { ObservationFacade } from "../../src/features/kafka/facade/observation-facade";
import {
  command,
  ControlledMessageStream,
  createFacade,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

function deferred<Value>(): {
  readonly promise: Promise<Value>;
  resolve(value: Value): void;
  reject(error: unknown): void;
} {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((complete, fail) => {
    resolve = complete;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function pluginRuntime(close: (host: PluginHostBindings) => Promise<void>): PluginRuntimePort {
  let host: PluginHostBindings;
  return {
    bindHost: (bindings): void => {
      host = bindings;
    },
    start: (): Promise<void> => Promise.resolve(),
    execute: () => Promise.resolve(null),
    validateProfile: () => Promise.resolve(),
    withProfileConnection: (_source, _brokers, connect) => connect(),
    subscribe: () => (): void => undefined,
    subscribeChanges: () => (): void => undefined,
    list: () => Promise.resolve({ revision: 0, plugins: [] }),
    catalog: () => Promise.resolve({ plugins: [] }),
    prepareChange: () => Promise.resolve(null),
    install: () => Promise.resolve({ revision: 0, plugins: [] }),
    remove: () => Promise.resolve({ revision: 0, plugins: [] }),
    rendererFailed: () => Promise.resolve({ revision: 0, plugins: [] }),
    restart: () => Promise.resolve(),
    prepareExit: () => Promise.resolve(null),
    resolveExit: () => Promise.resolve(true),
    close: () => close(host),
  };
}

function createProfile(id: string): Extract<HostCommand, { command: "profiles.create" }> {
  return {
    command: "profiles.create",
    id,
    payload: {
      profile: { name: id, brokers: ["localhost:9092"], transport: "plaintext" },
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

function terminalEvents(events: readonly HostEvent[]): readonly HostEvent[] {
  return events.filter(
    (event) => event.event === "backend.availability" && event.payload.state === "unavailable",
  );
}

describe("Kafka facade shutdown ownership", () => {
  it("waits for every cleanup after an early failure before final accounting and unavailable", async () => {
    const pluginCleanup = deferred<void>();
    const closePlugins = vi.fn(() => pluginCleanup.promise);
    const stream = new ControlledMessageStream();
    const connection = new RecordingActiveConnection();
    connection.messageStreamOperations.push(() => Promise.resolve(stream));
    const closeConnection = vi
      .spyOn(connection, "close")
      .mockRejectedValue(new Error("private-kafka-cleanup-token"));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(
      port,
      () => undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      pluginRuntime(closePlugins),
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => events.push(event));
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    stream.push(message("1"));
    await vi.waitFor(() => expect(stream.deliveredMessages).toBe(1));

    const shutdown = facade.shutdown();
    const settled = vi.fn();
    const result = shutdown.then(
      () => {
        settled();
        return undefined;
      },
      (error: unknown) => {
        settled();
        return error;
      },
    );
    expect(facade.shutdown()).toBe(shutdown);
    await vi.waitFor(() => {
      expect(closeConnection).toHaveBeenCalledOnce();
      expect(closePlugins).toHaveBeenCalledOnce();
    });
    await settleAsyncIteration();
    expect(settled).not.toHaveBeenCalled();
    expect(terminalEvents(events)).toHaveLength(0);
    expect(events.filter((event) => event.event === "messages.batch")).toHaveLength(0);

    pluginCleanup.reject({ token: "private-plugin-cleanup-token" });
    const failure = await result;
    expect(failure).toBeInstanceOf(AggregateError);
    if (!(failure instanceof AggregateError))
      throw new Error("Expected aggregated cleanup errors.");
    expect(failure.errors.map((error: Error) => error.message)).toEqual([
      "Kafka session cleanup failed.",
      "Plugins cleanup failed.",
    ]);
    expect(failure.errors.every((error: Error) => error.cause === undefined)).toBe(true);
    expect(JSON.stringify({ errors: failure.errors, events })).not.toContain("private-");
    expect(events.filter((event) => event.event === "messages.batch")).toHaveLength(1);
    expect(events.filter((event) => event.event === "consumption.state").at(-1)).toMatchObject({
      payload: { state: "failed" },
    });
    expect(events.at(-1)).toMatchObject({
      event: "backend.availability",
      payload: { state: "unavailable" },
    });
    expect(terminalEvents(events)).toHaveLength(1);
    expect(facade.shutdown()).toBe(shutdown);
  });

  it("closes external profile admission synchronously while owned plugin cleanup can finish", async () => {
    const releaseCleanup = deferred<void>();
    const cleanupStarted = deferred<void>();
    let profileId = "";
    const runtime = pluginRuntime(async (host) => {
      const response = await host.execute({
        command: "profiles.delete",
        id: "owned-cleanup",
        payload: { profileId },
        version: HOST_PROTOCOL_VERSION,
      });
      expect(response).toMatchObject({ ok: true });
      cleanupStarted.resolve();
      await releaseCleanup.promise;
    });
    const facade = createFacade(
      new RecordingConnectionPort(),
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      runtime,
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
      if (event.event === "profiles.changed")
        profileId = event.payload.profiles[0]?.id ?? profileId;
    });
    await facade.execute(createProfile("owned-profile"));
    const beforeShutdown = events.length;

    const shutdown = facade.shutdown();
    const rejected = await facade.execute(createProfile("new-external-profile"));
    expect(rejected).toMatchObject({
      ok: false,
      error: { code: "BACKEND_UNAVAILABLE", stage: "backend", activeStateChanged: false },
    });
    await cleanupStarted.promise;
    const profileEvents = events
      .slice(beforeShutdown)
      .filter((event) => event.event === "profiles.changed");
    expect(profileEvents).toHaveLength(1);
    expect(profileEvents[0]).toMatchObject({ payload: { profiles: [] } });
    expect(terminalEvents(events)).toHaveLength(0);

    releaseCleanup.resolve();
    await shutdown;
    await expect(facade.execute(createProfile("after-shutdown"))).resolves.toMatchObject({
      ok: false,
      error: { code: "BACKEND_UNAVAILABLE" },
    });
    expect(terminalEvents(events)).toHaveLength(1);
  });

  it("contains synchronous cleanup throws and still drains the other owners", async () => {
    const connectionCleanup = deferred<void>();
    const connection = new RecordingActiveConnection();
    const closeConnection = vi
      .spyOn(connection, "close")
      .mockReturnValue(connectionCleanup.promise);
    const closePlugins = vi.fn((): Promise<void> => {
      throw new Error("private-synchronous-plugin-token");
    });
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(
      port,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      pluginRuntime(closePlugins),
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => events.push(event));
    await facade.execute(command("connection.connect", "connect"));
    const shutdown = facade.shutdown();
    const result = shutdown.catch((error: unknown) => error);
    await vi.waitFor(() => expect(closeConnection).toHaveBeenCalledOnce());
    expect(closePlugins).toHaveBeenCalledOnce();
    expect(terminalEvents(events)).toHaveLength(0);
    connectionCleanup.resolve();
    const failure = await result;
    expect(failure).toBeInstanceOf(AggregateError);
    expect(terminalEvents(events)).toHaveLength(1);
  });

  it("rejects commands waiting for authorization when shutdown closes admission", async () => {
    const preferences = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore({ durability: "session", state: "ready" }),
    );
    const loading = deferred<Awaited<ReturnType<typeof preferences.get>>>();
    vi.spyOn(preferences, "get").mockReturnValue(loading.promise);
    const port = new RecordingConnectionPort();
    const open = vi.spyOn(port, "openConnection");
    const facade = createFacade(port, undefined, undefined, undefined, preferences);
    const connecting = facade.execute(command("connection.connect", "waiting-authorization"));

    const shutdown = facade.shutdown();
    loading.resolve(preferences.currentSnapshot());
    await expect(connecting).resolves.toMatchObject({
      ok: false,
      error: { code: "BACKEND_UNAVAILABLE" },
    });
    await shutdown;
    expect(open).not.toHaveBeenCalled();
  });

  it("drains a profile save already dispatched to its local store before unavailable", async () => {
    const committing = deferred<void>();
    const commitStarted = deferred<void>();
    // The original method is called with the mocked store as its receiver below.
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const commit = InMemoryKafkaProfileStore.prototype.commit;
    vi.spyOn(InMemoryKafkaProfileStore.prototype, "commit").mockImplementation(function (
      this: InMemoryKafkaProfileStore,
      records,
      signal,
    ): Promise<void> {
      commitStarted.resolve();
      return committing.promise.then(() => commit.call(this, records, signal));
    });
    const facade = createFacade(new RecordingConnectionPort());
    const events: HostEvent[] = [];
    facade.subscribe((event) => events.push(event));
    const creating = facade.execute(createProfile("already-saving"));
    await commitStarted.promise;

    const shutdown = facade.shutdown();
    const settled = vi.fn();
    const shuttingDown = shutdown.then(settled);
    await settleAsyncIteration();
    expect(settled).not.toHaveBeenCalled();
    expect(terminalEvents(events)).toHaveLength(0);
    committing.resolve();
    await expect(creating).resolves.toMatchObject({ ok: true });
    await shuttingDown;
    expect(events.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject({
      payload: { profiles: [expect.objectContaining({ name: "already-saving" })] },
    });
    expect(events.at(-1)).toMatchObject({
      event: "backend.availability",
      payload: { state: "unavailable" },
    });
  });

  it("returns the shared barrier to a subscriber reentering during synchronous cancellation", async () => {
    const facade = createFacade(new RecordingConnectionPort());
    let stopping = false;
    let reentered: Promise<void> | undefined;
    facade.subscribe(() => {
      if (stopping) {
        stopping = false;
        reentered = facade.shutdown();
      }
    });
    stopping = true;
    const shutdown = facade.shutdown();
    expect(reentered).toBe(shutdown);
    await shutdown;
  });

  it.each([false, true])(
    "cancels and drains an environment capture when another cancellation fails (%s)",
    async (failCancellation) => {
      const metadata = deferred<KafkaClusterMetadata>();
      const captureStarted = deferred<AbortSignal>();
      const connection: KafkaActiveConnection = new RecordingActiveConnection();
      connection.describeClusterMetadata = (signal): Promise<KafkaClusterMetadata> => {
        if (signal === undefined) throw new Error("Expected a lifecycle signal.");
        captureStarted.resolve(signal);
        return metadata.promise;
      };
      connection.describeTopicIdentity = (): Promise<{
        clusterId: string;
        topicId: string;
        partitions: number;
      }> => Promise.resolve({ clusterId: "cluster", topicId: "topic-id", partitions: 1 });
      const port = new RecordingConnectionPort();
      port.openOperations.push(() => Promise.resolve(connection));
      const facade = createFacade(port);
      const events: HostEvent[] = [];
      facade.subscribe((event) => events.push(event));
      await facade.execute(command("connection.connect", "connect"));
      const capturing = facade.execute({
        command: "environments.capture",
        id: "capture",
        payload: { topics: ["topic"], profile: null },
        version: HOST_PROTOCOL_VERSION,
      });
      const signal = await captureStarted.promise;
      if (failCancellation)
        vi.spyOn(ObservationFacade.prototype, "cancel").mockImplementation(() => {
          throw new Error("private-observation-cancellation-token");
        });

      const shutdown = facade.shutdown();
      const settled = vi.fn();
      const shuttingDown = shutdown.then(
        () => {
          settled();
          return undefined;
        },
        (error: unknown) => {
          settled();
          return error;
        },
      );
      expect(signal.aborted).toBe(true);
      expect(settled).not.toHaveBeenCalled();
      expect(terminalEvents(events)).toHaveLength(0);
      metadata.resolve({ clusterId: "cluster", brokers: [], controllerId: null });
      await expect(capturing).resolves.toMatchObject({ ok: false });
      const failure = await shuttingDown;
      if (failCancellation) expect(failure).toBeInstanceOf(AggregateError);
      else expect(failure).toBeUndefined();
      expect(terminalEvents(events)).toHaveLength(1);
    },
  );
});
