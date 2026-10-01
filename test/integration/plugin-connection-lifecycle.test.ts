import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, it, vi, type MockInstance } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type ProfileSummary,
} from "../../src/features/kafka/contracts";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import type { PluginBackend, PluginBackendModule } from "../../src/plugins/api";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application";
import {
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

it.each([false, true])(
  "removal preserves profiles and disconnects only its owned connection (owned=%s)",
  async (owned) => {
    const directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-connection-"));
    const store = new PluginStore(directory);
    const bytes = encodePluginPackage(
      {
        id: "example.capture",
        name: "Capture",
        version: "1.0.0",
        apiVersion: 2,
        backend: "backend.cjs",
        renderer: "renderer.js",
      },
      new Map([
        ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    await store.install(bytes, pluginPackageSha256(bytes));
    let stops = 0;
    const runtime = new PluginRuntime({
      store,
      loadModule: (): Promise<PluginBackendModule> =>
        Promise.resolve({
          activate: (host): PluginBackend => ({
            execute: () => Promise.resolve(null),
            validateProfile: () => Promise.resolve(undefined),
            beforeExit: () => Promise.resolve(undefined),
            resolveExit: () => Promise.resolve(true),
            beforeChange: () =>
              Promise.resolve({
                message: "Capture is running",
                detail: "Remove temporary capture",
              }),
            prepareUnload: async (): Promise<void> => {
              await host.disconnectOwnedConnection();
              stops += 1;
            },
            close: () => Promise.resolve(undefined),
          }),
        }),
    });
    const port = new RecordingConnectionPort();
    const connection = new RecordingActiveConnection();
    port.openOperations.push(() => Promise.resolve(connection));
    const facade = createFacade(
      port,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      runtime,
    );
    let profiles: readonly ProfileSummary[] = [];
    facade.subscribe((event) => {
      if (event.event === "profiles.changed") profiles = event.payload.profiles;
    });
    const execute = <Command extends HostCommand>(
      command: Command,
    ): ReturnType<typeof facade.execute<Command>> => facade.execute(command);
    try {
      await runtime.start();
      expect(
        await execute({
          command: "profiles.create",
          id: "create",
          version: HOST_PROTOCOL_VERSION,
          payload: {
            profile: {
              name: "Saved connection",
              brokers: ["localhost:9092"],
              transport: "plaintext",
              ...(owned
                ? {
                    source: {
                      kind: "plugin" as const,
                      pluginId: "example.capture",
                      version: 1 as const,
                      data: {},
                    },
                  }
                : {}),
            },
          },
        }),
      ).toMatchObject({ ok: true });
      const before = await execute({
        command: "profiles.list",
        id: "profiles",
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!before.ok) throw new Error(before.error.summary);
      const profileId = profiles[0]!.id;
      expect(
        await execute({
          command: "profiles.connect",
          id: "connect",
          version: HOST_PROTOCOL_VERSION,
          payload: { profileId },
        }),
      ).toMatchObject({ ok: true });
      const denied = await execute({
        command: "plugins.remove",
        id: "unconfirmed",
        version: HOST_PROTOCOL_VERSION,
        payload: { pluginId: "example.capture" },
      });
      expect(denied.ok).toBe(false);
      expect(stops).toBe(0);
      expect(connection.closeCalls).toBe(0);
      const confirmation = await execute({
        command: "plugins.change.prepare",
        id: "prepare",
        version: HOST_PROTOCOL_VERSION,
        payload: { pluginId: "example.capture", operation: "remove" },
      });
      if (!confirmation.ok || !confirmation.result.pluginChange)
        throw new Error("Missing capture confirmation");
      expect(
        await execute({
          command: "plugins.remove",
          id: "remove",
          version: HOST_PROTOCOL_VERSION,
          payload: {
            pluginId: "example.capture",
            confirmationToken: confirmation.result.pluginChange.token,
          },
        }),
      ).toMatchObject({ ok: true, result: { pluginSnapshot: { plugins: [] } } });
      expect(stops).toBe(1);
      expect(connection.closeCalls).toBe(owned ? 1 : 0);
      expect(facade.connectionSnapshot().state).toBe(owned ? "disconnected" : "connected");
      const after = await execute({
        command: "profiles.list",
        id: "preserved",
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!after.ok) throw new Error(after.error.summary);
      expect(profiles).toHaveLength(1);
      expect(profiles[0]).toMatchObject({ id: profileId, active: !owned });
    } finally {
      await facade.shutdown();
      await rm(directory, { recursive: true, force: true });
    }
  },
);

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

it.each([false, true])(
  "drains a queued EDA profile activation without disconnecting a newer ordinary connection (switch=%s)",
  async (switchToOrdinary) => {
    const directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-connect-race-"));
    const store = new PluginStore(directory);
    const bytes = encodePluginPackage(
      {
        id: "example.capture",
        name: "Capture",
        version: "1.0.0",
        apiVersion: 2,
        backend: "backend.cjs",
        renderer: "renderer.js",
      },
      new Map([
        ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    await store.install(bytes, pluginPackageSha256(bytes));
    const unload = vi.fn();
    const runtime = new PluginRuntime({
      store,
      loadModule: (): Promise<PluginBackendModule> =>
        Promise.resolve({
          activate: (host): PluginBackend => ({
            execute: () => Promise.resolve(null),
            validateProfile: () => Promise.resolve(),
            beforeExit: () => Promise.resolve(undefined),
            resolveExit: () => Promise.resolve(true),
            beforeChange: () =>
              Promise.resolve({ message: "Capture active", detail: "Remove temporary capture" }),
            prepareUnload: async (): Promise<void> => {
              await host.disconnectOwnedConnection();
              unload();
            },
            close: () => Promise.resolve(),
          }),
        }),
    });
    const port = new RecordingConnectionPort();
    const captured = new RecordingActiveConnection();
    const ordinary = new RecordingActiveConnection();
    port.openOperations.push(
      () => Promise.resolve(captured),
      () => Promise.resolve(ordinary),
    );
    const facade = createFacade(
      port,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      runtime,
    );
    let profiles: readonly ProfileSummary[] = [];
    const opened = deferred();
    facade.subscribe((event) => {
      if (event.event === "profiles.changed") profiles = event.payload.profiles;
      if (
        event.event === "connection.state" &&
        event.payload.state === "connected" &&
        event.payload.connectionName === "EDA capture"
      )
        opened.resolve();
    });
    const execute = <Command extends HostCommand>(
      command: Command,
    ): ReturnType<typeof facade.execute<Command>> => facade.execute(command);
    const commitEntered = deferred();
    const releaseCommit = deferred();
    let commitSpy: MockInstance<InMemoryKafkaProfileStore["commit"]> | undefined;
    try {
      await runtime.start();
      await execute({
        command: "profiles.create",
        id: "create-eda",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profile: {
            name: "EDA capture",
            brokers: ["localhost:19092"],
            transport: "plaintext",
            source: { kind: "plugin", pluginId: "example.capture", version: 1, data: {} },
          },
        },
      });
      const profileId = profiles[0]!.id;
      // The wrapper below supplies the real store instance with .call().
      // eslint-disable-next-line @typescript-eslint/unbound-method
      const originalCommit = InMemoryKafkaProfileStore.prototype.commit;
      commitSpy = vi
        .spyOn(InMemoryKafkaProfileStore.prototype, "commit")
        .mockImplementationOnce(async function (this: InMemoryKafkaProfileStore, records, signal) {
          commitEntered.resolve();
          await releaseCommit.promise;
          return originalCommit.call(this, records, signal);
        });
      // An unrelated slow profile write holds the actual profile mutation queue.
      const writing = execute({
        command: "profiles.create",
        id: "slow-write",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profile: {
            name: "Another saved profile",
            brokers: ["localhost:29092"],
            transport: "plaintext",
          },
        },
      });
      await commitEntered.promise;
      const connecting = execute({
        command: "profiles.connect",
        id: "connect-eda",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId },
      });
      await opened.promise;
      const prompt = await runtime.prepareChange("example.capture", "remove");
      const removing = runtime.remove("example.capture", prompt!.token);
      await vi.waitFor(async () => {
        await expect(
          runtime.validateProfile(
            { kind: "plugin", pluginId: "example.capture", version: 1, data: {} },
            ["localhost:19092"],
          ),
        ).rejects.toThrow(/being changed/u);
      });
      expect(unload).not.toHaveBeenCalled();
      if (switchToOrdinary) {
        expect(
          await execute({
            command: "connection.connect",
            id: "ordinary",
            version: HOST_PROTOCOL_VERSION,
            payload: {
              name: "Ordinary Kafka",
              brokers: ["localhost:9092"],
              tls: { enabled: false },
            },
          }),
        ).toMatchObject({ ok: true });
      }
      releaseCommit.resolve();
      expect(await writing).toMatchObject({ ok: true });
      expect(await connecting).toMatchObject({ ok: !switchToOrdinary });
      expect((await removing).plugins).toEqual([]);
      expect(unload).toHaveBeenCalledOnce();
      expect(ordinary.closeCalls).toBe(0);
      expect(captured.closeCalls).toBe(1);
      expect(profiles.find((entry) => entry.id === profileId)).toMatchObject({
        active: false,
        source: { pluginId: "example.capture" },
      });
      expect(profiles).toHaveLength(2);
      expect(facade.connectionSnapshot()).toMatchObject(
        switchToOrdinary
          ? { state: "connected", connectionName: "Ordinary Kafka" }
          : { state: "disconnected" },
      );
    } finally {
      releaseCommit.resolve();
      commitSpy?.mockRestore();
      await facade.shutdown();
      await rm(directory, { recursive: true, force: true });
    }
  },
);

async function validationFixture(validateProfile: PluginBackend["validateProfile"]): Promise<{
  facade: ReturnType<typeof createFacade>;
  port: RecordingConnectionPort;
  profileIds: readonly string[];
  close: () => Promise<void>;
}> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-validation-race-"));
  const store = new PluginStore(directory);
  const bytes = encodePluginPackage(
    {
      id: "example.capture",
      name: "Capture",
      version: "1.0.0",
      apiVersion: 2,
      backend: "backend.cjs",
      renderer: "renderer.js",
    },
    new Map([
      ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
  await store.install(bytes, pluginPackageSha256(bytes));
  const runtime = new PluginRuntime({
    store,
    loadModule: (): Promise<PluginBackendModule> =>
      Promise.resolve({
        activate: (host): PluginBackend => ({
          execute: () => Promise.resolve(null),
          validateProfile,
          beforeExit: () => Promise.resolve(undefined),
          resolveExit: () => Promise.resolve(true),
          beforeChange: () => Promise.resolve(undefined),
          prepareUnload: () => host.disconnectOwnedConnection(),
          close: () => Promise.resolve(),
        }),
      }),
  });
  const port = new RecordingConnectionPort();
  const facade = createFacade(port, undefined, undefined, undefined, undefined, undefined, runtime);
  let profiles: readonly ProfileSummary[] = [];
  facade.subscribe((event) => {
    if (event.event === "profiles.changed") profiles = event.payload.profiles;
  });
  await runtime.start();
  for (const index of [0, 1]) {
    await facade.execute({
      command: "profiles.create",
      id: `create-${index}`,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: {
          name: `EDA ${index}`,
          brokers: [`localhost:${19092 + index}`],
          transport: "plaintext",
          source: { kind: "plugin", pluginId: "example.capture", version: 1, data: { index } },
        },
      },
    });
  }
  return {
    facade,
    port,
    profileIds: profiles.map((entry) => entry.id),
    close: async (): Promise<void> => {
      await facade.shutdown();
      await rm(directory, { recursive: true, force: true });
    },
  };
}

it("does not replace a newer ordinary Kafka connection after delayed EDA validation", async () => {
  const entered = deferred();
  const release = deferred();
  const fixture = await validationFixture(async () => {
    entered.resolve();
    await release.promise;
  });
  const ordinary = new RecordingActiveConnection();
  fixture.port.openOperations.push(() => Promise.resolve(ordinary));
  try {
    const earlier = fixture.facade.execute({
      command: "profiles.connect",
      id: "earlier",
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: fixture.profileIds[0]! },
    });
    await entered.promise;
    expect(
      await fixture.facade.execute({
        command: "connection.connect",
        id: "later",
        version: HOST_PROTOCOL_VERSION,
        payload: { name: "Ordinary Kafka", brokers: ["localhost:9092"], tls: { enabled: false } },
      }),
    ).toMatchObject({ ok: true });
    release.resolve();
    expect(await earlier).toMatchObject({ ok: false });
    expect(ordinary.closeCalls).toBe(0);
    expect(fixture.facade.connectionSnapshot()).toMatchObject({
      state: "connected",
      connectionName: "Ordinary Kafka",
    });
  } finally {
    release.resolve();
    await fixture.close();
  }
});

it.each([0, 1])(
  "keeps the latest of two pending profile requests when validation %s completes first",
  async (firstToFinish) => {
    const entered = [deferred(), deferred()];
    const release = [deferred(), deferred()];
    const fixture = await validationFixture(async (data) => {
      const index = Number(data.index);
      entered[index]!.resolve();
      await release[index]!.promise;
    });
    const opened: string[] = [];
    const connection = new RecordingActiveConnection();
    fixture.port.openOperations.push((input) => {
      opened.push(input.name);
      return Promise.resolve(connection);
    });
    try {
      const earlier = fixture.facade.execute({
        command: "profiles.connect",
        id: "earlier",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: fixture.profileIds[0]! },
      });
      await entered[0]!.promise;
      const later = fixture.facade.execute({
        command: "profiles.connect",
        id: "later",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: fixture.profileIds[1]! },
      });
      await entered[1]!.promise;
      if (firstToFinish === 0) {
        release[0]!.resolve();
        expect(await earlier).toMatchObject({ ok: false });
        expect(opened).toEqual([]);
        release[1]!.resolve();
      } else {
        release[1]!.resolve();
        expect(await later).toMatchObject({ ok: true });
        release[0]!.resolve();
      }
      expect(await earlier).toMatchObject({ ok: false });
      expect(await later).toMatchObject({ ok: true });
      expect(opened).toEqual(["EDA 1"]);
      expect(connection.closeCalls).toBe(0);
      expect(fixture.facade.connectionSnapshot()).toMatchObject({
        state: "connected",
        connectionName: "EDA 1",
      });
    } finally {
      for (const gate of release) gate.resolve();
      await fixture.close();
    }
  },
);
