import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import type { PluginBackend, PluginBackendModule, PluginHostBindings } from "../../src/plugins/api";
import type { PluginManifest, PluginSnapshot } from "../../src/plugins/contracts";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginRuntime, type PluginRuntimeOptions } from "../../src/platform/node/plugins/runtime";
import { PluginRuntimeInventory } from "../../src/platform/node/plugins/runtime-inventory";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { testHostExecute } from "../support/host-response";

const manifest: PluginManifest = {
  id: "example.capture",
  name: "Capture",
  version: "1.0.0",
  apiVersion: 2,
  backend: "backend.cjs",
  renderer: "renderer.js",
};
const releases: (() => void)[] = [];
const pending: Promise<unknown>[] = [];
const runtimes: PluginRuntime[] = [];
const directories: string[] = [];

function gate(): { promise: Promise<void>; release: () => void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  releases.push(release);
  return { promise, release };
}

function observe<T>(operation: Promise<T>): Promise<T> {
  pending.push(operation);
  void operation.catch(() => undefined);
  return operation;
}

function archive(value: PluginManifest = manifest): Uint8Array {
  return encodePluginPackage(
    value,
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
    ]),
  );
}

function backend(overrides: Partial<PluginBackend> = {}): PluginBackend {
  return {
    execute: () => Promise.resolve(null),
    validateProfile: () => Promise.resolve(),
    beforeExit: () => Promise.resolve(undefined),
    resolveExit: () => Promise.resolve(true),
    beforeChange: () => Promise.resolve(undefined),
    prepareUnload: () => Promise.resolve(),
    close: () => Promise.resolve(),
    ...overrides,
  };
}

function bindings(): PluginHostBindings {
  return {
    execute: testHostExecute((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "fixture" },
      }),
    ),
    connectionActive: () => false,
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.resolve(),
    disconnectPluginConnection: () => Promise.resolve(),
    recordActivity: () => undefined,
    failure: (_error, context) => ({
      activeStateChanged: false,
      code: "BACKEND_UNAVAILABLE",
      correlationId: context.correlationId,
      recovery: "Retry",
      retryable: false,
      stage: "backend",
      summary: "Unavailable",
    }),
  };
}

async function setup(
  options: Omit<PluginRuntimeOptions, "store"> = {},
  installed: readonly PluginManifest[] = [manifest],
): Promise<{ runtime: PluginRuntime; store: PluginStore }> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-transition-"));
  directories.push(directory);
  const store = new PluginStore(directory);
  for (const value of installed) {
    const bytes = archive(value);
    await store.install(bytes, pluginPackageSha256(bytes));
  }
  const runtime = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    loadModule: (): Promise<PluginBackendModule> => Promise.resolve({ activate: () => backend() }),
    ...options,
  });
  runtime.bindHost(bindings());
  runtimes.push(runtime);
  return { runtime, store };
}

async function request(runtime: PluginRuntime, id = manifest.id): Promise<unknown> {
  const activationId = (await runtime.list()).plugins.find(
    (plugin) => plugin.id === id,
  )!.activationId!;
  return runtime.execute({
    pluginId: id,
    activationId,
    requestId: "request",
    correlationId: "correlation",
    method: "status",
    input: null,
  });
}

afterEach(async () => {
  for (const release of releases.splice(0)) release();
  await Promise.allSettled(pending.splice(0));
  await Promise.allSettled(runtimes.splice(0).map((runtime) => runtime.close()));
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
  vi.restoreAllMocks();
});

describe("readable plugin transitions retain real operation ownership", () => {
  it("delivers captured inventory revisions in order through reentrant observers", async () => {
    const { store } = await setup();
    const inventory = new PluginRuntimeInventory(store);
    const received: PluginSnapshot[] = [];
    const view = { active: [], transitions: [], errors: new Map<string, string>() };
    inventory.subscribe((snapshot) => {
      if (snapshot.revision === 1) inventory.publish({ ...view, error: "second" });
    });
    inventory.subscribe((snapshot) => {
      received.push(snapshot);
    });
    inventory.publish({ ...view, error: "first" });
    expect(received.map((snapshot) => [snapshot.revision, snapshot.error])).toEqual([
      [1, "first"],
      [2, "second"],
    ]);
    expect(Object.isFrozen(received[0])).toBe(true);
    expect(Object.isFrozen(received[0]!.plugins)).toBe(true);
    expect(inventory.snapshot()).toBe(received[1]);
  });

  it("reports stalled startup without pretending that the full start barrier completed", async () => {
    const entered = gate();
    const activation = gate();
    const { runtime } = await setup({
      loadModule: () =>
        Promise.resolve({
          activate: async () => {
            entered.release();
            await activation.promise;
            return backend();
          },
        }),
    });
    let started = false;
    const starting = observe(
      runtime.start().then(() => {
        started = true;
      }),
    );
    await entered.promise;
    const snapshot = await runtime.list();
    expect(snapshot.plugins[0]).toMatchObject({
      installed: manifest,
      transition: { operation: "startup", stage: "load-candidate", state: "running" },
    });
    expect(snapshot.plugins[0]?.active).toBeUndefined();
    expect(started).toBe(false);
    activation.release();
    await starting;
    expect((await runtime.list()).plugins[0]).toMatchObject({ active: manifest });
    expect((await runtime.list()).plugins[0]?.transition).toBeUndefined();
  });

  it("publishes a waiting review while genuine recovery dispatch stays available", async () => {
    const entered = gate();
    const review = gate();
    const execute = vi.fn(() => Promise.resolve(null));
    const { runtime, store } = await setup({
      transitionWaitingAfterMs: 20,
      loadModule: () =>
        Promise.resolve({
          activate: () =>
            backend({
              execute,
              beforeChange: async () => {
                entered.release();
                await review.promise;
                return undefined;
              },
            }),
        }),
    });
    await runtime.start();
    const diskReads = vi.spyOn(store, "list");
    const snapshots: PluginSnapshot[] = [];
    runtime.subscribeChanges((snapshot) => {
      snapshots.push(snapshot);
    });
    const reviewing = observe(runtime.prepareChange(manifest.id, "remove"));
    await entered.promise;
    await vi.waitFor(async () => {
      expect((await runtime.list()).plugins[0]?.transition).toMatchObject({
        operation: "review-remove",
        stage: "review-change",
        state: "waiting",
        commit: "not-started",
      });
    });
    await expect(request(runtime)).resolves.toBeNull();
    expect(execute).toHaveBeenCalledOnce();
    await expect(runtime.remove(manifest.id)).rejects.toThrow(
      /busy|being changed|in progress|already.*progress/iu,
    );
    expect(diskReads).not.toHaveBeenCalled();
    expect(snapshots.some((snapshot) => snapshot.plugins[0]?.transition?.state === "waiting")).toBe(
      true,
    );
    review.release();
    await expect(reviewing).resolves.toBeNull();
    expect((await runtime.list()).plugins[0]?.transition).toBeUndefined();
    expect(
      snapshots.every(
        (snapshot, index) => index === 0 || snapshot.revision > snapshots[index - 1]!.revision,
      ),
    ).toBe(true);
  });

  it("refuses conflicting changes without superseding a removal waiting for its real request", async () => {
    const entered = gate();
    const reply = gate();
    const close = vi.fn(() => Promise.resolve());
    const { runtime, store } = await setup({
      loadModule: () =>
        Promise.resolve({
          activate: () =>
            backend({
              close,
              execute: async () => {
                entered.release();
                await reply.promise;
                return "acknowledged";
              },
            }),
        }),
    });
    await runtime.start();
    const running = observe(request(runtime));
    await entered.promise;
    const uninstall = vi.spyOn(store, "uninstall");
    const removing = observe(runtime.remove(manifest.id));
    await vi.waitFor(async () =>
      expect((await runtime.list()).plugins[0]?.transition).toMatchObject({
        operation: "remove",
        stage: "drain-requests",
        outstandingRequests: 1,
      }),
    );
    await expect(runtime.retryActivation(manifest.id)).rejects.toThrow(
      /busy|being changed|in progress|already.*progress/iu,
    );
    await expect(request(runtime)).rejects.toThrow(/being changed/u);
    expect(uninstall).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
    reply.release();
    await expect(running).resolves.toBe("acknowledged");
    expect((await removing).plugins).toEqual([]);
    expect(uninstall).toHaveBeenCalledOnce();
    expect(close).toHaveBeenCalledOnce();
  });

  it("marks storage as in progress until its actual operation returns", async () => {
    const entered = gate();
    const committed = gate();
    const { runtime, store } = await setup();
    await runtime.start();
    const original = store.uninstall.bind(store);
    vi.spyOn(store, "uninstall").mockImplementation(async (id) => {
      const result = await original(id);
      entered.release();
      await committed.promise;
      return result;
    });
    const removing = observe(runtime.remove(manifest.id));
    await entered.promise;
    expect((await runtime.list()).plugins[0]?.transition).toMatchObject({
      operation: "remove",
      stage: "commit-storage",
      commit: "in-progress",
    });
    expect(await store.getInstalled(manifest.id)).toBeUndefined();
    committed.release();
    expect((await removing).plugins).toEqual([]);
  });

  it("keeps a transition-only row after removal commits while owned close remains pending", async () => {
    const entered = gate();
    const closed = gate();
    const close = vi.fn(async () => {
      entered.release();
      await closed.promise;
    });
    const { runtime, store } = await setup({
      loadModule: () => Promise.resolve({ activate: () => backend({ close }) }),
    });
    await runtime.start();
    const removing = observe(runtime.remove(manifest.id));
    await entered.promise;
    expect(await store.getInstalled(manifest.id)).toBeUndefined();
    const plugin = (await runtime.list()).plugins[0]!;
    expect(plugin).toMatchObject({
      id: manifest.id,
      transition: { operation: "remove", commit: "confirmed" },
    });
    expect(plugin.installed).toBeUndefined();
    expect(plugin.active).toBeUndefined();
    await expect(runtime.remove(manifest.id)).rejects.toThrow(
      /busy|being changed|in progress|already.*progress/iu,
    );
    closed.release();
    expect((await removing).plugins).toEqual([]);
    expect(close).toHaveBeenCalledOnce();
  });

  it("shows another plugin queued without denying its dispatch before execution starts", async () => {
    const other = { ...manifest, id: "other.capture", name: "Other" };
    const entered = gate();
    const finish = gate();
    let activations = 0;
    const { runtime } = await setup(
      {
        loadModule: () =>
          Promise.resolve({
            activate: () => {
              activations += 1;
              return activations === 1
                ? backend({
                    prepareUnload: async () => {
                      entered.release();
                      await finish.promise;
                    },
                  })
                : backend();
            },
          }),
      },
      [manifest, other],
    );
    await runtime.start();
    const removing = observe(runtime.remove(manifest.id));
    await entered.promise;
    const queued = observe(runtime.remove(other.id));
    await vi.waitFor(async () =>
      expect(
        (await runtime.list()).plugins.find((plugin) => plugin.id === other.id)?.transition,
      ).toMatchObject({ operation: "remove", stage: "queued", state: "queued" }),
    );
    await expect(request(runtime, other.id)).resolves.toBeNull();
    finish.release();
    await removing;
    expect((await queued).plugins).toEqual([]);
  });

  it("shows a fresh installation before it has any committed package or renderer", async () => {
    const entered = gate();
    const activation = gate();
    const bytes = archive();
    const { runtime } = await setup(
      {
        catalog: {
          list: () => Promise.resolve([]),
          download: () => Promise.resolve({ bytes, sha256: pluginPackageSha256(bytes) }),
        },
        loadModule: () =>
          Promise.resolve({
            activate: async () => {
              entered.release();
              await activation.promise;
              return backend();
            },
          }),
      },
      [],
    );
    await runtime.start();
    const installing = observe(runtime.install(manifest.id));
    await entered.promise;
    const plugin = (await runtime.list()).plugins[0]!;
    expect(plugin).toMatchObject({
      id: manifest.id,
      transition: { operation: "install", stage: "load-candidate", commit: "not-started" },
    });
    expect(plugin.active).toBeUndefined();
    expect(plugin.installed).toBeUndefined();
    activation.release();
    expect((await installing).plugins[0]).toMatchObject({ installed: manifest, active: manifest });
    expect((await runtime.list()).plugins[0]?.transition).toBeUndefined();
  });

  it("starts sibling shutdown cleanup even when one owned backend never responds yet", async () => {
    const other = { ...manifest, id: "other.capture", name: "Other" };
    const entered = gate();
    const finish = gate();
    const sibling = vi.fn(() => Promise.resolve());
    let activations = 0;
    const { runtime } = await setup(
      {
        loadModule: () =>
          Promise.resolve({
            activate: () => {
              activations += 1;
              return backend({
                close:
                  activations === 1
                    ? async (): Promise<void> => {
                        entered.release();
                        await finish.promise;
                      }
                    : sibling,
              });
            },
          }),
      },
      [manifest, other],
    );
    await runtime.start();
    let settled = false;
    const closing = observe(
      runtime.close().then(() => {
        settled = true;
      }),
    );
    await entered.promise;
    await vi.waitFor(() => expect(sibling).toHaveBeenCalledOnce());
    expect(settled).toBe(false);
    expect(
      (await runtime.list()).plugins.find((plugin) => plugin.id === manifest.id)?.transition,
    ).toMatchObject({ operation: "shutdown", stage: "close-backend" });
    await expect(runtime.remove(other.id)).rejects.toThrow(/closing/u);
    finish.release();
    await closing;
  });
});
