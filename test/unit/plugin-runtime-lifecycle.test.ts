import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type ProfileSummary,
} from "../../src/features/kafka/contracts";
import type {
  PluginBackend,
  PluginBackendHost,
  PluginBackendModule,
  PluginHostBindings,
} from "../../src/plugins/api";
import type { PluginManifest, PluginRequest } from "../../src/plugins/contracts";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginRuntime, type PluginRuntimeOptions } from "../../src/platform/node/plugins/runtime";
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
const bytes = encodePluginPackage(
  manifest,
  new Map([
    ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
    ["renderer.js", Buffer.from("export default {};")],
  ]),
);
const sha256 = pluginPackageSha256(bytes);
const directories: string[] = [];
const runtimes: PluginRuntime[] = [];

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function bindings(overrides: Partial<PluginHostBindings> = {}): PluginHostBindings {
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
    ...overrides,
  };
}
function backend(): PluginBackend {
  return {
    execute: () => Promise.resolve(null),
    validateProfile: () => Promise.resolve(),
    beforeExit: () => Promise.resolve(undefined),
    resolveExit: () => Promise.resolve(true),
    beforeChange: () => Promise.resolve(undefined),
    prepareUnload: () => Promise.resolve(),
    close: () => Promise.resolve(),
  };
}
async function setup(
  options: Omit<PluginRuntimeOptions, "store"> = {},
  host = bindings(),
): Promise<{ runtime: PluginRuntime; store: PluginStore }> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-lifecycle-"));
  directories.push(directory);
  const store = new PluginStore(directory);
  await store.install(bytes, sha256);
  const runtime = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    loadModule: (): Promise<PluginBackendModule> => Promise.resolve({ activate: backend }),
    ...options,
  });
  runtime.bindHost(host);
  runtimes.push(runtime);
  return { runtime, store };
}
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((runtime) => runtime.close()));
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("plugin shutdown and profile boundaries", () => {
  it("does not ask to stop healthy active work for an idempotent renderer retry", async () => {
    const stop = vi.fn(() => Promise.resolve());
    const { runtime } = await setup({
      loadModule: () =>
        Promise.resolve({
          activate: (): PluginBackend => ({
            ...backend(),
            beforeChange: () => Promise.resolve({ message: "Capture active", detail: "Session A" }),
            prepareUnload: stop,
          }),
        }),
    });
    const before = (await runtime.list()).plugins[0]!;
    expect(await runtime.prepareChange(manifest.id, "retry")).toBeNull();
    expect((await runtime.retryActivation(manifest.id)).plugins[0]?.activationId).toBe(
      before.activationId,
    );
    expect(stop).not.toHaveBeenCalled();
  });
  it("retries retained installed bytes without GitHub after a failed first activation and remains idempotent", async () => {
    const activate = vi
      .fn()
      .mockImplementationOnce(() => {
        throw new Error("Temporary activation failure");
      })
      .mockImplementation(backend);
    const list = vi.fn(() => Promise.reject(new Error("No internet")));
    const download = vi.fn(() => Promise.reject(new Error("No internet")));
    const { runtime, store } = await setup({
      loadModule: () => Promise.resolve({ activate }),
      catalog: { list, download },
    });
    expect((await runtime.list()).plugins[0]).toMatchObject({
      installed: manifest,
      error: "Temporary activation failure",
    });
    expect((await runtime.list()).plugins[0]?.active).toBeUndefined();
    expect(await store.getActive(manifest.id)).toBeUndefined();
    const recovered = (await runtime.retryActivation(manifest.id)).plugins[0]!;
    expect(recovered).toMatchObject({
      installed: manifest,
      active: manifest,
      pending: null,
      restartRequired: false,
    });
    expect(recovered.error).toBeUndefined();
    expect((await runtime.retryActivation(manifest.id)).plugins[0]?.activationId).toBe(
      recovered.activationId,
    );
    expect(activate).toHaveBeenCalledTimes(2);
    expect(list).not.toHaveBeenCalled();
    expect(download).not.toHaveBeenCalled();
  });

  it("rejects tampered retained bytes before retry activation and still allows offline removal", async () => {
    const activate = vi.fn(() => {
      throw new Error("Activation failure");
    });
    const download = vi.fn(() => Promise.reject(new Error("No internet")));
    const { runtime, store } = await setup({
      loadModule: () => Promise.resolve({ activate }),
      catalog: { list: () => Promise.reject(new Error("No internet")), download },
    });
    await runtime.start();
    const retained = await store.getInstalled(manifest.id);
    await writeFile(retained!.backendPath, "tampered backend code");
    await expect(runtime.retryActivation(manifest.id)).rejects.toThrow(/verified package/u);
    expect(activate).toHaveBeenCalledTimes(1);
    expect((await runtime.list()).plugins[0]?.active).toBeUndefined();
    expect((await runtime.remove(manifest.id)).plugins).toEqual([]);
    expect(download).not.toHaveBeenCalled();
  });

  it("keeps a retryable installed package across host reopening after repeated activation failures", async () => {
    const activate = vi.fn(() => {
      throw new Error("Still unavailable");
    });
    const { runtime, store } = await setup({ loadModule: () => Promise.resolve({ activate }) });
    await runtime.start();
    await expect(runtime.retryActivation(manifest.id)).rejects.toThrow("Still unavailable");
    expect((await runtime.list()).plugins[0]).toMatchObject({
      installed: manifest,
      error: "Still unavailable",
    });
    await runtime.close();
    const reopened = new PluginRuntime({
      store,
      hostRelease: "v0.2.0",
      loadModule: (): Promise<PluginBackendModule> => Promise.resolve({ activate: backend }),
    });
    reopened.bindHost(bindings());
    runtimes.push(reopened);
    expect((await reopened.list()).plugins[0]?.installed).toEqual(manifest);
    expect((await reopened.list()).plugins[0]?.active).toBeUndefined();
    expect((await reopened.retryActivation(manifest.id)).plugins[0]?.active).toEqual(manifest);
  });

  it("asks again about earlier plugin work after another plugin prevents exit", async () => {
    let activations = 0;
    const prompt = {
      title: "Pending work",
      message: "Keep this capture?",
      detail: "Capture is running",
      actions: [
        { id: "keep", label: "Keep" },
        { id: "cancel", label: "Cancel" },
      ],
      cancelAction: "cancel",
    };
    const { runtime, store } = await setup({
      loadModule: () =>
        Promise.resolve({
          activate: () => {
            activations += 1;
            return activations === 1
              ? {
                  ...backend(),
                  beforeExit: (): ReturnType<PluginBackend["beforeExit"]> =>
                    Promise.resolve(prompt),
                }
              : {
                  ...backend(),
                  beforeExit: (): ReturnType<PluginBackend["beforeExit"]> =>
                    Promise.reject(new Error("Could not determine cleanup state")),
                };
          },
        }),
    });
    const second = encodePluginPackage(
      { ...manifest, id: "example.second" },
      new Map([
        ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    await store.install(second, pluginPackageSha256(second));
    expect(await runtime.prepareExit()).toMatchObject({ pluginId: manifest.id });
    expect(await runtime.resolveExit(manifest.id, "keep")).toBe(true);
    await expect(runtime.prepareExit()).rejects.toThrow(/cleanup state/u);
    expect(await runtime.prepareExit()).toMatchObject({ pluginId: manifest.id });
  });

  it("cannot activate installed code after being closed before startup", async () => {
    const activate = vi.fn(backend);
    const { runtime } = await setup({ loadModule: () => Promise.resolve({ activate }) });
    await runtime.close();
    await expect(runtime.start()).rejects.toThrow(/closing/u);
    await expect(runtime.list()).rejects.toThrow(/closing/u);
    await expect(
      runtime.rendererAsset(
        `/plugins/${manifest.id}/${sha256}/00000000-0000-0000-0000-000000000000/renderer.js`,
      ),
    ).rejects.toThrow(/closing/u);
    expect(activate).not.toHaveBeenCalled();
  });

  it("does not dispatch a request that was waiting for startup when shutdown begins", async () => {
    const entered = deferred<void>();
    const activated = deferred<PluginBackend>();
    const execute = vi.fn(() => Promise.resolve(null));
    const close = vi.fn(() => Promise.resolve());
    const { runtime } = await setup({
      loadModule: () =>
        Promise.resolve({
          activate: () => {
            entered.resolve();
            return activated.promise;
          },
        }),
    });
    const request = runtime.execute({
      pluginId: manifest.id,
      activationId: "startup-pending",
      method: "capture",
      input: {},
      requestId: "request",
      correlationId: "correlation",
    });
    const rejected = expect(request).rejects.toThrow(/closing/u);
    await entered.promise;
    const shuttingDown = runtime.close();
    activated.resolve({ ...backend(), execute, close });
    await rejected;
    await shuttingDown;
    expect(execute).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("does not stage a download that completes after shutdown", async () => {
    const entered = deferred<void>();
    const download = deferred<{ bytes: Uint8Array; sha256: string }>();
    const { runtime, store } = await setup({
      catalog: {
        list: () => Promise.resolve([]),
        download: () => {
          entered.resolve();
          return download.promise;
        },
      },
    });
    const pending = runtime.install(manifest.id);
    const rejected = expect(pending).rejects.toThrow("Plugin acquisition was cancelled.");
    await entered.promise;
    await runtime.close();
    download.resolve({ bytes, sha256 });
    await rejected;
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(await store.list()).toMatchObject([{ pending: null, restartRequired: false }]);
  });

  it("does not reinstall a removed plugin when its older update download finishes", async () => {
    const entered = deferred<void>();
    const download = deferred<{ bytes: Uint8Array; sha256: string }>();
    const { runtime, store } = await setup({
      catalog: {
        list: () => Promise.resolve([]),
        download: () => {
          entered.resolve();
          return download.promise;
        },
      },
    });
    const updating = runtime.install(manifest.id);
    await entered.promise;
    expect((await runtime.remove(manifest.id)).plugins).toEqual([]);
    const rejected = expect(updating).rejects.toThrow(/superseded/u);
    download.resolve(updatedPackage());
    await rejected;
    expect((await runtime.list()).plugins).toEqual([]);
    expect(await store.list()).toEqual([]);
  });

  it("does not replace a newer installation with an older same-version download", async () => {
    const entered = [deferred<void>(), deferred<void>()];
    const downloads = [
      deferred<{ bytes: Uint8Array; sha256: string }>(),
      deferred<{ bytes: Uint8Array; sha256: string }>(),
    ];
    let index = 0;
    const { runtime } = await setup({
      catalog: {
        list: () => Promise.resolve([]),
        download: () => {
          const current = index++;
          entered[current]!.resolve();
          return downloads[current]!.promise;
        },
      },
    });
    const first = runtime.install(manifest.id);
    await entered[0]!.promise;
    const second = runtime.install(manifest.id);
    await entered[1]!.promise;
    downloads[1]!.resolve(updatedPackage());
    const current = (await second).plugins[0]!;
    const rejected = expect(first).rejects.toThrow(/superseded/u);
    downloads[0]!.resolve(updatedPackage());
    await rejected;
    expect((await runtime.list()).plugins[0]?.activationId).toBe(current.activationId);
  });

  it("protects another profile from raw SDK update/delete commands as well as helper deletion", async () => {
    const plain: ProfileSummary = {
      id: "ordinary",
      name: "Ordinary Kafka",
      brokers: ["localhost:9092"],
      transport: "plaintext",
      active: false,
      createdAt: "2026-09-30",
      updatedAt: "2026-09-30",
    };
    const owned: ProfileSummary = {
      ...plain,
      id: "owned",
      source: { kind: "plugin", pluginId: manifest.id, version: 1, data: {} },
    };
    const dispatched = vi.fn((command: HostCommand) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "fixture" },
      }),
    );
    const host = deferred<PluginBackendHost>();
    const { runtime } = await setup(
      {
        loadModule: () =>
          Promise.resolve({
            activate: (value) => {
              host.resolve(value);
              return backend();
            },
          }),
      },
      bindings({
        profiles: () => Promise.resolve([plain, owned]),
        execute: testHostExecute(dispatched),
      }),
    );
    await runtime.start();
    const sdk = await host.promise;
    await expect(
      sdk.execute({
        command: "profiles.delete",
        id: "delete",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: plain.id },
      }),
    ).rejects.toThrow(/own profiles/u);
    await expect(sdk.deleteProfile(plain.id)).rejects.toThrow(/own profiles/u);
    await expect(
      sdk.execute({
        command: "profiles.update",
        id: "update",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profileId: plain.id,
          profile: { brokers: plain.brokers, name: "Overwritten", transport: "plaintext" },
        },
      }),
    ).rejects.toThrow(/own profiles/u);
    await expect(
      sdk.execute({
        command: "profiles.update",
        id: "transfer",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profileId: owned.id,
          profile: {
            brokers: owned.brokers,
            name: "Transferred",
            transport: "plaintext",
            source: { kind: "plugin", pluginId: "different.plugin", version: 1, data: {} },
          },
        },
      }),
    ).rejects.toThrow(/another plugin/u);
    expect(dispatched).not.toHaveBeenCalled();
    await expect(
      sdk.execute({
        command: "profiles.delete",
        id: "own-delete",
        version: HOST_PROTOCOL_VERSION,
        payload: { profileId: owned.id },
      }),
    ).resolves.toMatchObject({ ok: true });
    expect(dispatched).toHaveBeenCalledTimes(1);
  });
});

function updatedPackage(): { bytes: Uint8Array; sha256: string } {
  const update = encodePluginPackage(
    { ...manifest, version: "1.1.0" },
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {updated:true};")],
    ]),
  );
  return { bytes: update, sha256: pluginPackageSha256(update) };
}
const request = (activationId: string): PluginRequest => ({
  pluginId: manifest.id,
  activationId,
  method: "inspect",
  input: {},
  requestId: "request",
  correlationId: "correlation",
});
const source = { kind: "plugin", pluginId: manifest.id, version: 1, data: {} } as const;

async function hotSetup(
  first: PluginBackend,
  next: PluginBackend,
): Promise<{
  runtime: PluginRuntime;
  store: PluginStore;
  sdk: PluginBackendHost[];
}> {
  const sdk: PluginBackendHost[] = [];
  const update = updatedPackage();
  const result = await setup({
    catalog: { list: () => Promise.resolve([]), download: () => Promise.resolve(update) },
    loadModule: () =>
      Promise.resolve({
        activate: (host) => {
          sdk.push(host);
          return sdk.length === 1 ? first : next;
        },
      }),
  });
  await result.runtime.start();
  return { ...result, sdk };
}

describe("hot plugin lifecycle", () => {
  it("confirms retained active work before retrying rejected UI from verified local bytes with a fresh activation", async () => {
    const close = vi.fn(() => Promise.resolve());
    const prepareUnload = vi.fn(() => Promise.resolve());
    const original: PluginBackend = {
      ...backend(),
      close,
      prepareUnload,
      beforeChange: () => Promise.resolve({ message: "Capture active", detail: "Session A" }),
    };
    let activations = 0;
    const list = vi.fn(() => Promise.reject(new Error("Offline")));
    const download = vi.fn(() => Promise.reject(new Error("Offline")));
    const { runtime, store } = await setup({
      catalog: { list, download },
      loadModule: () =>
        Promise.resolve({
          activate: () => {
            activations += 1;
            return activations === 1 ? original : backend();
          },
        }),
    });
    const before = (await runtime.list()).plugins[0]!;
    await runtime.rendererFailed(manifest.id, before.activationId!, "Renderer rejected");
    await expect(runtime.retryActivation(manifest.id)).rejects.toThrow(/confirmation/u);
    expect(activations).toBe(1);
    expect(prepareUnload).not.toHaveBeenCalled();
    expect((await runtime.list()).plugins[0]?.activationId).toBe(before.activationId);
    const confirmation = await runtime.prepareChange(manifest.id, "retry");
    const recovered = (await runtime.retryActivation(manifest.id, confirmation!.token)).plugins[0]!;
    expect(recovered.active).toEqual(manifest);
    expect(recovered.activationId).not.toBe(before.activationId);
    expect(recovered.error).toBeUndefined();
    expect((await store.getActive(manifest.id))?.sha256).toBe(sha256);
    expect(prepareUnload).toHaveBeenCalledWith("update");
    expect(close).toHaveBeenCalledTimes(1);
    expect(await runtime.rendererAsset(before.rendererUrl!)).toBeUndefined();
    expect(await runtime.rendererAsset(recovered.rendererUrl!)).toBeDefined();
    await expect(runtime.execute(request(before.activationId!))).rejects.toThrow(
      /no longer active/u,
    );
    expect((await runtime.retryActivation(manifest.id)).plugins[0]?.activationId).toBe(
      recovered.activationId,
    );
    expect(activations).toBe(2);
    expect(list).not.toHaveBeenCalled();
    expect(download).not.toHaveBeenCalled();
  });

  it.each(["verification", "activation", "cleanup"] as const)(
    "retains the original active instance when local retry fails during %s",
    async (failure) => {
      const originalClose = vi.fn(() => Promise.resolve());
      const candidateClose = vi.fn(() => Promise.resolve());
      const original: PluginBackend = {
        ...backend(),
        close: originalClose,
        beforeChange: () => Promise.resolve({ message: "Capture active", detail: "Session A" }),
        prepareUnload: () =>
          failure === "cleanup"
            ? Promise.reject(new Error("Remote cleanup failed"))
            : Promise.resolve(),
      };
      let activations = 0;
      const { runtime, store } = await setup({
        loadModule: () =>
          Promise.resolve({
            activate: () => {
              activations += 1;
              if (activations === 1) return original;
              if (failure === "activation") throw new Error("Candidate activation failed");
              return { ...backend(), close: candidateClose };
            },
          }),
      });
      const before = (await runtime.list()).plugins[0]!;
      await runtime.rendererFailed(manifest.id, before.activationId!, "Renderer rejected");
      if (failure === "verification") {
        const installation = await store.getInstalled(manifest.id);
        await writeFile(installation!.backendPath, "tampered code");
      }
      const prompt = await runtime.prepareChange(manifest.id, "retry");
      await expect(runtime.retryActivation(manifest.id, prompt!.token)).rejects.toThrow();
      expect((await runtime.list()).plugins[0]?.activationId).toBe(before.activationId);
      await expect(runtime.execute(request(before.activationId!))).resolves.toBeNull();
      expect(originalClose).not.toHaveBeenCalled();
      expect(activations).toBe(failure === "verification" ? 1 : 2);
      expect(candidateClose).toHaveBeenCalledTimes(failure === "cleanup" ? 1 : 0);
    },
  );

  it("replaces one backend, invalidates old requests/assets/capabilities and publishes its new activation", async () => {
    const close = vi.fn(() => Promise.resolve());
    const prepareUnload = vi.fn(() => Promise.resolve());
    const { runtime, sdk, store } = await hotSetup(
      { ...backend(), close, prepareUnload },
      backend(),
    );
    const before = (await runtime.list()).plugins[0]!;
    const changed = vi.fn();
    const events = vi.fn();
    runtime.subscribeChanges(changed);
    runtime.subscribe(events);
    const snapshot = await runtime.install(manifest.id);
    const after = snapshot.plugins[0]!;
    expect(after.active?.version).toBe("1.1.0");
    expect(after.activationId).not.toBe(before.activationId);
    expect(snapshot.revision).toBe(1);
    expect(changed).toHaveBeenCalledWith(snapshot);
    expect(prepareUnload).toHaveBeenCalledWith("update");
    expect(close).toHaveBeenCalledTimes(1);
    expect(await runtime.rendererAsset(before.rendererUrl!)).toBeUndefined();
    expect(await runtime.rendererAsset(after.rendererUrl!)).toBeDefined();
    await expect(runtime.execute(request(before.activationId!))).rejects.toThrow(
      /no longer active/u,
    );
    await expect(runtime.execute(request(after.activationId!))).resolves.toBeNull();
    sdk[0]!.publish("late", {});
    expect(events).not.toHaveBeenCalled();
    await expect(sdk[0]!.profiles()).rejects.toThrow(/no longer active/u);
    await expect(sdk[0]!.disconnectOwnedConnection()).rejects.toThrow(/no longer active/u);
    expect(() => sdk[0]!.connectionActive()).toThrow(/no longer active/u);
    await expect(
      sdk[0]!.execute({
        command: "connection.disconnect",
        id: "stale",
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      }),
    ).rejects.toThrow(/no longer active/u);
    expect((await store.getActive(manifest.id))?.manifest.version).toBe("1.1.0");
  });

  it("keeps the running version and discards a candidate when remote cleanup fails", async () => {
    const oldClose = vi.fn(() => Promise.resolve());
    const newClose = vi.fn(() => Promise.resolve());
    const { runtime, store } = await hotSetup(
      {
        ...backend(),
        close: oldClose,
        prepareUnload: () => Promise.reject(new Error("EDA cleanup failed")),
      },
      { ...backend(), close: newClose },
    );
    const before = (await runtime.list()).plugins[0]!;
    await expect(runtime.install(manifest.id)).rejects.toThrow("EDA cleanup failed");
    expect((await runtime.list()).plugins[0]).toMatchObject({
      activationId: before.activationId,
      pending: null,
      active: manifest,
    });
    expect(oldClose).not.toHaveBeenCalled();
    expect(newClose).toHaveBeenCalledTimes(1);
    expect((await store.getActive(manifest.id))?.manifest).toEqual(manifest);
    await expect(runtime.execute(request(before.activationId!))).resolves.toBeNull();
    await expect(runtime.remove(manifest.id)).rejects.toThrow("EDA cleanup failed");
    expect((await runtime.list()).plugins[0]?.active).toEqual(manifest);
  });

  it("accepts immediate renderer commands when the activation event is published", async () => {
    const { runtime } = await hotSetup(backend(), backend());
    const response = deferred<Promise<unknown>>();
    runtime.subscribeChanges((snapshot) => {
      const activationId = snapshot.plugins[0]?.activationId;
      if (activationId !== undefined) response.resolve(runtime.execute(request(activationId)));
    });
    await runtime.install(manifest.id);
    await expect(response.promise).resolves.toBeNull();
  });

  it("keeps the original backend usable after a failed installation state write", async () => {
    const close = vi.fn(() => Promise.resolve());
    const candidateClose = vi.fn(() => Promise.resolve());
    const { runtime, store } = await hotSetup(
      { ...backend(), close },
      { ...backend(), close: candidateClose },
    );
    const before = (await runtime.list()).plugins[0]!;
    vi.spyOn(store, "commitInstall").mockRejectedValueOnce(new Error("Disk full"));
    await expect(runtime.install(manifest.id)).rejects.toThrow("Disk full");
    expect(close).not.toHaveBeenCalled();
    expect(candidateClose).toHaveBeenCalledTimes(1);
    expect((await store.getActive(manifest.id))?.manifest).toEqual(manifest);
    await expect(runtime.execute(request(before.activationId!))).resolves.toBeNull();
  });

  it("keeps post-removal cleanup failures actionable without disabling all plugin management", async () => {
    const { runtime, store } = await hotSetup(backend(), backend());
    const uninstall = store.uninstall.bind(store);
    vi.spyOn(store, "uninstall").mockImplementationOnce(async (id) => {
      await uninstall(id);
      return "Retained package files could not be deleted. Retry removal.";
    });
    const removed = await runtime.remove(manifest.id);
    expect(removed.error).toBeUndefined();
    expect(removed.plugins[0]).toMatchObject({
      id: manifest.id,
      error: expect.stringContaining("Retry removal") as unknown,
    });
    expect(removed.plugins[0]?.active).toBeUndefined();
    expect((await runtime.remove(manifest.id)).plugins).toEqual([]);
    expect((await runtime.install(manifest.id)).plugins[0]?.active).toBeDefined();
  });

  it("lets unload cancel an in-flight request, drains it, and blocks new dispatch", async () => {
    const entered = deferred<void>();
    const result = deferred<null>();
    const unloading = deferred<void>();
    const finishUnload = deferred<void>();
    const close = vi.fn(() => Promise.resolve());
    const { runtime } = await hotSetup(
      {
        ...backend(),
        close,
        execute: () => {
          entered.resolve();
          return result.promise;
        },
        prepareUnload: async () => {
          unloading.resolve();
          result.resolve(null);
          await finishUnload.promise;
        },
      },
      backend(),
    );
    const activationId = (await runtime.list()).plugins[0]!.activationId!;
    const running = runtime.execute(request(activationId));
    await entered.promise;
    const removal = runtime.remove(manifest.id);
    await unloading.promise;
    await expect(running).resolves.toBeNull();
    await expect(runtime.execute(request(activationId))).rejects.toThrow(/being changed/u);
    expect(close).not.toHaveBeenCalled();
    finishUnload.resolve();
    expect((await removal).plugins).toEqual([]);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("waits for an existing profile connection before confirming and cleaning up its backend", async () => {
    const entered = deferred<void>();
    const finishConnection = deferred<void>();
    let connected = false;
    const prepareUnload = vi.fn(() => {
      expect(connected).toBe(true);
      return Promise.resolve();
    });
    const { runtime } = await hotSetup({ ...backend(), prepareUnload }, backend());
    const connecting = runtime.withProfileConnection(source, [], async () => {
      entered.resolve();
      await finishConnection.promise;
      connected = true;
    });
    await entered.promise;
    const removing = runtime.remove(manifest.id);
    await vi.waitFor(async () => {
      await expect(
        runtime.withProfileConnection(source, [], () => Promise.resolve()),
      ).rejects.toThrow(/being changed/u);
    });
    expect(prepareUnload).not.toHaveBeenCalled();
    finishConnection.resolve();
    await connecting;
    await removing;
    expect(prepareUnload).toHaveBeenCalledOnce();
  });

  it("rejects confirmation for a different action or changed capture state", async () => {
    let detail = "Session A";
    const prepareUnload = vi.fn(() => Promise.resolve());
    const { runtime } = await hotSetup(
      {
        ...backend(),
        prepareUnload,
        beforeChange: () => Promise.resolve({ message: "Capture active", detail }),
      },
      backend(),
    );
    const install = await runtime.prepareChange(manifest.id, "install");
    await expect(runtime.remove(manifest.id, install!.token)).rejects.toThrow(/confirmation/u);
    const first = await runtime.prepareChange(manifest.id, "remove");
    detail = "Session B";
    await expect(runtime.remove(manifest.id, first!.token)).rejects.toThrow(/confirmation/u);
    expect(prepareUnload).not.toHaveBeenCalled();
    const second = await runtime.prepareChange(manifest.id, "remove");
    await runtime.remove(manifest.id, second!.token);
    expect(prepareUnload).toHaveBeenCalledOnce();
  });

  it("expires confirmations and rejects work that started after a no-warning preparation", async () => {
    let busy = false;
    const { runtime } = await hotSetup(
      {
        ...backend(),
        beforeChange: () =>
          Promise.resolve(busy ? { message: "Active", detail: "Session A" } : undefined),
      },
      backend(),
    );
    expect(await runtime.prepareChange(manifest.id, "remove")).toBeNull();
    busy = true;
    await expect(runtime.remove(manifest.id)).rejects.toThrow(/confirmation/u);
    const prompt = await runtime.prepareChange(manifest.id, "remove");
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 6 * 60_000);
    await expect(runtime.remove(manifest.id, prompt!.token)).rejects.toThrow(/confirmation/u);
  });

  it("does not apply an old exit decision to a replacement backend", async () => {
    const resolveExit = vi.fn(() => Promise.resolve(true));
    const beforeExit = (): ReturnType<PluginBackend["beforeExit"]> =>
      Promise.resolve({
        title: "Pending work",
        message: "Clean up?",
        detail: "Capture session",
        actions: [{ id: "cleanup", label: "Clean up" }],
        cancelAction: "cancel",
      });
    const { runtime } = await hotSetup(
      { ...backend(), beforeExit },
      { ...backend(), beforeExit, resolveExit },
    );
    await runtime.prepareExit();
    await runtime.install(manifest.id);
    await expect(runtime.resolveExit(manifest.id, "cleanup")).rejects.toThrow(
      /changed after this exit prompt/u,
    );
    expect(resolveExit).not.toHaveBeenCalled();
    await runtime.prepareExit();
    await expect(runtime.resolveExit(manifest.id, "cleanup")).resolves.toBe(true);
    expect(resolveExit).toHaveBeenCalledOnce();
  });

  it("rolls back an update whose UI fails and ignores delayed errors from the retired UI", async () => {
    const { runtime, store } = await hotSetup(backend(), backend());
    const first = (await runtime.list()).plugins[0]!;
    const updated = (await runtime.install(manifest.id)).plugins[0]!;
    const recovered = await runtime.rendererFailed(
      manifest.id,
      updated.activationId!,
      "Invalid renderer export",
    );
    expect(recovered.plugins[0]).toMatchObject({
      active: manifest,
      error: expect.stringContaining("previous version was restored") as unknown,
    });
    expect(recovered.plugins[0]!.activationId).not.toBe(first.activationId);
    expect((await store.getActive(manifest.id))?.manifest).toEqual(manifest);
    expect(await runtime.rendererFailed(manifest.id, updated.activationId!, "Late error")).toEqual(
      recovered,
    );
  });

  it("retains active capture work when a delayed renderer error arrives", async () => {
    const prepareUnload = vi.fn(() => Promise.resolve());
    const { runtime } = await hotSetup(backend(), {
      ...backend(),
      prepareUnload,
      beforeChange: () => Promise.resolve({ message: "Active capture", detail: "Session A" }),
    });
    const installed = (await runtime.install(manifest.id)).plugins[0]!;
    const failed = await runtime.rendererFailed(
      manifest.id,
      installed.activationId!,
      "UI failed late",
    );
    expect(failed.plugins[0]).toMatchObject({
      activationId: installed.activationId,
      active: { version: "1.1.0" },
      error: expect.stringContaining("Active work was retained") as unknown,
    });
    expect(prepareUnload).not.toHaveBeenCalled();
  });

  it("isolates another plugin's pending installation during a hot update", async () => {
    const { runtime, store, sdk } = await hotSetup(backend(), backend());
    const other = encodePluginPackage(
      { ...manifest, id: "example.other" },
      new Map([
        ["backend.cjs", Buffer.from("exports.activate=()=>({});")],
        ["renderer.js", Buffer.from("export default {};")],
      ]),
    );
    await store.install(other, pluginPackageSha256(other));
    const updated = await runtime.install(manifest.id);
    expect(sdk).toHaveLength(2);
    expect(updated.plugins.find((entry) => entry.id === "example.other")).toMatchObject({
      pending: "install",
    });
    expect(updated.plugins.find((entry) => entry.id === "example.other")?.active).toBeUndefined();
  });

  it("keeps the same package activation on healthy install and removes all stored versions", async () => {
    const { runtime, store } = await hotSetup(backend(), backend());
    const first = (await runtime.install(manifest.id)).plugins[0]!;
    const second = (await runtime.install(manifest.id)).plugins[0]!;
    expect(second.activationId).toBe(first.activationId);
    expect(second.rendererUrl).toBe(first.rendererUrl);
    expect(await runtime.rendererAsset(first.rendererUrl!)).toBeDefined();
    const installation = (await store.getActive(manifest.id))!;
    await runtime.remove(manifest.id);
    await expect(
      import("node:fs/promises").then(({ stat }) => stat(installation.directory)),
    ).rejects.toMatchObject({ code: "ENOENT" });
    expect(await store.list()).toEqual([]);
    const reinstalled = (await runtime.install(manifest.id)).plugins[0]!;
    expect(reinstalled.active?.version).toBe("1.1.0");
    expect(reinstalled.activationId).not.toBe(second.activationId);
  });

  it("serializes overlapping mutations and cannot publish a candidate after shutdown", async () => {
    const entered = deferred<void>();
    const finishActivation = deferred<void>();
    const candidateClose = vi.fn(() => Promise.resolve());
    let activations = 0;
    const update = updatedPackage();
    const { runtime, store } = await setup({
      catalog: { list: () => Promise.resolve([]), download: () => Promise.resolve(update) },
      loadModule: () =>
        Promise.resolve({
          activate: async () => {
            if (++activations === 1) return backend();
            entered.resolve();
            await finishActivation.promise;
            return { ...backend(), close: candidateClose };
          },
        }),
    });
    await runtime.start();
    const installing = runtime.install(manifest.id);
    const rejectedInstall = expect(installing).rejects.toThrow(/closing/u);
    await entered.promise;
    const removing = runtime.remove(manifest.id);
    const rejectedRemove = expect(removing).rejects.toThrow(/closing/u);
    const closing = runtime.close();
    finishActivation.resolve();
    await rejectedInstall;
    await rejectedRemove;
    await closing;
    expect(candidateClose).toHaveBeenCalledOnce();
    expect((await store.getActive(manifest.id))?.manifest).toEqual(manifest);
  });
});
