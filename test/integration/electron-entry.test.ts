import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type AppListener = (...arguments_: unknown[]) => void;

const native = vi.hoisted(() => {
  const listeners = new Map<string, AppListener[]>();
  const protectionReceipts: { kafka: unknown; nats: unknown } = {
    kafka: undefined,
    nats: undefined,
  };
  return {
    listeners,
    exitCodes: [] as number[],
    relaunches: 0,
    shellCloseCalls: 0,
    shutdownCalls: 0,
    natsShutdownCalls: 0,
    protectionCalls: 0,
    profileProtection: {
      capability: { durability: "durable", state: "unavailable", protection: "unavailable" },
    },
    protectionReceipts,
    providerIds: [] as string[],
    bindingIds: [] as string[],
    natsCreateFailure: undefined as Error | undefined,
    shellCreated: 0,
    restart: undefined as (() => void) | undefined,
    closeOperation: (): Promise<void> => Promise.resolve(),
    shutdownOperation: (): Promise<void> => Promise.resolve(),
    natsShutdownOperation: (): Promise<void> => Promise.resolve(),
    close: (): Promise<void> => {
      native.shellCloseCalls += 1;
      for (const listener of listeners.get("window-all-closed") ?? []) listener();
      return native.closeOperation();
    },
    app: {
      whenReady: (): Promise<void> => Promise.resolve(),
      getPath: (): string => "/tmp/streamskope-entry-fixture",
      on: (event: string, listener: AppListener): void => {
        const owned = listeners.get(event) ?? [];
        owned.push(listener);
        listeners.set(event, owned);
      },
      exit: (code: number): void => {
        native.exitCodes.push(code);
      },
      relaunch: (): void => {
        native.relaunches += 1;
      },
    },
  };
});

vi.mock("electron", () => ({
  app: native.app,
  dialog: { showMessageBox: (): Promise<{ response: number }> => Promise.resolve({ response: 0 }) },
  safeStorage: {},
}));
vi.mock("../../src/platform/node/plugins/runtime", () => ({
  PluginRuntime: class {
    constructor(options: { readonly restart: () => void }) {
      native.restart = options.restart;
    }
  },
}));
vi.mock("../../src/platform/node/plugins/store", () => ({ PluginStore: class {} }));
vi.mock("../../src/platform/electron/main/packaged-renderer-protocol", () => ({
  registerPackagedRendererScheme: (): void => undefined,
  installPackagedRendererProtocol: (): void => undefined,
  PACKAGED_RENDERER_URL: "streamskope://app/",
}));
vi.mock("../../src/platform/electron/main/plugin-exit", () => ({
  confirmPluginExit: (): Promise<boolean> => Promise.resolve(true),
}));
vi.mock("../../src/platform/electron/main/electron-kafka-backend", () => ({
  createElectronKafkaBackend: (options: {
    readonly profileProtection: unknown;
  }): Promise<object> => {
    native.protectionReceipts.kafka = options.profileProtection;
    return Promise.resolve({
      execute: (): Promise<never> => Promise.reject(new Error("Unexpected entry fixture command.")),
      subscribe: (): (() => void) => (): void => undefined,
      stopStream: (): Promise<void> => Promise.resolve(),
      shutdown: (): Promise<void> => {
        native.shutdownCalls += 1;
        return native.shutdownOperation();
      },
    });
  },
}));
vi.mock("../../src/platform/electron/main/electron-profile-protection", () => ({
  initializeElectronProfileProtection: (): Promise<object> => {
    native.protectionCalls += 1;
    return Promise.resolve(native.profileProtection);
  },
}));
vi.mock("../../src/platform/electron/main/electron-nats-profile-store", () => ({
  createElectronNatsProfileStore: (options: { readonly profileProtection: unknown }): object => {
    native.protectionReceipts.nats = options.profileProtection;
    return {};
  },
}));
vi.mock("../../src/platform/node/nats-backend", () => ({
  createNatsBackend: (): object => {
    if (native.natsCreateFailure !== undefined) throw native.natsCreateFailure;
    return {
      execute: (): Promise<never> =>
        Promise.reject(new Error("Unexpected NATS entry fixture command.")),
      subscribe: (): (() => void) => (): void => undefined,
      stopStream: (): Promise<void> => Promise.resolve(),
      shutdown: (): Promise<void> => {
        native.natsShutdownCalls += 1;
        return native.natsShutdownOperation();
      },
    };
  },
}));
vi.mock("../../src/platform/electron/main/electron-shell", () => ({
  createElectronShell: (options: {
    readonly registry: { endpoints(): readonly { readonly id: string }[] };
    readonly deliveryBindings: readonly { readonly id: string }[];
  }): Promise<object> => {
    native.shellCreated += 1;
    native.providerIds = options.registry.endpoints().map(({ id }) => id);
    native.bindingIds = options.deliveryBindings.map(({ id }) => id);
    return Promise.resolve({
      window: { on: (): void => undefined },
      close: native.close,
    });
  },
}));

beforeEach(() => {
  vi.resetModules();
  native.listeners.clear();
  native.exitCodes.splice(0);
  native.relaunches = 0;
  native.shellCloseCalls = 0;
  native.shutdownCalls = 0;
  native.natsShutdownCalls = 0;
  native.protectionCalls = 0;
  native.protectionReceipts.kafka = undefined;
  native.protectionReceipts.nats = undefined;
  native.providerIds = [];
  native.bindingIds = [];
  native.natsCreateFailure = undefined;
  native.shellCreated = 0;
  native.restart = undefined;
  native.closeOperation = (): Promise<void> => Promise.resolve();
  native.shutdownOperation = (): Promise<void> => Promise.resolve();
  native.natsShutdownOperation = (): Promise<void> => Promise.resolve();
});
afterEach(() => vi.useRealTimers());

async function start(): Promise<void> {
  await import("../../src/platform/electron/main/electron-entry");
  await vi.waitFor(() => expect(native.shellCreated).toBe(1));
  await new Promise<void>((resolve) => setImmediate(resolve));
}

function closeWindows(): void {
  for (const listener of native.listeners.get("window-all-closed") ?? []) listener();
}

describe("Electron entry cleanup ownership", () => {
  it("initializes OS protection once and selects both typed providers and delivery bindings", async () => {
    await start();
    expect(native.protectionCalls).toBe(1);
    expect(native.protectionReceipts.kafka).toBe(native.profileProtection);
    expect(native.protectionReceipts.nats).toBe(native.profileProtection);
    expect(native.providerIds).toEqual(["kafka", "nats"]);
    expect(native.bindingIds).toEqual(["kafka", "nats"]);
    closeWindows();
    await vi.waitFor(() => expect(native.exitCodes).toEqual([0]));
    expect(native.shutdownCalls).toBe(1);
    expect(native.natsShutdownCalls).toBe(1);
  });

  it("awaits the already-created provider when second-provider startup fails", async () => {
    let complete = (): void => undefined;
    native.shutdownOperation = (): Promise<void> =>
      new Promise<void>((resolve) => {
        complete = resolve;
      });
    native.natsCreateFailure = new Error("NATS fixture startup failed.");
    await import("../../src/platform/electron/main/electron-entry");
    await vi.waitFor(() => expect(native.shutdownCalls).toBe(1));
    expect(native.shellCreated).toBe(0);
    expect(native.natsShutdownCalls).toBe(0);
    expect(native.exitCodes).toEqual([]);
    complete();
    await vi.waitFor(() => expect(native.exitCodes).toEqual([1]));
  });

  it("retains the NATS shutdown barrier and refuses relaunch after its failure", async () => {
    let rejectNats = (_error: Error): void => undefined;
    native.natsShutdownOperation = (): Promise<void> =>
      new Promise<void>((_resolve, reject) => {
        rejectNats = reject;
      });
    await start();
    vi.useFakeTimers();
    native.restart?.();
    await vi.advanceTimersByTimeAsync(100);
    closeWindows();
    expect(native.shellCloseCalls).toBe(1);
    expect(native.shutdownCalls).toBe(1);
    expect(native.natsShutdownCalls).toBe(1);
    expect(native.exitCodes).toEqual([]);
    expect(native.relaunches).toBe(0);
    rejectNats(new Error("NATS fixture cleanup failed."));
    await vi.advanceTimersByTimeAsync(0);
    expect(native.exitCodes).toEqual([1]);
    expect(native.relaunches).toBe(0);
  });

  it("waits for both shell cleanup and provider shutdown before exiting once", async () => {
    let completeClose = (): void => undefined;
    let completeShutdown = (): void => undefined;
    native.closeOperation = (): Promise<void> =>
      new Promise<void>((resolve) => {
        completeClose = resolve;
      });
    native.shutdownOperation = (): Promise<void> =>
      new Promise<void>((resolve) => {
        completeShutdown = resolve;
      });
    await start();
    closeWindows();
    closeWindows();
    expect(native.shellCloseCalls).toBe(1);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(native.shutdownCalls).toBe(1);
    expect(native.exitCodes).toEqual([]);
    completeShutdown();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(native.exitCodes).toEqual([]);
    completeClose();
    await vi.waitFor(() => expect(native.exitCodes).toEqual([0]));
    expect(native.relaunches).toBe(0);
  });

  it.each(["synchronous", "asynchronous"] as const)(
    "still attempts provider shutdown after a %s shell cleanup failure",
    async (failure) => {
      native.closeOperation = (): Promise<void> => {
        if (failure === "synchronous") throw new Error("private native cleanup detail");
        return Promise.reject(new Error("private native cleanup detail"));
      };
      await start();
      closeWindows();
      await vi.waitFor(() => expect(native.exitCodes).toEqual([1]));
      expect(native.shellCloseCalls).toBe(1);
      expect(native.shutdownCalls).toBe(1);
      expect(native.relaunches).toBe(0);
    },
  );

  it.each([true, false])(
    "relaunches only after every owned cleanup succeeds: %s",
    async (succeeds) => {
      let completeClose = (): void => undefined;
      let rejectClose = (_error: unknown): void => undefined;
      native.closeOperation = (): Promise<void> =>
        new Promise<void>((resolve, reject) => {
          completeClose = resolve;
          rejectClose = reject;
        });
      await start();
      vi.useFakeTimers();
      native.restart?.();
      await vi.advanceTimersByTimeAsync(100);
      expect(native.shellCloseCalls).toBe(1);
      expect(native.shutdownCalls).toBe(1);
      expect(native.exitCodes).toEqual([]);
      expect(native.relaunches).toBe(0);
      if (succeeds) completeClose();
      else rejectClose(new Error("private cleanup failure"));
      await vi.advanceTimersByTimeAsync(0);
      expect(native.exitCodes).toEqual([succeeds ? 0 : 1]);
      expect(native.relaunches).toBe(succeeds ? 1 : 0);
    },
  );
});
