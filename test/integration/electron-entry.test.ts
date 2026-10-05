import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type AppListener = (...arguments_: unknown[]) => void;

const native = vi.hoisted(() => {
  const listeners = new Map<string, AppListener[]>();
  return {
    listeners,
    exitCodes: [] as number[],
    relaunches: 0,
    shellCloseCalls: 0,
    shutdownCalls: 0,
    shellCreated: 0,
    restart: undefined as (() => void) | undefined,
    closeOperation: (): Promise<void> => Promise.resolve(),
    shutdownOperation: (): Promise<void> => Promise.resolve(),
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
  createElectronKafkaBackend: (): Promise<object> =>
    Promise.resolve({
      execute: (): Promise<never> => Promise.reject(new Error("Unexpected entry fixture command.")),
      subscribe: (): (() => void) => (): void => undefined,
      stopStream: (): Promise<void> => Promise.resolve(),
      shutdown: (): Promise<void> => {
        native.shutdownCalls += 1;
        return native.shutdownOperation();
      },
    }),
}));
vi.mock("../../src/platform/electron/main/electron-shell", () => ({
  createElectronShell: (): Promise<object> => {
    native.shellCreated += 1;
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
  native.shellCreated = 0;
  native.restart = undefined;
  native.closeOperation = (): Promise<void> => Promise.resolve();
  native.shutdownOperation = (): Promise<void> => Promise.resolve();
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
