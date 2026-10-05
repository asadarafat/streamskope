import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ProviderHostPort } from "../../src/platform/providers/host";
import {
  ProviderHostRegistry,
  ProviderWireValidationError,
} from "../../src/platform/node/provider-host";
import {
  createElectronShell,
  ElectronShellStartupError,
} from "../../src/platform/electron/main/electron-shell";
import { providerIpcChannels } from "../../src/platform/electron/preload/channels";
import {
  createPreloadProviderWire,
  type PreloadIpcRenderer,
} from "../../src/platform/electron/preload/provider-wire";
import { exposeStreamSkopeHost } from "../../src/platform/electron/preload/host-bridge";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../../src/features/kafka/contracts";
import {
  createProviderFixture,
  type ProviderFixture,
  type FixtureCommand,
  type FixtureResponse,
  type FixtureEvent,
} from "../support/provider-fixture";

type Handler = (event: { readonly sender: unknown }, value: unknown) => unknown;
type Listener = (...arguments_: unknown[]) => void;

const native = vi.hoisted(() => {
  const handlers = new Map<string, Handler>();
  class Window {
    readonly webContents = {
      sent: [] as Array<{ channel: string; value: unknown }>,
      send: (channel: string, value: unknown): void => {
        this.webContents.sent.push({ channel, value });
      },
      on: (): void => undefined,
      setWindowOpenHandler: (): void => undefined,
    };
    private readonly listeners = new Map<string, Listener>();
    destroyed = false;
    constructor() {
      windows.push(this);
    }
    once(event: string, listener: Listener): void {
      this.listeners.set(event, listener);
    }
    show(): void {
      /* Window rendering is outside mock IPC qualification. */
    }
    loadURL(): Promise<void> {
      return Promise.resolve();
    }
    isDestroyed(): boolean {
      return this.destroyed;
    }
    close(): void {
      if (this.destroyed) return;
      this.destroyed = true;
      this.listeners.get("closed")?.();
    }
  }
  const windows: Window[] = [];
  return {
    windows,
    handlers,
    BrowserWindow: Window,
    ipcMain: {
      handle(channel: string, handler: Handler): void {
        if (handlers.has(channel)) throw new Error("Occupied fixture IPC channel.");
        handlers.set(channel, handler);
      },
      removeHandler(channel: string): void {
        handlers.delete(channel);
      },
    },
    Menu: { buildFromTemplate: (): object => ({}), setApplicationMenu: (): void => undefined },
    dialog: {
      showSaveDialog: (): Promise<{ canceled: true }> => Promise.resolve({ canceled: true }),
    },
    shell: { openExternal: (): Promise<void> => Promise.resolve() },
  };
});

vi.mock("electron", () => native);

async function invoke(channel: string, sender: unknown, value: unknown): Promise<unknown> {
  const handler = native.handlers.get(channel);
  if (handler === undefined) throw new Error("Unregistered fixture IPC channel.");
  return await handler({ sender }, value);
}
function window(): (typeof native.windows)[number] {
  const owned = native.windows[0];
  if (owned === undefined) throw new Error("Expected an owned Electron window.");
  return owned;
}
async function shellFor(
  providers: readonly ProviderFixture[],
): Promise<Awaited<ReturnType<typeof createElectronShell>>> {
  return createElectronShell({
    registry: new ProviderHostRegistry(providers.map((p) => p.endpoint)),
    preloadPath: "/tmp/streamskope/provider-preload.js",
    rendererUrl: "http://127.0.0.1:5173/",
  });
}

beforeEach(() => {
  native.handlers.clear();
  native.windows.splice(0);
});

describe("registered Electron provider routes", () => {
  it("rejects foreign senders, foreign protocols and unregistered channels before provider execution", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 11 });
    const shell = await shellFor([alpha, beta]);
    const a = providerIpcChannels("alpha"),
      b = providerIpcChannels("beta");
    try {
      for (const [channel, value] of [
        [a.command, alpha.command()],
        [a.subscribe, 7],
        [a.acknowledge, 1],
      ] as const) {
        await expect(invoke(channel, {}, value)).rejects.toThrow("must originate");
      }
      expect(alpha.requests).toEqual([]);
      expect(alpha.subscribeCalls).toBe(1);
      await expect(invoke(a.command, window().webContents, beta.command())).rejects.toMatchObject({
        stage: "command",
      });
      await expect(
        invoke(a.command, window().webContents, { ...alpha.command(), version: 11 }),
      ).rejects.toMatchObject({ stage: "command" });
      expect(alpha.requests).toEqual([]);
      expect(beta.requests).toEqual([]);
      expect(native.handlers.has(providerIpcChannels("unknown").command)).toBe(false);
      await expect(
        invoke(providerIpcChannels("unknown").command, window().webContents, alpha.command()),
      ).rejects.toThrow("Unregistered");
      const command = beta.command("set", "beta-only");
      await expect(invoke(b.command, window().webContents, command)).resolves.toEqual(
        beta.response(command, "beta-only"),
      );
      expect(alpha.requests).toEqual([]);
      expect(beta.requests).toEqual([command]);
    } finally {
      shell.close();
    }
  });

  it("acknowledges equal sequence numbers only in their own provider queue", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 11 });
    const shell = await shellFor([alpha, beta]);
    const a = providerIpcChannels("alpha"),
      b = providerIpcChannels("beta");
    try {
      alpha.emit(alpha.event("alpha-1", 1));
      beta.emit(beta.event("beta-1", 1));
      alpha.emit(alpha.event("alpha-2", 2));
      beta.emit(beta.event("beta-2", 2));
      expect(window().webContents.sent).toEqual([
        { channel: a.event, value: alpha.event("alpha-1", 1) },
        { channel: b.event, value: beta.event("beta-1", 1) },
      ]);
      await expect(invoke(a.acknowledge, {}, 1)).rejects.toThrow("must originate");
      expect(window().webContents.sent).toHaveLength(2);
      await invoke(a.acknowledge, window().webContents, 1);
      expect(window().webContents.sent.at(-1)).toEqual({
        channel: a.event,
        value: alpha.event("alpha-2", 2),
      });
      expect(window().webContents.sent.filter(({ channel }) => channel === b.event)).toHaveLength(
        1,
      );
      await invoke(b.acknowledge, window().webContents, 1);
      expect(window().webContents.sent.at(-1)).toEqual({
        channel: b.event,
        value: beta.event("beta-2", 2),
      });
    } finally {
      shell.close();
    }
  });

  it("closes only the malformed event route while its sibling keeps receiving and executing", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 11 });
    const shell = await shellFor([alpha, beta]);
    const b = providerIpcChannels("beta");
    try {
      alpha.emit(beta.event("foreign-event", 1));
      await Promise.resolve();
      expect(alpha.listenerCount()).toBe(0);
      expect(beta.listenerCount()).toBe(1);
      expect(
        window().webContents.sent.find(
          ({ channel }) => channel === providerIpcChannels("alpha").event,
        )?.value,
      ).toMatchObject({
        type: "fixture.availability",
        state: "unavailable",
        provider: "alpha",
      });
      const command = beta.command();
      await expect(invoke(b.command, window().webContents, command)).resolves.toEqual(
        beta.response(command),
      );
      beta.emit(beta.event("still-alive", 1));
      expect(window().webContents.sent.at(-1)).toEqual({
        channel: b.event,
        value: beta.event("still-alive", 1),
      });
      expect(alpha.requests).toEqual([]);
    } finally {
      shell.close();
    }
  });

  it.each(["requestId", "action", "version"] as const)(
    "rejects a valid response for a different %s",
    async (mismatch) => {
      const fixture = createProviderFixture({ id: "probe", version: 7 });
      const shell = await shellFor([fixture]);
      try {
        const command = fixture.command();
        fixture.nextResponse = {
          ...fixture.response(command),
          [mismatch]:
            mismatch === "version" ? 11 : mismatch === "action" ? "set" : "another-request",
        };
        await expect(
          invoke(providerIpcChannels("probe").command, window().webContents, command),
        ).rejects.toBeInstanceOf(ProviderWireValidationError);
        expect(fixture.requests).toEqual([command]);
      } finally {
        shell.close();
      }
    },
  );

  it("cleans earlier owned subscriptions and channels after a sibling channel collision", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 11 });
    const occupied: Handler = (): string => "original-owner";
    const collision = providerIpcChannels("beta").command;
    native.handlers.set(collision, occupied);
    await expect(shellFor([alpha, beta])).rejects.toBeInstanceOf(ElectronShellStartupError);
    expect(alpha.listenerCount()).toBe(0);
    expect(beta.listenerCount()).toBe(0);
    expect(native.handlers).toEqual(new Map([[collision, occupied]]));
    expect(window().destroyed).toBe(true);
  });

  it("cleans all route subscriptions and preserves unrelated handlers when a sibling subscription throws", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 11 });
    beta.subscribeFailure = new Error("Fixture subscriber refused attachment.");
    const unrelated: Handler = (): string => "other-owner";
    native.handlers.set("unrelated-owner", unrelated);
    await expect(shellFor([alpha, beta])).rejects.toBeInstanceOf(ElectronShellStartupError);
    expect(alpha.listenerCount()).toBe(0);
    expect(beta.listenerCount()).toBe(0);
    expect(native.handlers).toEqual(new Map([["unrelated-owner", unrelated]]));
    expect(window().destroyed).toBe(true);
  });
});

class Ipc implements PreloadIpcRenderer {
  readonly responses = new Map<string, unknown>();
  readonly rejected = new Set<string>();
  readonly invocations: Array<{ channel: string; value: unknown }> = [];
  private readonly listeners = new Map<string, Set<(event: unknown, value: unknown) => void>>();
  invoke(channel: string, value: unknown): Promise<unknown> {
    this.invocations.push({ channel, value });
    return this.rejected.has(channel)
      ? Promise.reject(new Error("Fixture IPC request failed."))
      : Promise.resolve(this.responses.get(channel));
  }
  on(channel: string, listener: (event: unknown, value: unknown) => void): void {
    const listeners = this.listeners.get(channel) ?? new Set();
    listeners.add(listener);
    this.listeners.set(channel, listeners);
  }
  removeListener(channel: string, listener: (event: unknown, value: unknown) => void): void {
    this.listeners.get(channel)?.delete(listener);
  }
  emit(channel: string, value: unknown): void {
    for (const listener of this.listeners.get(channel) ?? []) listener({}, value);
  }
  listenerCount(channel: string): number {
    return this.listeners.get(channel)?.size ?? 0;
  }
  retainHandler(channel: string): (event: unknown, value: unknown) => void {
    const handler = this.listeners.get(channel)?.values().next().value;
    if (handler === undefined) throw new Error("Expected an attached fixture event handler.");
    return handler;
  }
}
function fixtureHost(
  fixture: ProviderFixture,
  ipc: Ipc,
): ProviderHostPort<(command: FixtureCommand) => Promise<FixtureResponse>, FixtureEvent> {
  const channels = providerIpcChannels(fixture.endpoint.id);
  ipc.responses.set(channels.subscribe, fixture.endpoint.version);
  const wire = createPreloadProviderWire(ipc, channels, fixture.codec);
  return {
    execute: async (submitted): Promise<FixtureResponse> =>
      fixture.correlateResponse(await wire.invoke(fixture.parseCommand(submitted)), submitted),
    subscribe: wire.subscribe,
  };
}

describe("independent typed preload provider clients", () => {
  it.each(["event", "acknowledgement", "handshake"] as const)(
    "contains a failed %s to its provider while a sibling remains usable",
    async (failure) => {
      const alpha = createProviderFixture({ id: "alpha", version: 7 });
      const beta = createProviderFixture({ id: "beta", version: 11 });
      const ipc = new Ipc();
      const a = providerIpcChannels("alpha"),
        b = providerIpcChannels("beta");
      const hostA = fixtureHost(alpha, ipc),
        hostB = fixtureHost(beta, ipc);
      if (failure === "handshake") ipc.responses.set(a.subscribe, beta.endpoint.version);
      if (failure === "acknowledgement") ipc.rejected.add(a.acknowledge);
      const alphaListener = vi.fn(),
        betaListener = vi.fn();
      const stopA = hostA.subscribe(alphaListener),
        stopB = hostB.subscribe(betaListener);
      try {
        if (failure === "event") expect(() => ipc.emit(a.event, beta.event())).not.toThrow();
        else if (failure === "acknowledgement") ipc.emit(a.event, alpha.event());
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(ipc.listenerCount(a.event)).toBe(0);
        expect(ipc.listenerCount(b.event)).toBe(1);
        expect(alphaListener).toHaveBeenLastCalledWith(
          expect.objectContaining({ type: "fixture.availability", state: "unavailable" }),
        );
        await expect(hostA.execute(alpha.command())).rejects.toThrow("event stream is unavailable");
        const command = beta.command();
        ipc.responses.set(b.command, beta.response(command));
        await expect(hostB.execute(command)).resolves.toEqual(beta.response(command));
        ipc.emit(b.event, beta.event("healthy-sibling", 1));
        expect(betaListener).toHaveBeenLastCalledWith(beta.event("healthy-sibling", 1));
        expect(ipc.invocations.filter(({ channel }) => channel === a.command)).toEqual([]);
        expect(ipc.invocations.filter(({ channel }) => channel === b.acknowledge)).toEqual([
          { channel: b.acknowledge, value: 1 },
        ]);
      } finally {
        stopA();
        stopB();
      }
    },
  );

  it("ignores retained callbacks after their subscription is replaced", () => {
    const fixture = createProviderFixture({ id: "probe", version: 7 });
    const ipc = new Ipc(),
      host = fixtureHost(fixture, ipc);
    const channels = providerIpcChannels("probe");
    const original: FixtureEvent[] = [],
      replacement: FixtureEvent[] = [];
    const stopOriginal = host.subscribe((event) => original.push(event));
    const oldHandler = ipc.retainHandler(channels.event);
    stopOriginal();
    const stopReplacement = host.subscribe((event) => replacement.push(event));
    try {
      oldHandler({}, fixture.event("obsolete", 1));
      oldHandler({}, { ...fixture.event("foreign", 2), version: 11 });
      expect(original).toEqual([]);
      expect(replacement).toEqual([]);
      expect(ipc.listenerCount(channels.event)).toBe(1);
      expect(ipc.invocations.filter(({ channel }) => channel === channels.acknowledge)).toEqual([]);
      ipc.emit(channels.event, fixture.event("current", 1));
      expect(replacement).toEqual([fixture.event("current", 1)]);
      expect(ipc.invocations.filter(({ channel }) => channel === channels.acknowledge)).toEqual([
        { channel: channels.acknowledge, value: 1 },
      ]);
    } finally {
      stopReplacement();
    }
  });

  it("does not acknowledge an old event into a subscription replaced by its view callback", () => {
    const fixture = createProviderFixture({ id: "probe", version: 7 });
    const ipc = new Ipc(),
      host = fixtureHost(fixture, ipc);
    const channels = providerIpcChannels("probe");
    const replacement: FixtureEvent[] = [];
    let stopReplacement: (() => void) | undefined;
    const stopOriginal = host.subscribe(() => {
      stopOriginal();
      stopReplacement = host.subscribe((event) => replacement.push(event));
    });
    const oldHandler = ipc.retainHandler(channels.event);
    try {
      oldHandler({}, fixture.event("old-queue", 1));
      expect(replacement).toEqual([]);
      expect(ipc.listenerCount(channels.event)).toBe(1);
      expect(ipc.invocations.filter(({ channel }) => channel === channels.subscribe)).toHaveLength(
        2,
      );
      expect(ipc.invocations.filter(({ channel }) => channel === channels.acknowledge)).toEqual([]);
      ipc.emit(channels.event, fixture.event("new-queue", 1));
      expect(replacement).toEqual([fixture.event("new-queue", 1)]);
      expect(ipc.invocations.filter(({ channel }) => channel === channels.acknowledge)).toEqual([
        { channel: channels.acknowledge, value: 1 },
      ]);
    } finally {
      stopOriginal();
      stopReplacement?.();
    }
  });

  it("rejects a correlated-looking response belonging to another typed command", async () => {
    const fixture = createProviderFixture({ id: "probe", version: 7 });
    const ipc = new Ipc(),
      host = fixtureHost(fixture, ipc);
    const command = fixture.command();
    ipc.responses.set(
      providerIpcChannels("probe").command,
      fixture.response({ ...command, action: "set", value: "foreign-action" }),
    );
    await expect(host.execute(command)).rejects.toThrow("submitted request");
    expect(ipc.invocations).toHaveLength(1);
  });

  it("shares the same Kafka client between named and legacy exposure including transient subscribers", async () => {
    const ipc = new Ipc(),
      exposed = new Map<string, unknown>();
    const channels = providerIpcChannels("kafka");
    ipc.responses.set(channels.subscribe, HOST_PROTOCOL_VERSION);
    exposeStreamSkopeHost(
      {
        exposeInMainWorld: (name, value): void => {
          exposed.set(name, value);
        },
      },
      ipc,
    );
    const legacy = exposed.get("streamSkopeHost") as StreamSkopeHost;
    const named = (exposed.get("streamSkopeProviders") as { readonly kafka: StreamSkopeHost })
      .kafka;
    expect(named).toBe(legacy);
    const workbench = vi.fn(),
      plugin = vi.fn();
    const stopWorkbench = legacy.subscribe(workbench),
      stopPlugin = named.subscribe(plugin);
    const ready = {
      event: "backend.availability",
      payload: { state: "ready" },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    };
    ipc.emit(channels.event, ready);
    stopPlugin();
    const stopReplacement = named.subscribe(vi.fn());
    ipc.emit(channels.event, { ...ready, sequence: 2 });
    expect(workbench).toHaveBeenCalledTimes(2);
    expect(plugin).toHaveBeenCalledTimes(1);
    expect(ipc.invocations.filter(({ channel }) => channel === channels.subscribe)).toHaveLength(1);
    expect(ipc.invocations.filter(({ channel }) => channel === channels.acknowledge)).toEqual([
      { channel: channels.acknowledge, value: 1 },
      { channel: channels.acknowledge, value: 2 },
    ]);
    stopReplacement();
    stopWorkbench();
    await Promise.resolve();
  });
});
