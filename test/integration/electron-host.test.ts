import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  KAFKA_MESSAGE_LIMITS,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type StreamSkopeBackend,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { createElectronShell, ElectronShellStartupError } from "../../src/main/electron-shell";
import { createKafkaBackend } from "../../src/main/kafka-backend";
import { DESKTOP_PLATFORM_VERSION, DesktopPlatformContractError } from "../../src/platform/desktop";
import {
  DESKTOP_ACTION_CHANNEL,
  DESKTOP_DOCUMENT_SAVE_CHANNEL,
  EXTERNAL_URL_OPEN_CHANNEL,
  HOST_COMMAND_CHANNEL,
  HOST_EVENT_CHANNEL,
  HOST_EVENT_ACK_CHANNEL,
  HOST_SUBSCRIBE_CHANNEL,
} from "../../src/preload/channels";
import {
  createStreamSkopePreloadHost,
  exposeStreamSkopeHost,
  type PreloadIpcRenderer,
} from "../../src/preload/host-bridge";

type UnknownListener = (...arguments_: unknown[]) => void;
type InvokeHandler = (event: unknown, value: unknown) => Promise<unknown>;

const electronMock = vi.hoisted(() => {
  class FakeWebContents {
    readonly listeners = new Map<string, UnknownListener[]>();
    readonly sent: Array<{ channel: string; value: unknown }> = [];
    windowOpenHandler: ((details: unknown) => unknown) | undefined;

    emit(event: string, ...arguments_: unknown[]): void {
      for (const listener of this.listeners.get(event) ?? []) {
        listener(...arguments_);
      }
    }

    on(event: string, listener: UnknownListener): void {
      const listeners = this.listeners.get(event) ?? [];
      listeners.push(listener);
      this.listeners.set(event, listeners);
    }

    send(channel: string, value: unknown): void {
      this.sent.push({ channel, value });
    }

    setWindowOpenHandler(handler: (details: unknown) => unknown): void {
      this.windowOpenHandler = handler;
    }
  }

  class FakeBrowserWindow {
    readonly listeners = new Map<string, UnknownListener[]>();
    readonly loadedUrls: string[] = [];
    readonly webContents = new FakeWebContents();
    destroyed = false;
    shown = false;

    constructor(readonly options: Readonly<Record<string, unknown>>) {
      windows.push(this);
    }

    close(): void {
      if (this.destroyed) {
        return;
      }
      this.destroyed = true;
      this.emit("closed");
    }

    emit(event: string, ...arguments_: unknown[]): void {
      for (const listener of this.listeners.get(event) ?? []) {
        listener(...arguments_);
      }
    }

    isDestroyed(): boolean {
      return this.destroyed;
    }

    loadURL(url: string): Promise<void> {
      this.loadedUrls.push(url);
      return Promise.resolve();
    }

    once(event: string, listener: UnknownListener): void {
      this.listeners.set(event, [listener]);
    }

    show(): void {
      this.shown = true;
    }
  }

  const handlers = new Map<string, InvokeHandler>();
  const externalUrls: string[] = [];
  const menuTemplates: unknown[][] = [];
  const saveDialogCalls: unknown[] = [];
  let externalFailure: Error | undefined;
  let saveDialogResult: { readonly canceled: boolean; readonly filePath?: string } = {
    canceled: true,
  };
  const windows: FakeBrowserWindow[] = [];
  const ipcMain = {
    handle(channel: string, handler: InvokeHandler): void {
      if (handlers.has(channel)) {
        throw new Error(`Handler already registered for ${channel}.`);
      }
      handlers.set(channel, handler);
    },
    removeHandler(channel: string): void {
      handlers.delete(channel);
    },
  };

  return {
    dialog: {
      showSaveDialog(_window: unknown, options: unknown): Promise<unknown> {
        saveDialogCalls.push(options);
        return Promise.resolve(saveDialogResult);
      },
    },
    externalUrls,
    FakeBrowserWindow,
    handlers,
    ipcMain,
    Menu: {
      buildFromTemplate(template: unknown[]): { readonly template: unknown[] } {
        menuTemplates.push(template);
        return { template };
      },
      setApplicationMenu(): void {
        // The application menu remains process-owned for the application lifetime.
      },
    },
    menuTemplates,
    saveDialogCalls,
    setSaveDialogResult(result: { readonly canceled: boolean; readonly filePath?: string }): void {
      saveDialogResult = result;
    },
    setExternalFailure(error: Error | undefined): void {
      externalFailure = error;
    },
    shell: {
      openExternal(url: string): Promise<void> {
        externalUrls.push(url);
        return externalFailure === undefined ? Promise.resolve() : Promise.reject(externalFailure);
      },
    },
    windows,
  };
});

vi.mock("electron", () => ({
  BrowserWindow: electronMock.FakeBrowserWindow,
  dialog: electronMock.dialog,
  ipcMain: electronMock.ipcMain,
  Menu: electronMock.Menu,
  shell: electronMock.shell,
}));

class FakeBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<(event: HostEvent) => void>();

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `correlation-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  listenerCount(): number {
    return this.listeners.size;
  }

  subscribe(listener: (event: HostEvent) => void): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

class FakePreloadIpc implements PreloadIpcRenderer {
  readonly invocations: Array<{ channel: string; value: unknown }> = [];
  private readonly listeners = new Map<string, Set<UnknownListener>>();
  response: unknown;

  emit(channel: string, value: unknown): void {
    for (const listener of this.listeners.get(channel) ?? []) {
      listener({}, value);
    }
  }

  invoke(channel: string, value: unknown): Promise<unknown> {
    this.invocations.push({ channel, value });
    return Promise.resolve(this.response);
  }

  on(channel: string, listener: UnknownListener): void {
    const listeners = this.listeners.get(channel) ?? new Set();
    listeners.add(listener);
    this.listeners.set(channel, listeners);
  }

  removeListener(channel: string, listener: UnknownListener): void {
    this.listeners.get(channel)?.delete(listener);
  }
}

const disconnectCommand: HostCommand = {
  command: "connection.disconnect",
  id: "request-1",
  payload: {},
  version: HOST_PROTOCOL_VERSION,
};

const latencyStartCommand: Extract<HostCommand, { readonly command: "latency.start" }> = {
  command: "latency.start",
  id: "latency-start",
  payload: {
    acknowledgements: 1,
    messageCount: 5,
    timeoutMs: 10_000,
    topic: "orders.events",
  },
  version: HOST_PROTOCOL_VERSION,
};

function streamMonitorEvent(
  sequence: number,
): Extract<HostEvent, { readonly event: "streamMetrics.changed" }> {
  return {
    event: "streamMetrics.changed",
    payload: {
      connectionName: "Electron local aio",
      delivery: {
        batchCount: 1,
        batchSize: 200,
        deliveredMessages: 1,
        historySamples: 50,
        intervalMs: 20,
        lastBatchMessages: 1,
        messagesPerSecond: 10,
        publicationDurationMs: 0.5,
        queueWaitMs: 1,
        receivedMessages: 1,
        tuningSource: "confirmed",
      },
      queue: {
        capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
        capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
        currentBytes: 0,
        currentMessages: 0,
        droppedMessages: 0,
        droppedPerSecond: 0,
        droppedSincePrevious: 0,
        peakBytes: 64,
        peakMessages: 1,
      },
      request: {
        maxMessages: 10,
        mode: "earliest",
        topic: "orders.events",
      },
      sampledAt: "2026-07-26T12:00:00.000Z",
      state: "complete",
      status: "nominal",
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

beforeEach(() => {
  electronMock.externalUrls.splice(0);
  electronMock.handlers.clear();
  electronMock.menuTemplates.splice(0);
  electronMock.saveDialogCalls.splice(0);
  electronMock.setSaveDialogResult({ canceled: true });
  electronMock.setExternalFailure(undefined);
  electronMock.windows.splice(0);
});

describe("Electron main boundary", () => {
  it("creates a constrained desktop window and blocks external content", async () => {
    const backend = new FakeBackend();
    const rendererUrl = "http://127.0.0.1:5173/#host=http%3A%2F%2F127.0.0.1%3A4319&token=test";
    const shell = await createElectronShell({
      backend,
      preloadPath: "/tmp/streamskope/preload.js",
      rendererUrl,
    });
    const window = electronMock.windows[0];
    if (window === undefined) {
      throw new Error("Expected an Electron window.");
    }

    expect(window.options).toMatchObject({
      height: 900,
      minHeight: 650,
      minWidth: 1000,
      show: false,
      webPreferences: {
        contextIsolation: true,
        nodeIntegration: false,
        preload: "/tmp/streamskope/preload.js",
        sandbox: true,
        webSecurity: true,
      },
      width: 1440,
    });
    expect(window.loadedUrls).toEqual([rendererUrl]);
    expect(window.shown).toBe(false);
    window.emit("ready-to-show");
    expect(window.shown).toBe(true);
    expect(window.webContents.windowOpenHandler?.({})).toEqual({ action: "deny" });

    const externalNavigation = { preventDefault: vi.fn() };
    window.webContents.emit("will-navigate", externalNavigation, "https://example.invalid/escape");
    expect(externalNavigation.preventDefault).toHaveBeenCalledOnce();

    const sameOriginNavigation = { preventDefault: vi.fn() };
    window.webContents.emit("will-navigate", sameOriginNavigation, "http://127.0.0.1:5173/topics");
    expect(sameOriginNavigation.preventDefault).not.toHaveBeenCalled();

    const webview = { preventDefault: vi.fn() };
    window.webContents.emit("will-attach-webview", webview);
    expect(webview.preventDefault).toHaveBeenCalledOnce();

    shell.close();
  });

  it("loads one owned packaged origin without granting file or alternate-path navigation", async () => {
    const backend = new FakeBackend();
    const rendererUrl = "streamskope://app/";
    const shell = await createElectronShell({
      backend,
      preloadPath: "/opt/StreamSkope/resources/electron/preload.cjs",
      rendererUrl,
    });
    const window = electronMock.windows[0];
    if (window === undefined) {
      throw new Error("Expected an Electron window.");
    }

    expect(window.loadedUrls).toEqual([rendererUrl]);
    const sameDocument = { preventDefault: vi.fn() };
    window.webContents.emit("will-navigate", sameDocument, `${rendererUrl}#selected-topic=test`);
    expect(sameDocument.preventDefault).not.toHaveBeenCalled();

    const alternatePath = { preventDefault: vi.fn() };
    window.webContents.emit("will-navigate", alternatePath, "streamskope://app/other.html");
    expect(alternatePath.preventDefault).toHaveBeenCalledOnce();

    const localFile = { preventDefault: vi.fn() };
    window.webContents.emit(
      "will-navigate",
      localFile,
      "file:///opt/StreamSkope/resources/renderer/other.html",
    );
    expect(localFile.preventDefault).toHaveBeenCalledOnce();

    shell.close();
  });

  it("rejects undeclared IPC commands before backend execution and relays valid events", async () => {
    const backend = new FakeBackend();
    const shell = await createElectronShell({
      backend,
      preloadPath: "/tmp/streamskope/preload.js",
      rendererUrl: "http://127.0.0.1:5173/",
    });
    const handler = electronMock.handlers.get(HOST_COMMAND_CHANNEL);
    const window = electronMock.windows[0];
    if (handler === undefined || window === undefined) {
      throw new Error("Expected registered Electron host bindings.");
    }

    const undeclaredCommand = {
      command: "cluster.delete",
      id: "bad",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    };
    await expect(handler({ sender: window.webContents }, undeclaredCommand)).rejects.toBeInstanceOf(
      HostContractValidationError,
    );
    expect(backend.commands).toEqual([]);

    await expect(handler({ sender: {} }, latencyStartCommand)).rejects.toThrow(
      "must originate from the StreamSkope renderer",
    );
    await expect(handler({ sender: {} }, undeclaredCommand)).rejects.toThrow(
      "must originate from the StreamSkope renderer",
    );
    expect(backend.commands).toEqual([]);

    await expect(
      handler({ sender: window.webContents }, latencyStartCommand),
    ).resolves.toMatchObject({
      command: "latency.start",
      id: "latency-start",
      ok: true,
    });
    expect(backend.commands).toEqual([latencyStartCommand]);

    const event: HostEvent = {
      event: "latency.changed",
      payload: {
        evidence: null,
        request: latencyStartCommand.payload,
        state: "running",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    };
    backend.emit(event);
    expect(window.webContents.sent).toEqual([{ channel: HOST_EVENT_CHANNEL, value: event }]);
    const monitorEvent = streamMonitorEvent(2);
    backend.emit(monitorEvent);
    expect(window.webContents.sent).toHaveLength(1);
    const acknowledge = electronMock.handlers.get(HOST_EVENT_ACK_CHANNEL);
    if (acknowledge === undefined) throw new Error("Missing acknowledgement handler");
    await expect(async () => acknowledge({ sender: {} }, 1)).rejects.toThrow();
    await expect(async () => acknowledge({ sender: window.webContents }, "1")).rejects.toThrow();
    await acknowledge({ sender: window.webContents }, 1);
    expect(window.webContents.sent).toEqual([
      { channel: HOST_EVENT_CHANNEL, value: event },
      { channel: HOST_EVENT_CHANNEL, value: monitorEvent },
    ]);
    expect(backend.listenerCount()).toBe(1);
    const subscribe = electronMock.handlers.get(HOST_SUBSCRIBE_CHANNEL);
    await subscribe?.({ sender: window.webContents }, HOST_PROTOCOL_VERSION);
    await subscribe?.({ sender: window.webContents }, HOST_PROTOCOL_VERSION);
    expect(backend.listenerCount()).toBe(1);

    shell.close();
    expect(electronMock.handlers.has(HOST_COMMAND_CHANNEL)).toBe(false);
    expect(electronMock.handlers.has(EXTERNAL_URL_OPEN_CHANNEL)).toBe(false);
    expect(electronMock.handlers.has(HOST_EVENT_ACK_CHANNEL)).toBe(false);
    expect(backend.listenerCount()).toBe(0);
  });

  it("revalidates and awaits one exact HTTPS external action without renderer navigation", async () => {
    const backend = new FakeBackend();
    const rendererUrl = "http://127.0.0.1:5173/";
    const shell = await createElectronShell({
      backend,
      preloadPath: "/tmp/streamskope/preload.js",
      rendererUrl,
    });
    const handler = electronMock.handlers.get(EXTERNAL_URL_OPEN_CHANNEL);
    const window = electronMock.windows[0];
    if (handler === undefined || window === undefined) {
      throw new Error("Expected registered external URL binding.");
    }
    const request = {
      url: "https://runbooks.example.test/kafka/latency?cluster=local#recovery",
      version: HOST_PROTOCOL_VERSION,
    } as const;

    await expect(handler({ sender: window.webContents }, request)).resolves.toEqual({
      state: "accepted",
      version: HOST_PROTOCOL_VERSION,
    });
    expect(electronMock.externalUrls).toEqual([request.url]);
    expect(window.loadedUrls).toEqual([rendererUrl]);

    await expect(handler({ sender: {} }, request)).rejects.toThrow(
      "must originate from the StreamSkope renderer",
    );
    expect(electronMock.externalUrls).toEqual([request.url]);

    for (const value of [
      { url: "file:///tmp/private-runbook.html", version: HOST_PROTOCOL_VERSION },
      {
        url: "https://operator:private@runbooks.example.test/kafka",
        version: HOST_PROTOCOL_VERSION,
      },
      { command: "open /tmp/private", url: request.url, version: HOST_PROTOCOL_VERSION },
    ]) {
      await expect(handler({ sender: window.webContents }, value)).rejects.toBeInstanceOf(
        HostContractValidationError,
      );
    }
    expect(electronMock.externalUrls).toEqual([request.url]);

    electronMock.setExternalFailure(new Error("operating system rejected external browser"));
    await expect(handler({ sender: window.webContents }, request)).rejects.toThrow(
      "operating system rejected",
    );
    expect(electronMock.externalUrls).toEqual([request.url, request.url]);
    shell.close();
    expect(electronMock.handlers.has(EXTERNAL_URL_OPEN_CHANNEL)).toBe(false);
  });

  it("owns native menus and saves only an authorized validated document", async () => {
    const outputDirectory = await mkdtemp(join(tmpdir(), "streamskope-native-save-"));
    const outputPath = join(outputDirectory, "evidence.json");
    const backend = new FakeBackend();
    const shell = await createElectronShell({
      backend,
      preloadPath: "/tmp/streamskope/preload.js",
      rendererUrl: "http://127.0.0.1:5173/",
    });
    const handler = electronMock.handlers.get(DESKTOP_DOCUMENT_SAVE_CHANNEL);
    const window = electronMock.windows[0];
    if (handler === undefined || window === undefined) {
      throw new Error("Expected registered native desktop bindings.");
    }
    const document = {
      byteSize: 17,
      content: '{\n  "ok": true\n}\n',
      fileName: "evidence.json",
      mediaType: "application/json",
    } as const;

    try {
      const menu = electronMock.menuTemplates.at(-1) as
        | Array<{
            readonly label?: string;
            readonly submenu?: Array<{
              readonly click?: () => void;
              readonly label?: string;
              readonly role?: string;
            }>;
          }>
        | undefined;
      expect(menu?.map((item) => item.label)).toEqual(["File", "Edit", "View", "Window", "Help"]);
      const viewMenu = menu?.find((item) => item.label === "View");
      const activity = viewMenu?.submenu?.find((item) => item.label === "Activity");
      activity?.click?.();
      expect(window.webContents.sent.at(-1)).toEqual({
        channel: DESKTOP_ACTION_CHANNEL,
        value: {
          action: "activity.open",
          version: DESKTOP_PLATFORM_VERSION,
        },
      });

      electronMock.setSaveDialogResult({ canceled: false, filePath: outputPath });
      await expect(handler({ sender: window.webContents }, document)).resolves.toEqual({
        state: "saved",
        version: DESKTOP_PLATFORM_VERSION,
      });
      await expect(readFile(outputPath, "utf8")).resolves.toBe(document.content);
      expect(electronMock.saveDialogCalls).toHaveLength(1);

      await expect(handler({ sender: {} }, document)).rejects.toThrow(
        "must originate from the StreamSkope renderer",
      );
      await expect(
        handler({ sender: window.webContents }, { ...document, fileName: "../private.json" }),
      ).rejects.toBeInstanceOf(DesktopPlatformContractError);
      expect(electronMock.saveDialogCalls).toHaveLength(1);

      electronMock.setSaveDialogResult({ canceled: true });
      await expect(handler({ sender: window.webContents }, document)).resolves.toEqual({
        state: "cancelled",
        version: DESKTOP_PLATFORM_VERSION,
      });
      expect(electronMock.saveDialogCalls).toHaveLength(2);
    } finally {
      shell.close();
      await rm(outputDirectory, { force: true, recursive: true });
    }
    expect(electronMock.handlers.has(DESKTOP_DOCUMENT_SAVE_CHANNEL)).toBe(false);
  });

  it("preserves an existing IPC handler when its channel is occupied", async () => {
    const existingHandler: InvokeHandler = () => Promise.resolve("existing owner");
    electronMock.handlers.set(HOST_COMMAND_CHANNEL, existingHandler);
    const backend = new FakeBackend();

    await expect(
      createElectronShell({
        backend,
        preloadPath: "/tmp/streamskope/preload.js",
        rendererUrl: "http://127.0.0.1:5173/",
      }),
    ).rejects.toBeInstanceOf(ElectronShellStartupError);

    expect(electronMock.handlers.get(HOST_COMMAND_CHANNEL)).toBe(existingHandler);
    expect(backend.listenerCount()).toBe(0);
    expect(electronMock.windows[0]?.destroyed).toBe(true);
  });

  it("preserves an occupied external-action handler and removes only its own command handler", async () => {
    const existingHandler: InvokeHandler = () => Promise.resolve("existing owner");
    electronMock.handlers.set(EXTERNAL_URL_OPEN_CHANNEL, existingHandler);
    const backend = new FakeBackend();

    await expect(
      createElectronShell({
        backend,
        preloadPath: "/tmp/streamskope/preload.js",
        rendererUrl: "http://127.0.0.1:5173/",
      }),
    ).rejects.toBeInstanceOf(ElectronShellStartupError);

    expect(electronMock.handlers.get(EXTERNAL_URL_OPEN_CHANNEL)).toBe(existingHandler);
    expect(electronMock.handlers.has(HOST_COMMAND_CHANNEL)).toBe(false);
    expect(backend.listenerCount()).toBe(0);
    expect(electronMock.windows[0]?.destroyed).toBe(true);
  });
});

describe("Electron preload boundary", () => {
  it("delivers fresh facade availability after the renderer attaches and reattaches", async () => {
    const backend = createKafkaBackend();
    const shell = await createElectronShell({
      backend,
      preloadPath: "/tmp/streamskope/preload.js",
      rendererUrl: "http://127.0.0.1:5173/",
    });
    try {
      const window = electronMock.windows[0];
      if (window === undefined) throw new Error("Expected Electron window.");
      const subscribe = electronMock.handlers.get(HOST_SUBSCRIBE_CHANNEL);
      expect(subscribe).toBeDefined();
      if (subscribe === undefined) throw new Error("Missing renderer subscription handshake.");
      const beforeAttach = window.webContents.sent.length;
      await expect(async () => subscribe({ sender: {} }, HOST_PROTOCOL_VERSION)).rejects.toThrow();
      await expect(async () => subscribe({ sender: window.webContents }, -1)).rejects.toThrow();
      expect(window.webContents.sent).toHaveLength(beforeAttach);
      const sequences: number[] = [];
      for (let attachment = 0; attachment < 2; attachment += 1) {
        const before = window.webContents.sent.length;
        await subscribe({ sender: window.webContents }, HOST_PROTOCOL_VERSION);
        expect(window.webContents.sent).toHaveLength(before + 1);
        const event = window.webContents.sent.at(-1)?.value as HostEvent;
        expect(event).toMatchObject({ event: "backend.availability", payload: { state: "ready" } });
        sequences.push(event.sequence);
      }
      expect(sequences[1]).toBeGreaterThan(sequences[0] ?? -1);
      await backend.shutdown();
      await subscribe({ sender: window.webContents }, HOST_PROTOCOL_VERSION);
      expect(window.webContents.sent.at(-1)?.value).toMatchObject({
        event: "backend.availability",
        payload: { state: "unavailable" },
      });
    } finally {
      shell.close();
      await backend.shutdown();
    }
    expect(electronMock.handlers.has(HOST_SUBSCRIBE_CHANNEL)).toBe(false);
  });

  it("stops consumption and reports delivery overload without silently growing IPC", async () => {
    const backend = new FakeBackend();
    const shell = await createElectronShell({
      backend,
      preloadPath: "/tmp/streamskope/preload.js",
      rendererUrl: "http://127.0.0.1:5173/",
    });
    try {
      for (let sequence = 1; sequence <= 100; sequence += 1) {
        backend.emit({
          event: "backend.availability",
          payload: { state: "ready" },
          sequence,
          version: HOST_PROTOCOL_VERSION,
        });
      }
      await Promise.resolve();
      expect(backend.commands).toHaveLength(1);
      expect(backend.commands[0]?.command).toBe("messages.stop");
      expect(backend.listenerCount()).toBe(0);
      expect(electronMock.windows[0]?.webContents.sent).toHaveLength(2);
      expect(electronMock.windows[0]?.webContents.sent.at(-1)?.value).toMatchObject({
        event: "backend.availability",
        payload: { state: "unavailable" },
      });
      expect(JSON.stringify(electronMock.windows[0]?.webContents.sent.at(-1)?.value)).toContain(
        "Consumption stopped",
      );
    } finally {
      shell.close();
    }
  });

  it("attaches the event listener before requesting host availability", async () => {
    const ipc = new FakePreloadIpc();
    const ready: HostEvent = {
      event: "backend.availability",
      payload: { state: "ready" },
      sequence: 0,
      version: HOST_PROTOCOL_VERSION,
    };
    vi.spyOn(ipc, "invoke").mockImplementation((channel, version) => {
      if (channel === HOST_EVENT_ACK_CHANNEL) {
        expect(version).toBe(ready.sequence);
        return Promise.resolve(undefined);
      }
      expect(channel).toBe(HOST_SUBSCRIBE_CHANNEL);
      expect(version).toBe(HOST_PROTOCOL_VERSION);
      ipc.emit(HOST_EVENT_CHANNEL, ready);
      return Promise.resolve(HOST_PROTOCOL_VERSION);
    });
    const listener = vi.fn();
    const unsubscribe = createStreamSkopePreloadHost(ipc).subscribe(listener);
    await Promise.resolve();
    expect(listener).toHaveBeenCalledExactlyOnceWith(ready);
    unsubscribe();
    ipc.emit(HOST_EVENT_CHANNEL, ready);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it.each([
    { disposed: false, malformed: false },
    { disposed: true, malformed: false },
    { disposed: false, malformed: true },
  ])(
    "handles handshake failure (disposed: $disposed, malformed: $malformed)",
    async ({ disposed, malformed }) => {
      const ipc = new FakePreloadIpc();
      const invoke = vi.spyOn(ipc, "invoke");
      if (malformed) invoke.mockResolvedValue(-1);
      else invoke.mockRejectedValue(new Error("Host unavailable"));
      const listener = vi.fn();
      const unsubscribe = createStreamSkopePreloadHost(ipc).subscribe(listener);
      if (disposed) unsubscribe();
      await new Promise<void>((resolve) => setImmediate(resolve));
      await vi.waitFor(() => {
        if (disposed) expect(listener).not.toHaveBeenCalled();
        else {
          expect(listener).toHaveBeenCalledTimes(1);
          expect(listener.mock.calls[0]?.[0]).toMatchObject({
            event: "backend.availability",
            payload: { state: "unavailable" },
          });
        }
      });
      unsubscribe();
    },
  );

  it("exposes only the typed host operations and validates both directions", async () => {
    const ipc = new FakePreloadIpc();
    const exposed = new Map<string, unknown>();
    exposeStreamSkopeHost(
      {
        exposeInMainWorld(name, value): void {
          exposed.set(name, value);
        },
      },
      ipc,
    );
    const host = exposed.get("streamSkopeHost") as StreamSkopeHost | undefined;
    if (host === undefined) {
      throw new Error("Expected the StreamSkope preload host.");
    }

    expect(Object.keys(host).sort()).toEqual(["execute", "openExternalUrl", "subscribe"]);
    await expect(
      host.execute({
        command: "cluster.delete",
        id: "bad",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      } as unknown as HostCommand),
    ).rejects.toBeInstanceOf(HostContractValidationError);
    expect(ipc.invocations).toEqual([]);

    ipc.response = {
      command: "connection.disconnect",
      id: "request-1",
      ok: true,
      result: { correlationId: "correlation-request-1" },
      version: HOST_PROTOCOL_VERSION,
    };
    await expect(host.execute(disconnectCommand)).resolves.toEqual(ipc.response);
    expect(ipc.invocations).toEqual([{ channel: HOST_COMMAND_CHANNEL, value: disconnectCommand }]);

    const runbookUrl = "https://runbooks.example.test/kafka/latency?cluster=local#recovery";
    ipc.response = {
      state: "accepted",
      version: HOST_PROTOCOL_VERSION,
    };
    await expect(host.openExternalUrl(runbookUrl)).resolves.toEqual(ipc.response);
    expect(ipc.invocations.at(-1)).toEqual({
      channel: EXTERNAL_URL_OPEN_CHANNEL,
      value: { url: runbookUrl, version: HOST_PROTOCOL_VERSION },
    });
    const invocationCount = ipc.invocations.length;
    await expect(host.openExternalUrl("file:///tmp/private-runbook.html")).rejects.toBeInstanceOf(
      HostContractValidationError,
    );
    expect(ipc.invocations).toHaveLength(invocationCount);
    ipc.response = { state: "opened", version: HOST_PROTOCOL_VERSION };
    await expect(host.openExternalUrl(runbookUrl)).rejects.toBeInstanceOf(
      HostContractValidationError,
    );

    const received: HostEvent[] = [];
    const unsubscribe = host.subscribe((event) => {
      received.push(event);
    });
    const event: HostEvent = {
      event: "backend.availability",
      payload: { recovery: "Restart StreamSkope.", state: "unavailable" },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };
    ipc.emit(HOST_EVENT_CHANNEL, event);
    expect(received).toEqual([event]);
    const monitorEvent = streamMonitorEvent(3);
    ipc.emit(HOST_EVENT_CHANNEL, monitorEvent);
    expect(received).toEqual([event, monitorEvent]);
    expect(JSON.stringify(received)).not.toMatch(
      /clientSecret|oauth-secret|message-key|message-payload/u,
    );

    expect(() =>
      ipc.emit(HOST_EVENT_CHANNEL, {
        event: "process.execute",
        payload: {},
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
    unsubscribe();
    ipc.emit(HOST_EVENT_CHANNEL, event);
    expect(received).toEqual([event, monitorEvent]);
  });
});
