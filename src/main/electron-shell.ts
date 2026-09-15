import { writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { isAbsolute } from "node:path";

import {
  BrowserWindow,
  dialog,
  ipcMain,
  Menu,
  shell,
  type MenuItemConstructorOptions,
} from "electron";

import {
  HOST_PROTOCOL_VERSION,
  parseExternalUrlOpenRequest,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostEvent,
  type StreamSkopeBackend,
} from "../kafka/contracts";
import {
  DESKTOP_PLATFORM_VERSION,
  parseDesktopTextDocument,
  type DesktopActionName,
} from "../platform/desktop";
import {
  DESKTOP_ACTION_CHANNEL,
  DESKTOP_DOCUMENT_SAVE_CHANNEL,
  EXTERNAL_URL_OPEN_CHANNEL,
  HOST_COMMAND_CHANNEL,
  HOST_EVENT_CHANNEL,
  HOST_EVENT_ACK_CHANNEL,
  HOST_SUBSCRIBE_CHANNEL,
} from "../preload/channels";

import { PACKAGED_RENDERER_HOST, PACKAGED_RENDERER_SCHEME } from "./packaged-renderer-origin";
import { ElectronEventDelivery } from "./electron-event-delivery";

export interface ElectronShellOptions {
  readonly backend: StreamSkopeBackend & {
    /** Host-only flow control; does not pause Kafka processing. */
    setMessagePresentationPaused?(paused: boolean): void;
  };
  readonly preloadPath: string;
  readonly rendererUrl: string;
}

export interface RunningElectronShell {
  readonly window: BrowserWindow;
  close(): void;
}

export class ElectronShellStartupError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ElectronShellStartupError";
  }
}

function validatePreloadPath(preloadPath: string): void {
  if (!isAbsolute(preloadPath)) {
    throw new ElectronShellStartupError("Electron preload path must be absolute.");
  }
}

interface RendererNavigationPolicy {
  allows(targetUrl: string): boolean;
}

function assertOwnedRendererSender(event: { readonly sender: unknown }, renderer: unknown): void {
  if (event.sender !== renderer) {
    throw new Error("Privileged requests must originate from the StreamSkope renderer.");
  }
}

function rendererNavigationPolicy(rendererUrl: string): RendererNavigationPolicy {
  let parsed: URL;
  try {
    parsed = new URL(rendererUrl);
  } catch (error) {
    throw new ElectronShellStartupError("Electron renderer URL must be absolute.", {
      cause: error,
    });
  }
  if (parsed.protocol === "http:" && parsed.hostname === "127.0.0.1") {
    return {
      allows: (targetUrl): boolean => {
        try {
          return new URL(targetUrl).origin === parsed.origin;
        } catch {
          return false;
        }
      },
    };
  }
  if (
    parsed.protocol === `${PACKAGED_RENDERER_SCHEME}:` &&
    parsed.hostname === PACKAGED_RENDERER_HOST &&
    parsed.pathname === "/" &&
    parsed.search.length === 0
  ) {
    return {
      allows: (targetUrl): boolean => {
        try {
          const target = new URL(targetUrl);
          return (
            target.protocol === parsed.protocol &&
            target.hostname === parsed.hostname &&
            target.pathname === "/" &&
            target.search.length === 0
          );
        } catch {
          return false;
        }
      },
    };
  }
  throw new ElectronShellStartupError(
    "Electron renderer must use loopback HTTP or the owned StreamSkope origin.",
  );
}

function desktopAction(
  window: BrowserWindow,
  action: DesktopActionName,
): MenuItemConstructorOptions {
  const label = action === "activity.open" ? "Activity" : "Preferences";
  return {
    accelerator: action === "activity.open" ? "CommandOrControl+Shift+A" : "CommandOrControl+,",
    click: (): void => {
      if (!window.isDestroyed()) {
        window.webContents.send(DESKTOP_ACTION_CHANNEL, {
          action,
          version: DESKTOP_PLATFORM_VERSION,
        });
      }
    },
    label,
  };
}

export function createStreamSkopeMenuTemplate(
  window: BrowserWindow,
  platform: NodeJS.Platform = process.platform,
): MenuItemConstructorOptions[] {
  const template: MenuItemConstructorOptions[] = [
    {
      label: "File",
      submenu:
        platform === "darwin"
          ? [{ role: "close" }]
          : [desktopAction(window, "preferences.open"), { type: "separator" }, { role: "quit" }],
    },
    {
      label: "Edit",
      submenu: [
        { role: "undo" },
        { role: "redo" },
        { type: "separator" },
        { role: "cut" },
        { role: "copy" },
        { role: "paste" },
        { role: "selectAll" },
      ],
    },
    {
      label: "View",
      submenu: [
        desktopAction(window, "activity.open"),
        { type: "separator" },
        { role: "resetZoom" },
        { role: "zoomIn" },
        { role: "zoomOut" },
        { type: "separator" },
        { role: "togglefullscreen" },
      ],
    },
    {
      label: "Window",
      submenu: [{ role: "minimize" }, { role: "zoom" }, { role: "close" }],
    },
    {
      label: "Help",
      submenu: [{ role: "about" }],
    },
  ];
  return platform === "darwin"
    ? [
        {
          label: "StreamSkope",
          submenu: [
            { role: "about" },
            { type: "separator" },
            desktopAction(window, "preferences.open"),
            { type: "separator" },
            { role: "services" },
            { type: "separator" },
            { role: "hide" },
            { role: "hideOthers" },
            { role: "unhide" },
            { type: "separator" },
            { role: "quit" },
          ],
        },
        ...template,
      ]
    : template;
}

export async function createElectronShell(
  options: ElectronShellOptions,
): Promise<RunningElectronShell> {
  validatePreloadPath(options.preloadPath);
  const navigationPolicy = rendererNavigationPolicy(options.rendererUrl);
  const window = new BrowserWindow({
    height: 900,
    minHeight: 650,
    minWidth: 1000,
    show: false,
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      preload: options.preloadPath,
      sandbox: true,
      webSecurity: true,
    },
    width: 1440,
  });

  window.webContents.setWindowOpenHandler(() => ({ action: "deny" }));
  window.webContents.on("will-attach-webview", (event) => {
    event.preventDefault();
  });
  window.webContents.on("will-navigate", (event, targetUrl) => {
    if (!navigationPolicy.allows(targetUrl)) {
      event.preventDefault();
    }
  });

  let cleaned = false;
  let commandHandlerRegistered = false;
  let desktopDocumentHandlerRegistered = false;
  let externalUrlHandlerRegistered = false;
  let subscribeHandlerRegistered = false;
  let acknowledgeHandlerRegistered = false;
  let delivery: ElectronEventDelivery | undefined;
  let lastEventSequence = 0;
  let unsubscribe: (() => void) | undefined;
  const cleanup = (): void => {
    if (cleaned) {
      return;
    }
    cleaned = true;
    if (commandHandlerRegistered) {
      ipcMain.removeHandler(HOST_COMMAND_CHANNEL);
    }
    if (desktopDocumentHandlerRegistered) {
      ipcMain.removeHandler(DESKTOP_DOCUMENT_SAVE_CHANNEL);
    }
    if (externalUrlHandlerRegistered) {
      ipcMain.removeHandler(EXTERNAL_URL_OPEN_CHANNEL);
    }
    if (subscribeHandlerRegistered) {
      ipcMain.removeHandler(HOST_SUBSCRIBE_CHANNEL);
    }
    unsubscribe?.();
    delivery?.close();
    if (acknowledgeHandlerRegistered) ipcMain.removeHandler(HOST_EVENT_ACK_CHANNEL);
  };
  window.once("closed", cleanup);
  window.once("ready-to-show", () => {
    if (!window.isDestroyed()) {
      window.show();
    }
  });

  try {
    Menu.setApplicationMenu(Menu.buildFromTemplate(createStreamSkopeMenuTemplate(window)));
    ipcMain.handle(HOST_COMMAND_CHANNEL, async (event, value) => {
      assertOwnedRendererSender(event, window.webContents);
      const command = parseHostCommand(value);
      const response = await options.backend.execute(command);
      return parseCorrelatedHostResponse(response, command);
    });
    commandHandlerRegistered = true;
    ipcMain.handle(DESKTOP_DOCUMENT_SAVE_CHANNEL, async (event, value) => {
      assertOwnedRendererSender(event, window.webContents);
      const document = parseDesktopTextDocument(value);
      const selected = await dialog.showSaveDialog(window, {
        defaultPath: document.fileName,
        filters: [{ extensions: ["json"], name: "JSON" }],
        properties: ["createDirectory", "showOverwriteConfirmation"],
      });
      if (selected.canceled || selected.filePath === undefined) {
        return {
          state: "cancelled",
          version: DESKTOP_PLATFORM_VERSION,
        };
      }
      if (!isAbsolute(selected.filePath)) {
        throw new Error("Native Save returned a non-absolute path.");
      }
      await writeFile(selected.filePath, document.content, {
        encoding: "utf8",
        mode: 0o600,
      });
      return {
        state: "saved",
        version: DESKTOP_PLATFORM_VERSION,
      };
    });
    desktopDocumentHandlerRegistered = true;
    ipcMain.handle(EXTERNAL_URL_OPEN_CHANNEL, async (event, value) => {
      assertOwnedRendererSender(event, window.webContents);
      const request = parseExternalUrlOpenRequest(value);
      await shell.openExternal(request.url);
      return {
        state: "accepted",
        version: HOST_PROTOCOL_VERSION,
      };
    });
    externalUrlHandlerRegistered = true;
    const subscribeToBackend = (): void => {
      unsubscribe?.();
      delivery?.close();
      let failed = false;
      const currentDelivery = new ElectronEventDelivery(
        (event) => {
          if (!window.isDestroyed()) window.webContents.send(HOST_EVENT_CHANNEL, event);
        },
        (reason) => {
          failed = true;
          unsubscribe?.();
          const report = (stopped: boolean): void => {
            if (cleaned || window.isDestroyed() || delivery !== currentDelivery) return;
            window.webContents.send(HOST_EVENT_CHANNEL, {
              event: "backend.availability",
              payload: {
                state: "unavailable",
                recovery: stopped
                  ? `Renderer delivery failed (${reason}). Consumption stopped; records may be missing from this view. Reload the workbench and restart consumption.`
                  : "Renderer delivery failed and consumption stop could not be confirmed. Restart StreamSkope before consuming again.",
              },
              sequence: ++lastEventSequence,
              version: HOST_PROTOCOL_VERSION,
            });
          };
          void options.backend
            .execute({
              command: "messages.stop",
              id: randomUUID(),
              payload: {},
              version: HOST_PROTOCOL_VERSION,
            })
            .then(
              (response) => report(response.ok),
              () => report(false),
            );
        },
        (paused) => options.backend.setMessagePresentationPaused?.(paused),
      );
      delivery = currentDelivery;
      unsubscribe = options.backend.subscribe((value) => {
        const event = parseHostEvent(value);
        lastEventSequence = Math.max(lastEventSequence, event.sequence);
        delivery?.enqueue(event);
      });
      if (failed) unsubscribe();
    };
    ipcMain.handle(HOST_EVENT_ACK_CHANNEL, (event, sequence: unknown) => {
      assertOwnedRendererSender(event, window.webContents);
      if (typeof sequence !== "number" || !Number.isSafeInteger(sequence) || sequence < 0) {
        throw new Error("Invalid desktop event acknowledgement.");
      }
      delivery?.acknowledge(sequence);
    });
    acknowledgeHandlerRegistered = true;
    ipcMain.handle(HOST_SUBSCRIBE_CHANNEL, (event, version) => {
      assertOwnedRendererSender(event, window.webContents);
      if (version !== HOST_PROTOCOL_VERSION) {
        throw new Error("Unsupported desktop host subscription version.");
      }
      subscribeToBackend();
      return HOST_PROTOCOL_VERSION;
    });
    subscribeHandlerRegistered = true;
    subscribeToBackend();
    await window.loadURL(options.rendererUrl);
  } catch (error) {
    cleanup();
    if (!window.isDestroyed()) {
      window.close();
    }
    throw new ElectronShellStartupError("Electron shell could not start.", {
      cause: error,
    });
  }

  return {
    close: (): void => {
      if (!window.isDestroyed()) {
        window.close();
      } else {
        cleanup();
      }
    },
    window,
  };
}
