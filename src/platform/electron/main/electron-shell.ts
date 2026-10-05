import { writeFile } from "node:fs/promises";
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
  type StreamSkopeBackend,
} from "../../../features/kafka/contracts";
import {
  DESKTOP_PLATFORM_VERSION,
  parseDesktopTextDocument,
  type DesktopActionName,
} from "../../desktop";
import {
  DESKTOP_ACTION_CHANNEL,
  DESKTOP_DOCUMENT_SAVE_CHANNEL,
  EXTERNAL_URL_OPEN_CHANNEL,
} from "../preload/channels";
import { ProviderHostRegistry } from "../../node/provider-host";
import { createKafkaProviderEndpoint } from "../../node/kafka-provider";

import { PACKAGED_RENDERER_HOST, PACKAGED_RENDERER_SCHEME } from "./packaged-renderer-origin";
import { attachElectronProviders } from "./electron-provider-routes";
import { createKafkaElectronDeliveryBinding } from "./kafka-provider-delivery";
import type { ElectronProviderDeliveryBinding } from "./provider-delivery";

interface ElectronShellBaseOptions {
  readonly preloadPath: string;
  readonly rendererUrl: string;
}

type KafkaDesktopBackend = StreamSkopeBackend & {
  /** Host-only flow control; does not pause Kafka processing. */
  setMessagePresentationPaused?(paused: boolean): void;
};

export type ElectronShellOptions = ElectronShellBaseOptions &
  (
    | {
        readonly registry: ProviderHostRegistry;
        readonly deliveryBindings?: readonly ElectronProviderDeliveryBinding[];
        readonly backend?: never;
      }
    | {
        readonly backend: KafkaDesktopBackend;
        readonly registry?: never;
        readonly deliveryBindings?: never;
      }
  );

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
  let desktopDocumentHandlerRegistered = false;
  let externalUrlHandlerRegistered = false;
  let detachProviders: (() => void) | undefined;
  const cleanup = (): void => {
    if (cleaned) {
      return;
    }
    cleaned = true;
    if (desktopDocumentHandlerRegistered) {
      ipcMain.removeHandler(DESKTOP_DOCUMENT_SAVE_CHANNEL);
    }
    if (externalUrlHandlerRegistered) {
      ipcMain.removeHandler(EXTERNAL_URL_OPEN_CHANNEL);
    }
    detachProviders?.();
  };
  window.once("closed", cleanup);
  window.once("ready-to-show", () => {
    if (!window.isDestroyed()) {
      window.show();
    }
  });

  try {
    Menu.setApplicationMenu(Menu.buildFromTemplate(createStreamSkopeMenuTemplate(window)));
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
    const registry =
      options.registry ?? new ProviderHostRegistry([createKafkaProviderEndpoint(options.backend)]);
    const deliveryBindings =
      options.registry === undefined
        ? [createKafkaElectronDeliveryBinding(options.backend)]
        : (options.deliveryBindings ?? []);
    detachProviders = attachElectronProviders(window, registry, deliveryBindings);
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
