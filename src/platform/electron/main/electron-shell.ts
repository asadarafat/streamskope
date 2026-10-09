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
  parseArtifactReference,
  type DesktopSaveResult,
  type DesktopActionName,
} from "../../desktop";
import {
  DESKTOP_ACTION_CHANNEL,
  DESKTOP_DOCUMENT_SAVE_CHANNEL,
  DESKTOP_ARTIFACT_SAVE_CHANNEL,
  EXTERNAL_URL_OPEN_CHANNEL,
} from "../preload/channels";
import { ProviderHostRegistry } from "../../node/provider-host";
import { createKafkaProviderEndpoint } from "../../node/kafka-provider";
import type { RecordExportDelivery } from "../../node/record-export-artifacts";
import { NodeRecordExportSaver } from "../../node/record-export-save";

import { PACKAGED_RENDERER_HOST, PACKAGED_RENDERER_SCHEME } from "./packaged-renderer-origin";
import { attachElectronProviders } from "./electron-provider-routes";
import { createKafkaElectronDeliveryBinding } from "./kafka-provider-delivery";
import type { ElectronProviderDeliveryBinding } from "./provider-delivery";

interface ElectronShellBaseOptions {
  readonly exportFiles?: RecordExportDelivery;
  readonly preloadPath: string;
  readonly rendererUrl: string;
}

type KafkaDesktopBackend = StreamSkopeBackend & {
  /** Host-only flow control; does not pause Kafka processing. */
  setMessagePresentationPaused?(paused: boolean): void;
  /** Host-owned stream cleanup; no renderer command is fabricated during teardown. */
  stopStream?(): Promise<void>;
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
  close(): Promise<void>;
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
  let desktopArtifactHandlerRegistered = false;
  const artifactAuthorization = new AbortController();
  const artifactSaves = new Set<Promise<unknown>>();
  const artifactSaver =
    options.exportFiles === undefined ? undefined : new NodeRecordExportSaver(options.exportFiles);
  let externalUrlHandlerRegistered = false;
  let detachProviders: (() => Promise<void>) | undefined;
  let cleanupPromise: Promise<void> | undefined;
  let closePromise: Promise<void> | undefined;
  const cleanup = (): Promise<void> => {
    if (cleanupPromise !== undefined) return cleanupPromise;
    cleaned = true;
    artifactAuthorization.abort();
    let complete = (): void => undefined;
    let reject = (_error: unknown): void => undefined;
    cleanupPromise = new Promise<void>((resolve, fail) => {
      complete = resolve;
      reject = fail;
    });
    const failures: unknown[] = [];
    for (const [registered, channel] of [
      [desktopDocumentHandlerRegistered, DESKTOP_DOCUMENT_SAVE_CHANNEL],
      [desktopArtifactHandlerRegistered, DESKTOP_ARTIFACT_SAVE_CHANNEL],
      [externalUrlHandlerRegistered, EXTERNAL_URL_OPEN_CHANNEL],
    ] as const) {
      if (!registered) continue;
      try {
        ipcMain.removeHandler(channel);
      } catch (cause) {
        failures.push(new Error("Desktop shell handler cleanup failed.", { cause }));
      }
    }
    let detached: Promise<void>;
    try {
      detached = detachProviders?.() ?? Promise.resolve();
    } catch (cause) {
      detached = Promise.reject(new Error("Desktop provider detach failed.", { cause }));
    }
    const finishSaves = async (): Promise<void> => {
      await Promise.allSettled([...artifactSaves]);
      await artifactSaver?.drain();
    };
    void Promise.allSettled([detached, finishSaves()]).then((results) => {
      for (const result of results)
        if (result.status === "rejected") failures.push(result.reason as unknown);
      if (failures.length > 0) {
        reject(new AggregateError(failures, "Desktop shell cleanup failed."));
      } else complete();
    });
    return cleanupPromise;
  };
  const close = (): Promise<void> => {
    if (closePromise !== undefined) return closePromise;
    let complete = (): void => undefined;
    let reject = (_error: unknown): void => undefined;
    closePromise = new Promise<void>((resolve, fail) => {
      complete = resolve;
      reject = fail;
    });
    const cleaning = cleanup();
    let windowFailure: unknown;
    let windowFailed = false;
    try {
      if (!window.isDestroyed()) window.close();
    } catch (cause) {
      windowFailed = true;
      windowFailure = cause;
    }
    void Promise.allSettled([cleaning]).then(([result]) => {
      const failures: unknown[] = windowFailed ? [windowFailure] : [];
      if (result?.status === "rejected") failures.push(result.reason as unknown);
      if (failures.length > 0) {
        reject(new AggregateError(failures, "Desktop shell cleanup failed."));
      } else complete();
    });
    return closePromise;
  };
  window.once("closed", () => {
    void cleanup().catch(() => {
      // EventEmitter does not observe Promise rejection; explicit close retains this failure.
      process.stderr.write("StreamSkope desktop window cleanup failed.\n");
    });
  });
  window.once("ready-to-show", () => {
    if (!window.isDestroyed()) {
      window.show();
    }
  });

  try {
    Menu.setApplicationMenu(Menu.buildFromTemplate(createStreamSkopeMenuTemplate(window)));
    const registry =
      options.registry ?? new ProviderHostRegistry([createKafkaProviderEndpoint(options.backend)]);
    const deliveryBindings =
      options.registry === undefined
        ? [createKafkaElectronDeliveryBinding(options.backend)]
        : (options.deliveryBindings ?? []);
    detachProviders = await attachElectronProviders(window, registry, deliveryBindings);
    if (cleaned) {
      await detachProviders();
      throw new Error("Desktop window closed during startup.");
    }
    ipcMain.handle(DESKTOP_DOCUMENT_SAVE_CHANNEL, async (event, value) => {
      assertOwnedRendererSender(event, window.webContents);
      if (cleaned) throw new Error("Desktop shell is closing.");
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
    ipcMain.handle(DESKTOP_ARTIFACT_SAVE_CHANNEL, (event, value) => {
      assertOwnedRendererSender(event, window.webContents);
      const reference = parseArtifactReference(value);
      const delivery = options.exportFiles;
      const assertCurrent = (): void => {
        artifactAuthorization.signal.throwIfAborted();
        if (cleaned || delivery === undefined) throw new Error("Desktop export is unavailable.");
      };
      assertCurrent();
      const operation = (async (): Promise<DesktopSaveResult> => {
        const metadata = delivery!.describe(reference);
        const selected = await dialog.showSaveDialog(window, {
          defaultPath: metadata.fileName,
          filters: [
            {
              extensions: [
                reference.part === "receipt"
                  ? "json"
                  : metadata.fileName.endsWith(".csv")
                    ? "csv"
                    : "jsonl",
              ],
              name: "Export",
            },
          ],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (selected.canceled || selected.filePath === undefined)
          return { state: "cancelled", version: DESKTOP_PLATFORM_VERSION };
        assertCurrent();
        await artifactSaver!.save(reference, selected.filePath, {
          signal: artifactAuthorization.signal,
          assertCurrent,
        });
        return { state: "saved", version: DESKTOP_PLATFORM_VERSION };
      })();
      artifactSaves.add(operation);
      void operation.then(
        () => artifactSaves.delete(operation),
        () => artifactSaves.delete(operation),
      );
      return operation;
    });
    desktopArtifactHandlerRegistered = true;
    ipcMain.handle(EXTERNAL_URL_OPEN_CHANNEL, async (event, value) => {
      assertOwnedRendererSender(event, window.webContents);
      if (cleaned) throw new Error("Desktop shell is closing.");
      const request = parseExternalUrlOpenRequest(value);
      await shell.openExternal(request.url);
      return {
        state: "accepted",
        version: HOST_PROTOCOL_VERSION,
      };
    });
    externalUrlHandlerRegistered = true;
    await window.loadURL(options.rendererUrl);
  } catch (error) {
    try {
      await close();
    } catch (cleanupCause) {
      throw new ElectronShellStartupError("Electron shell startup cleanup failed.", {
        cause: new AggregateError([error, cleanupCause], "Desktop shell startup failed."),
      });
    }
    throw new ElectronShellStartupError("Electron shell could not start.", {
      cause: error,
    });
  }

  return {
    close,
    window,
  };
}
