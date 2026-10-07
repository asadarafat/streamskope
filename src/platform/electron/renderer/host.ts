import {
  HOST_PROTOCOL_VERSION,
  KAFKA_PROVIDER_EVENT_CODEC,
  parseCorrelatedHostResponse,
  parseExternalUrlOpenRequest,
  parseHostCommand,
  type ExternalUrlOpenRequest,
  type ExternalUrlOpenResult,
  type HostCommandResponse,
  type HostCommand,
  type StreamSkopeHost,
} from "../../../features/kafka/contracts";
import type { NatsHost } from "../../../features/nats/contracts";

import {
  BrowserDevelopmentHostError,
  createBrowserProviderTransport,
} from "./browser-provider-transport";

export { BrowserDevelopmentHostError };

declare global {
  interface Window {
    streamSkopeHost?: StreamSkopeHost;
    /** Native preload exposes only named, typed built-in provider ports. */
    streamSkopeProviders?: Readonly<{ kafka: StreamSkopeHost; nats?: NatsHost }>;
    /** Set only by the authenticated production browser host. */
    streamSkopeBrowserRuntime?: Readonly<{
      pluginFileUpload: boolean;
      lockVault?: () => Promise<void>;
    }>;
  }
}

type BrowserDevelopmentWindow = Pick<Window, "open"> &
  Partial<Pick<Window, "location" | "document" | "streamSkopeBrowserRuntime">>;

const MAX_BROWSER_PLUGIN_ARCHIVE_BYTES = 48 * 1024 * 1024;

function chooseBrowserPluginFile(
  browserDocument: Document,
  signal: AbortSignal,
): Promise<File | null> {
  return new Promise<File | null>((resolve) => {
    const input = browserDocument.createElement("input");
    input.type = "file";
    input.accept = ".skope-plugin";
    input.hidden = true;
    const finish = (file: File | null): void => {
      signal.removeEventListener("abort", abort);
      input.remove();
      resolve(file);
    };
    const abort = (): void => finish(null);
    signal.addEventListener("abort", abort, { once: true });
    input.addEventListener("change", () => finish(input.files?.[0] ?? null), { once: true });
    input.addEventListener("cancel", () => finish(null), { once: true });
    browserDocument.body.append(input);
    input.click();
  });
}

function pluginFileUrl(browserWindow: BrowserDevelopmentWindow, commandId: string): string {
  return `${browserWindow.location!.origin}/__streamskope_session/plugin-file/${encodeURIComponent(commandId)}`;
}

async function stageBrowserPluginFile(
  browserWindow: BrowserDevelopmentWindow,
  commandId: string,
  signal: AbortSignal,
): Promise<boolean> {
  const browserDocument = browserWindow.document;
  if (browserDocument === undefined) {
    throw new BrowserDevelopmentHostError("Browser file selection is unavailable.");
  }
  const file = await chooseBrowserPluginFile(browserDocument, signal);
  if (file === null || signal.aborted) return false;
  if (file.size === 0 || file.size > MAX_BROWSER_PLUGIN_ARCHIVE_BYTES) {
    throw new BrowserDevelopmentHostError("Choose a signed plugin file between 1 byte and 48 MiB.");
  }
  try {
    const response = await fetch(pluginFileUrl(browserWindow, commandId), {
      body: file,
      cache: "no-store",
      credentials: "same-origin",
      headers: { "content-type": "application/octet-stream" },
      method: "POST",
      mode: "same-origin",
      signal,
    });
    if (!response.ok) {
      throw new BrowserDevelopmentHostError(
        `StreamSkope host rejected the plugin file with HTTP ${response.status}.`,
      );
    }
    if (signal.aborted) {
      await discardBrowserPluginFile(browserWindow, commandId);
      return false;
    }
    return true;
  } catch (error) {
    await discardBrowserPluginFile(browserWindow, commandId);
    if (signal.aborted) return false;
    throw error;
  }
}

async function discardBrowserPluginFile(
  browserWindow: BrowserDevelopmentWindow,
  commandId: string,
): Promise<void> {
  try {
    await fetch(pluginFileUrl(browserWindow, commandId), {
      cache: "no-store",
      credentials: "same-origin",
      method: "DELETE",
      mode: "same-origin",
    });
  } catch {
    // The host expires staged bytes even if a lost session cannot send cleanup.
  }
}

export function createBrowserDevelopmentHost(
  browserWindow: BrowserDevelopmentWindow = window,
): StreamSkopeHost {
  const transport = createBrowserProviderTransport(browserWindow, {
    codec: KAFKA_PROVIDER_EVENT_CODEC,
  });
  const selections = new Map<string, AbortController>();
  return {
    execute: async <Command extends HostCommand>(
      value: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      const submitted = { ...value };
      const command = parseHostCommand(submitted);
      if (command.command === "plugins.network.cancel") {
        selections.get(command.payload.requestId)?.abort();
      }
      const fileInspection =
        browserWindow.streamSkopeBrowserRuntime?.pluginFileUpload === true &&
        command.command === "plugins.package.inspect" &&
        command.payload.source === "file";
      if (fileInspection && selections.has(command.id)) {
        throw new BrowserDevelopmentHostError("This plugin file selection is already in progress.");
      }
      let staged = false;
      try {
        if (fileInspection) {
          const controller = new AbortController();
          selections.set(command.id, controller);
          staged = await stageBrowserPluginFile(browserWindow, command.id, controller.signal);
          if (!staged) {
            return parseCorrelatedHostResponse(
              {
                command: command.command,
                id: command.id,
                ok: true,
                result: { correlationId: "browser-file-selection-canceled", pluginPackage: null },
                version: command.version,
              },
              submitted,
            );
          }
        }
        return parseCorrelatedHostResponse(await transport.invoke(command), submitted);
      } finally {
        if (fileInspection) {
          selections.delete(command.id);
          if (staged) await discardBrowserPluginFile(browserWindow, command.id);
        }
      }
    },
    openExternalUrl: (url): Promise<ExternalUrlOpenResult> => {
      let request: ExternalUrlOpenRequest;
      try {
        request = parseExternalUrlOpenRequest({
          url,
          version: HOST_PROTOCOL_VERSION,
        });
      } catch (error) {
        return Promise.reject(
          error instanceof Error
            ? error
            : new BrowserDevelopmentHostError("External runbook request is invalid."),
        );
      }
      try {
        browserWindow.open(request.url, "_blank", "noopener,noreferrer");
      } catch (error) {
        return Promise.reject(
          new BrowserDevelopmentHostError("Browser rejected the external runbook request.", {
            cause: error,
          }),
        );
      }
      return Promise.resolve({
        state: "accepted",
        version: HOST_PROTOCOL_VERSION,
      });
    },
    subscribe: transport.subscribe,
  };
}

export function resolveStreamSkopeHost(browserWindow: Window): StreamSkopeHost {
  if (browserWindow.streamSkopeHost !== undefined) {
    return browserWindow.streamSkopeHost;
  }
  if (browserWindow.location.hash.length > 0 || browserWindow.location.search.length > 0) {
    browserWindow.history.replaceState(
      {},
      browserWindow.document.title,
      browserWindow.location.pathname,
    );
  }
  return createBrowserDevelopmentHost(browserWindow);
}
