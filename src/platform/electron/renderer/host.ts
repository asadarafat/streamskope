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

import {
  BrowserDevelopmentHostError,
  createBrowserProviderTransport,
} from "./browser-provider-transport";

export { BrowserDevelopmentHostError };

declare global {
  interface Window {
    streamSkopeHost?: StreamSkopeHost;
  }
}

type BrowserDevelopmentWindow = Pick<Window, "open"> & Partial<Pick<Window, "location">>;

export function createBrowserDevelopmentHost(
  browserWindow: BrowserDevelopmentWindow = window,
): StreamSkopeHost {
  const transport = createBrowserProviderTransport(browserWindow, {
    codec: KAFKA_PROVIDER_EVENT_CODEC,
  });
  return {
    execute: async <Command extends HostCommand>(
      value: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      const submitted = { ...value };
      const command = parseHostCommand(submitted);
      return parseCorrelatedHostResponse(await transport.invoke(command), submitted);
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
