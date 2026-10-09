import { randomBytes } from "node:crypto";

import type { PluginRendererAsset } from "../node/plugins/runtime";
import type { RecordExportDelivery } from "../node/record-export-artifacts";

import {
  developmentOrigin,
  resolveDevelopmentNetwork,
  type DevelopmentNetworkOptions,
} from "./network";
import {
  startDevelopmentHost,
  resolveDevelopmentProviders,
  type DevelopmentProviderSource,
  type RunningDevelopmentHost,
} from "./server";
import { startViteRenderer } from "./vite-renderer";

export type WebDevelopmentLaunchOptions = DevelopmentNetworkOptions &
  DevelopmentProviderSource & {
    readonly hostPort: number;
    readonly rendererPort: number;
    readonly rendererRoot: string;
    readonly token?: string;
    readonly pluginAsset?: (pathname: string) => Promise<PluginRendererAsset | undefined>;
    readonly exportFiles?: RecordExportDelivery;
  };

export interface RunningWebDevelopment {
  readonly browserUrl: string;
  readonly host: RunningDevelopmentHost;
  readonly rendererOrigin: string;
  close(): Promise<void>;
}

async function closeAll(services: readonly { close(): Promise<void> }[]): Promise<void> {
  const results = await Promise.allSettled(
    services.map((service) => Promise.resolve().then(() => service.close())),
  );
  const failures = results
    .filter((result): result is PromiseRejectedResult => result.status === "rejected")
    .map((result) => result.reason as unknown);
  if (failures.length > 0) {
    throw new AggregateError(failures, "One or more development services failed to stop.");
  }
}

export async function launchWebDevelopment(
  options: WebDevelopmentLaunchOptions,
): Promise<RunningWebDevelopment> {
  const token = options.token ?? randomBytes(32).toString("base64url");
  const gatewayToken = randomBytes(32).toString("base64url");
  const providers = resolveDevelopmentProviders(options);
  const pluginAsset = options.pluginAsset ?? options.backend?.pluginAsset?.bind(options.backend);
  const exportFiles = options.exportFiles ?? options.backend?.exportFiles;
  let network;
  let renderer;
  try {
    network = resolveDevelopmentNetwork(options);
    renderer = await startViteRenderer({
      ...network,
      gatewayToken,
      hostOrigin: developmentOrigin(network.publicHostname, options.hostPort),
      hostToken: token,
      port: options.rendererPort,
      root: options.rendererRoot,
      ...(pluginAsset === undefined ? {} : { pluginAsset }),
      ...(exportFiles === undefined ? {} : { exportFiles }),
    });
  } catch (error) {
    try {
      await providers.shutdown();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Renderer startup failed and the application backend could not be stopped.",
        { cause: cleanupError },
      );
    }
    throw error;
  }

  let host: RunningDevelopmentHost;
  try {
    host = await startDevelopmentHost({
      providers,
      ...network,
      port: options.hostPort,
      rendererOrigin: renderer.origin,
      token,
    });
  } catch (error) {
    try {
      await renderer.close();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Development host startup failed and the renderer could not be stopped.",
        { cause: cleanupError },
      );
    }
    throw error;
  }

  const browserUrl = `${renderer.origin}/`;
  let closePromise: Promise<void> | undefined;
  return {
    browserUrl,
    close: (): Promise<void> => {
      closePromise ??= closeAll([host, renderer]);
      return closePromise;
    },
    host,
    rendererOrigin: renderer.origin,
  };
}
