import { randomBytes } from "node:crypto";

import {
  developmentOrigin,
  resolveDevelopmentNetwork,
  type DevelopmentNetworkOptions,
} from "./network";
import {
  startDevelopmentHost,
  type DevelopmentBackend,
  type RunningDevelopmentHost,
} from "./server";
import { startViteRenderer } from "./vite-renderer";

export interface WebDevelopmentLaunchOptions extends DevelopmentNetworkOptions {
  readonly backend: DevelopmentBackend;
  readonly hostPort: number;
  readonly rendererPort: number;
  readonly rendererRoot: string;
  readonly token?: string;
}

export interface RunningWebDevelopment {
  readonly browserUrl: string;
  readonly host: RunningDevelopmentHost;
  readonly rendererOrigin: string;
  close(): Promise<void>;
}

async function closeAll(services: readonly { close(): Promise<void> }[]): Promise<void> {
  const results = await Promise.allSettled(services.map((service) => service.close()));
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
    });
  } catch (error) {
    try {
      await options.backend.shutdown();
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
      backend: options.backend,
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
