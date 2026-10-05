import {
  NATS_PROVIDER_EVENT_CODEC,
  NATS_PROVIDER_ID,
  parseNatsCommand,
  parseCorrelatedNatsResponse,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsHost,
} from "../../../features/nats/contracts";
import type { NatsWorkspaceSource } from "../../../features/nats/ui/workspace-types";

import { createBrowserProviderTransport } from "./browser-provider-transport";

export function createBrowserNatsHost(
  browserWindow: Partial<Pick<Window, "location">> = window,
): NatsHost {
  const transport = createBrowserProviderTransport(browserWindow, {
    codec: NATS_PROVIDER_EVENT_CODEC,
    providerId: NATS_PROVIDER_ID,
  });
  return {
    execute: async <Command extends NatsCommand>(
      value: Command,
    ): Promise<NatsCommandResponse<Command["command"]>> => {
      const submitted = { ...value };
      const command = parseNatsCommand(submitted);
      return parseCorrelatedNatsResponse(await transport.invoke(command), submitted);
    },
    subscribe: transport.subscribe,
  };
}

/** Resolve at the first NATS visit so Kafka-only launchers never acquire NATS transport. */
export function resolveNatsWorkspaceSource(browserWindow: Window): NatsWorkspaceSource {
  const native = browserWindow.streamSkopeProviders?.nats;
  if (native !== undefined) return { state: "ready", host: native };
  if (
    browserWindow.streamSkopeHost !== undefined ||
    browserWindow.streamSkopeProviders !== undefined ||
    browserWindow.streamSkopeDesktop !== undefined ||
    browserWindow.location.protocol !== "http:"
  ) {
    return {
      state: "unavailable",
      recovery:
        "This desktop host does not expose Core NATS. Use a desktop build with the Core NATS provider.",
    };
  }
  return { state: "ready", host: createBrowserNatsHost(browserWindow) };
}
