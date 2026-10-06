import { useMemo, useState } from "react";

import type {
  ProviderWorkspaceControls,
  ProviderWorkspaceRegistration,
  ProviderDeactivationResult,
} from "../../../platform/ui/provider-workspaces";
import {
  NATS_PROTOCOL_VERSION,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsHost,
} from "../contracts";

import { NatsWorkspace } from "./NatsWorkspace";
import { createNatsProfilesFacet } from "./profiles-facet";
import type { NatsWorkspaceSource } from "./workspace-types";

/** Revoke new ownership; admitted receipts and listeners still observe original cleanup. */
export function createInteractiveNatsHost(host: NatsHost, isInteractive: () => boolean): NatsHost {
  return {
    execute: async <Command extends NatsCommand>(
      command: Command,
    ): Promise<NatsCommandResponse<Command["command"]>> => {
      if (!isInteractive()) {
        throw new Error(
          "This NATS workspace is inactive. Finish the connection change before submitting another request.",
        );
      }
      return host.execute(command);
    },
    subscribe: (listener): (() => void) =>
      isInteractive() ? host.subscribe(listener) : (): void => undefined,
  };
}

export interface NatsWorkspaceRegistrationProperties {
  readonly resolveSource: () => NatsWorkspaceSource;
}

export function createNatsWorkspaceRegistration({
  resolveSource,
}: NatsWorkspaceRegistrationProperties): ProviderWorkspaceRegistration {
  let resolvedSource: NatsWorkspaceSource | undefined;
  const source = (): NatsWorkspaceSource => (resolvedSource ??= resolveSource());
  function RegisteredNatsWorkspace({
    controls,
  }: {
    readonly controls: ProviderWorkspaceControls;
  }): React.JSX.Element {
    const [original] = useState(source);
    const gatedSource = useMemo<NatsWorkspaceSource>(
      () =>
        original.state === "ready"
          ? {
              state: "ready",
              host: createInteractiveNatsHost(original.host, controls.isInteractive),
            }
          : original,
      [original, controls.isInteractive],
    );
    return (
      <NatsWorkspace
        source={gatedSource}
        profilesPage={controls.profilesPage}
        isInteractive={controls.isInteractive}
      />
    );
  }
  const blocked = (phase: "stop" | "disconnect"): ProviderDeactivationResult =>
    phase === "stop"
      ? {
          state: "blocked",
          summary: "NATS subscription could not be stopped.",
          recovery:
            "Stop the NATS subscription successfully, then retry Connect from Connection Profiles.",
        }
      : {
          state: "blocked",
          summary: "NATS could not be disconnected.",
          recovery: "Disconnect NATS successfully, then retry Connect from Connection Profiles.",
        };
  return {
    id: "nats",
    label: "NATS",
    profiles: createNatsProfilesFacet(source),
    render: (controls): React.JSX.Element => <RegisteredNatsWorkspace controls={controls} />,
    deactivate: async (): Promise<ProviderDeactivationResult> => {
      // A never-rendered or unavailable workspace owns no host resources.
      if (resolvedSource === undefined || resolvedSource.state === "unavailable")
        return { state: "ready" };
      const host = resolvedSource.host;
      let phase: "stop" | "disconnect" = "stop";
      try {
        const stopped = await host.execute({
          command: "subscription.stop",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: NATS_PROTOCOL_VERSION,
        });
        if (!stopped.ok) return blocked(phase);
        phase = "disconnect";
        const disconnected = await host.execute({
          command: "connection.disconnect",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: NATS_PROTOCOL_VERSION,
        });
        return disconnected.ok ? { state: "ready" } : blocked(phase);
      } catch {
        return blocked(phase);
      }
    },
  };
}
