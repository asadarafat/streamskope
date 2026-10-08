import { useCallback, useMemo, useState } from "react";

import type {
  ProviderWorkspaceControls,
  ProviderWorkspaceRegistration,
  ProviderDeactivationResult,
  ProviderConnectionOutcome,
} from "../../../platform/ui/provider-workspaces";
import {
  HOST_PROTOCOL_VERSION,
  type ExternalUrlOpenResult,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
  type KafkaInvestigationQuery,
} from "../contracts";

import { KafkaWorkspace, type StreamSkopeAppProperties } from "./StreamSkopeApp";
import { KafkaProfileCatalog } from "./profile-catalog";
import { createKafkaQueryConnectionHandoff } from "./query-connection-handoff";

/** New requests require this activation's authority; already admitted results remain unchanged. */
export function createInteractiveKafkaHost(
  host: StreamSkopeHost,
  isInteractive: () => boolean,
): StreamSkopeHost {
  const requireInteractive = (): void => {
    if (!isInteractive())
      throw new Error(
        "This Kafka workspace is inactive. Finish the connection change before submitting another request.",
      );
  };
  return {
    execute: async <Command extends HostCommand>(
      command: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      requireInteractive();
      return host.execute(command);
    },
    openExternalUrl: async (url): Promise<ExternalUrlOpenResult> => {
      requireInteractive();
      return host.openExternalUrl(url);
    },
    // Replayed effects and stale views may attempt a new subscription after revocation.
    // Leave admitted listeners alone, but never give a new listener host ownership.
    subscribe: (listener): (() => void) =>
      isInteractive() ? host.subscribe(listener) : (): void => undefined,
  };
}

/** Kafka alone owns the typed stop/disconnect protocol and its connection plugins. */
export function createKafkaWorkspaceRegistration(
  properties: StreamSkopeAppProperties,
): ProviderWorkspaceRegistration {
  let initialQueryImportConsumed = false;
  const catalog = new KafkaProfileCatalog(properties);
  const queryHandoff = createKafkaQueryConnectionHandoff();
  function RegisteredKafkaWorkspace({
    controls,
  }: {
    readonly controls: ProviderWorkspaceControls;
  }): React.JSX.Element {
    const [initialQueryImport] = useState(() =>
      initialQueryImportConsumed ? undefined : properties.initialQueryImport,
    );
    const [initialConnectionEvent] = useState(catalog.initialConnectionEvent);
    const [initialRestoredQuery] = useState(queryHandoff.restored);
    const onPendingQueryConnection = useCallback(
      (query: KafkaInvestigationQuery, profileId: string | undefined): void => {
        if (controls.isInteractive()) queryHandoff.prepare(query, profileId);
      },
      [controls.isInteractive],
    );
    const host = useMemo(
      () => createInteractiveKafkaHost(properties.host, controls.isInteractive),
      [properties.host, controls.isInteractive],
    );
    return (
      <KafkaWorkspace
        {...properties}
        host={host}
        initialQueryImport={initialQueryImport}
        providerControl={undefined}
        profilesPage={controls.profilesPage}
        initialConnectionEvent={initialConnectionEvent}
        initialRestoredQuery={initialRestoredQuery}
        onPendingQueryConnection={onPendingQueryConnection}
        isInteractive={controls.isInteractive}
      />
    );
  }
  return {
    id: "kafka",
    label: "Kafka",
    profiles: {
      ...catalog.facet,
      connect: async (profile): Promise<ProviderConnectionOutcome> => {
        const outcome = await catalog.facet.connect(profile);
        if (outcome.ok) queryHandoff.connected(profile.id);
        return outcome;
      },
    },
    render: (controls): React.JSX.Element => <RegisteredKafkaWorkspace controls={controls} />,
    deactivate: async (destination): Promise<ProviderDeactivationResult> => {
      let phase: "stop" | "disconnect" = "stop";
      try {
        const stopped = await properties.host.execute({
          command: "messages.stop",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (!stopped.ok)
          return {
            state: "blocked",
            summary: "Kafka consumption could not be stopped.",
            recovery:
              "Stop Kafka consumption successfully, then retry Connect from Connection Profiles.",
          };
        phase = "disconnect";
        const disconnected = await properties.host.execute({
          command: "connection.disconnect",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (!disconnected.ok)
          return {
            state: "blocked",
            summary: "Kafka could not be disconnected.",
            recovery: "Disconnect Kafka successfully, then retry Connect from Connection Profiles.",
          };
        // An import belongs to the first visit; effect replay and failed cleanup keep that visit.
        initialQueryImportConsumed = true;
        queryHandoff.disconnected(destination);
        return { state: "ready" };
      } catch {
        return phase === "stop"
          ? {
              state: "blocked",
              summary: "Kafka consumption could not be stopped.",
              recovery:
                "Stop Kafka consumption successfully, then retry Connect from Connection Profiles.",
            }
          : {
              state: "blocked",
              summary: "Kafka could not be disconnected.",
              recovery:
                "Disconnect Kafka successfully, then retry Connect from Connection Profiles.",
            };
      }
    },
  };
}
