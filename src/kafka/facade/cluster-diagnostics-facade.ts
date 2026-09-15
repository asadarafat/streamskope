import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaClusterProfileContext,
} from "../contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaClusterDiagnosticsService,
  NoActiveKafkaConnectionError,
  type KafkaApplicationSession,
  type KafkaClusterDiagnosticsServicePort,
  type KafkaProfileService,
} from "../application";

import type { KafkaBackendFacadeOptions } from "./types";
import {
  failureResponse,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";

export type ClusterDetailsHostCommand = Extract<
  HostCommand,
  {
    readonly command: "clusterDetails.export" | "clusterDetails.load";
  }
>;

export interface ClusterDetailsFacadeBindings {
  readonly nextSequence: () => number;
  readonly profiles: Pick<KafkaProfileService, "currentSnapshot">;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly service: KafkaClusterDiagnosticsServicePort;
  readonly session: Pick<KafkaApplicationSession, "activeConnectionContext">;
}

export function createClusterDetailsService(
  session: KafkaApplicationSession,
  options: KafkaBackendFacadeOptions,
): KafkaClusterDiagnosticsServicePort {
  return (
    options.clusterDiagnostics ??
    new KafkaClusterDiagnosticsService(session, {
      ...(options.now === undefined ? {} : { now: options.now }),
    })
  );
}

interface ResolvedClusterContext {
  readonly endpoint: string;
  readonly profile: KafkaClusterProfileContext;
}

function resolveClusterContext(bindings: ClusterDetailsFacadeBindings): ResolvedClusterContext {
  const connection = bindings.session.activeConnectionContext();
  if (connection === null) {
    throw new NoActiveKafkaConnectionError("reading cluster details");
  }
  const activeProfile = bindings.profiles
    .currentSnapshot()
    .profiles.find(
      (profile) =>
        profile.active &&
        profile.name === connection.connectionName &&
        profile.brokers.length === connection.connectionBrokers.length &&
        profile.brokers.every((broker, index) => broker === connection.connectionBrokers[index]),
    );
  return {
    endpoint: connection.connectionTarget,
    profile:
      activeProfile === undefined
        ? {
            brokers: connection.connectionBrokers,
            id: null,
            name: connection.connectionName,
          }
        : {
            brokers: activeProfile.brokers,
            id: activeProfile.id,
            name: activeProfile.name,
          },
  };
}

function publishClusterDetails(
  bindings: ClusterDetailsFacadeBindings,
  payload: Extract<HostEvent, { readonly event: "clusterDetails.changed" }>["payload"],
): void {
  bindings.publish({
    event: "clusterDetails.changed",
    payload,
    sequence: bindings.nextSequence(),
    version: HOST_PROTOCOL_VERSION,
  });
}

function activityObject(context: ResolvedClusterContext | undefined): string {
  if (context === undefined) {
    return "No active Kafka connection";
  }
  const primaryBroker = context.profile.brokers[0] ?? context.endpoint;
  const remaining = context.profile.brokers.length - 1;
  return `${context.profile.name} · ${primaryBroker}${
    remaining > 0 ? ` +${String(remaining)} more` : ""
  }`;
}

function exportResponse(
  command: ClusterDetailsHostCommand,
  correlationId: string,
  document: ReturnType<KafkaClusterDiagnosticsServicePort["exportDocument"]>,
): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result: {
      correlationId,
      document,
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

export async function executeClusterDetailsCommand(
  command: ClusterDetailsHostCommand,
  correlationId: string,
  bindings: ClusterDetailsFacadeBindings,
): Promise<HostCommandResponse> {
  const operation =
    command.command === "clusterDetails.load"
      ? "Refresh cluster details"
      : "Export cluster details";
  let context: ResolvedClusterContext | undefined;
  try {
    context = resolveClusterContext(bindings);
    if (command.command === "clusterDetails.export") {
      const document = bindings.service.exportDocument();
      bindings.recordActivity({
        correlationId,
        detail: `Produced a ${String(document.byteSize)}-byte JSON document from the current cluster snapshot.`,
        object: activityObject(context),
        operation,
        outcome: "succeeded",
        severity: "info",
      });
      return exportResponse(command, correlationId, document);
    }

    publishClusterDetails(bindings, {
      cluster: null,
      endpoint: context.endpoint,
      fetchedAt: null,
      profile: context.profile,
      state: "loading",
    });
    const result = await bindings.service.load(context.profile);
    publishClusterDetails(bindings, {
      ...result.document,
      state: result.state,
    });
    bindings.recordActivity({
      correlationId,
      detail: `Loaded ${String(result.document.cluster.brokers.length)} broker${
        result.document.cluster.brokers.length === 1 ? "" : "s"
      }; broker configuration is ${result.state === "ready" ? "available" : "partially unavailable"}.`,
      object: activityObject(context),
      operation,
      outcome: "succeeded",
      severity: result.state === "ready" ? "info" : "warning",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      },
      true,
    );
    const cancelled =
      error instanceof ConnectionAttemptSupersededError ||
      (error instanceof Error && error.name === "AbortError");
    if (!cancelled) {
      const stale = bindings.service.staleDocument();
      if (stale !== null) {
        publishClusterDetails(bindings, {
          ...stale,
          error: translated.error,
          state: "stale",
        });
      } else if (context !== undefined) {
        publishClusterDetails(bindings, {
          cluster: null,
          endpoint: context.endpoint,
          error: translated.error,
          fetchedAt: null,
          profile: context.profile,
          state: "failed",
        });
      } else {
        publishClusterDetails(bindings, {
          cluster: null,
          endpoint: null,
          fetchedAt: null,
          profile: null,
          state: "unavailable",
        });
      }
    }
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object: activityObject(context),
      operation,
      outcome: cancelled ? "cancelled" : "failed",
      severity: cancelled ? "warning" : "error",
    });
    return failureResponse(command, translated.error);
  }
}
