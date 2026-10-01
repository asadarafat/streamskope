import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaOperationalPreferenceSnapshot,
} from "../contracts";
import type { KafkaOperationalPreferenceService } from "../application";

import { failureResponse, translateFacadeFailure, type ActivityInput } from "./facade-support";

type PreferenceHostCommand = Extract<
  HostCommand,
  {
    readonly command: "preferences.get" | "preferences.reset" | "preferences.update";
  }
>;

interface OperationalPreferenceFacadeDependencies {
  readonly nextSequence: () => number;
  readonly preferences: KafkaOperationalPreferenceService;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
}

const PREFERENCE_GROUPS = ["fetch", "latency", "rules", "stream"] as const;

function operation(command: PreferenceHostCommand): string {
  switch (command.command) {
    case "preferences.get":
      return "Load preferences";
    case "preferences.reset":
      return "Reset preferences";
    case "preferences.update":
      return "Save preferences";
  }
}

function activityObject(command: PreferenceHostCommand): string {
  if (command.command !== "preferences.update") {
    return "Operational preferences";
  }
  return PREFERENCE_GROUPS.filter((group) => command.payload.patch[group] !== undefined).join(", ");
}

function changedEvent(
  snapshot: KafkaOperationalPreferenceSnapshot,
  sequence: number,
): Extract<HostEvent, { readonly event: "preferences.changed" }> {
  return {
    event: "preferences.changed",
    payload: snapshot,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function successResponse(
  command: PreferenceHostCommand,
  correlationId: string,
  snapshot: KafkaOperationalPreferenceSnapshot,
): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result: { correlationId, snapshot },
    version: HOST_PROTOCOL_VERSION,
  };
}

export async function executeOperationalPreferenceCommand(
  command: PreferenceHostCommand,
  correlationId: string,
  dependencies: OperationalPreferenceFacadeDependencies,
): Promise<HostCommandResponse> {
  const commandOperation = operation(command);
  const object = activityObject(command);
  try {
    const snapshot =
      command.command === "preferences.get"
        ? await dependencies.preferences.get()
        : command.command === "preferences.reset"
          ? await dependencies.preferences.reset()
          : await dependencies.preferences.update(command.payload.patch);
    dependencies.publish(changedEvent(snapshot, dependencies.nextSequence()));
    const fallback = snapshot.store.state === "unavailable";
    dependencies.recordActivity({
      correlationId,
      detail: fallback
        ? "Factory defaults are active because operational preference storage is unavailable."
        : command.command === "preferences.update"
          ? "The selected operational preference groups were committed atomically."
          : command.command === "preferences.reset"
            ? "Factory operational preferences were committed atomically."
            : "The complete operational preference snapshot was loaded.",
      object,
      operation: commandOperation,
      outcome: "succeeded",
      severity: fallback ? "warning" : "info",
    });
    return successResponse(command, correlationId, snapshot);
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
    dependencies.publish(
      changedEvent(dependencies.preferences.currentSnapshot(), dependencies.nextSequence()),
    );
    dependencies.recordActivity({
      correlationId,
      detail: translated.detail,
      object,
      operation: commandOperation,
      outcome: "failed",
      severity: "error",
    });
    return failureResponse(command, translated.error);
  }
}
