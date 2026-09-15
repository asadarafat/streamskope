import type {
  HostCommand,
  HostCommandResponse,
  HostEvent,
  ProfileTestInput,
  SecureConnectionInput,
} from "../contracts";
import { HOST_PROTOCOL_VERSION } from "../contracts";
import type {
  KafkaApplicationSession,
  KafkaProfileService,
  KafkaProfileSnapshot,
} from "../application";

import {
  failureResponse,
  profileActivityObject,
  profileOperation,
  profilesChangedEvent,
  sensitiveValues,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";

export type ProfileHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "profiles.create"
      | "profiles.delete"
      | "profiles.list"
      | "profiles.update"
      | "profiles.binding.get";
  }
>;

export interface ProfileFacadeBindings {
  readonly available: () => boolean;
  readonly nextSequence: () => number;
  readonly profiles: KafkaProfileService;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
}

type ProfileTestCommand = Extract<HostCommand, { readonly command: "profiles.test" }>;

async function executeProfileBindingDetail(
  command: Extract<HostCommand, { readonly command: "profiles.binding.get" }>,
  correlationId: string,
  profiles: KafkaProfileService,
  available: boolean,
): Promise<HostCommandResponse> {
  try {
    return {
      command: command.command,
      id: command.id,
      ok: true,
      version: HOST_PROTOCOL_VERSION,
      result: {
        correlationId,
        bindingDetail: await profiles.bindingDetail(command.payload.profileId),
      },
    };
  } catch (error) {
    return failureResponse(
      command,
      translateFacadeFailure(
        error,
        { activeStateChanged: false, connection: undefined, correlationId },
        available,
      ).error,
    );
  }
}

export interface ProfileTestFacadeBindings {
  readonly available: () => boolean;
  readonly profiles: Pick<KafkaProfileService, "resolveTestContext">;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly session: Pick<KafkaApplicationSession, "testConnection">;
}

function replacementValues(input: ProfileTestInput): readonly string[] {
  const profile = input.profile;
  return [
    ...(profile.trust.material.mode === "replace"
      ? [profile.trust.material.value, "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----"]
      : []),
    ...(profile.trust.password.mode === "replace" ? [profile.trust.password.value] : []),
    ...(profile.oauth?.clientSecret.mode === "replace" ? [profile.oauth.clientSecret.value] : []),
  ];
}

export async function executeProfileTestCommand(
  command: ProfileTestCommand,
  correlationId: string,
  bindings: ProfileTestFacadeBindings,
): Promise<HostCommandResponse> {
  let connection: SecureConnectionInput | undefined;
  const draftSensitiveValues = replacementValues(command.payload);
  try {
    const resolved = await bindings.profiles.resolveTestContext(command.payload);
    connection = resolved.connection;
    resolved.lifetimeSignal?.throwIfAborted();
    const result = await bindings.session.testConnection(connection, resolved.lifetimeSignal);
    resolved.lifetimeSignal?.throwIfAborted();
    bindings.recordActivity({
      correlationId,
      detail: `Confirmed checks: ${result.checks.join(", ")}. Visible topics: ${String(result.topicCount)}. No profile or active connection changed.`,
      object: profileActivityObject(connection),
      operation: "Test profile connection",
      outcome: "succeeded",
      sensitiveValues: [...sensitiveValues(connection), ...draftSensitiveValues],
      severity: "info",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection,
        correlationId,
        sensitiveValues: draftSensitiveValues,
      },
      bindings.available(),
    );
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object: profileActivityObject(connection ?? command.payload.profile),
      operation: "Test profile connection",
      outcome: "failed",
      sensitiveValues: [...sensitiveValues(connection), ...draftSensitiveValues],
      severity: "error",
    });
    return failureResponse(command, translated.error);
  }
}

export async function executeProfileCommand(
  command: ProfileHostCommand,
  correlationId: string,
  bindings: ProfileFacadeBindings,
): Promise<HostCommandResponse> {
  if (command.command === "profiles.binding.get")
    return executeProfileBindingDetail(
      command,
      correlationId,
      bindings.profiles,
      bindings.available(),
    );
  const operation = profileOperation(command);
  let before: KafkaProfileSnapshot | undefined;
  try {
    if (command.command !== "profiles.list") {
      before = await bindings.profiles.list();
    }
    let snapshot: KafkaProfileSnapshot;
    switch (command.command) {
      case "profiles.create":
        snapshot = await bindings.profiles.create(command.payload.profile);
        break;
      case "profiles.delete":
        snapshot = await bindings.profiles.delete(command.payload.profileId);
        break;
      case "profiles.list":
        snapshot = await bindings.profiles.list();
        break;
      case "profiles.update":
        snapshot = await bindings.profiles.update(
          command.payload.profileId,
          command.payload.profile,
        );
        break;
    }
    const profile =
      command.command === "profiles.create"
        ? snapshot.profiles.find(
            (candidate) =>
              before?.profiles.some((previous) => previous.id === candidate.id) !== true,
          )
        : command.command === "profiles.update"
          ? snapshot.profiles.find((candidate) => candidate.id === command.payload.profileId)
          : command.command === "profiles.delete"
            ? before?.profiles.find((candidate) => candidate.id === command.payload.profileId)
            : undefined;
    bindings.publish(profilesChangedEvent(snapshot, bindings.nextSequence()));
    bindings.recordActivity({
      correlationId,
      detail: `${operation} completed and the safe profile inventory was refreshed.`,
      object: profile === undefined ? "Kafka profiles" : profileActivityObject(profile),
      operation,
      outcome: "succeeded",
      severity: "info",
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
      bindings.available(),
    );
    const object =
      command.command === "profiles.update" || command.command === "profiles.delete"
        ? profileActivityObject(
            before?.profiles.find((candidate) => candidate.id === command.payload.profileId) ?? {
              brokers: [],
              name: "Kafka profile",
            },
          )
        : command.command === "profiles.list"
          ? "Kafka profiles"
          : "Kafka profile request";
    bindings.publish(
      profilesChangedEvent(bindings.profiles.currentSnapshot(), bindings.nextSequence()),
    );
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object,
      operation,
      outcome: "failed",
      severity: "error",
    });
    return failureResponse(command, translated.error);
  }
}
