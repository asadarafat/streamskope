import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type RemoteSshHostKeySummary,
  type RemoteTrustAcquisitionSummary,
} from "../contracts";
import type { KafkaTrustAcquisitionServicePort, KafkaProfileService } from "../application";
import { KafkaTrustAcquisitionValidationError } from "../application";

import { failureResponse, translateFacadeFailure, type ActivityInput } from "./facade-support";

export type TrustAcquisitionHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "trustAcquisition.discard"
      | "trustAcquisition.cancel"
      | "trustAcquisition.capabilities"
      | "trustAcquisition.editor.open"
      | "trustAcquisition.editor.advance"
      | "trustAcquisition.editor.close"
      | "trustAcquisition.apply"
      | "trustAcquisition.hostKey.discover"
      | "trustAcquisition.material.fetch"
      | "trustAcquisition.https.fetch"
      | "trustAcquisition.password.fetch";
  }
>;

export function isTrustAcquisitionCommand(
  command: HostCommand,
): command is TrustAcquisitionHostCommand {
  return command.command.startsWith("trustAcquisition.");
}

export interface TrustAcquisitionFacadeBindings {
  readonly profiles?: Pick<KafkaProfileService, "bindingDetail">;
  readonly acquisitions: KafkaTrustAcquisitionServicePort | undefined;
  readonly available: boolean;
  readonly recordActivity: (input: ActivityInput) => void;
}

function operation(command: TrustAcquisitionHostCommand): string {
  switch (command.command) {
    case "trustAcquisition.editor.open":
      return "Open trust editor";
    case "trustAcquisition.editor.advance":
      return "Change trust editor context";
    case "trustAcquisition.editor.close":
      return "Close trust editor";
    case "trustAcquisition.apply":
      return "Use acquired trust";
    case "trustAcquisition.capabilities":
      return "Check SSH capabilities";
    case "trustAcquisition.hostKey.discover":
      return "Discover SSH host identity";
    case "trustAcquisition.password.fetch":
      return "Fetch remote trust password";
    case "trustAcquisition.material.fetch":
      return "Fetch remote trust material";
    case "trustAcquisition.https.fetch":
      return "Fetch HTTPS trust material";
    case "trustAcquisition.discard":
      return "Discard remote trust acquisition";
    case "trustAcquisition.cancel":
      return "Cancel remote trust acquisition";
  }
}

function target(command: TrustAcquisitionHostCommand): string {
  if (
    command.command.startsWith("trustAcquisition.editor.") ||
    command.command === "trustAcquisition.apply"
  )
    return "Profile trust editor";
  if (command.command === "trustAcquisition.capabilities") return "Application host";
  if (command.command === "trustAcquisition.cancel") return command.payload.requestId;
  if (command.command === "trustAcquisition.discard") return "Trust candidate";
  return "target" in command.payload
    ? `${command.payload.target.host}:${String(command.payload.target.port)}`
    : "Profile trust editor";
}

function submittedSecrets(command: TrustAcquisitionHostCommand): readonly string[] {
  if (command.command === "trustAcquisition.https.fetch") {
    const input = command.payload;
    const auth = input.api.authentication;
    return [
      ...Object.values(input.recipe.overrides),
      ...Object.values(input.secretParameters ?? {}),
      ...(input.truststorePassword === undefined ? [] : [input.truststorePassword]),
      ...(auth.mode === "bearer"
        ? [auth.token]
        : auth.mode === "basic"
          ? [auth.username, auth.password]
          : []),
      ...(input.api.tls.mode === "custom" ? [input.api.tls.caPem] : []),
    ];
  }
  if (
    !("target" in command.payload) ||
    command.command === "trustAcquisition.discard" ||
    command.command === "trustAcquisition.capabilities" ||
    command.command === "trustAcquisition.cancel" ||
    command.command === "trustAcquisition.hostKey.discover"
  ) {
    return [];
  }
  const submitted = command.payload.target;
  const authentication = submitted.authentication;
  return [
    submitted.username,
    ...(command.command === "trustAcquisition.material.fetch"
      ? [
          ...Object.values(command.payload.recipe?.overrides ?? {}),
          ...Object.values(command.payload.secretParameters ?? {}),
          ...(command.payload.truststorePassword === undefined
            ? []
            : [command.payload.truststorePassword]),
        ]
      : []),
    ...(submitted.password === undefined ? [] : [submitted.password]),
    ...(authentication?.mode === "password" ? [authentication.password] : []),
    ...(authentication?.mode === "private-key"
      ? [
          authentication.privateKey,
          ...(authentication.passphrase === undefined ? [] : [authentication.passphrase]),
        ]
      : []),
  ];
}

function acquisitionResponse(
  command: Extract<
    TrustAcquisitionHostCommand,
    {
      readonly command:
        | "trustAcquisition.material.fetch"
        | "trustAcquisition.password.fetch"
        | "trustAcquisition.https.fetch";
    }
  >,
  correlationId: string,
  acquisition: RemoteTrustAcquisitionSummary,
): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result: {
      acquisition,
      correlationId,
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

function hostKeyResponse(
  command: Extract<
    TrustAcquisitionHostCommand,
    { readonly command: "trustAcquisition.hostKey.discover" }
  >,
  correlationId: string,
  hostKey: RemoteSshHostKeySummary,
): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result: {
      correlationId,
      hostKey,
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

function successDetail(
  command: TrustAcquisitionHostCommand,
  acquisition: RemoteTrustAcquisitionSummary | undefined,
  hostKey: RemoteSshHostKeySummary | undefined,
): string {
  switch (command.command) {
    case "trustAcquisition.editor.open":
    case "trustAcquisition.editor.advance":
    case "trustAcquisition.editor.close":
      return "Trust editor context updated; saved trust is unchanged.";
    case "trustAcquisition.apply":
      return "Acquired trust attached to the current editor; no save, test or connection was performed.";
    case "trustAcquisition.capabilities":
      return "Host SSH configuration checked; no authentication was attempted.";
    case "trustAcquisition.hostKey.discover":
      return `Discovered SSH host identity ${hostKey?.fingerprint ?? "unknown"}; no authentication or remote template was attempted.`;
    case "trustAcquisition.password.fetch":
      return `Remote password acquisition completed with selected template ${
        acquisition?.password.templateName ?? "unknown"
      }; the value remains host-owned until ${acquisition?.expiresAt ?? "its bounded expiry"}.`;
    case "trustAcquisition.https.fetch":
    case "trustAcquisition.material.fetch":
      return `${
        acquisition?.password.present === true
          ? `Remote password template ${acquisition.password.templateName ?? "unknown"} and `
          : "Remote "
      }${acquisition?.material?.kind ?? command.payload.kind} trust material template ${
        acquisition?.material?.templateName ?? "unknown"
      } completed; ${String(acquisition?.material?.byteCount ?? 0)} bytes remain host-owned until ${
        acquisition?.expiresAt ?? "its bounded expiry"
      }.`;
    case "trustAcquisition.discard":
      return "The host-owned remote trust acquisition was discarded.";
    case "trustAcquisition.cancel":
      return "Cancellation was requested for pending remote trust work; existing trust is unchanged.";
  }
}

export async function executeTrustAcquisitionCommand(
  command: TrustAcquisitionHostCommand,
  correlationId: string,
  bindings: TrustAcquisitionFacadeBindings,
): Promise<HostCommandResponse> {
  const commandOperation = operation(command);
  const commandTarget = target(command);
  const secrets = submittedSecrets(command);

  try {
    if (bindings.acquisitions === undefined) {
      throw new Error("The remote trust acquisition owner is unavailable.");
    }
    if (
      (command.command === "trustAcquisition.hostKey.discover" ||
        command.command === "trustAcquisition.material.fetch") &&
      command.payload.editor === undefined
    ) {
      throw new KafkaTrustAcquisitionValidationError(
        "Reopen the profile editor before acquiring trust material.",
      );
    }
    let acquisition: RemoteTrustAcquisitionSummary | undefined;
    let hostKey: RemoteSshHostKeySummary | undefined;
    let response: HostCommandResponse;
    switch (command.command) {
      case "trustAcquisition.editor.open": {
        const profile = command.payload.profile;
        const detail =
          profile === undefined ? undefined : await bindings.profiles?.bindingDetail(profile.id);
        if (profile !== undefined && (detail === undefined || detail.revision !== profile.revision))
          throw new KafkaTrustAcquisitionValidationError(
            "This profile changed or is unavailable. Reopen it before acquiring trust.",
          );
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: {
            correlationId,
            editor: bindings.acquisitions.openEditor(detail?.binding?.identity),
          },
        };
      }
      case "trustAcquisition.editor.advance":
      case "trustAcquisition.editor.close":
      case "trustAcquisition.apply":
        if (command.command === "trustAcquisition.editor.advance")
          bindings.acquisitions.advanceEditor(command.payload.id, command.payload.generation);
        else if (command.command === "trustAcquisition.editor.close")
          bindings.acquisitions.closeEditor(command.payload.editorId);
        else bindings.acquisitions.apply(command.payload.acquisitionId, command.payload.editorId);
        response = {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId },
        };
        break;
      case "trustAcquisition.capabilities":
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, ...bindings.acquisitions.capabilities() },
        };
      case "trustAcquisition.hostKey.discover":
        hostKey = await bindings.acquisitions.discoverHostKey(
          command.payload,
          undefined,
          command.id,
        );
        response = hostKeyResponse(command, correlationId, hostKey);
        break;
      case "trustAcquisition.password.fetch":
        throw new KafkaTrustAcquisitionValidationError(
          "Separate password acquisition is no longer supported. Use Acquire trust material to retrieve and validate the complete candidate.",
        );
      case "trustAcquisition.https.fetch":
        acquisition = await bindings.acquisitions.fetchHttpsMaterial(
          command.payload,
          undefined,
          command.id,
        );
        response = acquisitionResponse(command, correlationId, acquisition);
        break;
      case "trustAcquisition.material.fetch":
        acquisition = await bindings.acquisitions.fetchMaterial(
          command.payload,
          undefined,
          command.id,
        );
        response = acquisitionResponse(command, correlationId, acquisition);
        break;
      case "trustAcquisition.cancel":
      case "trustAcquisition.discard":
        if (command.command === "trustAcquisition.cancel")
          bindings.acquisitions.cancel(command.payload.requestId, command.payload.editorId);
        else bindings.acquisitions.discard(command.payload.acquisitionId, command.payload.editorId);
        response = {
          command: command.command,
          id: command.id,
          ok: true,
          result: { correlationId },
          version: HOST_PROTOCOL_VERSION,
        };
        break;
    }
    bindings.recordActivity({
      correlationId,
      detail: successDetail(command, acquisition, hostKey),
      object: commandTarget,
      operation: commandOperation,
      outcome: "succeeded",
      severity: "info",
    });
    return response;
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
        sensitiveValues: secrets,
      },
      bindings.available && bindings.acquisitions !== undefined,
    );
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object: commandTarget,
      operation: commandOperation,
      outcome: translated.error.code === "CANCELLED" ? "cancelled" : "failed",
      severity: translated.error.code === "CANCELLED" ? "info" : "error",
    });
    return failureResponse(command, translated.error);
  }
}
