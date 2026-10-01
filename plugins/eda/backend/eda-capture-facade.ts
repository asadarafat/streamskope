import {
  HOST_PROTOCOL_VERSION,
  type EdaCaptureProgress,
  type HostCommand,
  type HostCommandResponse,
} from "../contracts";
import type { PluginActivity as ActivityInput, PluginBackendHost } from "../../../src/plugins/api";
import type { HostError } from "../../../src/features/kafka/contracts";

import type { EdaCapturePort } from "./eda-capture-port";
interface StructuredOperationFailure extends Error {
  readonly code: HostError["code"];
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostError["stage"];
}
function failureResponse(command: EdaCaptureCommand, error: HostError): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: false,
    error,
    version: HOST_PROTOCOL_VERSION,
  };
}

export type EdaCaptureCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "edaCapture.deploy"
      | "edaCapture.application.status"
      | "edaCapture.application.install"
      | "edaCapture.inspect"
      | "edaCapture.preflight"
      | "edaCapture.status"
      | "edaCapture.stop"
      | "edaCapture.remove"
      | "edaCapture.cancel";
  }
>;

export interface EdaCaptureFacadeBindings {
  readonly removeCaptureProfiles?: (
    source: import("../contracts").ProfileEdaCaptureSource,
  ) => Promise<void>;
  readonly capture: EdaCapturePort | undefined;
  readonly failure: PluginBackendHost["failure"];
  readonly publishProgress?: (progress: EdaCaptureProgress) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly operations?: Map<string, AbortController>;
  readonly connectionActive?: () => boolean;
}

class EdaCaptureUnavailableError extends Error implements StructuredOperationFailure {
  readonly code = "UNSUPPORTED_OPERATION" as const;
  readonly recovery = "Run StreamSkope on a host with configured EDA capture support, then retry.";
  readonly retryable = false;
  readonly stage = "backend" as const;

  constructor() {
    super("EDA capture is unavailable in this StreamSkope host.");
    this.name = "EdaCaptureUnavailableError";
  }
}

class EdaCaptureConflictError extends Error implements StructuredOperationFailure {
  readonly code = "VALIDATION" as const;
  readonly stage = "validation" as const;
  readonly retryable = false;
  readonly recovery =
    "Refresh capture status, finish or cancel the current operation, and disconnect Kafka before retrying.";
}

export async function executeEdaCaptureCommand(
  command: EdaCaptureCommand,
  correlationId: string,
  bindings: EdaCaptureFacadeBindings,
): Promise<HostCommandResponse> {
  const translateFacadeFailure = (
    error: unknown,
    context: {
      correlationId: string;
      sensitiveValues?: readonly string[];
      activeStateChanged: boolean;
      connection: unknown;
    },
    _available?: boolean,
  ): { error: HostError; detail: string } => {
    const failure = bindings.failure(error, context);
    return { error: failure, detail: `${failure.summary} ${failure.recovery}` };
  };
  if (
    command.command === "edaCapture.application.status" ||
    command.command === "edaCapture.application.install"
  ) {
    const operation =
      command.command === "edaCapture.application.status"
        ? "Check EDA capture application"
        : "Install EDA capture application";
    const sensitiveValues = [
      command.payload.edaApi.password,
      ...(command.payload.authorization === undefined
        ? []
        : [command.payload.authorization.password]),
    ];
    try {
      if (bindings.capture === undefined) throw new EdaCaptureUnavailableError();
      const application = await (command.command === "edaCapture.application.status"
        ? bindings.capture.applicationStatus?.(command.payload)
        : bindings.capture.installApplication?.(command.payload));
      if (application === undefined) throw new EdaCaptureUnavailableError();
      bindings.recordActivity({
        correlationId,
        detail:
          application.state === "installed"
            ? "The EDA-managed StreamSkope Capture application is ready."
            : "The StreamSkope Capture application is not installed in this EDA system.",
        object: command.payload.edaApi.baseUrl,
        operation,
        outcome: "succeeded",
        severity: "info",
      });
      return {
        command: command.command,
        id: command.id,
        ok: true,
        result: { application, correlationId },
        version: HOST_PROTOCOL_VERSION,
      };
    } catch (error) {
      const translated = translateFacadeFailure(
        error,
        {
          activeStateChanged: false,
          connection: undefined,
          correlationId,
          sensitiveValues,
        },
        true,
      );
      bindings.recordActivity({
        correlationId,
        detail: translated.detail,
        object: command.payload.edaApi.baseUrl,
        operation,
        outcome: "failed",
        severity: "error",
      });
      return failureResponse(command, translated.error);
    }
  }
  if (command.command !== "edaCapture.inspect" && command.command !== "edaCapture.deploy") {
    try {
      if (bindings.capture === undefined) throw new EdaCaptureUnavailableError();
      if (command.command === "edaCapture.cancel") {
        bindings.operations?.get(command.payload.requestId)?.abort();
        return {
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: { correlationId },
        };
      }
      if (command.command === "edaCapture.preflight") {
        return {
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: { correlationId, captureHost: await bindings.capture.preflight() },
        };
      }
      if (command.command === "edaCapture.stop" || command.command === "edaCapture.remove") {
        if (bindings.connectionActive?.())
          throw new EdaCaptureConflictError(
            "Disconnect Kafka before stopping capture or removing deployed resources.",
          );
        if (bindings.operations?.size)
          throw new EdaCaptureConflictError(
            "A capture operation is still running. Cancel it and wait for completion before stopping or removing resources.",
          );
        const lock = new AbortController();
        bindings.operations?.set(command.id, lock);
        try {
          await bindings.capture.stop(
            command.payload.source,
            command.command === "edaCapture.remove",
          );
          if (command.command === "edaCapture.remove")
            await bindings.removeCaptureProfiles?.(command.payload.source);
        } finally {
          bindings.operations?.delete(command.id);
        }
        bindings.recordActivity({
          correlationId,
          detail: bindings.capture.status().detail,
          object: command.payload.source.source.name,
          operation:
            command.command === "edaCapture.stop" ? "Stop capture" : "Remove capture resources",
          outcome: "succeeded",
          severity: "info",
        });
      }
      return {
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId, captureSession: bindings.capture.status() },
      };
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
      bindings.recordActivity({
        correlationId,
        detail: translated.detail,
        object: "EDA capture",
        operation: command.command,
        outcome: "failed",
        severity: "error",
      });
      return failureResponse(command, translated.error);
    }
  }
  const operation =
    command.command === "edaCapture.inspect" ? "Inspect EDA capture sources" : "Deploy EDA capture";
  const object =
    command.command === "edaCapture.inspect"
      ? command.payload.edaApi.baseUrl
      : `${command.payload.source.namespace}/${command.payload.source.name}`;
  const sensitiveValues = [
    ...(command.payload.edaApi === undefined ? [] : [command.payload.edaApi.password]),
    ...(command.command === "edaCapture.deploy" && command.payload.imageDelivery === "embedded"
      ? [command.payload.registry.certificatePem, command.payload.registry.privateKeyPem]
      : []),
  ];
  try {
    if (bindings.capture === undefined) throw new EdaCaptureUnavailableError();
    if (command.command === "edaCapture.inspect") {
      const inspection = await bindings.capture.inspect(command.payload);
      bindings.recordActivity({
        correlationId,
        detail: `Found ${String(inspection.sources.length)} EDA Kafka export source(s) in ${inspection.namespace}.`,
        object,
        operation,
        outcome: "succeeded",
        severity: "info",
      });
      return {
        command: command.command,
        id: command.id,
        ok: true,
        result: { correlationId, inspection },
        version: HOST_PROTOCOL_VERSION,
      };
    }

    if (bindings.connectionActive?.())
      throw new EdaCaptureConflictError("Disconnect Kafka before starting or resuming capture.");
    if (bindings.operations?.size)
      throw new EdaCaptureConflictError(
        "Another capture operation is running. Wait for completion or cancel it first.",
      );
    const controller = new AbortController();
    bindings.operations?.set(command.id, controller);
    let deployment;
    try {
      deployment = await bindings.capture.deploy(
        command.payload,
        (phase, detail) => {
          bindings.publishProgress?.({ detail, phase, requestId: command.id });
        },
        controller.signal,
      );
    } finally {
      bindings.operations?.delete(command.id);
    }
    bindings.recordActivity({
      correlationId,
      detail: `Deployed ${deployment.workloadName}, cloned ${deployment.exporterName}, and forwarded ${deployment.broker}.`,
      object,
      operation,
      outcome: "succeeded",
      severity: "info",
    });
    return {
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId, deployment },
      version: HOST_PROTOCOL_VERSION,
    };
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
        sensitiveValues,
      },
      true,
    );
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object,
      operation,
      outcome: "failed",
      sensitiveValues,
      severity: "error",
    });
    return failureResponse(command, translated.error);
  }
}
