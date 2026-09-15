import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaLatencyHistorySnapshot,
  type KafkaLatencyProbeEvidence,
  type KafkaLatencySnapshot,
} from "../contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaLatencyProbeService,
  type KafkaApplicationSession,
  type KafkaLatencyProbeServicePort,
} from "../application";

import type { KafkaBackendFacadeOptions } from "./types";
import {
  failureResponse,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
  type StructuredOperationFailure,
} from "./facade-support";

export type LatencyHostCommand = Extract<
  HostCommand,
  {
    readonly command: "latency.export" | "latency.start" | "latency.stop";
  }
>;

export interface LatencyFacadeBindings {
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly service: KafkaLatencyProbeServicePort;
}

export function createLatencyService(
  session: KafkaApplicationSession,
  options: KafkaBackendFacadeOptions,
): KafkaLatencyProbeServicePort {
  return (
    options.latencyProbe ??
    new KafkaLatencyProbeService(session, {
      ...(options.now === undefined ? {} : { now: options.now }),
    })
  );
}

export function latencyEvent(
  payload: KafkaLatencySnapshot,
  sequence: number,
): Extract<HostEvent, { readonly event: "latency.changed" }> {
  return {
    event: "latency.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function latencyHistoryEvent(
  payload: KafkaLatencyHistorySnapshot,
  sequence: number,
): Extract<HostEvent, { readonly event: "latency.history.changed" }> {
  return {
    event: "latency.history.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function invalidateLatencyEvent(
  service: KafkaLatencyProbeServicePort,
  sequence: number,
): Extract<HostEvent, { readonly event: "latency.changed" }> {
  service.invalidate();
  const stale = service.staleEvidence();
  return latencyEvent(
    stale === null
      ? { evidence: null, request: null, state: "unavailable" }
      : { evidence: stale, request: null, state: "stale" },
    sequence,
  );
}

function publish(bindings: LatencyFacadeBindings, payload: KafkaLatencySnapshot): void {
  bindings.publish(latencyEvent(payload, bindings.nextSequence()));
}

function activityObject(evidence: KafkaLatencyProbeEvidence): string {
  return `${evidence.connection.name} · ${evidence.topic}`;
}

function cancellationFailure(topic: string): StructuredOperationFailure {
  return Object.assign(new Error("The Kafka latency probe was cancelled."), {
    code: "CANCELLED" as const,
    recovery: "Run another bounded latency probe when the connection is stable.",
    retryable: true,
    stage: "kafka" as const,
    target: topic,
  });
}

function isCancellation(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.name === "AbortError" || ("code" in error && error.code === "CANCELLED"))
  );
}

function exportResponse(
  command: LatencyHostCommand,
  correlationId: string,
  document: ReturnType<KafkaLatencyProbeServicePort["exportDocument"]>,
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

export async function executeLatencyCommand(
  command: LatencyHostCommand,
  correlationId: string,
  bindings: LatencyFacadeBindings,
): Promise<HostCommandResponse> {
  if (command.command === "latency.stop") {
    try {
      const stopped = await bindings.service.stop();
      bindings.recordActivity({
        correlationId,
        detail:
          stopped === null
            ? "No active Kafka latency probe remained to stop."
            : `Stopped the ${String(stopped.messageCount)}-record latency probe and waited for owned resources to close.`,
        object: stopped?.topic ?? "No active latency probe",
        operation: "Stop latency probe",
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
        true,
      );
      bindings.recordActivity({
        correlationId,
        detail: translated.detail,
        object: bindings.service.activeRequest()?.topic ?? "Kafka latency probe",
        operation: "Stop latency probe",
        outcome: "failed",
        severity: "error",
      });
      return failureResponse(command, translated.error);
    }
  }

  if (command.command === "latency.export") {
    try {
      const evidence = bindings.service.currentEvidence();
      const document = bindings.service.exportDocument();
      bindings.recordActivity({
        correlationId,
        detail: `Produced a ${String(document.byteSize)}-byte JSON document from the current latency snapshot.`,
        object: evidence === null ? "Kafka latency evidence" : activityObject(evidence),
        operation: "Export latency evidence",
        outcome: "succeeded",
        severity: "info",
      });
      return exportResponse(command, correlationId, document);
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
        object: "Kafka latency evidence",
        operation: "Export latency evidence",
        outcome: "failed",
        severity: "error",
      });
      return failureResponse(command, translated.error);
    }
  }

  const startCommand = command as Extract<HostCommand, { readonly command: "latency.start" }>;
  const existingRequest = bindings.service.activeRequest();
  if (existingRequest === null) {
    publish(bindings, {
      evidence: null,
      request: startCommand.payload,
      state: "running",
    });
  }
  try {
    const result = await bindings.service.start(startCommand.payload);
    publish(bindings, {
      evidence: result.evidence,
      request: null,
      state: result.state,
    });
    bindings.publish(
      latencyHistoryEvent(bindings.service.historySnapshot(), bindings.nextSequence()),
    );
    bindings.recordActivity({
      correlationId,
      detail: `${String(result.evidence.observedMessages)} of ${String(
        result.evidence.requestedMessages,
      )} synthetic probe records were observed; ${String(
        result.evidence.issues.length,
      )} metric issue(s) were reported.`,
      object: activityObject(result.evidence),
      operation: "Run latency probe",
      outcome: "succeeded",
      severity: result.state === "ready" ? "info" : "warning",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const superseded = error instanceof ConnectionAttemptSupersededError;
    const cancelled = isCancellation(error);
    const overlapping = existingRequest !== null;
    const translated = translateFacadeFailure(
      cancelled ? cancellationFailure(startCommand.payload.topic) : error,
      {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      },
      true,
    );
    if (!superseded && !overlapping) {
      publish(bindings, {
        error: translated.error,
        evidence: null,
        request: startCommand.payload,
        state: cancelled ? "cancelled" : "failed",
      });
    }
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object: startCommand.payload.topic,
      operation: "Run latency probe",
      outcome: superseded || cancelled ? "cancelled" : "failed",
      severity: superseded || cancelled || overlapping ? "warning" : "error",
    });
    return failureResponse(command, translated.error);
  }
}
