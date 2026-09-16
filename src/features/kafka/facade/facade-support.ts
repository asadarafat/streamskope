import {
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  HOST_PROTOCOL_VERSION,
  KAFKA_FETCH_MODE_LABELS,
  type ActivityEntry,
  type ConsumptionState,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostErrorCode,
  type HostErrorStage,
  type HostEvent,
  type HostSecureConnectionInput,
  type KafkaExploredMessage,
  type KafkaFetchRequest,
  type KafkaRulePreferences,
  type KafkaStreamPreferences,
  type KafkaStreamTuningSource,
  type ProfileSummary,
  type SecureConnectionInput,
} from "../contracts";
import {
  ConnectionAttemptSupersededError,
  type KafkaConnectionSnapshot,
  NoActiveKafkaConnectionError,
} from "../application";
import { translateHostFailure } from "../../../platform/activity";

export interface StructuredOperationFailure extends Error {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target?: string;
}

export interface FailureContext {
  readonly activeStateChanged: boolean;
  readonly connection: HostSecureConnectionInput | undefined;
  readonly correlationId: string;
  readonly sensitiveValues?: readonly string[];
}

export interface ActivityInput {
  readonly correlationId: string;
  readonly detail: string;
  readonly object: string;
  readonly operation: string;
  readonly outcome: ActivityEntry["outcome"];
  readonly severity: ActivityEntry["severity"];
  readonly sensitiveValues?: readonly string[];
}

export interface ActiveFacadeConsumption {
  cancelScheduledFlush: (() => void) | undefined;
  readonly correlationId: string;
  droppedMessages: number;
  flushScheduled: boolean;
  readonly messages: QueuedFacadeMessage[];
  queuedBytes: number;
  receivedMessages: number;
  readonly request: KafkaFetchRequest;
  ruleFailureRecorded: boolean;
  state: ConsumptionState;
  readonly streamTuning: KafkaStreamPreferences & {
    readonly source: KafkaStreamTuningSource;
  };
  readonly streamMonitoring: {
    batchCount: number;
    deliveredMessages: number;
    droppedPerSecond: number | null;
    lastBatchMessages: number;
    lastMeasuredAtMs: number;
    lastPublicationDurationMs: number | null;
    lastQueueWaitMs: number | null;
    lastReportedDeliveredMessages: number;
    lastReportedDroppedMessages: number;
    messagesPerSecond: number | null;
    peakQueuedBytes: number;
    peakQueuedMessages: number;
    queueStartedAtMs: number | null;
  };
}

export interface QueuedFacadeMessage {
  readonly message: KafkaExploredMessage;
  readonly ruleOutput: KafkaRulePreferences;
}

export function defaultCorrelationId(): string {
  return globalThis.crypto.randomUUID();
}

export function defaultScheduleMessageFlush(flush: () => void, delayMs: number): () => void {
  const handle = setTimeout(flush, delayMs);
  return (): void => {
    clearTimeout(handle);
  };
}

export function backendAvailabilityEvent(
  payload:
    { readonly state: "ready" } | { readonly recovery: string; readonly state: "unavailable" },
  sequence: number,
): HostEvent {
  return {
    event: "backend.availability",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function connectionStateEvent(
  snapshot: KafkaConnectionSnapshot,
  sequence: number,
  error?: HostError,
): Extract<HostEvent, { readonly event: "connection.state" }> {
  return {
    event: "connection.state",
    payload: {
      connectionName: snapshot.connectionName,
      ...(error === undefined ? {} : { error }),
      state: snapshot.state,
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function profilesChangedEvent(
  payload: Extract<HostEvent, { readonly event: "profiles.changed" }>["payload"],
  sequence: number,
): HostEvent {
  return {
    event: "profiles.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function templatesChangedEvent(
  payload: Extract<HostEvent, { readonly event: "templates.changed" }>["payload"],
  sequence: number,
): HostEvent {
  return {
    event: "templates.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function ruleEvaluationEvent(
  payload: Extract<HostEvent, { readonly event: "rules.evaluation" }>["payload"],
  sequence: number,
): Extract<HostEvent, { readonly event: "rules.evaluation" }> {
  return {
    event: "rules.evaluation",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function rulesChangedEvent(
  payload: Extract<HostEvent, { readonly event: "rules.changed" }>["payload"],
  sequence: number,
): Extract<HostEvent, { readonly event: "rules.changed" }> {
  return {
    event: "rules.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function topicsChangedEvent(
  payload: Extract<HostEvent, { readonly event: "topics.changed" }>["payload"],
  sequence: number,
): Extract<HostEvent, { readonly event: "topics.changed" }> {
  return {
    event: "topics.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function connectionFromCommand(command: HostCommand): HostSecureConnectionInput | undefined {
  return command.command === "connection.connect" || command.command === "connection.test"
    ? command.payload
    : undefined;
}

export function failureResponse(command: HostCommand, error: HostError): HostCommandResponse {
  return {
    command: command.command,
    error,
    id: command.id,
    ok: false,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function successResponse(command: HostCommand, correlationId: string): HostCommandResponse {
  return {
    command: command.command,
    id: command.id,
    ok: true,
    result: { correlationId },
    version: HOST_PROTOCOL_VERSION,
  };
}

export function isStructuredFailure(error: unknown): error is StructuredOperationFailure {
  if (error === null || typeof error !== "object" || !(error instanceof Error)) {
    return false;
  }
  const candidate = error as Partial<StructuredOperationFailure>;
  return (
    typeof candidate.code === "string" &&
    HOST_ERROR_CODES.includes(candidate.code) &&
    typeof candidate.recovery === "string" &&
    typeof candidate.retryable === "boolean" &&
    typeof candidate.stage === "string" &&
    HOST_ERROR_STAGES.includes(candidate.stage) &&
    (candidate.target === undefined || typeof candidate.target === "string")
  );
}

export function sensitiveValues(
  connection: HostSecureConnectionInput | undefined,
): readonly string[] {
  if (connection === undefined) {
    return [];
  }
  return [
    ...(connection.oauth === undefined ? [] : [connection.oauth.clientSecret]),
    ...("caPem" in connection.tls
      ? [connection.tls.caPem, "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----"]
      : []),
  ];
}

function failureSensitiveValues(context: FailureContext): readonly string[] {
  return [...sensitiveValues(context.connection), ...(context.sensitiveValues ?? [])];
}

export function fetchDescription(request: KafkaFetchRequest): string {
  const window =
    request.mode === "time-window"
      ? ` from ${new Date(request.startTimeMs).toISOString()} to ${new Date(
          request.endTimeMs,
        ).toISOString()}`
      : "";
  return `${KAFKA_FETCH_MODE_LABELS[request.mode]} for ${request.topic} with a topic-wide maximum of ${String(request.maxMessages)}${window}`;
}

export function translateFacadeFailure(
  error: unknown,
  context: FailureContext,
  available: boolean,
): ReturnType<typeof translateHostFailure> {
  if (error instanceof ConnectionAttemptSupersededError) {
    return translateHostFailure({
      activeStateChanged: context.activeStateChanged,
      cause: error,
      code: "CANCELLED",
      correlationId: context.correlationId,
      recovery: "Continue with the newer connection operation.",
      retryable: true,
      sensitiveValues: failureSensitiveValues(context),
      stage: "internal",
      summary: "The connection operation was superseded.",
    });
  }
  if (error instanceof NoActiveKafkaConnectionError) {
    return translateHostFailure({
      activeStateChanged: false,
      cause: error,
      code: "VALIDATION",
      correlationId: context.correlationId,
      recovery: "Connect to a Kafka cluster, then retry the Kafka operation.",
      retryable: true,
      sensitiveValues: failureSensitiveValues(context),
      stage: "validation",
      summary: error.message,
      target: "active-connection",
    });
  }
  if (isStructuredFailure(error)) {
    return translateHostFailure({
      activeStateChanged: context.activeStateChanged,
      cause: error,
      code: error.code,
      correlationId: context.correlationId,
      recovery: error.recovery,
      retryable: error.retryable,
      sensitiveValues: failureSensitiveValues(context),
      stage: error.stage,
      summary: error.message,
      ...(error.target === undefined ? {} : { target: error.target }),
    });
  }
  return translateHostFailure({
    activeStateChanged: context.activeStateChanged,
    cause: error,
    code: available ? "INTERNAL" : "BACKEND_UNAVAILABLE",
    correlationId: context.correlationId,
    recovery: available
      ? "Open activity for the correlation ID and inspect the host diagnostics."
      : "Restart StreamSkope to create a new application session.",
    retryable: !available,
    sensitiveValues: failureSensitiveValues(context),
    stage: available ? "internal" : "backend",
    summary: available
      ? "The Kafka operation failed unexpectedly."
      : "The StreamSkope application host is unavailable.",
  });
}

export function profileOperation(
  command: Extract<
    HostCommand,
    {
      readonly command: "profiles.create" | "profiles.delete" | "profiles.list" | "profiles.update";
    }
  >,
): string {
  switch (command.command) {
    case "profiles.create":
      return "Create profile";
    case "profiles.delete":
      return "Delete profile";
    case "profiles.list":
      return "Load profiles";
    case "profiles.update":
      return "Update profile";
  }
}

export function profileActivityObject(
  profile:
    Pick<ProfileSummary, "brokers" | "name"> | Pick<SecureConnectionInput, "brokers" | "name">,
): string {
  const primaryBroker = profile.brokers[0] ?? "No broker";
  const remaining = profile.brokers.length - 1;
  return `${profile.name} · ${primaryBroker}${remaining > 0 ? ` +${String(remaining)} more` : ""}`;
}
