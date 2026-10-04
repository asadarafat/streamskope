import {
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  type HostErrorCode,
  type HostErrorStage,
} from "../contracts";
import type { ObservationIssue } from "../contracts/observations";

export class ObservationOperationError extends Error {
  readonly stage: HostErrorStage;
  constructor(
    readonly code: HostErrorCode,
    summary: string,
    readonly recovery: string,
    readonly retryable: boolean,
    options: {
      readonly cause?: unknown;
      readonly stage?: HostErrorStage;
      readonly retryAfterMs?: number;
    } = {},
  ) {
    super(summary, { cause: options.cause });
    this.name = "ObservationOperationError";
    this.stage = options.stage ?? "kafka";
    this.retryAfterMs = options.retryAfterMs;
  }
  readonly retryAfterMs: number | undefined;
}

export function observationIssue(
  error: unknown,
  measurement: ObservationIssue["measurement"],
): ObservationIssue {
  // Ports return normalized structured failures. Arbitrary server errors never become UI text.
  const e =
    error instanceof Error
      ? (error as Error & {
          code?: HostErrorCode;
          stage?: HostErrorStage;
          recovery?: string;
          retryable?: boolean;
        })
      : undefined;
  if (
    e?.code &&
    HOST_ERROR_CODES.includes(e.code) &&
    e.stage &&
    HOST_ERROR_STAGES.includes(e.stage) &&
    typeof e.recovery === "string" &&
    typeof e.retryable === "boolean"
  ) {
    return {
      measurement,
      code: e.code,
      summary: e.message.slice(0, 512),
      recovery: e.recovery.slice(0, 1024),
      retryable: e.retryable,
    };
  }
  return {
    measurement,
    code: "OBSERVATION_INCOMPLETE",
    summary: "This measurement could not be read; its value is unknown.",
    recovery: "Check the selected resource, Kafka permissions and connection, then capture again.",
    retryable: true,
  };
}

export function observationAborted(
  signal: AbortSignal,
  cause?: unknown,
): ObservationOperationError {
  const timeout = signal.reason instanceof Error && signal.reason.name === "TimeoutError";
  return new ObservationOperationError(
    timeout ? "TIMEOUT" : "CANCELLED",
    timeout ? "The observation deadline expired." : "Observation collection was stopped.",
    timeout
      ? "Check Kafka response time and connectivity, then capture again."
      : "Start a new observation when ready.",
    true,
    { cause },
  );
}
