import type { HostError, HostErrorCode, HostErrorStage } from "../../kafka/contracts";

import { ACTIVITY_DETAIL_CHARACTER_LIMIT, redactSensitiveText } from "./redaction";

export interface HostFailureInput {
  readonly activeStateChanged: boolean;
  readonly cause?: unknown;
  readonly code: HostErrorCode;
  readonly correlationId: string;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly sensitiveValues?: readonly string[];
  readonly stage: HostErrorStage;
  readonly summary: string;
  readonly target?: string;
}

export interface TranslatedHostFailure {
  readonly detail: string;
  readonly error: HostError;
}

function causeMessage(cause: unknown): string {
  if (cause instanceof Error) {
    return cause.message;
  }
  if (typeof cause === "string") {
    return cause;
  }
  if (cause === undefined) {
    return "No upstream detail was provided.";
  }
  try {
    return JSON.stringify(cause);
  } catch {
    return "The upstream failure detail could not be represented.";
  }
}

function stageLabel(stage: HostErrorStage): string {
  switch (stage) {
    case "backend":
      return "Backend";
    case "broker":
      return "Broker";
    case "internal":
      return "Internal";
    case "kafka":
      return "Kafka";
    case "oauth":
      return "OAuth";
    case "profile":
      return "Profile";
    case "preference":
      return "Operational preferences";
    case "storage":
      return "Storage";
    case "template":
      return "Connection templates";
    case "tls":
      return "TLS";
    case "trust":
      return "Trust material";
    case "authorization":
      return "Authorization";
    case "validation":
      return "Validation";
    case "rule":
      return "Kafka rules";
    case "ssh":
      return "SSH";
    case "remote-command":
      return "Remote command";
    case "remote-transfer":
      return "Remote transfer";
    case "acquisition":
      return "Remote trust acquisition";
  }
}

export function translateHostFailure(input: HostFailureInput): TranslatedHostFailure {
  const sensitiveValues = input.sensitiveValues ?? [];
  const summary = redactSensitiveText(input.summary, sensitiveValues, 2_048);
  const recovery = redactSensitiveText(input.recovery, sensitiveValues, 2_048);
  const target =
    input.target === undefined
      ? undefined
      : redactSensitiveText(input.target, sensitiveValues, 2_048);
  const errorBase = {
    activeStateChanged: input.activeStateChanged,
    code: input.code,
    correlationId: input.correlationId,
    recovery,
    retryable: input.retryable,
    stage: input.stage,
    summary,
  };
  const error: HostError = target === undefined ? errorBase : { ...errorBase, target };
  const detailParts = [
    `Stage: ${stageLabel(input.stage)}`,
    `Category: ${input.code}`,
    ...(target === undefined ? [] : [`Target: ${target}`]),
    `Active connection changed: ${input.activeStateChanged ? "Yes" : "No"}`,
    `Next action: ${recovery}`,
    `Cause: ${redactSensitiveText(
      causeMessage(input.cause),
      sensitiveValues,
      ACTIVITY_DETAIL_CHARACTER_LIMIT,
    )}`,
  ];

  return {
    detail: redactSensitiveText(
      detailParts.join("\n"),
      sensitiveValues,
      ACTIVITY_DETAIL_CHARACTER_LIMIT,
    ),
    error,
  };
}
