import {
  HostContractValidationError,
  KAFKA_FETCH_LIMITS,
  parseKafkaQueryTimestamp,
} from "../contracts";

export interface KafkaTimeWindowDraft {
  readonly mode: "recent" | "custom";
  readonly start: string;
  readonly end: string;
}

export function initialKafkaTimeWindow(): KafkaTimeWindowDraft {
  const now = Date.now();
  return {
    mode: "recent",
    start: new Date(now - KAFKA_FETCH_LIMITS.defaultTimeWindowMs).toISOString(),
    end: new Date(now).toISOString(),
  };
}

export function resolveKafkaTimeWindow(
  draft: KafkaTimeWindowDraft,
  now = Date.now(),
): { readonly startTimeMs: number; readonly endTimeMs: number } {
  if (draft.mode === "recent") {
    return {
      startTimeMs: Math.max(0, now - KAFKA_FETCH_LIMITS.defaultTimeWindowMs),
      endTimeMs: now,
    };
  }
  const startTimeMs = parseKafkaQueryTimestamp(draft.start, "Start time");
  const endTimeMs = parseKafkaQueryTimestamp(draft.end, "End time");
  if (endTimeMs <= startTimeMs) {
    throw new HostContractValidationError("End time", "must be after start time");
  }
  return { startTimeMs, endTimeMs };
}

export function kafkaTimeWindowError(draft: KafkaTimeWindowDraft): string | undefined {
  try {
    resolveKafkaTimeWindow(draft);
    return undefined;
  } catch (error) {
    return error instanceof HostContractValidationError ? error.message : "Enter a valid interval.";
  }
}
