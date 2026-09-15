import type { HostError } from "./types";

export const REDPANDA_TRANSFORM_STATES = [
  "unavailable",
  "not-configured",
  "loading",
  "ready",
  "empty",
  "stale",
  "denied",
  "unsupported",
  "invalid-response",
  "not-found",
  "failed",
] as const;
export const REDPANDA_TRANSFORM_STATUSES = ["running", "inactive", "errored", "unknown"] as const;
export const REDPANDA_TRANSFORM_LIMITS = {
  environmentNames: 256,
  fieldCharacters: 1_024,
  logs: 200,
  outputTopics: 128,
  transforms: 5_000,
} as const;
export const REDPANDA_TRANSFORM_LOG_TOPIC = "_redpanda.transform_logs" as const;

export type RedpandaTransformState = (typeof REDPANDA_TRANSFORM_STATES)[number];
export type RedpandaTransformStatus = (typeof REDPANDA_TRANSFORM_STATUSES)[number];

export interface RedpandaTransformPartitionStatus {
  readonly lag: number;
  readonly nodeId: number;
  readonly partition: number;
  readonly status: RedpandaTransformStatus;
}

export interface RedpandaTransformEnvironmentVariable {
  readonly name: string;
  readonly valuePresent: boolean;
}

export interface RedpandaTransformSummary {
  readonly aggregateStatus: RedpandaTransformStatus;
  readonly compression: string;
  readonly environment: readonly RedpandaTransformEnvironmentVariable[];
  readonly inputTopic: string;
  readonly name: string;
  readonly maximumLag: number;
  readonly offset: { readonly format: string; readonly value: string } | null;
  readonly outputTopics: readonly string[];
  readonly statuses: readonly RedpandaTransformPartitionStatus[];
}

export interface RedpandaTransformInventorySnapshot {
  readonly connectionName: string | null;
  readonly endpoint: string | null;
  readonly error?: HostError;
  readonly omittedTransforms: number;
  readonly refreshedAt: string | null;
  readonly state: RedpandaTransformState;
  readonly transforms: readonly RedpandaTransformSummary[];
}

export interface RedpandaTransformDetailSnapshot {
  readonly connectionName: string | null;
  readonly endpoint: string | null;
  readonly error?: HostError;
  readonly refreshedAt: string | null;
  readonly state: RedpandaTransformState;
  readonly transform: RedpandaTransformSummary | null;
  readonly transformName: string | null;
}

export interface RedpandaTransformLogEntry {
  readonly level: string;
  readonly message: string;
  readonly offset: string;
  readonly partition: number;
  readonly timestamp: string | null;
}

export interface RedpandaTransformLogsSnapshot {
  readonly connectionName: string | null;
  readonly error?: HostError;
  readonly logs: readonly RedpandaTransformLogEntry[];
  readonly omittedLogs: number;
  readonly refreshedAt: string | null;
  readonly state: RedpandaTransformState;
  readonly transformName: string | null;
}

export function summarizeRedpandaTransformStatuses(
  statuses: readonly RedpandaTransformPartitionStatus[],
): { readonly aggregateStatus: RedpandaTransformStatus; readonly maximumLag: number } {
  const priority: Readonly<Record<RedpandaTransformStatus, number>> = {
    errored: 4,
    inactive: 3,
    unknown: 2,
    running: 1,
  };
  const aggregateStatus = statuses.reduce<RedpandaTransformStatus>(
    (current, status) => (priority[status.status] > priority[current] ? status.status : current),
    statuses.length === 0 ? "unknown" : "running",
  );
  return {
    aggregateStatus,
    maximumLag: statuses.reduce((maximum, status) => Math.max(maximum, status.lag), 0),
  };
}

export interface RedpandaTransformDeletionInput {
  readonly confirmation: string;
  readonly name: string;
}
