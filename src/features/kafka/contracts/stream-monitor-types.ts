import type { KafkaFetchRequest } from "./types";

export const KAFKA_STREAM_MONITOR_HISTORY_LIMIT = 400 as const;
export const KAFKA_STREAM_TUNING_SOURCES = ["confirmed", "factory-fallback"] as const;

export const KAFKA_STREAM_MONITOR_STATES = [
  "unavailable",
  "loading",
  "fetching",
  "streaming",
  "complete",
  "stopped",
  "empty",
  "failed",
  "stale",
] as const;

export const KAFKA_STREAM_MONITOR_STATUSES = [
  "unavailable",
  "idle",
  "nominal",
  "backpressure",
  "degraded",
  "stale",
] as const;

export type KafkaStreamMonitorState = (typeof KAFKA_STREAM_MONITOR_STATES)[number];
export type KafkaStreamMonitorStatus = (typeof KAFKA_STREAM_MONITOR_STATUSES)[number];
export type KafkaStreamTuningSource = (typeof KAFKA_STREAM_TUNING_SOURCES)[number];

export interface KafkaStreamQueueMetrics {
  readonly capacityBytes: number;
  readonly capacityMessages: number;
  readonly currentBytes: number;
  readonly currentMessages: number;
  readonly droppedMessages: number;
  readonly droppedPerSecond: number | null;
  readonly droppedSincePrevious: number;
  readonly peakBytes: number;
  readonly peakMessages: number;
}

export interface KafkaStreamDeliveryMetrics {
  readonly batchCount: number;
  readonly batchSize: number;
  readonly deliveredMessages: number;
  readonly historySamples: number;
  readonly intervalMs: number;
  readonly lastBatchMessages: number;
  readonly messagesPerSecond: number | null;
  readonly publicationDurationMs: number | null;
  readonly queueWaitMs: number | null;
  readonly receivedMessages: number;
  readonly tuningSource: KafkaStreamTuningSource;
}

export interface KafkaStreamMonitorSnapshot {
  readonly connectionName: string | null;
  readonly delivery: KafkaStreamDeliveryMetrics | null;
  readonly queue: KafkaStreamQueueMetrics | null;
  readonly request: KafkaFetchRequest | null;
  readonly sampledAt: string | null;
  readonly state: KafkaStreamMonitorState;
  readonly status: KafkaStreamMonitorStatus;
}
