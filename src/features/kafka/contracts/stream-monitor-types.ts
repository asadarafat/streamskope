import { KAFKA_MESSAGE_LIMITS, type KafkaFetchRequest } from "./types";

export const KAFKA_STREAM_MONITOR_HISTORY_LIMIT = 400 as const;
/** One canonical batch or less of remaining retained-byte capacity is current pressure. */
export const KAFKA_STREAM_QUEUE_BYTE_PRESSURE_THRESHOLD =
  KAFKA_MESSAGE_LIMITS.queuedBytes - KAFKA_MESSAGE_LIMITS.batchBytes;
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

export const KAFKA_STREAM_PRESSURE_REASONS = [
  "transport",
  "count-capacity",
  "byte-capacity",
] as const;
export type KafkaStreamPressureReason = (typeof KAFKA_STREAM_PRESSURE_REASONS)[number];

/** Mutually exclusive host display omissions; these are not Kafka record loss. */
export interface KafkaStreamDropReasons {
  readonly countCapacity: number;
  readonly byteCapacity: number;
  readonly oversized: number;
  readonly terminalDiscarded: number;
}

export interface KafkaStreamQueueMetrics {
  readonly dropReasons: KafkaStreamDropReasons;
  readonly oldestMessageAgeMs: number | null;
  readonly pressureReasons: readonly KafkaStreamPressureReason[];
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
  /** Published to host event subscribers, without asserting transport or renderer receipt. */
  readonly publishedMessages: number;
  readonly rateSampledAt: string | null;
  readonly rateWindowMs: number | null;
  readonly publicationSampledAt: string | null;
  readonly queueWaitSampledAt: string | null;
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
  /** The originating messages.start command ID; null only when unavailable. */
  readonly operationId: string | null;
  readonly connectionName: string | null;
  readonly delivery: KafkaStreamDeliveryMetrics | null;
  readonly queue: KafkaStreamQueueMetrics | null;
  readonly request: KafkaFetchRequest | null;
  readonly sampledAt: string | null;
  readonly state: KafkaStreamMonitorState;
  readonly status: KafkaStreamMonitorStatus;
}
