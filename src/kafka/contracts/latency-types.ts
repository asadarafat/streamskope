import type { HostTextDocument } from "./text-document-types";
import type { HostError } from "./types";

export const KAFKA_LATENCY_ACKNOWLEDGEMENTS = [-1, 0, 1] as const;
export const KAFKA_LATENCY_STATES = [
  "unavailable",
  "idle",
  "running",
  "ready",
  "partial",
  "cancelled",
  "failed",
  "stale",
] as const;
export const KAFKA_LATENCY_ISSUE_STAGES = [
  "tcp",
  "tls",
  "produce",
  "fetch",
  "end-to-end",
  "cleanup",
] as const;
export const KAFKA_LATENCY_SCHEMA = "streamskope.kafka-latency.v1" as const;
export const KAFKA_LATENCY_HISTORY_LIMIT = 20 as const;
export const KAFKA_LATENCY_LIMITS = {
  brokerMetrics: 1_024,
  defaultMessageCount: 20,
  defaultTimeoutMs: 10_000,
  endpointCharacters: 16_384,
  exportBytes: 1_048_576,
  fileNameCharacters: 255,
  identifierCharacters: 128,
  issues: KAFKA_LATENCY_ISSUE_STAGES.length,
  maxDurationMs: 3_600_000,
  maxMessageCount: 200,
  maxMetricSamples: 4_096,
  maxSampleIds: 10,
  maxTimeoutMs: 60_000,
  minMessageCount: 1,
  minTimeoutMs: 1_000,
  profileNameCharacters: 256,
  snapshotBytes: 1_048_576,
  topicCharacters: 512,
} as const;

export type KafkaLatencyAcknowledgements = (typeof KAFKA_LATENCY_ACKNOWLEDGEMENTS)[number];
export type KafkaLatencyState = (typeof KAFKA_LATENCY_STATES)[number];
export type KafkaLatencyIssueStage = (typeof KAFKA_LATENCY_ISSUE_STAGES)[number];

export interface KafkaLatencyProbeRequest {
  readonly acknowledgements: KafkaLatencyAcknowledgements;
  readonly messageCount: number;
  readonly timeoutMs: number;
  readonly topic: string;
}

export interface KafkaLatencyMetricSummary {
  readonly averageMs: number;
  readonly p95Ms: number;
  readonly samples: number;
}

export interface KafkaLatencyBrokerMetric {
  readonly broker: string;
  readonly nodeId: number;
  readonly summary: KafkaLatencyMetricSummary;
}

export interface KafkaLatencyProbeIssue {
  readonly recovery: string;
  readonly stage: KafkaLatencyIssueStage;
  readonly summary: string;
}

export interface KafkaLatencyProbeEvidence {
  readonly acknowledgements: KafkaLatencyAcknowledgements;
  readonly completedAt: string;
  readonly connection: {
    readonly endpoint: string;
    readonly name: string;
  };
  readonly endToEnd: KafkaLatencyMetricSummary | null;
  readonly fetch: {
    readonly perBroker: readonly KafkaLatencyBrokerMetric[];
    readonly summary: KafkaLatencyMetricSummary | null;
  };
  readonly issues: readonly KafkaLatencyProbeIssue[];
  readonly network: {
    readonly endpoint: string;
    readonly tcpConnectMs: number | null;
    readonly tlsHandshakeMs: number | null;
  };
  readonly observedMessages: number;
  readonly producer: {
    readonly semantics: "acknowledged" | "send-completion";
    readonly summary: KafkaLatencyMetricSummary | null;
  };
  readonly requestedMessages: number;
  readonly runId: string;
  readonly sampleIds: readonly string[];
  readonly schema: typeof KAFKA_LATENCY_SCHEMA;
  readonly startedAt: string;
  readonly topic: string;
}

export type KafkaLatencySnapshot =
  | {
      readonly evidence: null;
      readonly request: null;
      readonly state: "idle" | "unavailable";
    }
  | {
      readonly evidence: null;
      readonly request: KafkaLatencyProbeRequest;
      readonly state: "running";
    }
  | {
      readonly evidence: KafkaLatencyProbeEvidence;
      readonly request: null;
      readonly state: "partial" | "ready" | "stale";
    }
  | {
      readonly error: HostError;
      readonly evidence: null;
      readonly request: KafkaLatencyProbeRequest;
      readonly state: "cancelled" | "failed";
    };

export interface KafkaLatencyExportResult {
  readonly correlationId: string;
  readonly document: HostTextDocument;
}

export interface KafkaLatencyHistoryMetric {
  readonly averageMs: number;
  readonly p95Ms: number;
}

export interface KafkaLatencyHistoryEntry {
  readonly acknowledgements: KafkaLatencyAcknowledgements;
  readonly completedAt: string;
  readonly endToEnd: KafkaLatencyHistoryMetric | null;
  readonly fetch: KafkaLatencyHistoryMetric | null;
  readonly issueCount: number;
  readonly observedMessages: number;
  readonly producer: KafkaLatencyHistoryMetric | null;
  readonly requestedMessages: number;
  readonly runId: string;
  readonly state: "partial" | "ready";
  readonly topic: string;
}

export interface KafkaLatencyHistorySnapshot {
  readonly connectionName: string | null;
  readonly entries: readonly KafkaLatencyHistoryEntry[];
}
