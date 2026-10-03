/** Kafka observations are bounded evidence, never a cluster-wide health verdict. */
export const OBSERVATION_LIMITS = {
  intervalMs: 10_000,
  deadlineMs: 15_000,
  staleMs: 45_000,
  retentionMs: 86_400_000,
  series: 8,
  samples: 240,
  partitions: 128,
  fileBytes: 4 * 1_048_576,
  sampleRecords: 200,
  sampleBytes: 2 * 1_048_576,
} as const;

export interface ObservationInput {
  readonly sampleRecords?: boolean;
  readonly topic: string;
  readonly groupId: string | null;
  readonly thresholds: { readonly lag: number | null; readonly requestMs: number | null };
}
export interface TopicHealth {
  readonly clusterId: string;
  readonly topicId: string;
  readonly topic: string;
  readonly brokerCount: number;
  readonly controllerKnown: boolean;
  readonly partitions: readonly {
    readonly partition: number;
    readonly leader: number | null;
    readonly replicas: number;
    readonly inSyncReplicas: number;
    readonly endOffset: string | null;
  }[];
}
export interface ObservationPartition {
  readonly partition: number;
  readonly leader: number | null;
  readonly replicas: number;
  readonly inSyncReplicas: number;
  readonly endOffset: string | null;
  readonly committedOffset: string | null;
  readonly lag: string | null;
}
export interface KafkaObservation {
  readonly id: string;
  readonly segmentId: string;
  readonly startedAt: number;
  readonly observedAt: number;
  readonly source: "kafka-api";
  readonly requestMs: number;
  readonly providerCalls: number;
  readonly state: "ready" | "partial";
  readonly groupState: string | null;
  readonly members: number | null;
  readonly brokerCount: number;
  readonly controllerKnown: boolean;
  readonly groupCoverage: "complete" | "partial" | "unavailable" | "not-selected";
  readonly partitions: readonly ObservationPartition[];
  readonly records: import("./observation-records").ObservationRecords | null;
  readonly alerts: readonly {
    readonly metric: "lag" | "requestMs";
    readonly observed: number;
    readonly threshold: number;
  }[];
}
export interface ObservationSeries {
  readonly clusterId: string;
  readonly topicId: string;
  readonly topic: string;
  readonly groupId: string | null;
  readonly samples: readonly KafkaObservation[];
}
export interface ObservationHistory {
  readonly schemaVersion: 1;
  readonly series: readonly ObservationSeries[];
}
export interface ObservationSnapshot extends ObservationHistory {
  readonly durability: "session" | "durable";
}
export interface ObservationCapture {
  readonly series: ObservationSeries;
  readonly durability: "session" | "durable";
}
export function observationIdentity(
  series: Pick<ObservationSeries, "clusterId" | "topicId" | "topic" | "groupId">,
): string {
  return JSON.stringify([series.clusterId, series.topicId, series.topic, series.groupId]);
}
export function observationLag(sample: KafkaObservation): number | null {
  if (sample.groupCoverage !== "complete" || sample.partitions.some((p) => p.lag === null))
    return null;
  const total = sample.partitions.reduce((n, p) => n + BigInt(p.lag!), 0n);
  return total <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(total) : null;
}
