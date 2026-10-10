import type { HostErrorCode } from "./types";

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
  rollupMs: 300_000,
  rollupsPerSeries: 288,
} as const;

export interface ObservationInput {
  readonly sampleRecords?: boolean;
  readonly topic: string;
  readonly groupId: string | null;
  readonly thresholds: { readonly lag: number | null; readonly requestMs: number | null };
}
export interface TopicHealth {
  readonly issues?: readonly ObservationIssue[];
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
/** Safe, measurement-specific explanations; no raw server text or member identities. */
export interface ObservationIssue {
  readonly measurement: "end-offsets" | "group-offsets" | "group-members" | "records";
  readonly code: HostErrorCode;
  readonly summary: string;
  readonly recovery: string;
  readonly retryable: boolean;
}
export interface ObservationGroupHealth {
  readonly id: string;
  readonly state: string | null;
  readonly members: number | null;
  readonly offsets: readonly {
    readonly partition: number;
    readonly committedOffset: string | null;
  }[];
  readonly issues?: readonly ObservationIssue[];
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
  /** Absent in historical schema-1 files; parsers default to an empty list. */
  readonly issues?: readonly ObservationIssue[];
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
/** Desired selection only: no credential, reconnect, timer or original connection authority. */
export interface ObservationSettings {
  readonly input: ObservationInput;
  readonly connectionName: string;
  readonly clusterId: string;
  readonly topicId: string;
  readonly savedAt: number;
}
/** Direct measurements only. A bucket does not establish continuous or complete coverage. */
export interface ObservationRollup extends Pick<
  ObservationSeries,
  "clusterId" | "topicId" | "topic" | "groupId"
> {
  readonly source: "kafka-api";
  readonly bucketStart: number;
  readonly bucketEnd: number;
  readonly firstObservedAt: number;
  readonly lastObservedAt: number;
  readonly samples: number;
  readonly partial: number;
  readonly lagKnown: number;
  readonly lagMin: number | null;
  readonly lagMax: number | null;
  readonly lastLag: number | null;
  readonly requestMin: number;
  readonly requestMax: number;
  readonly lastRequest: number;
  readonly boundary: ObservationRollupBoundary;
  readonly firstSampleId: string;
  readonly lastSampleId: string;
}
export type ObservationRollupBoundary =
  | import("./observation-continuity").ObservationContinuityBreak
  | "initial"
  | "window"
  | "unverified"
  | "incomplete"
  | "group-state";
export interface LegacyObservationHistory {
  readonly schemaVersion: 1;
  readonly series: readonly ObservationSeries[];
}
export interface RetainedObservationHistory {
  readonly schemaVersion: 2;
  readonly series: readonly ObservationSeries[];
  readonly settings: ObservationSettings | null;
  readonly rollups: readonly ObservationRollup[];
}
export type ObservationHistory = LegacyObservationHistory | RetainedObservationHistory;
export type ObservationSnapshot = ObservationHistory & {
  readonly durability: "session" | "durable";
};
export type ObservationResource = Pick<
  ObservationSeries,
  "clusterId" | "topicId" | "topic" | "groupId"
>;
export function observationResources(history: ObservationHistory): readonly ObservationResource[] {
  const resources = new Map<string, ObservationResource>();
  for (const resource of [
    ...history.series,
    ...(history.schemaVersion === 2 ? history.rollups : []),
  ])
    resources.set(observationIdentity(resource), resource);
  return [...resources.values()];
}
export function emptyObservationHistory(): RetainedObservationHistory {
  return { schemaVersion: 2, series: [], settings: null, rollups: [] };
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
