/** Declared exporter gauges only; an endpoint/selector does not establish Kafka identity. */
export const EXTERNAL_METRICS = [
  {
    name: "kafka_server_replicamanager_underreplicatedpartitions",
    label: "Under-replicated partitions",
    scope: "selected broker",
    unit: "partitions",
  },
  {
    name: "kafka_controller_kafkacontroller_offlinepartitionscount",
    label: "Offline topic partitions",
    scope: "selected controller; non-internal topics",
    unit: "partitions",
  },
] as const;
export const EXTERNAL_METRIC_STATES = [
  "ready",
  "missing",
  "ambiguous",
  "annotated",
  "invalid",
  "stale",
  "future",
] as const;
export const EXTERNAL_METRIC_LIMITS = {
  deadlineMs: 15_000,
  staleMs: 45_000,
  selectorCharacters: 256,
  labels: 32,
  queryRows: 2,
  sourceWindowMs: 300_000,
  samplesPerSeries: 500,
} as const;
export interface ExternalMetricsInput {
  /** Configured URL fence: never read an old connected service after its URL was edited. */
  readonly endpoint: string;
  readonly job: string;
  readonly instance: string;
}
export interface ExternalMetric {
  readonly name: (typeof EXTERNAL_METRICS)[number]["name"];
  readonly state: (typeof EXTERNAL_METRIC_STATES)[number];
  readonly value: number | null;
  /** Original Prometheus sample time, possibly exporter-supplied; not verified scrape success. */
  readonly sampledAt: number | null;
}
export interface ExternalMetricsSnapshot extends ExternalMetricsInput {
  readonly source: "prometheus";
  readonly identity: "not-established";
  readonly evaluatedAt: number;
  readonly collectedAt: number;
  readonly metrics: readonly ExternalMetric[];
}
