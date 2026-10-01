export const STREAM_REPLAY_QUALIFICATION_BUDGET = Object.freeze({
  maximumCpuPercentOneCore: 60,
  maximumPeakRssBytes: 768 * 1024 * 1024,
  maximumEventLoopP99Ms: 150,
  maximumDisplayDropRatio: 0.02,
  maximumIngestToReducerP95Ms: 1000,
  maximumIngestToReducerP99Ms: 2000,
  minimumGeneratedRatio: 0.99,
});

export interface StreamReplayQualificationMetrics {
  readonly boundsPassed: boolean;
  readonly cpuPercentOneCore: number;
  readonly generated: number;
  readonly hostDisplayDrops: number;
  readonly ingestToReducerP95Ms: number;
  readonly ingestToReducerP99Ms: number;
  readonly offered: number;
  readonly peakRss: number;
  readonly eventLoopP99Ms: number;
}

export interface StreamReplayQualificationResult {
  readonly failures: readonly string[];
  readonly passed: boolean;
}

export function evaluateStreamReplayQualification(
  metrics: StreamReplayQualificationMetrics,
): StreamReplayQualificationResult {
  const failures: string[] = [];
  const budget = STREAM_REPLAY_QUALIFICATION_BUDGET;
  for (const [name, value] of Object.entries(metrics)) {
    if (typeof value === "number" && (!Number.isFinite(value) || value < 0)) {
      failures.push(`${name} was not a finite non-negative measurement`);
    }
  }
  const generatedRatio = metrics.offered === 0 ? 0 : metrics.generated / metrics.offered;
  const displayDropRatio =
    metrics.generated === 0 ? 1 : metrics.hostDisplayDrops / metrics.generated;

  if (!metrics.boundsPassed) failures.push("application queue or retained-history bound failed");
  if (generatedRatio < budget.minimumGeneratedRatio) {
    failures.push("generated/offered throughput fell below the minimum ratio");
  }
  if (metrics.cpuPercentOneCore > budget.maximumCpuPercentOneCore) {
    failures.push("CPU exceeded the one-core percentage budget");
  }
  if (metrics.peakRss > budget.maximumPeakRssBytes) {
    failures.push("peak resident memory exceeded the budget");
  }
  if (metrics.eventLoopP99Ms > budget.maximumEventLoopP99Ms) {
    failures.push("event-loop p99 exceeded the budget");
  }
  if (displayDropRatio > budget.maximumDisplayDropRatio) {
    failures.push("accounted display drops exceeded the budget");
  }
  if (metrics.ingestToReducerP95Ms > budget.maximumIngestToReducerP95Ms) {
    failures.push("ingest-to-reducer p95 exceeded the budget");
  }
  if (metrics.ingestToReducerP99Ms > budget.maximumIngestToReducerP99Ms) {
    failures.push("ingest-to-reducer p99 exceeded the budget");
  }

  return Object.freeze({ passed: failures.length === 0, failures: Object.freeze(failures) });
}
