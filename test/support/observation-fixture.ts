import type {
  KafkaObservation,
  ObservationSeries,
} from "../../src/features/kafka/contracts/observations";

export const OBSERVED_AT = 1_800_000_000_000;
export function observation(
  index: number,
  overrides: Partial<KafkaObservation> = {},
): KafkaObservation {
  return {
    id: `sample-${index}`,
    segmentId: "process",
    startedAt: OBSERVED_AT + index * 10_000,
    observedAt: OBSERVED_AT + index * 10_000 + 20,
    source: "kafka-api",
    requestMs: 20,
    providerCalls: 2,
    state: "ready",
    groupState: "stable",
    members: 1,
    brokerCount: 1,
    controllerKnown: true,
    groupCoverage: "complete",
    partitions: [
      {
        partition: 0,
        leader: 1,
        replicas: 1,
        inSyncReplicas: 1,
        endOffset: String(1000 + index * 20),
        committedOffset: String(900 + index * 10),
        lag: String(100 + index * 10),
      },
    ],
    records: null,
    alerts: [],
    ...overrides,
  };
}
export function observationSeries(samples: readonly KafkaObservation[]): ObservationSeries {
  return {
    clusterId: "fixture-cluster",
    topicId: "fixture-topic",
    topic: "events",
    groupId: "workers",
    samples,
  };
}
