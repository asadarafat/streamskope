import type {
  KafkaClusterDiagnosticsServicePort,
  KafkaLatencyProbeServicePort,
  KafkaOperationalPreferenceService,
  KafkaTrustAcquisitionServicePort,
  SchemaRegistryPort,
  RedpandaTransformPort,
} from "../application";

export interface KafkaBackendFacadeOptions {
  readonly clusterDiagnostics?: KafkaClusterDiagnosticsServicePort;
  readonly createCorrelationId?: () => string;
  readonly latencyProbe?: KafkaLatencyProbeServicePort;
  readonly monotonicNow?: () => number;
  readonly now?: () => Date;
  readonly preferences?: KafkaOperationalPreferenceService;
  readonly scheduleMessageFlush?: (flush: () => void, delayMs: number) => (() => void) | void;
  readonly schemaRegistry?: SchemaRegistryPort;
  readonly transforms?: RedpandaTransformPort;
  readonly trustAcquisitions?: KafkaTrustAcquisitionServicePort;
}
