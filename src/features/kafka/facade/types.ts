import type {
  KafkaQueryLibrary,
  KafkaClusterDiagnosticsServicePort,
  KafkaLatencyProbeServicePort,
  KafkaOperationalPreferenceService,
  KafkaTrustAcquisitionServicePort,
  SchemaRegistryPort,
  RedpandaTransformPort,
} from "../application";

export interface KafkaBackendFacadeOptions {
  readonly replayConnections?: import("../application").KafkaConnectionPort;
  readonly sampleGenerator?: import("../application/record-codec-types").SchemaSamplePort;
  readonly recordCodec?: import("../application/record-codec-types").RecordCodecPort;
  readonly schemaLookup?: import("../application/record-codec-types").SchemaLookupPort;
  readonly queries?: KafkaQueryLibrary;
  readonly clusterDiagnostics?: KafkaClusterDiagnosticsServicePort;
  readonly createCorrelationId?: () => string;
  readonly plugins?: import("../../../plugins/api").PluginRuntimePort;
  readonly latencyProbe?: KafkaLatencyProbeServicePort;
  readonly monotonicNow?: () => number;
  readonly now?: () => Date;
  readonly preferences?: KafkaOperationalPreferenceService;
  readonly scheduleMessageFlush?: (flush: () => void, delayMs: number) => (() => void) | void;
  readonly schemaRegistry?: SchemaRegistryPort;
  readonly transforms?: RedpandaTransformPort;
  readonly trustAcquisitions?: KafkaTrustAcquisitionServicePort;
}
