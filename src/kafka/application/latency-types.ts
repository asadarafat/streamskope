import type {
  HostTextDocument,
  KafkaLatencyHistorySnapshot,
  KafkaLatencyProbeEvidence,
  KafkaLatencyProbeIssue,
  KafkaLatencyProbeRequest,
} from "../contracts";

export interface KafkaLatencyFetchSample {
  readonly broker: string;
  readonly durationMs: number;
  readonly nodeId: number;
}

export interface KafkaLatencyProbeMeasurement {
  readonly endToEndDurationsMs: readonly number[];
  readonly fetchSamples: readonly KafkaLatencyFetchSample[];
  readonly issues: readonly KafkaLatencyProbeIssue[];
  readonly network: {
    readonly endpoint: string;
    readonly tcpConnectMs: number | null;
    readonly tlsHandshakeMs: number | null;
  };
  readonly observedSampleIds: readonly string[];
  readonly producerDurationsMs: readonly number[];
}

export interface KafkaLatencyProbeSessionPort {
  activeConnectionContext(): {
    readonly connectionBrokers: readonly string[];
    readonly connectionName: string;
    readonly connectionTarget: string;
  } | null;
  runLatencyProbe(
    request: KafkaLatencyProbeRequest,
    runId: string,
    signal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement>;
}

export interface KafkaLatencyProbeServiceOptions {
  readonly createRunId?: () => string;
  readonly now?: () => Date;
}

export interface KafkaLatencyProbeResult {
  readonly evidence: KafkaLatencyProbeEvidence;
  readonly state: "partial" | "ready";
}

export interface KafkaLatencyProbeServicePort {
  activeRequest(): KafkaLatencyProbeRequest | null;
  currentEvidence(): KafkaLatencyProbeEvidence | null;
  exportDocument(): HostTextDocument;
  historySnapshot(): KafkaLatencyHistorySnapshot;
  invalidate(): void;
  staleEvidence(): KafkaLatencyProbeEvidence | null;
  start(request: KafkaLatencyProbeRequest): Promise<KafkaLatencyProbeResult>;
  stop(): Promise<KafkaLatencyProbeRequest | null>;
}
