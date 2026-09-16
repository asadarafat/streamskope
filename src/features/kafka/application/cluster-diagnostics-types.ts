import type {
  HostTextDocument,
  KafkaClusterDetailsDocument,
  KafkaClusterProfileContext,
  KafkaConfigurationEntry,
} from "../contracts";

import type { KafkaClusterMetadata } from "./types";

export interface KafkaClusterDiagnosticsSessionPort {
  activeConnectionContext(): {
    readonly connectionBrokers: readonly string[];
    readonly connectionName: string;
    readonly connectionTarget: string;
  } | null;
  describeBrokerConfiguration(
    brokerId: number,
    signal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]>;
  describeClusterMetadata(signal?: AbortSignal): Promise<KafkaClusterMetadata>;
}

export interface KafkaClusterDiagnosticsServiceOptions {
  readonly now?: () => Date;
}

export interface KafkaClusterDiagnosticsLoadResult {
  readonly document: KafkaClusterDetailsDocument;
  readonly state: "partial" | "ready";
}

export interface KafkaClusterDiagnosticsServicePort {
  clear(): void;
  currentDocument(): KafkaClusterDetailsDocument | null;
  exportDocument(): HostTextDocument;
  load(
    profile: KafkaClusterProfileContext,
    signal?: AbortSignal,
  ): Promise<KafkaClusterDiagnosticsLoadResult>;
  staleDocument(): KafkaClusterDetailsDocument | null;
}
