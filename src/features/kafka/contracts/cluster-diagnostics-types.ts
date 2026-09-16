import type { KafkaConfigurationEntry } from "./configuration-types";
import type { HostTextDocument } from "./text-document-types";
import type { HostError } from "./types";

export const KAFKA_CLUSTER_DIAGNOSTIC_STATES = [
  "unavailable",
  "loading",
  "ready",
  "partial",
  "failed",
  "stale",
] as const;

export const KAFKA_CLUSTER_CONFIGURATION_ISSUE_CODES = [
  "authorization-denied",
  "no-brokers",
  "unavailable",
] as const;

export const KAFKA_CLUSTER_DIAGNOSTIC_LIMITS = {
  brokerHostCharacters: 512,
  brokers: 4_096,
  clusterIdCharacters: 512,
  endpointCharacters: 16_384,
  exportBytes: 8 * 1_048_576,
  fileNameCharacters: 255,
  profileBrokers: 32,
  profileIdCharacters: 128,
  profileNameCharacters: 256,
  snapshotBytes: 8 * 1_048_576,
} as const;

export type KafkaClusterDiagnosticState = (typeof KAFKA_CLUSTER_DIAGNOSTIC_STATES)[number];
export type KafkaClusterConfigurationIssueCode =
  (typeof KAFKA_CLUSTER_CONFIGURATION_ISSUE_CODES)[number];

export interface KafkaClusterProfileContext {
  readonly brokers: readonly string[];
  readonly id: string | null;
  readonly name: string;
}

export interface KafkaClusterBroker {
  readonly host: string;
  readonly nodeId: number;
  readonly port: number;
  readonly rack: string | null;
}

export interface KafkaClusterConfigurationIssue {
  readonly code: KafkaClusterConfigurationIssueCode;
  readonly recovery: string;
  readonly summary: string;
}

export interface KafkaClusterDetails {
  readonly brokers: readonly KafkaClusterBroker[];
  readonly clusterId: string | null;
  readonly configuration: readonly KafkaConfigurationEntry[];
  readonly configurationIssue?: KafkaClusterConfigurationIssue;
  readonly configurationSourceBrokerId: number | null;
  readonly controllerId: number | null;
}

export interface KafkaClusterDetailsDocument {
  readonly cluster: KafkaClusterDetails;
  readonly endpoint: string;
  readonly fetchedAt: string;
  readonly profile: KafkaClusterProfileContext;
}

export type KafkaClusterDiagnosticsSnapshot =
  | {
      readonly cluster: null;
      readonly endpoint: null;
      readonly fetchedAt: null;
      readonly profile: null;
      readonly state: "unavailable";
    }
  | {
      readonly cluster: null;
      readonly endpoint: string;
      readonly fetchedAt: null;
      readonly profile: KafkaClusterProfileContext;
      readonly state: "loading";
    }
  | (KafkaClusterDetailsDocument & {
      readonly state: "ready" | "partial";
    })
  | {
      readonly cluster: null;
      readonly endpoint: string;
      readonly error: HostError;
      readonly fetchedAt: null;
      readonly profile: KafkaClusterProfileContext;
      readonly state: "failed";
    }
  | (KafkaClusterDetailsDocument & {
      readonly error: HostError;
      readonly state: "stale";
    });

export interface KafkaClusterDetailsExportResult {
  readonly correlationId: string;
  readonly document: HostTextDocument;
}
