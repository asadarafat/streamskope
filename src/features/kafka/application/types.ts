import type {
  ConnectionState,
  KafkaClusterBroker,
  KafkaAclBinding,
  KafkaConfigurationEntry,
  KafkaConsumerGroupDetails,
  KafkaConsumerGroupSummary,
  KafkaFetchRequest,
  KafkaLatencyProbeRequest,
  KafkaMessage,
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
  SecureConnectionInput,
} from "../contracts";

import type { KafkaLatencyProbeMeasurement } from "./latency-types";

export type ConnectionCheck = "oauth" | "tls" | "kafka-authentication" | "metadata";

export interface KafkaConnectionTestResult {
  readonly checks: readonly ConnectionCheck[];
  readonly topicCount: number;
}

export interface KafkaMessageStream extends AsyncIterable<KafkaMessage> {
  close(): Promise<void>;
}

export interface KafkaClusterMetadata {
  readonly brokers: readonly KafkaClusterBroker[];
  readonly clusterId: string | null;
  readonly controllerId: number | null;
}

export interface KafkaConsumerGroupInventory {
  readonly groups: readonly KafkaConsumerGroupSummary[];
  readonly omittedGroups: number;
}

export interface KafkaConsumptionObserver {
  onComplete(): void;
  onEmpty(): void;
  onFailure(error: unknown): void;
  onMessage(message: KafkaMessage): void;
}

export interface KafkaActiveConnection {
  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
    signal?: AbortSignal,
  ): Promise<void>;
  close(): Promise<void>;
  createAcl?(acl: KafkaAclBinding, signal?: AbortSignal): Promise<void>;
  deleteAcl?(acl: KafkaAclBinding, signal?: AbortSignal): Promise<void>;
  clusterServiceContext?(
    service: "redpandaAdmin" | "schemaRegistry",
  ): KafkaClusterServiceContext | null;
  describeBrokerConfiguration(
    brokerId: number,
    signal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]>;
  describeClusterMetadata(signal?: AbortSignal): Promise<KafkaClusterMetadata>;
  describeConsumerGroup?(groupId: string, signal?: AbortSignal): Promise<KafkaConsumerGroupDetails>;
  describeTopicConfiguration(
    topic: string,
    signal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]>;
  listTopics(signal?: AbortSignal): Promise<readonly string[]>;
  listConsumerGroups?(signal?: AbortSignal): Promise<KafkaConsumerGroupInventory>;
  listAcls?(signal?: AbortSignal): Promise<readonly KafkaAclBinding[]>;
  openMessageStream(request: KafkaFetchRequest, signal: AbortSignal): Promise<KafkaMessageStream>;
  runLatencyProbe?(
    request: KafkaLatencyProbeRequest,
    runId: string,
    signal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement>;
}

export interface KafkaClusterServiceContext {
  readonly baseUrl: string;
  readonly caPem: string;
  authorization(): Promise<string | undefined>;
}

export interface KafkaConnectionPort {
  openConnection(
    connection: SecureConnectionInput,
    signal: AbortSignal,
  ): Promise<KafkaActiveConnection>;
  testConnection(
    connection: SecureConnectionInput,
    signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult>;
}

export interface KafkaConnectionSnapshot {
  readonly connectionName: string | null;
  readonly failure?: unknown;
  readonly state: ConnectionState;
}

export type KafkaConnectionSnapshotListener = (snapshot: KafkaConnectionSnapshot) => void;
