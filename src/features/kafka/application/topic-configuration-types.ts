import type {
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
  KafkaTopicConfigurationHistoryEntry,
  KafkaTopicConfigurationHistorySnapshot,
  KafkaTopicConfigurationHistoryStoreCapability,
  KafkaTopicConfigurationOperationInput,
} from "../contracts";

export interface KafkaTopicConfigurationConnectionContext {
  readonly connectionName: string;
  readonly connectionTarget: string;
}

export interface KafkaTopicConfigurationSessionPort {
  activeConnectionContext(): KafkaTopicConfigurationConnectionContext | null;
  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
    signal?: AbortSignal,
  ): Promise<void>;
  describeTopicConfiguration(
    topic: string,
    signal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]>;
}

export interface KafkaTopicConfigurationHistoryDocument {
  readonly entries: readonly KafkaTopicConfigurationHistoryEntry[];
}

export interface KafkaTopicConfigurationHistoryStore {
  capability(): KafkaTopicConfigurationHistoryStoreCapability;
  commit(document: KafkaTopicConfigurationHistoryDocument, signal?: AbortSignal): Promise<void>;
  load(signal?: AbortSignal): Promise<KafkaTopicConfigurationHistoryDocument | undefined>;
}

export interface KafkaTopicConfigurationView {
  readonly connectionName: string;
  readonly connectionTarget: string;
  readonly entries: readonly KafkaTopicConfigurationEntry[];
  readonly refreshedAt: string;
  readonly topic: string;
}

export interface KafkaTopicConfigurationHistoryRead {
  readonly failure?: unknown;
  readonly snapshot: KafkaTopicConfigurationHistorySnapshot;
}

export interface KafkaTopicConfigurationOperationResult {
  readonly configuration: KafkaTopicConfigurationView;
  readonly history: KafkaTopicConfigurationHistorySnapshot;
  readonly historyFailure?: unknown;
  readonly refreshFailure?: unknown;
}

export interface KafkaTopicConfigurationServicePort {
  apply(
    input: KafkaTopicConfigurationOperationInput,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationOperationResult>;
  history(topic: string, signal?: AbortSignal): Promise<KafkaTopicConfigurationHistoryRead>;
  load(topic: string, signal?: AbortSignal): Promise<KafkaTopicConfigurationView>;
  validate(
    input: KafkaTopicConfigurationOperationInput,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationOperationResult>;
}

export interface KafkaTopicConfigurationServiceOptions {
  readonly createHistoryId?: () => string;
  readonly now?: () => Date;
}
