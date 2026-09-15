import type {
  ConnectionTemplateCatalogSnapshot,
  ConnectionTemplateIssue,
  ConnectionTemplateSnapshot,
  ConnectionTemplateStoreCapability,
  HostErrorCode,
  HostErrorStage,
} from "../contracts";

export interface KafkaConnectionTemplateDocument {
  readonly catalogs: readonly ConnectionTemplateCatalogSnapshot[];
}

export interface KafkaConnectionTemplateStore {
  capability(): ConnectionTemplateStoreCapability;
  commit(document: KafkaConnectionTemplateDocument, signal?: AbortSignal): Promise<void>;
  load(signal?: AbortSignal): Promise<KafkaConnectionTemplateDocument | undefined>;
}

export interface KafkaConnectionTemplateStructuredError extends Error {
  readonly code: HostErrorCode;
  readonly issues?: readonly ConnectionTemplateIssue[];
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target: string | undefined;
}

export type KafkaConnectionTemplateSnapshot = ConnectionTemplateSnapshot;
