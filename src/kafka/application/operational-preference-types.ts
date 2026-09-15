import type {
  HostErrorCode,
  HostErrorStage,
  KafkaOperationalPreferences,
  KafkaOperationalPreferenceStoreCapability,
} from "../contracts";

export interface KafkaOperationalPreferenceStore {
  capability(): KafkaOperationalPreferenceStoreCapability;
  commit(preferences: KafkaOperationalPreferences, signal?: AbortSignal): Promise<void>;
  load(signal?: AbortSignal): Promise<KafkaOperationalPreferences | undefined>;
}

export interface KafkaOperationalPreferenceStructuredError extends Error {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target: string | undefined;
}
