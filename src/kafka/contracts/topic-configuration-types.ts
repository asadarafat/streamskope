import type { HostError } from "./types";
import {
  KAFKA_CONFIGURATION_LIMITS,
  KAFKA_CONFIGURATION_SOURCES,
  KAFKA_CONFIGURATION_TYPES,
  type KafkaConfigurationEntry,
  type KafkaConfigurationSource,
  type KafkaConfigurationSynonym,
  type KafkaConfigurationType,
} from "./configuration-types";

export const KAFKA_TOPIC_CONFIGURATION_LIMITS = {
  changes: 50,
  configurationEntries: KAFKA_CONFIGURATION_LIMITS.entries,
  configurationNameCharacters: KAFKA_CONFIGURATION_LIMITS.nameCharacters,
  configurationValueCharacters: KAFKA_CONFIGURATION_LIMITS.valueCharacters,
  documentationCharacters: KAFKA_CONFIGURATION_LIMITS.documentationCharacters,
  historyEntries: 50,
  historyErrorCharacters: 2_048,
  historyVisibleEntries: 40,
  synonymsPerEntry: KAFKA_CONFIGURATION_LIMITS.synonymsPerEntry,
  topicCharacters: 512,
} as const;

export const KAFKA_TOPIC_CONFIGURATION_REDACTION = "<redacted>" as const;

export const KAFKA_TOPIC_CONFIGURATION_SOURCES = KAFKA_CONFIGURATION_SOURCES;

export const KAFKA_TOPIC_CONFIGURATION_TYPES = KAFKA_CONFIGURATION_TYPES;

export const KAFKA_TOPIC_CONFIGURATION_STATES = [
  "unavailable",
  "loading",
  "ready",
  "denied",
  "not-found",
  "failed",
  "stale",
] as const;

export const KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_DURABILITIES = ["durable", "session"] as const;
export const KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_STATES = ["ready", "unavailable"] as const;

export const KAFKA_TOPIC_CONFIGURATION_PRESETS = [
  {
    changes: [
      { name: "cleanup.policy", value: "delete" },
      { name: "retention.ms", value: "604800000" },
      { name: "segment.ms", value: "3600000" },
    ],
    description: "cleanup.policy=delete, retention.ms=7d, segment.ms=1h",
    id: "retention-7d",
    label: "Delete after 7 days",
  },
  {
    changes: [
      { name: "cleanup.policy", value: "delete" },
      { name: "retention.ms", value: "86400000" },
      { name: "segment.ms", value: "1800000" },
    ],
    description: "cleanup.policy=delete, retention.ms=24h, segment.ms=30m",
    id: "retention-24h",
    label: "Short TTL (24h)",
  },
  {
    changes: [
      { name: "cleanup.policy", value: "compact,delete" },
      { name: "delete.retention.ms", value: "86400000" },
      { name: "min.cleanable.dirty.ratio", value: "0.5" },
    ],
    description: "cleanup.policy=compact,delete with 24h delete retention",
    id: "compact-preferred",
    label: "Compact + 24h delete",
  },
  {
    changes: [
      { name: "cleanup.policy", value: "compact" },
      { name: "min.cleanable.dirty.ratio", value: "0.5" },
    ],
    description: "cleanup.policy=compact with conservative dirty ratio",
    id: "compact-only",
    label: "Compaction only",
  },
] as const;

export type KafkaTopicConfigurationSource = KafkaConfigurationSource;
export type KafkaTopicConfigurationType = KafkaConfigurationType;
export type KafkaTopicConfigurationState = (typeof KAFKA_TOPIC_CONFIGURATION_STATES)[number];
export type KafkaTopicConfigurationPresetId =
  (typeof KAFKA_TOPIC_CONFIGURATION_PRESETS)[number]["id"];
export type KafkaTopicConfigurationAction = "apply" | "validate";
export type KafkaTopicConfigurationHistoryStoreDurability =
  (typeof KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_DURABILITIES)[number];
export type KafkaTopicConfigurationHistoryStoreState =
  (typeof KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_STATES)[number];

export interface KafkaTopicConfigurationChange {
  readonly isSensitive: boolean;
  readonly name: string;
  readonly value: string;
}

export interface KafkaTopicConfigurationOperationInput {
  readonly changes: readonly KafkaTopicConfigurationChange[];
  readonly presetId?: KafkaTopicConfigurationPresetId;
  readonly topic: string;
}

export type KafkaTopicConfigurationSynonym = KafkaConfigurationSynonym;

export type KafkaTopicConfigurationEntry = KafkaConfigurationEntry;

export interface KafkaTopicConfigurationSnapshot {
  readonly connectionName: string | null;
  readonly entries: readonly KafkaTopicConfigurationEntry[];
  readonly error?: HostError;
  readonly refreshedAt: string | null;
  readonly state: KafkaTopicConfigurationState;
  readonly topic: string | null;
}

export interface KafkaTopicConfigurationHistoryChange {
  readonly from?: string | null;
  readonly isSensitive: boolean;
  readonly name: string;
  readonly to: string;
  readonly wasDefault: boolean;
}

export interface KafkaTopicConfigurationHistoryEntry {
  readonly action: KafkaTopicConfigurationAction;
  readonly at: string;
  readonly changes: readonly KafkaTopicConfigurationHistoryChange[];
  readonly connectionName: string;
  readonly connectionTarget: string;
  readonly error?: string;
  readonly id: string;
  readonly presetId?: KafkaTopicConfigurationPresetId;
  readonly success: boolean;
  readonly topic: string;
  readonly warning?: string;
}

export interface KafkaTopicConfigurationHistoryStoreCapability {
  readonly durability: KafkaTopicConfigurationHistoryStoreDurability;
  readonly recovery?: string;
  readonly state: KafkaTopicConfigurationHistoryStoreState;
}

export interface KafkaTopicConfigurationHistorySnapshot {
  readonly connectionName: string;
  readonly entries: readonly KafkaTopicConfigurationHistoryEntry[];
  readonly store: KafkaTopicConfigurationHistoryStoreCapability;
  readonly topic: string;
}
