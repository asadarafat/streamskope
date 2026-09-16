export const KAFKA_CONFIGURATION_LIMITS = {
  entries: 4_096,
  nameCharacters: 512,
  valueCharacters: 65_536,
  documentationCharacters: 16_384,
  synonymsPerEntry: 128,
} as const;

export const KAFKA_CONFIGURATION_SOURCES = [
  "unknown",
  "topic",
  "dynamic-broker",
  "dynamic-default-broker",
  "static-broker",
  "default",
  "dynamic-broker-logger",
  "client-metrics",
  "group",
] as const;

export const KAFKA_CONFIGURATION_TYPES = [
  "unknown",
  "boolean",
  "string",
  "int",
  "short",
  "long",
  "double",
  "list",
  "class",
  "password",
] as const;

export type KafkaConfigurationSource = (typeof KAFKA_CONFIGURATION_SOURCES)[number];
export type KafkaConfigurationType = (typeof KAFKA_CONFIGURATION_TYPES)[number];

export interface KafkaConfigurationSynonym {
  readonly name: string;
  readonly source: KafkaConfigurationSource;
  readonly value: string | null;
}

export interface KafkaConfigurationEntry {
  readonly documentation: string | null;
  readonly isDefault: boolean;
  readonly isSensitive: boolean;
  readonly name: string;
  readonly readOnly: boolean;
  readonly source: KafkaConfigurationSource;
  readonly synonyms: readonly KafkaConfigurationSynonym[];
  readonly type: KafkaConfigurationType;
  readonly value: string | null;
}
