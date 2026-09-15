import type { KafkaFetchMode } from "./types";
import type { KafkaLatencyAcknowledgements } from "./latency-types";

export const KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS = ["error", "warn", "info", "debug"] as const;
export const KAFKA_OPERATIONAL_PREFERENCE_STORE_DURABILITIES = ["session", "durable"] as const;
export const KAFKA_OPERATIONAL_PREFERENCE_STORE_STATES = ["ready", "unavailable"] as const;

export const KAFKA_OPERATIONAL_PREFERENCE_LIMITS = {
  batchSize: { maximum: 200, minimum: 10 },
  fetchMessages: { maximum: 1_000, minimum: 1 },
  historySamples: { maximum: 400, minimum: 10 },
  intervalMs: { maximum: 500, minimum: 5 },
  latencyMessages: { maximum: 200, minimum: 1 },
  latencyTimeoutMs: { maximum: 60_000, minimum: 1_000 },
  queueDepth: { maximum: 1_000, minimum: 100 },
  runbookCharacters: 2_048,
} as const;

export type KafkaOperationalPreferenceLogLevel =
  (typeof KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS)[number];
export type KafkaOperationalPreferenceStoreDurability =
  (typeof KAFKA_OPERATIONAL_PREFERENCE_STORE_DURABILITIES)[number];
export type KafkaOperationalPreferenceStoreState =
  (typeof KAFKA_OPERATIONAL_PREFERENCE_STORE_STATES)[number];

export interface KafkaFetchPreferences {
  readonly maxMessages: number;
  readonly mode: KafkaFetchMode;
}

export interface KafkaStreamPreferences {
  readonly batchSize: number;
  readonly historySamples: number;
  readonly intervalMs: number;
  readonly queueDepth: number;
}

export interface KafkaLatencyPreferences {
  readonly acknowledgements: KafkaLatencyAcknowledgements;
  readonly messageCount: number;
  readonly runbookUrl: string | null;
  readonly timeoutMs: number;
}

export interface KafkaRulePreferences {
  readonly logLevel: KafkaOperationalPreferenceLogLevel;
  readonly loggingEnabled: boolean;
  readonly notificationsEnabled: boolean;
}

export interface KafkaOperationalPreferences {
  readonly fetch: KafkaFetchPreferences;
  readonly latency: KafkaLatencyPreferences;
  readonly rules: KafkaRulePreferences;
  readonly stream: KafkaStreamPreferences;
}

export const KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS: KafkaOperationalPreferences = Object.freeze({
  fetch: Object.freeze({
    maxMessages: 1_000,
    mode: "tail",
  }),
  latency: Object.freeze({
    acknowledgements: 1,
    messageCount: 20,
    runbookUrl: null,
    timeoutMs: 10_000,
  }),
  rules: Object.freeze({
    logLevel: "info",
    loggingEnabled: true,
    notificationsEnabled: true,
  }),
  stream: Object.freeze({
    batchSize: 200,
    historySamples: 50,
    intervalMs: 20,
    queueDepth: 1_000,
  }),
});

export interface KafkaOperationalPreferenceStoreCapability {
  readonly durability: KafkaOperationalPreferenceStoreDurability;
  readonly recovery?: string;
  readonly state: KafkaOperationalPreferenceStoreState;
}

export interface KafkaOperationalPreferenceSnapshot {
  readonly preferences: KafkaOperationalPreferences;
  readonly store: KafkaOperationalPreferenceStoreCapability;
}

export interface KafkaOperationalPreferencePatch {
  readonly fetch?: Partial<KafkaFetchPreferences>;
  readonly latency?: Partial<KafkaLatencyPreferences>;
  readonly rules?: Partial<KafkaRulePreferences>;
  readonly stream?: Partial<KafkaStreamPreferences>;
}

export interface KafkaOperationalPreferenceUpdateInput {
  readonly patch: KafkaOperationalPreferencePatch;
}

export interface KafkaOperationalPreferenceResult {
  readonly correlationId: string;
  readonly snapshot: KafkaOperationalPreferenceSnapshot;
}
