import {
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  parseKafkaOperationalPreferencePatch,
  parseKafkaOperationalPreferences,
  type KafkaFetchMode,
  type KafkaLatencyAcknowledgements,
  type KafkaOperationalPreferenceLogLevel,
  type KafkaOperationalPreferencePatch,
  type KafkaOperationalPreferences,
} from "../contracts";

export interface KafkaOperationalPreferenceDraft {
  readonly fetchMaxMessages: string;
  readonly fetchMode: KafkaFetchMode;
  readonly latencyAcknowledgements: KafkaLatencyAcknowledgements;
  readonly latencyMessageCount: string;
  readonly latencyRunbookUrl: string;
  readonly latencyTimeoutMs: string;
  readonly ruleLogLevel: KafkaOperationalPreferenceLogLevel;
  readonly ruleLoggingEnabled: boolean;
  readonly ruleNotificationsEnabled: boolean;
  readonly streamBatchSize: string;
  readonly streamHistorySamples: string;
  readonly streamIntervalMs: string;
  readonly streamQueueDepth: string;
}

export type KafkaOperationalPreferenceDraftField =
  | "fetchMaxMessages"
  | "latencyMessageCount"
  | "latencyRunbookUrl"
  | "latencyTimeoutMs"
  | "streamBatchSize"
  | "streamHistorySamples"
  | "streamIntervalMs"
  | "streamQueueDepth";

export interface KafkaOperationalPreferenceDraftValidation {
  readonly issues: Partial<Record<KafkaOperationalPreferenceDraftField, string>>;
  readonly preferences: KafkaOperationalPreferences | null;
}

interface ParsedInteger {
  readonly issue?: string;
  readonly value?: number;
}

function parseInteger(
  value: string,
  limits: { readonly maximum: number; readonly minimum: number },
): ParsedInteger {
  if (value.length === 0) {
    return {
      issue: `Enter a whole number from ${limits.minimum.toLocaleString()} through ${limits.maximum.toLocaleString()}.`,
    };
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < limits.minimum || parsed > limits.maximum) {
    return {
      issue: `Enter a whole number from ${limits.minimum.toLocaleString()} through ${limits.maximum.toLocaleString()}.`,
    };
  }
  return { value: parsed };
}

export function createKafkaOperationalPreferenceDraft(
  preferences: KafkaOperationalPreferences,
): KafkaOperationalPreferenceDraft {
  return {
    fetchMaxMessages: String(preferences.fetch.maxMessages),
    fetchMode: preferences.fetch.mode,
    latencyAcknowledgements: preferences.latency.acknowledgements,
    latencyMessageCount: String(preferences.latency.messageCount),
    latencyRunbookUrl: preferences.latency.runbookUrl ?? "",
    latencyTimeoutMs: String(preferences.latency.timeoutMs),
    ruleLogLevel: preferences.rules.logLevel,
    ruleLoggingEnabled: preferences.rules.loggingEnabled,
    ruleNotificationsEnabled: preferences.rules.notificationsEnabled,
    streamBatchSize: String(preferences.stream.batchSize),
    streamHistorySamples: String(preferences.stream.historySamples),
    streamIntervalMs: String(preferences.stream.intervalMs),
    streamQueueDepth: String(preferences.stream.queueDepth),
  };
}

export function sameKafkaOperationalPreferenceDraft(
  left: KafkaOperationalPreferenceDraft,
  right: KafkaOperationalPreferenceDraft,
): boolean {
  return (
    left.fetchMaxMessages === right.fetchMaxMessages &&
    left.fetchMode === right.fetchMode &&
    left.latencyAcknowledgements === right.latencyAcknowledgements &&
    left.latencyMessageCount === right.latencyMessageCount &&
    left.latencyRunbookUrl === right.latencyRunbookUrl &&
    left.latencyTimeoutMs === right.latencyTimeoutMs &&
    left.ruleLogLevel === right.ruleLogLevel &&
    left.ruleLoggingEnabled === right.ruleLoggingEnabled &&
    left.ruleNotificationsEnabled === right.ruleNotificationsEnabled &&
    left.streamBatchSize === right.streamBatchSize &&
    left.streamHistorySamples === right.streamHistorySamples &&
    left.streamIntervalMs === right.streamIntervalMs &&
    left.streamQueueDepth === right.streamQueueDepth
  );
}

function rebaseField<K extends keyof KafkaOperationalPreferenceDraft>(
  field: K,
  previous: KafkaOperationalPreferenceDraft,
  next: KafkaOperationalPreferenceDraft,
  current: KafkaOperationalPreferenceDraft,
): KafkaOperationalPreferenceDraft[K] {
  return current[field] === previous[field] ? next[field] : current[field];
}

export function rebaseKafkaOperationalPreferenceDraft(
  previous: KafkaOperationalPreferenceDraft,
  next: KafkaOperationalPreferenceDraft,
  current: KafkaOperationalPreferenceDraft,
): KafkaOperationalPreferenceDraft {
  return {
    fetchMaxMessages: rebaseField("fetchMaxMessages", previous, next, current),
    fetchMode: rebaseField("fetchMode", previous, next, current),
    latencyAcknowledgements: rebaseField("latencyAcknowledgements", previous, next, current),
    latencyMessageCount: rebaseField("latencyMessageCount", previous, next, current),
    latencyRunbookUrl: rebaseField("latencyRunbookUrl", previous, next, current),
    latencyTimeoutMs: rebaseField("latencyTimeoutMs", previous, next, current),
    ruleLogLevel: rebaseField("ruleLogLevel", previous, next, current),
    ruleLoggingEnabled: rebaseField("ruleLoggingEnabled", previous, next, current),
    ruleNotificationsEnabled: rebaseField("ruleNotificationsEnabled", previous, next, current),
    streamBatchSize: rebaseField("streamBatchSize", previous, next, current),
    streamHistorySamples: rebaseField("streamHistorySamples", previous, next, current),
    streamIntervalMs: rebaseField("streamIntervalMs", previous, next, current),
    streamQueueDepth: rebaseField("streamQueueDepth", previous, next, current),
  };
}

export function validateKafkaOperationalPreferenceDraft(
  draft: KafkaOperationalPreferenceDraft,
): KafkaOperationalPreferenceDraftValidation {
  const integers = {
    fetchMaxMessages: parseInteger(
      draft.fetchMaxMessages,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages,
    ),
    latencyMessageCount: parseInteger(
      draft.latencyMessageCount,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyMessages,
    ),
    latencyTimeoutMs: parseInteger(
      draft.latencyTimeoutMs,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyTimeoutMs,
    ),
    streamBatchSize: parseInteger(
      draft.streamBatchSize,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize,
    ),
    streamHistorySamples: parseInteger(
      draft.streamHistorySamples,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.historySamples,
    ),
    streamIntervalMs: parseInteger(
      draft.streamIntervalMs,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.intervalMs,
    ),
    streamQueueDepth: parseInteger(
      draft.streamQueueDepth,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth,
    ),
  } as const;
  const issues: Partial<Record<KafkaOperationalPreferenceDraftField, string>> = {};
  for (const [field, parsed] of Object.entries(integers) as [
    Exclude<KafkaOperationalPreferenceDraftField, "latencyRunbookUrl">,
    ParsedInteger,
  ][]) {
    if (parsed.issue !== undefined) {
      issues[field] = parsed.issue;
    }
  }

  const runbookUrl = draft.latencyRunbookUrl.length === 0 ? null : draft.latencyRunbookUrl;
  try {
    parseKafkaOperationalPreferencePatch({ latency: { runbookUrl } });
  } catch {
    issues.latencyRunbookUrl =
      "Enter an absolute credential-free HTTPS URL no longer than 2,048 characters, or leave this empty.";
  }
  if (Object.keys(issues).length > 0) {
    return { issues, preferences: null };
  }

  const preferences = parseKafkaOperationalPreferences({
    fetch: {
      maxMessages: integers.fetchMaxMessages.value,
      mode: draft.fetchMode,
    },
    latency: {
      acknowledgements: draft.latencyAcknowledgements,
      messageCount: integers.latencyMessageCount.value,
      runbookUrl,
      timeoutMs: integers.latencyTimeoutMs.value,
    },
    rules: {
      logLevel: draft.ruleLogLevel,
      loggingEnabled: draft.ruleLoggingEnabled,
      notificationsEnabled: draft.ruleNotificationsEnabled,
    },
    stream: {
      batchSize: integers.streamBatchSize.value,
      historySamples: integers.streamHistorySamples.value,
      intervalMs: integers.streamIntervalMs.value,
      queueDepth: integers.streamQueueDepth.value,
    },
  });
  return { issues, preferences };
}

export function changedKafkaOperationalPreferenceGroups(
  confirmed: KafkaOperationalPreferences,
  next: KafkaOperationalPreferences,
): KafkaOperationalPreferencePatch {
  const patch: {
    fetch?: KafkaOperationalPreferences["fetch"];
    latency?: KafkaOperationalPreferences["latency"];
    rules?: KafkaOperationalPreferences["rules"];
    stream?: KafkaOperationalPreferences["stream"];
  } = {};
  if (
    confirmed.fetch.maxMessages !== next.fetch.maxMessages ||
    confirmed.fetch.mode !== next.fetch.mode
  ) {
    patch.fetch = next.fetch;
  }
  if (
    confirmed.latency.acknowledgements !== next.latency.acknowledgements ||
    confirmed.latency.messageCount !== next.latency.messageCount ||
    confirmed.latency.runbookUrl !== next.latency.runbookUrl ||
    confirmed.latency.timeoutMs !== next.latency.timeoutMs
  ) {
    patch.latency = next.latency;
  }
  if (
    confirmed.rules.logLevel !== next.rules.logLevel ||
    confirmed.rules.loggingEnabled !== next.rules.loggingEnabled ||
    confirmed.rules.notificationsEnabled !== next.rules.notificationsEnabled
  ) {
    patch.rules = next.rules;
  }
  if (
    confirmed.stream.batchSize !== next.stream.batchSize ||
    confirmed.stream.historySamples !== next.stream.historySamples ||
    confirmed.stream.intervalMs !== next.stream.intervalMs ||
    confirmed.stream.queueDepth !== next.stream.queueDepth
  ) {
    patch.stream = next.stream;
  }
  return patch;
}
