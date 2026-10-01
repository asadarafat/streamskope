import { KAFKA_LATENCY_ACKNOWLEDGEMENTS } from "./latency-types";
import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS,
  KAFKA_OPERATIONAL_PREFERENCE_STORE_DURABILITIES,
  KAFKA_OPERATIONAL_PREFERENCE_STORE_STATES,
  type KafkaFetchPreferences,
  type KafkaLatencyPreferences,
  type KafkaOperationalPreferencePatch,
  type KafkaOperationalPreferences,
  type KafkaOperationalPreferenceSnapshot,
  type KafkaOperationalPreferenceStoreCapability,
  type KafkaOperationalPreferenceUpdateInput,
  type KafkaRulePreferences,
  type KafkaStreamPreferences,
} from "./operational-preference-types";
import { KAFKA_FETCH_MODES } from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
  truth,
  type UnknownRecord,
} from "./validation-primitives";

function boundedInteger(
  value: unknown,
  path: string,
  limits: { readonly maximum: number; readonly minimum: number },
): number {
  const parsed = nonNegativeInteger(value, path);
  if (parsed < limits.minimum || parsed > limits.maximum) {
    throw new HostContractValidationError(
      path,
      `must be between ${String(limits.minimum)} and ${String(limits.maximum)}, inclusive`,
    );
  }
  return parsed;
}

export function parseKafkaRunbookUrl(value: unknown, path: string): string {
  const candidate = text(value, path, KAFKA_OPERATIONAL_PREFERENCE_LIMITS.runbookCharacters);
  let parsed: URL;
  try {
    parsed = new URL(candidate);
  } catch {
    throw new HostContractValidationError(path, "must be an absolute HTTPS URL");
  }
  if (
    parsed.protocol !== "https:" ||
    parsed.hostname.length === 0 ||
    parsed.username.length > 0 ||
    parsed.password.length > 0
  ) {
    throw new HostContractValidationError(path, "must be a credential-free absolute HTTPS URL");
  }
  return candidate;
}

function runbookUrl(value: unknown, path: string): string | null {
  return value === null ? null : parseKafkaRunbookUrl(value, path);
}

function parseFetchPreferences(value: unknown, path: string): KafkaFetchPreferences {
  const preferences = record(value, path);
  exactKeys(preferences, ["maxMessages", "mode"], path);
  return {
    maxMessages: boundedInteger(
      preferences.maxMessages,
      `${path}.maxMessages`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages,
    ),
    mode: declaredValue(preferences.mode, KAFKA_FETCH_MODES, `${path}.mode`),
  };
}

function parseStreamPreferences(value: unknown, path: string): KafkaStreamPreferences {
  const preferences = record(value, path);
  exactKeys(preferences, ["batchSize", "historySamples", "intervalMs", "queueDepth"], path);
  return {
    batchSize: boundedInteger(
      preferences.batchSize,
      `${path}.batchSize`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize,
    ),
    historySamples: boundedInteger(
      preferences.historySamples,
      `${path}.historySamples`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.historySamples,
    ),
    intervalMs: boundedInteger(
      preferences.intervalMs,
      `${path}.intervalMs`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.intervalMs,
    ),
    queueDepth: boundedInteger(
      preferences.queueDepth,
      `${path}.queueDepth`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth,
    ),
  };
}

function parseLatencyPreferences(value: unknown, path: string): KafkaLatencyPreferences {
  const preferences = record(value, path);
  exactKeys(preferences, ["acknowledgements", "messageCount", "runbookUrl", "timeoutMs"], path);
  const acknowledgements = preferences.acknowledgements;
  if (
    typeof acknowledgements !== "number" ||
    !KAFKA_LATENCY_ACKNOWLEDGEMENTS.includes(acknowledgements as -1 | 0 | 1)
  ) {
    throw new HostContractValidationError(`${path}.acknowledgements`, "must be one of -1, 0, 1");
  }
  return {
    acknowledgements: acknowledgements as -1 | 0 | 1,
    messageCount: boundedInteger(
      preferences.messageCount,
      `${path}.messageCount`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyMessages,
    ),
    runbookUrl: runbookUrl(preferences.runbookUrl, `${path}.runbookUrl`),
    timeoutMs: boundedInteger(
      preferences.timeoutMs,
      `${path}.timeoutMs`,
      KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyTimeoutMs,
    ),
  };
}

function parseRulePreferences(value: unknown, path: string): KafkaRulePreferences {
  const preferences = record(value, path);
  exactKeys(preferences, ["logLevel", "loggingEnabled", "notificationsEnabled"], path);
  return {
    logLevel: declaredValue(
      preferences.logLevel,
      KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS,
      `${path}.logLevel`,
    ),
    loggingEnabled: truth(preferences.loggingEnabled, `${path}.loggingEnabled`),
    notificationsEnabled: truth(preferences.notificationsEnabled, `${path}.notificationsEnabled`),
  };
}

export function parseKafkaOperationalPreferences(
  value: unknown,
  path = "preferences",
): KafkaOperationalPreferences {
  const preferences = record(value, path);
  exactKeys(preferences, ["fetch", "latency", "rules", "stream"], path);
  return {
    fetch: parseFetchPreferences(preferences.fetch, `${path}.fetch`),
    latency: parseLatencyPreferences(preferences.latency, `${path}.latency`),
    rules: parseRulePreferences(preferences.rules, `${path}.rules`),
    stream: parseStreamPreferences(preferences.stream, `${path}.stream`),
  };
}

function exactOptionalGroup(
  value: unknown,
  path: string,
  allowed: readonly string[],
): UnknownRecord {
  const group = record(value, path);
  exactKeys(group, allowed, path);
  if (Object.keys(group).length === 0) {
    throw new HostContractValidationError(path, "must change at least one declared field");
  }
  return group;
}

function parseFetchPatch(value: unknown, path: string): Partial<KafkaFetchPreferences> {
  const patch = exactOptionalGroup(value, path, ["maxMessages", "mode"]);
  return {
    ...(Object.hasOwn(patch, "maxMessages")
      ? {
          maxMessages: boundedInteger(
            patch.maxMessages,
            `${path}.maxMessages`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages,
          ),
        }
      : {}),
    ...(Object.hasOwn(patch, "mode")
      ? { mode: declaredValue(patch.mode, KAFKA_FETCH_MODES, `${path}.mode`) }
      : {}),
  };
}

function parseStreamPatch(value: unknown, path: string): Partial<KafkaStreamPreferences> {
  const patch = exactOptionalGroup(value, path, [
    "batchSize",
    "historySamples",
    "intervalMs",
    "queueDepth",
  ]);
  return {
    ...(Object.hasOwn(patch, "batchSize")
      ? {
          batchSize: boundedInteger(
            patch.batchSize,
            `${path}.batchSize`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize,
          ),
        }
      : {}),
    ...(Object.hasOwn(patch, "historySamples")
      ? {
          historySamples: boundedInteger(
            patch.historySamples,
            `${path}.historySamples`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.historySamples,
          ),
        }
      : {}),
    ...(Object.hasOwn(patch, "intervalMs")
      ? {
          intervalMs: boundedInteger(
            patch.intervalMs,
            `${path}.intervalMs`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.intervalMs,
          ),
        }
      : {}),
    ...(Object.hasOwn(patch, "queueDepth")
      ? {
          queueDepth: boundedInteger(
            patch.queueDepth,
            `${path}.queueDepth`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth,
          ),
        }
      : {}),
  };
}

function parseLatencyPatch(value: unknown, path: string): Partial<KafkaLatencyPreferences> {
  const patch = exactOptionalGroup(value, path, [
    "acknowledgements",
    "messageCount",
    "runbookUrl",
    "timeoutMs",
  ]);
  const acknowledgements = patch.acknowledgements;
  if (
    Object.hasOwn(patch, "acknowledgements") &&
    (typeof acknowledgements !== "number" ||
      !KAFKA_LATENCY_ACKNOWLEDGEMENTS.includes(acknowledgements as -1 | 0 | 1))
  ) {
    throw new HostContractValidationError(`${path}.acknowledgements`, "must be one of -1, 0, 1");
  }
  return {
    ...(Object.hasOwn(patch, "acknowledgements")
      ? { acknowledgements: acknowledgements as -1 | 0 | 1 }
      : {}),
    ...(Object.hasOwn(patch, "messageCount")
      ? {
          messageCount: boundedInteger(
            patch.messageCount,
            `${path}.messageCount`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyMessages,
          ),
        }
      : {}),
    ...(Object.hasOwn(patch, "runbookUrl")
      ? { runbookUrl: runbookUrl(patch.runbookUrl, `${path}.runbookUrl`) }
      : {}),
    ...(Object.hasOwn(patch, "timeoutMs")
      ? {
          timeoutMs: boundedInteger(
            patch.timeoutMs,
            `${path}.timeoutMs`,
            KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyTimeoutMs,
          ),
        }
      : {}),
  };
}

function parseRulePatch(value: unknown, path: string): Partial<KafkaRulePreferences> {
  const patch = exactOptionalGroup(value, path, [
    "logLevel",
    "loggingEnabled",
    "notificationsEnabled",
  ]);
  return {
    ...(Object.hasOwn(patch, "logLevel")
      ? {
          logLevel: declaredValue(
            patch.logLevel,
            KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS,
            `${path}.logLevel`,
          ),
        }
      : {}),
    ...(Object.hasOwn(patch, "loggingEnabled")
      ? { loggingEnabled: truth(patch.loggingEnabled, `${path}.loggingEnabled`) }
      : {}),
    ...(Object.hasOwn(patch, "notificationsEnabled")
      ? {
          notificationsEnabled: truth(patch.notificationsEnabled, `${path}.notificationsEnabled`),
        }
      : {}),
  };
}

export function parseKafkaOperationalPreferencePatch(
  value: unknown,
  path = "preferences.patch",
): KafkaOperationalPreferencePatch {
  const patch = record(value, path);
  exactKeys(patch, ["fetch", "latency", "rules", "stream"], path);
  if (Object.keys(patch).length === 0) {
    throw new HostContractValidationError(path, "must change at least one preference group");
  }
  return {
    ...(Object.hasOwn(patch, "fetch")
      ? { fetch: parseFetchPatch(patch.fetch, `${path}.fetch`) }
      : {}),
    ...(Object.hasOwn(patch, "latency")
      ? { latency: parseLatencyPatch(patch.latency, `${path}.latency`) }
      : {}),
    ...(Object.hasOwn(patch, "rules")
      ? { rules: parseRulePatch(patch.rules, `${path}.rules`) }
      : {}),
    ...(Object.hasOwn(patch, "stream")
      ? { stream: parseStreamPatch(patch.stream, `${path}.stream`) }
      : {}),
  };
}

export function parseKafkaOperationalPreferenceUpdateInput(
  value: unknown,
  path = "preferences.update",
): KafkaOperationalPreferenceUpdateInput {
  const input = record(value, path);
  exactKeys(input, ["patch"], path);
  return {
    patch: parseKafkaOperationalPreferencePatch(input.patch, `${path}.patch`),
  };
}

function parseStoreCapability(
  value: unknown,
  path: string,
): KafkaOperationalPreferenceStoreCapability {
  const store = record(value, path);
  exactKeys(store, ["durability", "recovery", "state"], path);
  const durability = declaredValue(
    store.durability,
    KAFKA_OPERATIONAL_PREFERENCE_STORE_DURABILITIES,
    `${path}.durability`,
  );
  const state = declaredValue(
    store.state,
    KAFKA_OPERATIONAL_PREFERENCE_STORE_STATES,
    `${path}.state`,
  );
  if (state === "ready") {
    if (Object.hasOwn(store, "recovery")) {
      throw new HostContractValidationError(`${path}.recovery`, "must be absent while ready");
    }
    return { durability, state };
  }
  return {
    durability,
    recovery: text(store.recovery, `${path}.recovery`, 2_048),
    state,
  };
}

function factoryPreferences(preferences: KafkaOperationalPreferences): boolean {
  return (
    preferences.fetch.maxMessages === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.fetch.maxMessages &&
    preferences.fetch.mode === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.fetch.mode &&
    preferences.latency.acknowledgements ===
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.latency.acknowledgements &&
    preferences.latency.messageCount ===
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.latency.messageCount &&
    preferences.latency.runbookUrl === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.latency.runbookUrl &&
    preferences.latency.timeoutMs === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.latency.timeoutMs &&
    preferences.rules.logLevel === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.rules.logLevel &&
    preferences.rules.loggingEnabled ===
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.rules.loggingEnabled &&
    preferences.rules.notificationsEnabled ===
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.rules.notificationsEnabled &&
    preferences.stream.batchSize === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.stream.batchSize &&
    preferences.stream.historySamples ===
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.stream.historySamples &&
    preferences.stream.intervalMs === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.stream.intervalMs &&
    preferences.stream.queueDepth === KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.stream.queueDepth
  );
}

export function parseKafkaOperationalPreferenceSnapshot(
  value: unknown,
  path = "preferences.snapshot",
): KafkaOperationalPreferenceSnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["preferences", "store"], path);
  const preferences = parseKafkaOperationalPreferences(snapshot.preferences, `${path}.preferences`);
  const store = parseStoreCapability(snapshot.store, `${path}.store`);
  if (store.state === "unavailable" && !factoryPreferences(preferences)) {
    throw new HostContractValidationError(
      `${path}.preferences`,
      "must contain factory fallback values while storage is unavailable",
    );
  }
  return { preferences, store };
}
