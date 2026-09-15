import {
  KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_DURABILITIES,
  KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_STATES,
  KAFKA_TOPIC_CONFIGURATION_LIMITS,
  KAFKA_TOPIC_CONFIGURATION_PRESETS,
  KAFKA_TOPIC_CONFIGURATION_REDACTION,
  KAFKA_TOPIC_CONFIGURATION_STATES,
  type KafkaTopicConfigurationChange,
  type KafkaTopicConfigurationHistoryChange,
  type KafkaTopicConfigurationHistoryEntry,
  type KafkaTopicConfigurationHistorySnapshot,
  type KafkaTopicConfigurationHistoryStoreCapability,
  type KafkaTopicConfigurationOperationInput,
  type KafkaTopicConfigurationPresetId,
  type KafkaTopicConfigurationSnapshot,
} from "./topic-configuration-types";
import { parseKafkaConfigurationEntries } from "./configuration-validation";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  optionalText,
  record,
  text,
  truth,
} from "./validation-primitives";
import type { HostError } from "./types";

function nullableBoundedText(value: unknown, path: string, maximum: number): string | null {
  return value === null ? null : boundedText(value, path, maximum);
}

function parsePresetId(value: unknown, path: string): KafkaTopicConfigurationPresetId {
  return declaredValue(
    value,
    KAFKA_TOPIC_CONFIGURATION_PRESETS.map((preset) => preset.id),
    path,
  );
}

export function parseKafkaTopicConfigurationTopic(value: unknown, path: string): string {
  return text(value, path, KAFKA_TOPIC_CONFIGURATION_LIMITS.topicCharacters);
}

function parseChange(value: unknown, path: string): KafkaTopicConfigurationChange {
  const change = record(value, path);
  exactKeys(change, ["isSensitive", "name", "value"], path);
  return {
    isSensitive: truth(change.isSensitive, `${path}.isSensitive`),
    name: text(
      change.name,
      `${path}.name`,
      KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationNameCharacters,
    ),
    value: boundedText(
      change.value,
      `${path}.value`,
      KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationValueCharacters,
    ),
  };
}

export function parseKafkaTopicConfigurationChanges(
  value: unknown,
  path: string,
): readonly KafkaTopicConfigurationChange[] {
  if (
    !Array.isArray(value) ||
    value.length < 1 ||
    value.length > KAFKA_TOPIC_CONFIGURATION_LIMITS.changes
  ) {
    throw new HostContractValidationError(
      path,
      `must contain between 1 and ${KAFKA_TOPIC_CONFIGURATION_LIMITS.changes} changes`,
    );
  }
  const changes = value.map((change, index) => parseChange(change, `${path}[${index}]`));
  const names = new Set<string>();
  for (const change of changes) {
    const canonical = change.name.trim();
    if (canonical.length === 0) {
      throw new HostContractValidationError(`${path}.name`, "must not be blank");
    }
    if (names.has(canonical)) {
      throw new HostContractValidationError(path, "must contain unique configuration names");
    }
    names.add(canonical);
  }
  return changes.map((change) => ({ ...change, name: change.name.trim() }));
}

export function parseKafkaTopicConfigurationOperationInput(
  value: unknown,
  path: string,
): KafkaTopicConfigurationOperationInput {
  const input = record(value, path);
  exactKeys(input, ["changes", "presetId", "topic"], path);
  const presetId = Object.hasOwn(input, "presetId")
    ? parsePresetId(input.presetId, `${path}.presetId`)
    : undefined;
  const base = {
    changes: parseKafkaTopicConfigurationChanges(input.changes, `${path}.changes`),
    topic: parseKafkaTopicConfigurationTopic(input.topic, `${path}.topic`),
  };
  return presetId === undefined ? base : { ...base, presetId };
}

export function parseKafkaTopicConfigurationIdentity(
  value: unknown,
  path: string,
): { readonly topic: string } {
  const identity = record(value, path);
  exactKeys(identity, ["topic"], path);
  return {
    topic: parseKafkaTopicConfigurationTopic(identity.topic, `${path}.topic`),
  };
}

export function parseKafkaTopicConfigurationSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): KafkaTopicConfigurationSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "entries", "error", "refreshedAt", "state", "topic"],
    path,
  );
  const state = declaredValue(snapshot.state, KAFKA_TOPIC_CONFIGURATION_STATES, `${path}.state`);
  const connectionName =
    snapshot.connectionName === null
      ? null
      : text(snapshot.connectionName, `${path}.connectionName`, 512);
  const topic =
    snapshot.topic === null
      ? null
      : parseKafkaTopicConfigurationTopic(snapshot.topic, `${path}.topic`);
  const refreshedAt =
    snapshot.refreshedAt === null
      ? null
      : canonicalIsoTimestamp(snapshot.refreshedAt, `${path}.refreshedAt`);
  const entries = parseKafkaConfigurationEntries(snapshot.entries, `${path}.entries`);
  const error = Object.hasOwn(snapshot, "error")
    ? parseError(snapshot.error, `${path}.error`)
    : undefined;

  if (state === "unavailable") {
    if (
      connectionName !== null ||
      topic !== null ||
      refreshedAt !== null ||
      entries.length > 0 ||
      error !== undefined
    ) {
      throw new HostContractValidationError(path, "contains inconsistent unavailable state");
    }
  } else if (connectionName === null || topic === null) {
    throw new HostContractValidationError(path, "must identify connection and topic");
  }
  if (state === "loading" && (entries.length > 0 || refreshedAt !== null || error !== undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent loading state");
  }
  if (state === "ready" && (refreshedAt === null || error !== undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent ready state");
  }
  if (state === "stale" && (refreshedAt === null || error === undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent stale state");
  }
  if (
    (state === "denied" || state === "not-found" || state === "failed") &&
    (entries.length > 0 || refreshedAt !== null || error === undefined)
  ) {
    throw new HostContractValidationError(path, "contains inconsistent failure state");
  }

  return {
    connectionName,
    entries,
    ...(error === undefined ? {} : { error }),
    refreshedAt,
    state,
    topic,
  };
}

function parseHistoryChange(value: unknown, path: string): KafkaTopicConfigurationHistoryChange {
  const change = record(value, path);
  exactKeys(change, ["from", "isSensitive", "name", "to", "wasDefault"], path);
  const isSensitive = truth(change.isSensitive, `${path}.isSensitive`);
  const from = Object.hasOwn(change, "from")
    ? nullableBoundedText(
        change.from,
        `${path}.from`,
        KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationValueCharacters,
      )
    : undefined;
  const parsed: KafkaTopicConfigurationHistoryChange = {
    ...(from === undefined ? {} : { from }),
    isSensitive,
    name: text(
      change.name,
      `${path}.name`,
      KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationNameCharacters,
    ),
    to: boundedText(
      change.to,
      `${path}.to`,
      KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationValueCharacters,
    ),
    wasDefault: truth(change.wasDefault, `${path}.wasDefault`),
  };
  if (
    isSensitive &&
    (parsed.to !== KAFKA_TOPIC_CONFIGURATION_REDACTION ||
      (parsed.from !== undefined && parsed.from !== KAFKA_TOPIC_CONFIGURATION_REDACTION))
  ) {
    throw new HostContractValidationError(path, "must redact sensitive history values");
  }
  return parsed;
}

export function parseKafkaTopicConfigurationHistoryEntry(
  value: unknown,
  path: string,
): KafkaTopicConfigurationHistoryEntry {
  const entry = record(value, path);
  exactKeys(
    entry,
    [
      "action",
      "at",
      "changes",
      "connectionName",
      "connectionTarget",
      "error",
      "id",
      "presetId",
      "success",
      "topic",
      "warning",
    ],
    path,
  );
  if (
    !Array.isArray(entry.changes) ||
    entry.changes.length < 1 ||
    entry.changes.length > KAFKA_TOPIC_CONFIGURATION_LIMITS.changes
  ) {
    throw new HostContractValidationError(
      `${path}.changes`,
      `must contain between 1 and ${KAFKA_TOPIC_CONFIGURATION_LIMITS.changes} changes`,
    );
  }
  const changes = entry.changes.map((change, index) =>
    parseHistoryChange(change, `${path}.changes[${index}]`),
  );
  if (new Set(changes.map((change) => change.name)).size !== changes.length) {
    throw new HostContractValidationError(`${path}.changes`, "must contain unique names");
  }
  const error = optionalText(
    entry,
    "error",
    path,
    KAFKA_TOPIC_CONFIGURATION_LIMITS.historyErrorCharacters,
  );
  const warning = optionalText(
    entry,
    "warning",
    path,
    KAFKA_TOPIC_CONFIGURATION_LIMITS.historyErrorCharacters,
  );
  const success = truth(entry.success, `${path}.success`);
  if ((success && error !== undefined) || (!success && warning !== undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent operation outcome");
  }
  const presetId = Object.hasOwn(entry, "presetId")
    ? parsePresetId(entry.presetId, `${path}.presetId`)
    : undefined;
  return {
    action: declaredValue(entry.action, ["apply", "validate"], `${path}.action`),
    at: canonicalIsoTimestamp(entry.at, `${path}.at`),
    changes,
    connectionName: text(entry.connectionName, `${path}.connectionName`, 512),
    connectionTarget: text(entry.connectionTarget, `${path}.connectionTarget`, 2_048),
    ...(error === undefined ? {} : { error }),
    id: text(entry.id, `${path}.id`, 128),
    ...(presetId === undefined ? {} : { presetId }),
    success,
    topic: parseKafkaTopicConfigurationTopic(entry.topic, `${path}.topic`),
    ...(warning === undefined ? {} : { warning }),
  };
}

function parseStoreCapability(
  value: unknown,
  path: string,
): KafkaTopicConfigurationHistoryStoreCapability {
  const store = record(value, path);
  exactKeys(store, ["durability", "recovery", "state"], path);
  const recovery = optionalText(store, "recovery", path, 2_048);
  const state = declaredValue(
    store.state,
    KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_STATES,
    `${path}.state`,
  );
  if (
    (state === "ready" && recovery !== undefined) ||
    (state === "unavailable" && recovery === undefined)
  ) {
    throw new HostContractValidationError(path, "contains inconsistent store capability");
  }
  return {
    durability: declaredValue(
      store.durability,
      KAFKA_TOPIC_CONFIGURATION_HISTORY_STORE_DURABILITIES,
      `${path}.durability`,
    ),
    ...(recovery === undefined ? {} : { recovery }),
    state,
  };
}

export function parseKafkaTopicConfigurationHistorySnapshot(
  value: unknown,
  path: string,
): KafkaTopicConfigurationHistorySnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["connectionName", "entries", "store", "topic"], path);
  if (
    !Array.isArray(snapshot.entries) ||
    snapshot.entries.length > KAFKA_TOPIC_CONFIGURATION_LIMITS.historyVisibleEntries
  ) {
    throw new HostContractValidationError(
      `${path}.entries`,
      `must contain at most ${KAFKA_TOPIC_CONFIGURATION_LIMITS.historyVisibleEntries} entries`,
    );
  }
  const entries = snapshot.entries.map((entry, index) =>
    parseKafkaTopicConfigurationHistoryEntry(entry, `${path}.entries[${index}]`),
  );
  const connectionName = text(snapshot.connectionName, `${path}.connectionName`, 512);
  const topic = parseKafkaTopicConfigurationTopic(snapshot.topic, `${path}.topic`);
  if (entries.some((entry) => entry.connectionName !== connectionName || entry.topic !== topic)) {
    throw new HostContractValidationError(
      `${path}.entries`,
      "must match the snapshot connection and topic",
    );
  }
  const store = parseStoreCapability(snapshot.store, `${path}.store`);
  if (store.state === "unavailable" && entries.length > 0) {
    throw new HostContractValidationError(
      `${path}.entries`,
      "must be empty while the history store is unavailable",
    );
  }
  return { connectionName, entries, store, topic };
}
