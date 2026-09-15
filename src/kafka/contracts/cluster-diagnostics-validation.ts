import { parseKafkaConfigurationEntries } from "./configuration-validation";
import {
  KAFKA_CLUSTER_CONFIGURATION_ISSUE_CODES,
  KAFKA_CLUSTER_DIAGNOSTIC_LIMITS,
  KAFKA_CLUSTER_DIAGNOSTIC_STATES,
  type KafkaClusterBroker,
  type KafkaClusterConfigurationIssue,
  type KafkaClusterDetails,
  type KafkaClusterDetailsDocument,
  type KafkaClusterDiagnosticsSnapshot,
  type KafkaClusterProfileContext,
} from "./cluster-diagnostics-types";
import { utf8ByteLength } from "./message-limits";
import type { HostTextDocument } from "./text-document-types";
import { HostContractValidationError } from "./validation-error";
import {
  boundedUtf8Text,
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";
import type { HostError } from "./types";

function positiveInteger(value: unknown, path: string, maximum: number): number {
  const parsed = nonNegativeInteger(value, path);
  if (parsed < 1 || parsed > maximum) {
    throw new HostContractValidationError(path, `must be between 1 and ${maximum}, inclusive`);
  }
  return parsed;
}

function nullableText(value: unknown, path: string, maximum: number): string | null {
  return value === null ? null : text(value, path, maximum);
}

function parseProfile(value: unknown, path: string): KafkaClusterProfileContext {
  const profile = record(value, path);
  exactKeys(profile, ["brokers", "id", "name"], path);
  if (
    !Array.isArray(profile.brokers) ||
    profile.brokers.length < 1 ||
    profile.brokers.length > KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.profileBrokers
  ) {
    throw new HostContractValidationError(
      `${path}.brokers`,
      `must contain between 1 and ${KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.profileBrokers} brokers`,
    );
  }
  const brokers = profile.brokers.map((broker, index) =>
    text(broker, `${path}.brokers[${index}]`, KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.brokerHostCharacters),
  );
  if (new Set(brokers).size !== brokers.length) {
    throw new HostContractValidationError(`${path}.brokers`, "must contain unique brokers");
  }
  return {
    brokers,
    id:
      profile.id === null
        ? null
        : text(profile.id, `${path}.id`, KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.profileIdCharacters),
    name: text(profile.name, `${path}.name`, KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.profileNameCharacters),
  };
}

function parseBroker(value: unknown, path: string): KafkaClusterBroker {
  const broker = record(value, path);
  exactKeys(broker, ["host", "nodeId", "port", "rack"], path);
  return {
    host: text(broker.host, `${path}.host`, KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.brokerHostCharacters),
    nodeId: nonNegativeInteger(broker.nodeId, `${path}.nodeId`),
    port: positiveInteger(broker.port, `${path}.port`, 65_535),
    rack: nullableText(
      broker.rack,
      `${path}.rack`,
      KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.brokerHostCharacters,
    ),
  };
}

function parseBrokers(value: unknown, path: string): readonly KafkaClusterBroker[] {
  if (!Array.isArray(value) || value.length > KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.brokers) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.brokers} brokers`,
    );
  }
  const brokers = value.map((broker, index) => parseBroker(broker, `${path}[${index}]`));
  if (new Set(brokers.map((broker) => broker.nodeId)).size !== brokers.length) {
    throw new HostContractValidationError(path, "must contain unique broker node IDs");
  }
  if (brokers.some((broker, index) => index > 0 && brokers[index - 1]!.nodeId > broker.nodeId)) {
    throw new HostContractValidationError(path, "must be sorted by broker node ID");
  }
  return brokers;
}

function parseConfigurationIssue(value: unknown, path: string): KafkaClusterConfigurationIssue {
  const issue = record(value, path);
  exactKeys(issue, ["code", "recovery", "summary"], path);
  return {
    code: declaredValue(issue.code, KAFKA_CLUSTER_CONFIGURATION_ISSUE_CODES, `${path}.code`),
    recovery: text(issue.recovery, `${path}.recovery`, 2_048),
    summary: text(issue.summary, `${path}.summary`, 2_048),
  };
}

export function parseKafkaClusterDetails(value: unknown, path: string): KafkaClusterDetails {
  const cluster = record(value, path);
  exactKeys(
    cluster,
    [
      "brokers",
      "clusterId",
      "configuration",
      "configurationIssue",
      "configurationSourceBrokerId",
      "controllerId",
    ],
    path,
  );
  const brokers = parseBrokers(cluster.brokers, `${path}.brokers`);
  const configuration = parseKafkaConfigurationEntries(
    cluster.configuration,
    `${path}.configuration`,
  );
  if (
    configuration.some(
      (entry, index) => index > 0 && configuration[index - 1]!.name.localeCompare(entry.name) > 0,
    )
  ) {
    throw new HostContractValidationError(
      `${path}.configuration`,
      "must be sorted by configuration name",
    );
  }
  const configurationIssue = Object.hasOwn(cluster, "configurationIssue")
    ? parseConfigurationIssue(cluster.configurationIssue, `${path}.configurationIssue`)
    : undefined;
  const configurationSourceBrokerId =
    cluster.configurationSourceBrokerId === null
      ? null
      : nonNegativeInteger(
          cluster.configurationSourceBrokerId,
          `${path}.configurationSourceBrokerId`,
        );
  const parsed: KafkaClusterDetails = {
    brokers,
    clusterId: nullableText(
      cluster.clusterId,
      `${path}.clusterId`,
      KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.clusterIdCharacters,
    ),
    configuration,
    ...(configurationIssue === undefined ? {} : { configurationIssue }),
    configurationSourceBrokerId,
    controllerId:
      cluster.controllerId === null
        ? null
        : nonNegativeInteger(cluster.controllerId, `${path}.controllerId`),
  };
  const sourceExists =
    configurationSourceBrokerId === null
      ? false
      : brokers.some((broker) => broker.nodeId === configurationSourceBrokerId);
  if (
    (configurationSourceBrokerId !== null && !sourceExists) ||
    (configurationIssue === undefined &&
      ((brokers.length === 0 && configurationSourceBrokerId !== null) ||
        (brokers.length > 0 && configurationSourceBrokerId === null))) ||
    (configurationIssue !== undefined && configuration.length > 0) ||
    (configurationIssue?.code === "no-brokers" &&
      (brokers.length > 0 || configurationSourceBrokerId !== null)) ||
    (brokers.length === 0 && configurationIssue?.code !== "no-brokers")
  ) {
    throw new HostContractValidationError(path, "contains inconsistent broker configuration state");
  }
  return parsed;
}

export function parseKafkaClusterDetailsDocument(
  value: unknown,
  path: string,
): KafkaClusterDetailsDocument {
  const document = record(value, path);
  exactKeys(document, ["cluster", "endpoint", "fetchedAt", "profile"], path);
  const parsed: KafkaClusterDetailsDocument = {
    cluster: parseKafkaClusterDetails(document.cluster, `${path}.cluster`),
    endpoint: text(
      document.endpoint,
      `${path}.endpoint`,
      KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.endpointCharacters,
    ),
    fetchedAt: canonicalIsoTimestamp(document.fetchedAt, `${path}.fetchedAt`),
    profile: parseProfile(document.profile, `${path}.profile`),
  };
  if (utf8ByteLength(JSON.stringify(parsed)) > KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.snapshotBytes) {
    throw new HostContractValidationError(
      path,
      `must serialize within ${KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.snapshotBytes} UTF-8 bytes`,
    );
  }
  return parsed;
}

export function parseKafkaClusterDiagnosticsSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): KafkaClusterDiagnosticsSnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["cluster", "endpoint", "error", "fetchedAt", "profile", "state"], path);
  const state = declaredValue(snapshot.state, KAFKA_CLUSTER_DIAGNOSTIC_STATES, `${path}.state`);
  let parsed: KafkaClusterDiagnosticsSnapshot;
  if (state === "unavailable") {
    if (
      snapshot.cluster !== null ||
      snapshot.endpoint !== null ||
      snapshot.fetchedAt !== null ||
      snapshot.profile !== null ||
      Object.hasOwn(snapshot, "error")
    ) {
      throw new HostContractValidationError(path, "contains inconsistent unavailable state");
    }
    parsed = {
      cluster: null,
      endpoint: null,
      fetchedAt: null,
      profile: null,
      state,
    };
  } else if (state === "loading") {
    if (
      snapshot.cluster !== null ||
      snapshot.fetchedAt !== null ||
      Object.hasOwn(snapshot, "error")
    ) {
      throw new HostContractValidationError(path, "contains inconsistent loading state");
    }
    parsed = {
      cluster: null,
      endpoint: text(
        snapshot.endpoint,
        `${path}.endpoint`,
        KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.endpointCharacters,
      ),
      fetchedAt: null,
      profile: parseProfile(snapshot.profile, `${path}.profile`),
      state,
    };
  } else if (state === "failed") {
    if (
      snapshot.cluster !== null ||
      snapshot.fetchedAt !== null ||
      !Object.hasOwn(snapshot, "error")
    ) {
      throw new HostContractValidationError(path, "contains inconsistent failed state");
    }
    parsed = {
      cluster: null,
      endpoint: text(
        snapshot.endpoint,
        `${path}.endpoint`,
        KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.endpointCharacters,
      ),
      error: parseError(snapshot.error, `${path}.error`),
      fetchedAt: null,
      profile: parseProfile(snapshot.profile, `${path}.profile`),
      state,
    };
  } else {
    const details = parseKafkaClusterDetailsDocument(
      {
        cluster: snapshot.cluster,
        endpoint: snapshot.endpoint,
        fetchedAt: snapshot.fetchedAt,
        profile: snapshot.profile,
      },
      path,
    );
    const hasIssue = details.cluster.configurationIssue !== undefined;
    if (
      (state === "ready" && hasIssue) ||
      (state === "partial" && !hasIssue) ||
      (state === "stale" && !Object.hasOwn(snapshot, "error")) ||
      (state !== "stale" && Object.hasOwn(snapshot, "error"))
    ) {
      throw new HostContractValidationError(path, `contains inconsistent ${state} state`);
    }
    parsed =
      state === "stale"
        ? {
            ...details,
            error: parseError(snapshot.error, `${path}.error`),
            state,
          }
        : { ...details, state };
  }
  if (utf8ByteLength(JSON.stringify(parsed)) > KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.snapshotBytes) {
    throw new HostContractValidationError(
      path,
      `must serialize within ${KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.snapshotBytes} UTF-8 bytes`,
    );
  }
  return parsed;
}

export function parseHostTextDocument(value: unknown, path: string): HostTextDocument {
  const document = record(value, path);
  exactKeys(document, ["byteSize", "content", "fileName", "mediaType"], path);
  const content = boundedUtf8Text(
    document.content,
    `${path}.content`,
    KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.exportBytes,
  );
  const byteSize = nonNegativeInteger(document.byteSize, `${path}.byteSize`);
  if (byteSize !== utf8ByteLength(content)) {
    throw new HostContractValidationError(`${path}.byteSize`, "must equal the content UTF-8 size");
  }
  const fileName = text(
    document.fileName,
    `${path}.fileName`,
    KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.fileNameCharacters,
  );
  if (
    !fileName.endsWith(".json") ||
    fileName.includes("/") ||
    fileName.includes("\\") ||
    fileName === ".json"
  ) {
    throw new HostContractValidationError(`${path}.fileName`, "must be a safe JSON file name");
  }
  if (document.mediaType !== "application/json") {
    throw new HostContractValidationError(`${path}.mediaType`, "must equal application/json");
  }
  let decoded: unknown;
  try {
    decoded = JSON.parse(content) as unknown;
  } catch {
    throw new HostContractValidationError(`${path}.content`, "must contain valid JSON");
  }
  const parsed = parseKafkaClusterDetailsDocument(decoded, `${path}.content`);
  if (`${JSON.stringify(parsed, null, 2)}\n` !== content) {
    throw new HostContractValidationError(
      `${path}.content`,
      "must use the canonical indented cluster-details representation",
    );
  }
  return {
    byteSize,
    content,
    fileName,
    mediaType: "application/json",
  };
}
