import {
  REDPANDA_TRANSFORM_LIMITS,
  REDPANDA_TRANSFORM_STATES,
  REDPANDA_TRANSFORM_STATUSES,
  type RedpandaTransformDeletionInput,
  type RedpandaTransformDetailSnapshot,
  type RedpandaTransformInventorySnapshot,
  type RedpandaTransformLogEntry,
  type RedpandaTransformLogsSnapshot,
  type RedpandaTransformSummary,
} from "./transform-types";
import type { HostError } from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  record,
  text,
  truth,
} from "./validation-primitives";

export function parseRedpandaTransformIdentity(
  value: unknown,
  path: string,
): { readonly name: string } {
  const input = record(value, path);
  exactKeys(input, ["name"], path);
  return { name: text(input.name, `${path}.name`, REDPANDA_TRANSFORM_LIMITS.fieldCharacters) };
}

export function parseRedpandaTransformDeletionInput(
  value: unknown,
  path: string,
): RedpandaTransformDeletionInput {
  const input = record(value, path);
  exactKeys(input, ["confirmation", "name"], path);
  const name = text(input.name, `${path}.name`, REDPANDA_TRANSFORM_LIMITS.fieldCharacters);
  const confirmation = text(
    input.confirmation,
    `${path}.confirmation`,
    REDPANDA_TRANSFORM_LIMITS.fieldCharacters,
  );
  if (confirmation !== name) {
    throw new HostContractValidationError(
      `${path}.confirmation`,
      "must exactly match the transform name",
    );
  }
  return { confirmation, name };
}

function boundedTextArray(value: unknown, path: string, limit: number): readonly string[] {
  if (!Array.isArray(value) || value.length > limit) {
    throw new HostContractValidationError(path, "exceeds the collection bound");
  }
  return value.map((entry, index) =>
    text(entry, `${path}[${String(index)}]`, REDPANDA_TRANSFORM_LIMITS.fieldCharacters),
  );
}

function parseTransformEnvironment(
  value: unknown,
  path: string,
): RedpandaTransformSummary["environment"] {
  if (!Array.isArray(value) || value.length > REDPANDA_TRANSFORM_LIMITS.environmentNames) {
    throw new HostContractValidationError(path, "exceeds the environment bound");
  }
  return value.map((entry, index) => {
    const entryPath = `${path}[${String(index)}]`;
    const variable = record(entry, entryPath);
    exactKeys(variable, ["name", "valuePresent"], entryPath);
    return {
      name: text(variable.name, `${entryPath}.name`, REDPANDA_TRANSFORM_LIMITS.fieldCharacters),
      valuePresent: truth(variable.valuePresent, `${entryPath}.valuePresent`),
    };
  });
}

function parseTransformOffset(value: unknown, path: string): RedpandaTransformSummary["offset"] {
  if (value === null) {
    return null;
  }
  const offset = record(value, path);
  exactKeys(offset, ["format", "value"], path);
  return {
    format: text(offset.format, `${path}.format`, 128),
    value: text(offset.value, `${path}.value`, 128),
  };
}

export function parseRedpandaTransformSummary(
  value: unknown,
  path: string,
): RedpandaTransformSummary {
  const transform = record(value, path);
  exactKeys(
    transform,
    [
      "aggregateStatus",
      "compression",
      "environment",
      "inputTopic",
      "maximumLag",
      "name",
      "offset",
      "outputTopics",
      "statuses",
    ],
    path,
  );
  if (
    !Array.isArray(transform.statuses) ||
    transform.statuses.length > REDPANDA_TRANSFORM_LIMITS.transforms
  ) {
    throw new HostContractValidationError(`${path}.statuses`, "exceeds the status bound");
  }
  return {
    aggregateStatus: declaredValue(
      transform.aggregateStatus,
      REDPANDA_TRANSFORM_STATUSES,
      `${path}.aggregateStatus`,
    ),
    compression: text(
      transform.compression,
      `${path}.compression`,
      REDPANDA_TRANSFORM_LIMITS.fieldCharacters,
    ),
    environment: parseTransformEnvironment(transform.environment, `${path}.environment`),
    inputTopic: text(
      transform.inputTopic,
      `${path}.inputTopic`,
      REDPANDA_TRANSFORM_LIMITS.fieldCharacters,
    ),
    name: text(transform.name, `${path}.name`, REDPANDA_TRANSFORM_LIMITS.fieldCharacters),
    maximumLag: nonNegativeInteger(transform.maximumLag, `${path}.maximumLag`),
    offset: parseTransformOffset(transform.offset, `${path}.offset`),
    outputTopics: boundedTextArray(
      transform.outputTopics,
      `${path}.outputTopics`,
      REDPANDA_TRANSFORM_LIMITS.outputTopics,
    ),
    statuses: transform.statuses.map((entry, index) => {
      const status = record(entry, `${path}.statuses[${String(index)}]`);
      exactKeys(
        status,
        ["lag", "nodeId", "partition", "status"],
        `${path}.statuses[${String(index)}]`,
      );
      return {
        lag: nonNegativeInteger(status.lag, `${path}.statuses[${String(index)}].lag`),
        nodeId: nonNegativeInteger(status.nodeId, `${path}.statuses[${String(index)}].nodeId`),
        partition: nonNegativeInteger(
          status.partition,
          `${path}.statuses[${String(index)}].partition`,
        ),
        status: declaredValue(
          status.status,
          REDPANDA_TRANSFORM_STATUSES,
          `${path}.statuses[${String(index)}].status`,
        ),
      };
    }),
  };
}

function optionalError(
  value: Record<string, unknown>,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): HostError | undefined {
  return Object.hasOwn(value, "error") ? parseError(value.error, `${path}.error`) : undefined;
}

export function parseRedpandaTransformInventorySnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): RedpandaTransformInventorySnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    [
      "connectionName",
      "endpoint",
      "error",
      "omittedTransforms",
      "refreshedAt",
      "state",
      "transforms",
    ],
    path,
  );
  if (
    !Array.isArray(snapshot.transforms) ||
    snapshot.transforms.length > REDPANDA_TRANSFORM_LIMITS.transforms
  ) {
    throw new HostContractValidationError(`${path}.transforms`, "exceeds the transform bound");
  }
  const error = optionalError(snapshot, path, parseError);
  return {
    connectionName: nullableText(snapshot.connectionName, `${path}.connectionName`, 256),
    endpoint: nullableText(snapshot.endpoint, `${path}.endpoint`, 2_048),
    ...(error === undefined ? {} : { error }),
    omittedTransforms: nonNegativeInteger(snapshot.omittedTransforms, `${path}.omittedTransforms`),
    refreshedAt: nullableText(snapshot.refreshedAt, `${path}.refreshedAt`, 128),
    state: declaredValue(snapshot.state, REDPANDA_TRANSFORM_STATES, `${path}.state`),
    transforms: snapshot.transforms.map((transform, index) =>
      parseRedpandaTransformSummary(transform, `${path}.transforms[${String(index)}]`),
    ),
  };
}

export function parseRedpandaTransformDetailSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): RedpandaTransformDetailSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "endpoint", "error", "refreshedAt", "state", "transform", "transformName"],
    path,
  );
  const error = optionalError(snapshot, path, parseError);
  return {
    connectionName: nullableText(snapshot.connectionName, `${path}.connectionName`, 256),
    endpoint: nullableText(snapshot.endpoint, `${path}.endpoint`, 2_048),
    ...(error === undefined ? {} : { error }),
    refreshedAt: nullableText(snapshot.refreshedAt, `${path}.refreshedAt`, 128),
    state: declaredValue(snapshot.state, REDPANDA_TRANSFORM_STATES, `${path}.state`),
    transform:
      snapshot.transform === null
        ? null
        : parseRedpandaTransformSummary(snapshot.transform, `${path}.transform`),
    transformName: nullableText(
      snapshot.transformName,
      `${path}.transformName`,
      REDPANDA_TRANSFORM_LIMITS.fieldCharacters,
    ),
  };
}

function parseLog(value: unknown, path: string): RedpandaTransformLogEntry {
  const log = record(value, path);
  exactKeys(log, ["level", "message", "offset", "partition", "timestamp"], path);
  return {
    level: text(log.level, `${path}.level`, 64),
    message: text(log.message, `${path}.message`, 32_768),
    offset: text(log.offset, `${path}.offset`, 128),
    partition: nonNegativeInteger(log.partition, `${path}.partition`),
    timestamp: nullableText(log.timestamp, `${path}.timestamp`, 128),
  };
}

export function parseRedpandaTransformLogsSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): RedpandaTransformLogsSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "error", "logs", "omittedLogs", "refreshedAt", "state", "transformName"],
    path,
  );
  if (!Array.isArray(snapshot.logs) || snapshot.logs.length > REDPANDA_TRANSFORM_LIMITS.logs) {
    throw new HostContractValidationError(`${path}.logs`, "exceeds the transform log bound");
  }
  const error = optionalError(snapshot, path, parseError);
  return {
    connectionName: nullableText(snapshot.connectionName, `${path}.connectionName`, 256),
    ...(error === undefined ? {} : { error }),
    logs: snapshot.logs.map((log, index) => parseLog(log, `${path}.logs[${String(index)}]`)),
    omittedLogs: nonNegativeInteger(snapshot.omittedLogs, `${path}.omittedLogs`),
    refreshedAt: nullableText(snapshot.refreshedAt, `${path}.refreshedAt`, 128),
    state: declaredValue(snapshot.state, REDPANDA_TRANSFORM_STATES, `${path}.state`),
    transformName: nullableText(
      snapshot.transformName,
      `${path}.transformName`,
      REDPANDA_TRANSFORM_LIMITS.fieldCharacters,
    ),
  };
}
