import { KAFKA_RULE_LIMITS } from "./rule-types";
import { validateKafkaRuleExpression } from "./rule-expression-parser";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

export const KAFKA_QUERY_LIMITS = {
  filterCharacters: 256,
  scanRecords: 10_000,
  scanBytes: 32 * 1_048_576,
  durationMs: 30_000,
  partitions: 2_048,
} as const;

export interface KafkaSearchFilter {
  readonly expression?: string;
  readonly key: string;
  readonly value: string;
  readonly offset: string;
  /** Exact locator matching for bounded investigation reads; ordinary offset remains substring search. */
  readonly offsetExact?: string;
  readonly timestamp: string;
  readonly partition: number | null;
}

export function parseKafkaSearchFilter(value: unknown, path: string): KafkaSearchFilter {
  const filter = record(value, path);
  exactKeys(
    filter,
    ["key", "value", "offset", "offsetExact", "timestamp", "partition", "expression"],
    path,
  );
  let offsetExact: string | undefined;
  if (filter.offsetExact !== undefined) {
    offsetExact = text(filter.offsetExact, `${path}.offsetExact`, 20);
    if (!/^(0|[1-9]\d*)$/u.test(offsetExact) || BigInt(offsetExact) > 9_223_372_036_854_775_807n) {
      throw new HostContractValidationError(`${path}.offsetExact`, "must be an exact Kafka offset");
    }
  }
  let expression: string | undefined;
  if (filter.expression !== undefined) {
    expression = boundedText(
      filter.expression,
      `${path}.expression`,
      KAFKA_RULE_LIMITS.expressionCharacters,
    ).trim();
    if (expression.length > 0) {
      const result = validateKafkaRuleExpression(expression);
      if (!result.valid)
        throw new HostContractValidationError(
          `${path}.expression`,
          result.diagnostic ?? "Invalid expression",
        );
    }
  }
  return {
    ...(expression === undefined ? {} : { expression }),
    ...(offsetExact === undefined ? {} : { offsetExact }),
    key: boundedText(filter.key, `${path}.key`, KAFKA_QUERY_LIMITS.filterCharacters),
    value: boundedText(filter.value, `${path}.value`, KAFKA_QUERY_LIMITS.filterCharacters),
    offset: boundedText(filter.offset, `${path}.offset`, KAFKA_QUERY_LIMITS.filterCharacters),
    timestamp: boundedText(
      filter.timestamp,
      `${path}.timestamp`,
      KAFKA_QUERY_LIMITS.filterCharacters,
    ),
    partition:
      filter.partition === null ? null : nonNegativeInteger(filter.partition, `${path}.partition`),
  };
}

export function matchesKafkaSearchFilter(
  message: {
    readonly key: string | null;
    readonly payload: string | null;
    readonly offset: string;
    readonly timestamp: string;
    readonly partition: number;
  },
  filter: KafkaSearchFilter,
): boolean {
  const contains = (value: string | null, criterion: string): boolean =>
    criterion.trim().length === 0 ||
    (value !== null && value.toLowerCase().includes(criterion.trim().toLowerCase()));
  return (
    (filter.partition === null || filter.partition === message.partition) &&
    (filter.offsetExact === undefined || filter.offsetExact === message.offset) &&
    contains(message.key, filter.key) &&
    contains(message.payload, filter.value) &&
    contains(message.offset, filter.offset) &&
    contains(message.timestamp, filter.timestamp)
  );
}

export const KAFKA_READ_REASONS = [
  "reading",
  "range-complete",
  "result-limit",
  "scan-limit",
  "byte-limit",
  "fetch-limit",
  "deadline",
  "cancelled",
  "failed",
] as const;
export type KafkaReadReason = (typeof KAFKA_READ_REASONS)[number];

export interface KafkaReadCoverage {
  readonly reason: KafkaReadReason;
  readonly scannedRecords: number;
  readonly scannedBytes: number;
  readonly matchedRecords: number;
  readonly unavailableRecords: number;
  readonly partitions: readonly {
    readonly partition: number;
    readonly startOffset: string;
    readonly endOffset: string;
    readonly nextOffset: string;
  }[];
}

export function parseKafkaReadCoverage(value: unknown, path: string): KafkaReadCoverage {
  const coverage = record(value, path);
  exactKeys(
    coverage,
    [
      "reason",
      "scannedRecords",
      "scannedBytes",
      "matchedRecords",
      "unavailableRecords",
      "partitions",
    ],
    path,
  );
  if (
    !Array.isArray(coverage.partitions) ||
    coverage.partitions.length > KAFKA_QUERY_LIMITS.partitions
  ) {
    throw new HostContractValidationError(
      `${path}.partitions`,
      "must be a bounded partition inventory",
    );
  }
  const partitions = coverage.partitions.map((value: unknown, index: number) => {
    const itemPath = `${path}.partitions[${index}]`;
    const item = record(value, itemPath);
    exactKeys(item, ["partition", "startOffset", "endOffset", "nextOffset"], itemPath);
    const offset = (key: string): string => {
      const result = text(item[key], `${itemPath}.${key}`, 20);
      if (!/^(0|[1-9]\d*)$/u.test(result) || BigInt(result) > 9_223_372_036_854_775_807n) {
        throw new HostContractValidationError(`${itemPath}.${key}`, "must be a Kafka offset");
      }
      return result;
    };
    const startOffset = offset("startOffset");
    const endOffset = offset("endOffset");
    const nextOffset = offset("nextOffset");
    if (BigInt(startOffset) > BigInt(nextOffset) || BigInt(nextOffset) > BigInt(endOffset)) {
      throw new HostContractValidationError(itemPath, "must describe ordered half-open offsets");
    }
    return {
      partition: nonNegativeInteger(item.partition, `${itemPath}.partition`),
      startOffset,
      endOffset,
      nextOffset,
    };
  });
  if (new Set(partitions.map((item) => item.partition)).size !== partitions.length) {
    throw new HostContractValidationError(path, "must not repeat partitions");
  }
  const result: KafkaReadCoverage = {
    reason: declaredValue(coverage.reason, KAFKA_READ_REASONS, `${path}.reason`),
    scannedRecords: nonNegativeInteger(coverage.scannedRecords, `${path}.scannedRecords`),
    scannedBytes: nonNegativeInteger(coverage.scannedBytes, `${path}.scannedBytes`),
    matchedRecords: nonNegativeInteger(coverage.matchedRecords, `${path}.matchedRecords`),
    unavailableRecords: nonNegativeInteger(
      coverage.unavailableRecords,
      `${path}.unavailableRecords`,
    ),
    partitions,
  };
  if (
    result.matchedRecords + result.unavailableRecords > result.scannedRecords ||
    (result.reason === "range-complete" &&
      partitions.some((item) => item.nextOffset !== item.endOffset))
  ) {
    throw new HostContractValidationError(path, "contains inconsistent read coverage");
  }
  return result;
}
