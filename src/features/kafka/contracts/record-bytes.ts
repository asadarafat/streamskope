import { HostContractValidationError } from "./validation-error";
import { boundedText, declaredValue, exactKeys, record } from "./validation-primitives";

export const KAFKA_ORIGINAL_RECORD_LIMITS = {
  bytes: 256 * 1_024,
  headers: 128,
  headerKeyBytes: 512,
  headerValueBytes: 8_192,
  headerPreviewBytes: 64 * 1_024,
} as const;

export interface KafkaRecordHeaderBytes {
  readonly key: string;
  readonly value: string | null;
}

export interface KafkaCompleteRecord {
  readonly state: "complete";
  readonly encoding: "base64";
  readonly key: string | null;
  readonly value: string | null;
  readonly headers: readonly KafkaRecordHeaderBytes[];
}

/** Canonical Base64 permits exact bytes to be compared without decoding or coercion. */
export function sameKafkaCompleteRecord(
  left: KafkaCompleteRecord,
  right: KafkaCompleteRecord,
): boolean {
  return (
    left.key === right.key &&
    left.value === right.value &&
    left.headers.length === right.headers.length &&
    left.headers.every(
      (header, index) =>
        header.key === right.headers[index]!.key && header.value === right.headers[index]!.value,
    )
  );
}

export type KafkaOriginalRecord =
  | KafkaCompleteRecord
  | {
      readonly state: "unavailable";
      readonly reason: "size-limit" | "masked" | "not-captured";
    };

export function base64ByteLength(value: string | null): number {
  return value === null
    ? 0
    : (value.length / 4) * 3 - (value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0);
}

export function parseRecordBase64(value: unknown, path: string, maximumBytes: number): string {
  const parsed = boundedText(value, path, Math.ceil(maximumBytes / 3) * 4);
  let decoded: string;
  try {
    decoded = atob(parsed);
  } catch {
    throw new HostContractValidationError(path, "must contain canonical, bounded Base64 bytes");
  }
  if (decoded.length > maximumBytes || btoa(decoded) !== parsed) {
    throw new HostContractValidationError(path, "must contain canonical, bounded Base64 bytes");
  }
  return parsed;
}

export function kafkaOriginalRecordByteLength(value: KafkaCompleteRecord): number {
  return (
    base64ByteLength(value.key) +
    base64ByteLength(value.value) +
    value.headers.reduce(
      (total, header) => total + base64ByteLength(header.key) + base64ByteLength(header.value),
      0,
    )
  );
}

export function parseKafkaOriginalRecord(value: unknown, path = "original"): KafkaOriginalRecord {
  const input = record(value, path);
  if (input.state === "unavailable") {
    exactKeys(input, ["state", "reason"], path);
    return {
      state: "unavailable",
      reason: declaredValue(
        input.reason,
        ["size-limit", "masked", "not-captured"] as const,
        `${path}.reason`,
      ),
    };
  }
  exactKeys(input, ["state", "encoding", "key", "value", "headers"], path);
  if (input.state !== "complete" || input.encoding !== "base64") {
    throw new HostContractValidationError(
      path,
      "must describe complete Base64 bytes or an unavailable original",
    );
  }
  if (
    !Array.isArray(input.headers) ||
    input.headers.length > KAFKA_ORIGINAL_RECORD_LIMITS.headers
  ) {
    throw new HostContractValidationError(`${path}.headers`, "exceeds the ordered header limit");
  }
  const nullable = (item: unknown, itemPath: string, limit: number): string | null =>
    item === null ? null : parseRecordBase64(item, itemPath, limit);
  const parsed: KafkaCompleteRecord = {
    state: "complete",
    encoding: "base64",
    key: nullable(input.key, `${path}.key`, KAFKA_ORIGINAL_RECORD_LIMITS.bytes),
    value: nullable(input.value, `${path}.value`, KAFKA_ORIGINAL_RECORD_LIMITS.bytes),
    headers: input.headers.map((item: unknown, index: number) => {
      const itemPath = `${path}.headers[${index}]`;
      const header = record(item, itemPath);
      exactKeys(header, ["key", "value"], itemPath);
      return {
        key: parseRecordBase64(
          header.key,
          `${itemPath}.key`,
          KAFKA_ORIGINAL_RECORD_LIMITS.headerKeyBytes,
        ),
        value: nullable(
          header.value,
          `${itemPath}.value`,
          KAFKA_ORIGINAL_RECORD_LIMITS.headerValueBytes,
        ),
      };
    }),
  };
  if (kafkaOriginalRecordByteLength(parsed) > KAFKA_ORIGINAL_RECORD_LIMITS.bytes) {
    throw new HostContractValidationError(path, "exceeds the total original record byte limit");
  }
  return Object.freeze({
    ...parsed,
    headers: Object.freeze(parsed.headers.map((header) => Object.freeze(header))),
  });
}
