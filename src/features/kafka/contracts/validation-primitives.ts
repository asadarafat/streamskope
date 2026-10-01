import { utf8ByteLength } from "./message-limits";
import { HostContractValidationError } from "./validation-error";

export type UnknownRecord = Record<string, unknown>;

export function record(value: unknown, path: string): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new HostContractValidationError(path, "must be an object");
  }
  return value as UnknownRecord;
}

export function emptyRecord(value: unknown, path: string): Readonly<Record<string, never>> {
  const payload = record(value, path);
  exactKeys(payload, [], path);
  return {};
}

export function exactKeys(value: UnknownRecord, allowed: readonly string[], path: string): void {
  const allowedKeys = new Set(allowed);
  const unexpected = Object.keys(value).find((key) => !allowedKeys.has(key));
  if (unexpected !== undefined) {
    throw new HostContractValidationError(`${path}.${unexpected}`, "is not declared");
  }
}

export function boundedText(value: unknown, path: string, maximum: number): string {
  if (typeof value !== "string" || value.length > maximum) {
    throw new HostContractValidationError(
      path,
      `must be a string no longer than ${maximum} characters`,
    );
  }
  return value;
}

export function boundedUtf8Text(value: unknown, path: string, maximumBytes: number): string {
  if (typeof value !== "string" || utf8ByteLength(value) > maximumBytes) {
    throw new HostContractValidationError(
      path,
      `must be a string no larger than ${maximumBytes} UTF-8 bytes`,
    );
  }
  return value;
}

export function nullableBoundedUtf8Text(
  value: unknown,
  path: string,
  maximumBytes: number,
): string | null {
  return value === null ? null : boundedUtf8Text(value, path, maximumBytes);
}

export function utf8Text(value: unknown, path: string, maximumBytes: number): string {
  const parsed = boundedUtf8Text(value, path, maximumBytes);
  if (parsed.length === 0) {
    throw new HostContractValidationError(
      path,
      `must be a non-empty string no larger than ${maximumBytes} UTF-8 bytes`,
    );
  }
  return parsed;
}

export function text(value: unknown, path: string, maximum = 4_096): string {
  const parsed = boundedText(value, path, maximum);
  if (parsed.length === 0) {
    throw new HostContractValidationError(
      path,
      `must be a non-empty string no longer than ${maximum} characters`,
    );
  }
  return parsed;
}

export function nullableText(value: unknown, path: string, maximum = 4_096): string | null {
  return value === null ? null : text(value, path, maximum);
}

export function truth(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") {
    throw new HostContractValidationError(path, "must be a boolean");
  }
  return value;
}

export function nonNegativeInteger(value: unknown, path: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new HostContractValidationError(path, "must be a non-negative safe integer");
  }
  return value as number;
}

export function positiveBoundedInteger(value: unknown, path: string, maximum: number): number {
  const parsed = nonNegativeInteger(value, path);
  if (parsed < 1 || parsed > maximum) {
    throw new HostContractValidationError(path, `must be between 1 and ${maximum}, inclusive`);
  }
  return parsed;
}

export function parseBoundedBrokers(
  value: unknown,
  path: string,
  maximumItems: number,
  maximumCharacters: number,
): readonly string[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new HostContractValidationError(path, "must contain at least one broker");
  }
  if (value.length > maximumItems) {
    throw new HostContractValidationError(path, `must contain at most ${maximumItems} brokers`);
  }
  return value.map((broker, index) => text(broker, `${path}[${index}]`, maximumCharacters));
}

export function declaredValue<T extends string>(
  value: unknown,
  declarations: readonly T[],
  path: string,
): T {
  if (typeof value !== "string" || !declarations.includes(value as T)) {
    throw new HostContractValidationError(path, `must be one of ${declarations.join(", ")}`);
  }
  return value as T;
}

export function optionalText(
  value: UnknownRecord,
  key: string,
  path: string,
  maximum = 4_096,
): string | undefined {
  return Object.hasOwn(value, key) ? text(value[key], `${path}.${key}`, maximum) : undefined;
}

export function canonicalIsoTimestamp(value: unknown, path: string): string {
  const timestamp = text(value, path, 128);
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString() !== timestamp) {
    throw new HostContractValidationError(path, "must be a canonical ISO timestamp");
  }
  return timestamp;
}
