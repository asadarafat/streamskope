import { NATS_LIMITS } from "./types";

/** Paths and values are deliberately absent: validation errors must never echo secrets. */
export class NatsContractValidationError extends Error {
  constructor() {
    super("NATS input does not match the supported contract.");
    this.name = "NatsContractValidationError";
  }
}
export type NatsUnknownRecord = Record<string, unknown>;
export function natsObject(value: unknown): NatsUnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new NatsContractValidationError();
  return value as NatsUnknownRecord;
}
export function natsExactKeys(
  value: NatsUnknownRecord,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  if (
    required.some((key) => !Object.hasOwn(value, key)) ||
    Object.keys(value).some((key) => !required.includes(key) && !optional.includes(key))
  )
    throw new NatsContractValidationError();
}
export function natsUtf8Bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}
export function natsHasAsciiControl(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code < 32 || code === 127) return true;
  }
  return false;
}
export function natsText(value: unknown, maximumBytes: number, allowEmpty = false): string {
  if (
    typeof value !== "string" ||
    (!allowEmpty && value.length === 0) ||
    value.length > maximumBytes ||
    natsUtf8Bytes(value) > maximumBytes
  )
    throw new NatsContractValidationError();
  // Reject unpaired surrogates, which JSON and UTF-8 encode differently.
  if (
    new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      new TextEncoder().encode(value),
    ) !== value
  )
    throw new NatsContractValidationError();
  return value;
}
export function natsIdentifier(value: unknown): string {
  const parsed = natsText(value, NATS_LIMITS.identifierCharacters);
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(parsed) || /[^A-Za-z0-9._-]/u.test(parsed))
    throw new NatsContractValidationError();
  return parsed;
}
export function natsInteger(
  value: unknown,
  minimum = 0,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  )
    throw new NatsContractValidationError();
  return value;
}
export function natsBoolean(value: unknown): boolean {
  if (typeof value !== "boolean") throw new NatsContractValidationError();
  return value;
}
export function natsEnum<Value extends string>(value: unknown, declared: readonly Value[]): Value {
  if (typeof value !== "string" || !declared.includes(value as Value))
    throw new NatsContractValidationError();
  return value as Value;
}
export function natsTimestamp(value: unknown): string {
  const parsed = natsText(value, 32);
  const timestamp = new Date(parsed);
  if (!Number.isFinite(timestamp.getTime()) || timestamp.toISOString() !== parsed)
    throw new NatsContractValidationError();
  return parsed;
}
export function natsArray(value: unknown, maximum: number): readonly unknown[] {
  if (!Array.isArray(value) || value.length > maximum) throw new NatsContractValidationError();
  return value as readonly unknown[];
}
export function natsCanonicalBase64(
  value: unknown,
  maximumBytes: number,
): { readonly data: string; readonly bytes: number } {
  const data = natsText(value, Math.ceil(maximumBytes / 3) * 4, true);
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u.test(data))
    throw new NatsContractValidationError();
  const bytes = atob(data);
  if (bytes.length > maximumBytes || btoa(bytes) !== data) throw new NatsContractValidationError();
  return { data, bytes: bytes.length };
}
