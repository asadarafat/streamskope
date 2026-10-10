import { HostContractValidationError } from "./validation-error";
import { record, exactKeys, text, truth, declaredValue } from "./validation-primitives";

export const CLIENT_QUOTA_KEYS = [
  "producer_byte_rate",
  "consumer_byte_rate",
  "request_percentage",
  "controller_mutation_rate",
] as const;
export type ClientQuotaKey = (typeof CLIENT_QUOTA_KEYS)[number];
export interface ClientQuotaComponent {
  readonly type: "user" | "client-id";
  readonly name: string | null;
}
export type ClientQuotaEntity = readonly ClientQuotaComponent[];
export interface ClientQuotaValue {
  readonly key: string;
  readonly value: number;
}
export interface ClientQuotaChange {
  readonly key: ClientQuotaKey;
  readonly value: number | null;
}
export interface ClientQuotaInput {
  readonly entity: ClientQuotaEntity;
  readonly changes: readonly ClientQuotaChange[];
}
export interface ClientQuotaSnapshot {
  readonly clusterId: string;
  readonly entity: ClientQuotaEntity;
  readonly values: readonly ClientQuotaValue[];
  readonly alterSupported: boolean;
}
export interface ClientQuotaReview {
  readonly planId: string;
  readonly expiresAt: string;
  readonly connectionName: string;
  readonly input: ClientQuotaInput;
  readonly baseline: ClientQuotaSnapshot;
  readonly expected: readonly ClientQuotaValue[];
  readonly confirmation: string;
}
export interface ClientQuotaOutcome {
  readonly input: ClientQuotaInput;
  readonly state: "acknowledged" | "rejected" | "unknown" | "unsent";
  readonly verification: "verified" | "different" | "unavailable";
  readonly cleanup: "confirmed" | "unresolved";
  readonly observed: readonly ClientQuotaValue[] | null;
  readonly detail: string;
}
function finiteQuota(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
    throw new HostContractValidationError(path, "requires a finite nonnegative quota");
  return Object.is(value, -0) ? 0 : value;
}
export function parseClientQuotaEntity(value: unknown): ClientQuotaEntity {
  if (!Array.isArray(value) || value.length < 1 || value.length > 2)
    throw new HostContractValidationError("entity", "requires one or two exact quota dimensions");
  const result = value.map((item: unknown) => {
    const v = record(item, "entity");
    exactKeys(v, ["type", "name"], "entity");
    const type = declaredValue(v.type, ["user", "client-id"] as const, "entity.type");
    const name = v.name === null ? null : text(v.name, "entity.name", 128);
    if (
      name !== null &&
      Array.from(name).some(
        (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
      )
    )
      throw new HostContractValidationError("entity.name", "control characters are unsupported");
    if (name !== null && new TextDecoder().decode(new TextEncoder().encode(name)) !== name)
      throw new HostContractValidationError("entity.name", "requires valid Unicode scalar values");
    return { type, name };
  });
  if (new Set(result.map((c) => c.type)).size !== result.length)
    throw new HostContractValidationError("entity", "quota dimensions must be distinct");
  return result.sort((a, b) => a.type.localeCompare(b.type, "en-US"));
}
export function parseClientQuotaValues(value: unknown): readonly ClientQuotaValue[] {
  if (!Array.isArray(value) || value.length > 32)
    throw new HostContractValidationError("values", "requires at most 32 explicit quota values");
  const result = value.map((item: unknown) => {
    const v = record(item, "quotaValue");
    exactKeys(v, ["key", "value"], "quotaValue");
    return { key: text(v.key, "key", 200), value: finiteQuota(v.value, "value") };
  });
  if (new Set(result.map((v) => v.key)).size !== result.length)
    throw new HostContractValidationError("values", "quota keys must be distinct");
  return result.sort((a, b) => a.key.localeCompare(b.key, "en-US"));
}
export function parseClientQuotaInput(value: unknown): ClientQuotaInput {
  const v = record(value, "quotaInput");
  exactKeys(v, ["entity", "changes"], "quotaInput");
  if (!Array.isArray(v.changes) || !v.changes.length || v.changes.length > 4)
    throw new HostContractValidationError("changes", "requires one to four quota changes");
  const changes = v.changes.map((item: unknown) => {
    const c = record(item, "change");
    exactKeys(c, ["key", "value"], "change");
    const key = declaredValue(c.key, CLIENT_QUOTA_KEYS, "key"),
      value = c.value === null ? null : finiteQuota(c.value, "value");
    if (value !== null && value <= 0)
      throw new HostContractValidationError(
        "value",
        "set requires a positive quota; removal is explicit",
      );
    if (
      value !== null &&
      (key === "producer_byte_rate" || key === "consumer_byte_rate") &&
      !Number.isSafeInteger(value)
    )
      throw new HostContractValidationError(
        "value",
        "byte-rate quotas require a positive safe integer",
      );
    return { key, value };
  });
  if (new Set(changes.map((c) => c.key)).size !== changes.length)
    throw new HostContractValidationError("changes", "quota keys must be distinct");
  return {
    entity: parseClientQuotaEntity(v.entity),
    changes: changes.sort((a, b) => a.key.localeCompare(b.key, "en-US")),
  };
}
export function parseClientQuotaSnapshot(value: unknown): ClientQuotaSnapshot {
  const v = record(value, "quotaSnapshot");
  exactKeys(v, ["clusterId", "entity", "values", "alterSupported"], "quotaSnapshot");
  return {
    clusterId: text(v.clusterId, "clusterId", 128),
    entity: parseClientQuotaEntity(v.entity),
    values: parseClientQuotaValues(v.values),
    alterSupported: truth(v.alterSupported, "alterSupported"),
  };
}
export function sameClientQuotaEntity(a: ClientQuotaEntity, b: ClientQuotaEntity): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
export function sameClientQuotaBaseline(a: ClientQuotaSnapshot, b: ClientQuotaSnapshot): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
export function clientQuotaExpected(
  baseline: readonly ClientQuotaValue[],
  changes: readonly ClientQuotaChange[],
): readonly ClientQuotaValue[] {
  const values = new Map(baseline.map((v) => [v.key, v.value]));
  for (const change of changes) {
    if (change.value === null) values.delete(change.key);
    else values.set(change.key, change.value);
  }
  return parseClientQuotaValues([...values].map(([key, value]) => ({ key, value })));
}
export function clientQuotaLabel(entity: ClientQuotaEntity): string {
  return entity
    .map((c) => c.type + "=" + (c.name === null ? "(default)" : JSON.stringify(c.name)))
    .join(" / ");
}
export function clientQuotaConfirmation(entity: ClientQuotaEntity): string {
  return "ALTER QUOTAS " + clientQuotaLabel(entity);
}
export function parseClientQuotaReview(value: unknown): ClientQuotaReview {
  const v = record(value, "quotaReview");
  exactKeys(
    v,
    ["planId", "expiresAt", "connectionName", "input", "baseline", "expected", "confirmation"],
    "quotaReview",
  );
  const input = parseClientQuotaInput(v.input),
    baseline = parseClientQuotaSnapshot(v.baseline),
    expected = parseClientQuotaValues(v.expected),
    confirmation = text(v.confirmation, "confirmation", 1024);
  if (
    !sameClientQuotaEntity(input.entity, baseline.entity) ||
    JSON.stringify(expected) !==
      JSON.stringify(clientQuotaExpected(baseline.values, input.changes)) ||
    confirmation !== clientQuotaConfirmation(input.entity)
  )
    throw new HostContractValidationError(
      "quotaReview",
      "requires the reviewed entity, explicit changes and exact confirmation",
    );
  return {
    planId: text(v.planId, "planId", 128),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    connectionName: text(v.connectionName, "connectionName", 256),
    input,
    baseline,
    expected,
    confirmation,
  };
}
export function parseClientQuotaOutcome(value: unknown): ClientQuotaOutcome {
  const v = record(value, "quotaOutcome");
  exactKeys(v, ["input", "state", "verification", "cleanup", "observed", "detail"], "quotaOutcome");
  const verification = declaredValue(
      v.verification,
      ["verified", "different", "unavailable"] as const,
      "verification",
    ),
    observed = v.observed === null ? null : parseClientQuotaValues(v.observed);
  if (verification !== "unavailable" && observed === null)
    throw new HostContractValidationError("observed", "readback requires actual explicit values");
  return {
    input: parseClientQuotaInput(v.input),
    state: declaredValue(
      v.state,
      ["acknowledged", "rejected", "unknown", "unsent"] as const,
      "state",
    ),
    verification,
    observed,
    cleanup: declaredValue(v.cleanup, ["confirmed", "unresolved"] as const, "cleanup"),
    detail: text(v.detail, "detail", 2048),
  };
}
