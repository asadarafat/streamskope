import type { SchemaCompatibilityLevel, SchemaCompatibilityPolicy } from "./schema-changes";
import { SCHEMA_COMPATIBILITY_LEVELS, parseSchemaCompatibilityPolicy } from "./schema-changes";
import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  text,
  declaredValue,
  positiveBoundedInteger,
} from "./validation-primitives";

export type SchemaPolicyChange =
  { readonly mode: "inherit" } | { readonly mode: "set"; readonly level: SchemaCompatibilityLevel };
export interface SchemaPolicyBaseline {
  readonly writer: { readonly id: number; readonly version: number };
  readonly policy: SchemaCompatibilityPolicy;
}
export interface SchemaPolicyInput {
  readonly subject: string;
  readonly expectedWriter: SchemaPolicyBaseline["writer"];
  readonly change: SchemaPolicyChange;
}
export interface SchemaPolicyReview {
  readonly planId: string;
  readonly expiresAt: string;
  readonly connectionName: string;
  readonly input: SchemaPolicyInput;
  readonly before: SchemaPolicyBaseline;
  readonly after: SchemaCompatibilityPolicy;
}
export interface SchemaPolicyOutcome {
  readonly state: "acknowledged" | "unchanged" | "rejected" | "unknown";
  readonly verification: "verified" | "mismatch" | "unavailable" | "not-applicable";
  readonly acknowledgedLevel: SchemaCompatibilityLevel | null;
  readonly observed: SchemaCompatibilityPolicy | null;
  readonly detail: string;
}
function writer(value: unknown): SchemaPolicyBaseline["writer"] {
  const item = record(value, "writer");
  exactKeys(item, ["id", "version"], "writer");
  return {
    id: positiveBoundedInteger(item.id, "writer.id", 2147483647),
    version: positiveBoundedInteger(item.version, "writer.version", 10000),
  };
}
export function parseSchemaPolicyChange(value: unknown): SchemaPolicyChange {
  const item = record(value, "change");
  const mode = declaredValue(item.mode, ["set", "inherit"], "change.mode");
  exactKeys(item, mode === "set" ? ["mode", "level"] : ["mode"], "change");
  return mode === "set"
    ? { mode, level: declaredValue(item.level, SCHEMA_COMPATIBILITY_LEVELS, "change.level") }
    : { mode };
}
export function schemaPolicyAfter(
  before: SchemaCompatibilityPolicy,
  change: SchemaPolicyChange,
): SchemaCompatibilityPolicy {
  const subjectLevel = change.mode === "set" ? change.level : null;
  return {
    globalLevel: before.globalLevel,
    subjectLevel,
    effectiveLevel: subjectLevel ?? before.globalLevel,
  };
}
export function parseSchemaPolicyInput(value: unknown): SchemaPolicyInput {
  const item = record(value, "input");
  exactKeys(item, ["subject", "expectedWriter", "change"], "input");
  return {
    subject: text(item.subject, "input.subject", 512),
    expectedWriter: writer(item.expectedWriter),
    change: parseSchemaPolicyChange(item.change),
  };
}
export function parseSchemaPolicyBaseline(value: unknown): SchemaPolicyBaseline {
  const item = record(value, "baseline");
  exactKeys(item, ["writer", "policy"], "baseline");
  return { writer: writer(item.writer), policy: parseSchemaCompatibilityPolicy(item.policy) };
}
export function parseSchemaPolicyReview(value: unknown): SchemaPolicyReview {
  const item = record(value, "review");
  exactKeys(item, ["planId", "expiresAt", "connectionName", "input", "before", "after"], "review");
  const input = parseSchemaPolicyInput(item.input),
    before = parseSchemaPolicyBaseline(item.before),
    after = parseSchemaCompatibilityPolicy(item.after);
  const expiresAt = text(item.expiresAt, "review.expiresAt", 128);
  if (
    !Number.isFinite(Date.parse(expiresAt)) ||
    JSON.stringify(before.writer) !== JSON.stringify(input.expectedWriter) ||
    JSON.stringify(after) !== JSON.stringify(schemaPolicyAfter(before.policy, input.change))
  )
    throw new HostContractValidationError("review", "inconsistent policy review");
  return {
    planId: text(item.planId, "review.planId", 128),
    expiresAt,
    connectionName: text(item.connectionName, "review.connectionName", 256),
    input,
    before,
    after,
  };
}
export function parseSchemaPolicyOutcome(value: unknown): SchemaPolicyOutcome {
  const item = record(value, "outcome");
  exactKeys(item, ["state", "verification", "acknowledgedLevel", "observed", "detail"], "outcome");
  const state = declaredValue(
    item.state,
    ["acknowledged", "unchanged", "rejected", "unknown"],
    "outcome.state",
  );
  const verification = declaredValue(
    item.verification,
    ["verified", "mismatch", "unavailable", "not-applicable"],
    "outcome.verification",
  );
  const acknowledgedLevel =
    item.acknowledgedLevel === null
      ? null
      : declaredValue(
          item.acknowledgedLevel,
          SCHEMA_COMPATIBILITY_LEVELS,
          "outcome.acknowledgedLevel",
        );
  const observed = item.observed === null ? null : parseSchemaCompatibilityPolicy(item.observed);
  if (
    (state === "acknowledged") !== (acknowledgedLevel !== null) ||
    (state === "unchanged" && verification !== "verified") ||
    (verification === "verified" &&
      (observed === null || (state !== "acknowledged" && state !== "unchanged"))) ||
    (verification === "mismatch" && (state !== "acknowledged" || observed === null)) ||
    (verification === "not-applicable" && state !== "rejected") ||
    ((state === "unknown" || state === "rejected") && observed !== null) ||
    (verification === "unavailable" && observed !== null)
  )
    throw new HostContractValidationError("outcome", "inconsistent policy receipt");
  return {
    state,
    verification,
    acknowledgedLevel,
    observed,
    detail: text(item.detail, "outcome.detail", 1024),
  };
}
