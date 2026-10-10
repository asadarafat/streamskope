import type { SchemaRegistrationInput, SchemaVersionDetail } from "./schema-registry-types";
import { parseSchemaRegistrationInput, parseSchemaVersion } from "./schema-registry-validation";
import { utf8ByteLength } from "./message-limits";
import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  text,
  declaredValue,
  truth,
  positiveBoundedInteger,
} from "./validation-primitives";

export const SCHEMA_COMPATIBILITY_LEVELS = [
  "BACKWARD",
  "BACKWARD_TRANSITIVE",
  "FORWARD",
  "FORWARD_TRANSITIVE",
  "FULL",
  "FULL_TRANSITIVE",
  "NONE",
] as const;
export type SchemaCompatibilityLevel = (typeof SCHEMA_COMPATIBILITY_LEVELS)[number];
export const SCHEMA_CHANGE_LIMITS = {
  draftBytes: 128 * 1024,
  snapshotBytes: 512 * 1024,
  referenceNodes: 32,
  referenceDepth: 8,
} as const;
export interface SchemaCompatibilityPolicy {
  readonly globalLevel: SchemaCompatibilityLevel;
  readonly subjectLevel: SchemaCompatibilityLevel | null;
  readonly effectiveLevel: SchemaCompatibilityLevel;
}
export interface SchemaChangeInput {
  readonly draft: SchemaRegistrationInput;
  readonly expectedWriter: { readonly id: number; readonly version: number } | null;
}
export interface SchemaChangeReview {
  readonly planId: string;
  readonly expiresAt: string;
  readonly connectionName: string;
  readonly input: SchemaChangeInput;
  readonly before: SchemaVersionDetail | null;
  readonly policy: SchemaCompatibilityPolicy;
  readonly compatible: boolean;
}
export interface SchemaChangeOutcome {
  readonly state: "acknowledged" | "rejected" | "unknown";
  readonly verification: "verified" | "mismatch" | "unavailable" | "not-applicable";
  readonly id: number | null;
  readonly observed: SchemaVersionDetail | null;
  readonly detail: string;
}
function writer(value: unknown): SchemaChangeInput["expectedWriter"] {
  if (value === null) return null;
  const item = record(value, "writer");
  exactKeys(item, ["id", "version"], "writer");
  return {
    id: positiveBoundedInteger(item.id, "writer.id", 2147483647),
    version: positiveBoundedInteger(item.version, "writer.version", 10000),
  };
}
export function parseSchemaChangeInput(value: unknown): SchemaChangeInput {
  const item = record(value, "change");
  exactKeys(item, ["draft", "expectedWriter"], "change");
  const draft = parseSchemaRegistrationInput(item.draft, "change.draft");
  if (
    draft.version !== "latest" ||
    draft.schema.length === 0 ||
    utf8ByteLength(JSON.stringify(draft)) > SCHEMA_CHANGE_LIMITS.draftBytes
  )
    throw new HostContractValidationError(
      "change.draft",
      "requires a non-empty latest-version draft within 128 KiB UTF-8",
    );
  return { draft, expectedWriter: writer(item.expectedWriter) };
}
export function parseSchemaCompatibilityPolicy(value: unknown): SchemaCompatibilityPolicy {
  const item = record(value, "policy");
  exactKeys(item, ["globalLevel", "subjectLevel", "effectiveLevel"], "policy");
  const globalLevel = declaredValue(
    item.globalLevel,
    SCHEMA_COMPATIBILITY_LEVELS,
    "policy.globalLevel",
  );
  const subjectLevel =
    item.subjectLevel === null
      ? null
      : declaredValue(item.subjectLevel, SCHEMA_COMPATIBILITY_LEVELS, "policy.subjectLevel");
  const effectiveLevel = declaredValue(
    item.effectiveLevel,
    SCHEMA_COMPATIBILITY_LEVELS,
    "policy.effectiveLevel",
  );
  if (effectiveLevel !== (subjectLevel ?? globalLevel))
    throw new HostContractValidationError("policy", "inconsistent effective level");
  return { globalLevel, subjectLevel, effectiveLevel };
}
export function parseSchemaChangeReview(value: unknown): SchemaChangeReview {
  const item = record(value, "review");
  exactKeys(
    item,
    ["planId", "expiresAt", "connectionName", "input", "before", "policy", "compatible"],
    "review",
  );
  if (
    utf8ByteLength(JSON.stringify(item)) >
    SCHEMA_CHANGE_LIMITS.snapshotBytes + SCHEMA_CHANGE_LIMITS.draftBytes
  )
    throw new HostContractValidationError("review", "exceeds the review bound");
  const input = parseSchemaChangeInput(item.input);
  const before = item.before === null ? null : parseSchemaVersion(item.before, "review.before");
  if (
    (before?.subject !== undefined && before.subject !== input.draft.subject) ||
    JSON.stringify(writer(before === null ? null : { id: before.id, version: before.version })) !==
      JSON.stringify(input.expectedWriter)
  )
    throw new HostContractValidationError("review.before", "does not match the expected writer");
  const expiresAt = text(item.expiresAt, "review.expiresAt", 128);
  if (!Number.isFinite(Date.parse(expiresAt)))
    throw new HostContractValidationError("review.expiresAt", "invalid expiry");
  return {
    planId: text(item.planId, "review.planId", 128),
    expiresAt,
    connectionName: text(item.connectionName, "review.connectionName", 256),
    input,
    before,
    policy: parseSchemaCompatibilityPolicy(item.policy),
    compatible: truth(item.compatible, "review.compatible"),
  };
}
export function parseSchemaChangeOutcome(value: unknown): SchemaChangeOutcome {
  const item = record(value, "outcome");
  if (utf8ByteLength(JSON.stringify(item)) > SCHEMA_CHANGE_LIMITS.snapshotBytes)
    throw new HostContractValidationError("outcome", "exceeds the receipt bound");
  exactKeys(item, ["state", "verification", "id", "observed", "detail"], "outcome");
  const state = declaredValue(item.state, ["acknowledged", "rejected", "unknown"], "outcome.state");
  const verification = declaredValue(
    item.verification,
    ["verified", "mismatch", "unavailable", "not-applicable"],
    "outcome.verification",
  );
  const id = item.id === null ? null : positiveBoundedInteger(item.id, "outcome.id", 2147483647);
  const observed =
    item.observed === null ? null : parseSchemaVersion(item.observed, "outcome.observed");
  if (
    (state !== "acknowledged" && id !== null) ||
    (state === "acknowledged" && id === null) ||
    (verification === "verified" && (state !== "acknowledged" || observed?.id !== id))
  )
    throw new HostContractValidationError("outcome", "inconsistent registration receipt");
  return { state, verification, id, observed, detail: text(item.detail, "outcome.detail", 1024) };
}
