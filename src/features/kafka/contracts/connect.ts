import {
  record,
  exactKeys,
  text,
  boundedText,
  nonNegativeInteger,
  declaredValue,
} from "./validation-primitives";
import type { HostCommandBase } from "./types";

export const CONNECT_ACTIONS = [
  "create",
  "update",
  "pause",
  "resume",
  "restart-failed",
  "delete",
] as const;
export interface ConnectInput {
  readonly name: string;
  readonly action: (typeof CONNECT_ACTIONS)[number];
  readonly config: Readonly<Record<string, string>>;
  readonly remove?: readonly string[];
}
/** Display-only sentinel; never a replacement credential. */
export const CONNECT_PROTECTED_VALUE = "[protected — retained unless replaced]";
export interface ConnectIssue {
  readonly field: string;
  readonly message: string;
}
export interface ConnectValidation {
  readonly issues: readonly ConnectIssue[];
}
export interface ConnectInventory {
  readonly names: readonly string[];
  readonly plugins: readonly string[];
}
export interface ConnectDetail {
  readonly name: string;
  readonly state: string;
  readonly tasks: readonly {
    readonly id: number;
    readonly state: string;
    readonly failure: string;
  }[];
  readonly config: Readonly<Record<string, string>>;
  readonly dlq: string | null;
  readonly observedAt: string;
}
export interface ConnectReview {
  readonly planId: string;
  readonly expiresAt: string;
  readonly name: string;
  readonly action: ConnectInput["action"];
  readonly fields: readonly string[];
  readonly removedFields: readonly string[];
  readonly connectionName: string;
  readonly confirmation: string;
  readonly before: ConnectDetail | null;
}
export interface ConnectOutcome {
  readonly dispatch: "not-sent" | "attempted";
  readonly verification: "not-applicable" | "verified" | "different" | "unavailable";
  readonly cleanup: "confirmed" | "unresolved";
  readonly state: "acknowledged" | "rejected" | "unknown";
  readonly detail: string;
  readonly observed: ConnectDetail | null;
}
export type ConnectHostCommand =
  | (HostCommandBase & {
      readonly command: "connect.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "connect.load";
      readonly payload: { readonly name: string };
    })
  | (HostCommandBase & {
      readonly command: "connect.validate" | "connect.review";
      readonly payload: ConnectInput;
    })
  | (HostCommandBase & {
      readonly command: "connect.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    });
export function connectName(value: unknown): string {
  const name = text(value, "connector name", 200);
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/u.test(name))
    throw new Error("Use a connector name of letters, digits, dots, underscores or hyphens.");
  return name;
}
export function connectConfig(value: unknown): Readonly<Record<string, string>> {
  const p = record(value, "config");
  if (Object.keys(p).length > 200 || JSON.stringify(p).length > 65536)
    throw new Error("Connector configuration exceeds its limit.");
  return Object.fromEntries(
    Object.entries(p)
      .sort(([a], [b]) => a.localeCompare(b, "en-US"))
      .map(([k, v]) => [text(k, "config field", 200), boundedText(v, "config value", 8192)]),
  );
}
export function parseConnectInput(value: unknown): ConnectInput {
  const p = record(value, "connect input");
  exactKeys(p, ["name", "action", "config", "remove"], "connect input");
  const input = {
    name: connectName(p.name),
    action: declaredValue(p.action, CONNECT_ACTIONS, "action"),
    config: connectConfig(p.config),
    remove: connectFields(p.remove === undefined ? [] : p.remove),
  };
  if (Object.values(input.config).includes(CONNECT_PROTECTED_VALUE))
    throw new Error("Protected display values cannot replace actual configuration.");
  if (input.remove.some((key) => Object.hasOwn(input.config, key)))
    throw new Error("A field cannot be both set and removed.");
  if (input.remove.includes("name")) throw new Error("Connector name cannot be removed.");
  if (Object.hasOwn(input.config, "name") && input.config.name !== input.name)
    throw new Error("Configuration name must match the connector.");
  if (input.action !== "update" && input.remove.length)
    throw new Error("Only configuration updates can remove fields.");
  if (!["create", "update"].includes(input.action) && Object.keys(input.config).length)
    throw new Error("Lifecycle actions do not change configuration.");
  return input;
}
function connectFields(value: unknown): readonly string[] {
  if (!Array.isArray(value) || value.length > 200)
    throw new Error("Connector fields exceed their limit.");
  const fields = value.map((v) => text(v, "config field", 200)).sort();
  if (new Set(fields).size !== fields.length) throw new Error("Duplicate configuration field.");
  return fields;
}
function list<T>(v: unknown, parse: (x: unknown) => T): readonly T[] {
  if (!Array.isArray(v) || v.length > 1000) throw new Error("Connect list exceeds limit.");
  return v.map(parse);
}
export function parseConnectInventory(v: unknown): ConnectInventory {
  const p = record(v, "inventory");
  exactKeys(p, ["names", "plugins"], "inventory");
  return {
    names: list(p.names, connectName),
    plugins: list(p.plugins, (x) => text(x, "plugin", 512)),
  };
}
export function parseConnectValidation(v: unknown): ConnectValidation {
  const p = record(v, "validation");
  exactKeys(p, ["issues"], "validation");
  return {
    issues: list(p.issues, (x) => {
      const i = record(x, "issue");
      exactKeys(i, ["field", "message"], "issue");
      return { field: text(i.field, "field", 200), message: text(i.message, "message", 512) };
    }),
  };
}
export function parseConnectDetail(v: unknown): ConnectDetail {
  const p = record(v, "detail");
  exactKeys(p, ["name", "state", "tasks", "config", "dlq", "observedAt"], "detail");
  return {
    name: connectName(p.name),
    state: text(p.state, "state", 80),
    config: connectConfig(p.config),
    dlq: p.dlq === null ? null : text(p.dlq, "DLQ", 249),
    observedAt: text(p.observedAt, "time", 64),
    tasks: list(p.tasks, (x) => {
      const t = record(x, "task");
      exactKeys(t, ["id", "state", "failure"], "task");
      return {
        id: nonNegativeInteger(t.id, "task id"),
        state: text(t.state, "state", 80),
        failure: boundedText(t.failure, "failure", 512),
      };
    }),
  };
}
export function parseConnectReview(v: unknown): ConnectReview {
  const p = record(v, "review");
  exactKeys(
    p,
    [
      "planId",
      "expiresAt",
      "name",
      "action",
      "fields",
      "removedFields",
      "connectionName",
      "confirmation",
      "before",
    ],
    "review",
  );
  const review = {
    planId: text(p.planId, "planId", 128),
    expiresAt: text(p.expiresAt, "expiresAt", 64),
    name: connectName(p.name),
    action: declaredValue(p.action, CONNECT_ACTIONS, "action"),
    fields: connectFields(p.fields),
    removedFields: connectFields(p.removedFields),
    connectionName: text(p.connectionName, "connection name", 200),
    confirmation: text(p.confirmation, "confirmation", 512),
    before: p.before === null ? null : parseConnectDetail(p.before),
  };
  if (
    review.removedFields.some((key) => review.fields.includes(key)) ||
    (review.removedFields.length && review.action !== "update") ||
    review.removedFields.includes("name") ||
    review.confirmation !== `${review.action} ${review.name}` ||
    (review.before !== null && review.before.name !== review.name) ||
    (review.action === "create" ? review.before !== null : review.before === null)
  )
    throw new Error("Inconsistent Connect review.");
  return review;
}
export function parseConnectOutcome(v: unknown): ConnectOutcome {
  const p = record(v, "outcome");
  exactKeys(p, ["state", "dispatch", "verification", "cleanup", "detail", "observed"], "outcome");
  const outcome = {
    state: declaredValue(p.state, ["acknowledged", "rejected", "unknown"], "state"),
    dispatch: declaredValue(p.dispatch, ["not-sent", "attempted"], "dispatch"),
    verification: declaredValue(
      p.verification,
      ["not-applicable", "verified", "different", "unavailable"],
      "verification",
    ),
    cleanup: declaredValue(p.cleanup, ["confirmed", "unresolved"], "cleanup"),
    detail: text(p.detail, "detail", 512),
    observed: p.observed === null ? null : parseConnectDetail(p.observed),
  };
  if (
    (outcome.state !== "rejected" && outcome.dispatch !== "attempted") ||
    (outcome.state === "acknowledged"
      ? outcome.verification === "not-applicable"
      : outcome.verification !== "not-applicable") ||
    ((outcome.verification === "verified" || outcome.verification === "different") &&
      outcome.cleanup !== "confirmed")
  )
    throw new Error("Inconsistent Connect outcome.");
  return outcome;
}
