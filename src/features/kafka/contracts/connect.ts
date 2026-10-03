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
}
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
  readonly confirmation: string;
  readonly before: ConnectDetail | null;
}
export interface ConnectOutcome {
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
  exactKeys(p, ["name", "action", "config"], "connect input");
  return {
    name: connectName(p.name),
    action: declaredValue(p.action, CONNECT_ACTIONS, "action"),
    config: connectConfig(p.config),
  };
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
    ["planId", "expiresAt", "name", "action", "fields", "confirmation", "before"],
    "review",
  );
  return {
    planId: text(p.planId, "planId", 128),
    expiresAt: text(p.expiresAt, "expiresAt", 64),
    name: connectName(p.name),
    action: declaredValue(p.action, CONNECT_ACTIONS, "action"),
    fields: list(p.fields, (x) => text(x, "field", 200)),
    confirmation: text(p.confirmation, "confirmation", 512),
    before: p.before === null ? null : parseConnectDetail(p.before),
  };
}
export function parseConnectOutcome(v: unknown): ConnectOutcome {
  const p = record(v, "outcome");
  exactKeys(p, ["state", "detail", "observed"], "outcome");
  return {
    state: declaredValue(p.state, ["acknowledged", "rejected", "unknown"], "state"),
    detail: text(p.detail, "detail", 512),
    observed: p.observed === null ? null : parseConnectDetail(p.observed),
  };
}
