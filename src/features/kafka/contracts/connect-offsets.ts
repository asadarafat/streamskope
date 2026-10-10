import type { HostCommandBase } from "./types";
import { connectName, parseConnectOutcome, type ConnectOutcome } from "./connect";
import { record, exactKeys, text, declaredValue } from "./validation-primitives";

export type ConnectOffsetMapping = "kafka-sink" | "file-source";
export interface ConnectOffsetPosition {
  readonly partitionRef: string;
  readonly label: string;
  readonly position: number;
}
export interface ConnectOffsetsSnapshot {
  readonly name: string;
  readonly connectionName: string;
  readonly status: "available" | "unsupported" | "denied" | "missing" | "unavailable";
  readonly snapshotId: string | null;
  readonly expiresAt: string | null;
  readonly clusterId: string | null;
  readonly workerVersion: string | null;
  readonly connectorState: string;
  readonly mapping: ConnectOffsetMapping | null;
  readonly positions: readonly ConnectOffsetPosition[];
  readonly observedAt: string;
  readonly detail: string;
}
export interface ConnectOffsetsInput {
  readonly snapshotId: string;
  readonly action: "set" | "remove" | "reset";
  readonly partitionRef: string | null;
  readonly position: number | null;
}
export interface ConnectOffsetsReview {
  readonly planId: string;
  readonly expiresAt: string;
  readonly name: string;
  readonly connectionName: string;
  readonly clusterId: string;
  readonly mapping: ConnectOffsetMapping;
  readonly input: ConnectOffsetsInput;
  readonly changes: readonly {
    readonly label: string;
    readonly before: number;
    readonly after: number | null;
  }[];
  readonly confirmation: string;
}
export type ConnectOffsetsOutcome = Omit<ConnectOutcome, "observed"> & {
  readonly planId: string;
  readonly confirmation: string;
  readonly observed: ConnectOffsetsSnapshot | null;
};
export type ConnectOffsetsCommand =
  | (HostCommandBase & {
      readonly command: "connect.offsets.inspect";
      readonly payload: { readonly name: string };
    })
  | (HostCommandBase & {
      readonly command: "connect.offsets.review";
      readonly payload: ConnectOffsetsInput;
    })
  | (HostCommandBase & {
      readonly command: "connect.offsets.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    });
export function connectOffsetPosition(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw new Error("Offsets require a nonnegative safe integer.");
  return value;
}
function time(value: unknown): string {
  const result = text(value, "time", 64);
  if (!Number.isFinite(Date.parse(result))) throw new Error("Invalid offset observation time.");
  return result;
}
export function parseConnectOffsetsInput(value: unknown): ConnectOffsetsInput {
  const p = record(value, "offset input");
  exactKeys(p, ["snapshotId", "action", "partitionRef", "position"], "offset input");
  const input = {
    snapshotId: text(p.snapshotId, "snapshotId", 128),
    action: declaredValue(p.action, ["set", "remove", "reset"], "offset action"),
    partitionRef: p.partitionRef === null ? null : text(p.partitionRef, "partitionRef", 128),
    position: p.position === null ? null : connectOffsetPosition(p.position),
  };
  if (
    (input.action === "reset" ? input.partitionRef !== null : input.partitionRef === null) ||
    (input.action === "set" ? input.position === null : input.position !== null)
  )
    throw new Error("Choose one exact offset set, removal or complete reset.");
  return input;
}
export function parseConnectOffsetsSnapshot(value: unknown): ConnectOffsetsSnapshot {
  const p = record(value, "offset snapshot");
  exactKeys(
    p,
    [
      "name",
      "connectionName",
      "status",
      "snapshotId",
      "expiresAt",
      "clusterId",
      "workerVersion",
      "connectorState",
      "mapping",
      "positions",
      "observedAt",
      "detail",
    ],
    "offset snapshot",
  );
  if (!Array.isArray(p.positions) || p.positions.length > 128)
    throw new Error("Offset partitions exceed their limit.");
  const snapshot = {
    name: connectName(p.name),
    connectionName: text(p.connectionName, "connectionName", 200),
    status: declaredValue(
      p.status,
      ["available", "unsupported", "denied", "missing", "unavailable"],
      "offset status",
    ),
    snapshotId: p.snapshotId === null ? null : text(p.snapshotId, "snapshotId", 128),
    expiresAt: p.expiresAt === null ? null : time(p.expiresAt),
    clusterId: p.clusterId === null ? null : text(p.clusterId, "clusterId", 128),
    workerVersion: p.workerVersion === null ? null : text(p.workerVersion, "workerVersion", 80),
    connectorState: text(p.connectorState, "connectorState", 80),
    mapping:
      p.mapping === null
        ? null
        : declaredValue(p.mapping, ["kafka-sink", "file-source"], "mapping"),
    positions: p.positions.map((value: unknown) => {
      const item = record(value, "position");
      exactKeys(item, ["partitionRef", "label", "position"], "position");
      return {
        partitionRef: text(item.partitionRef, "partitionRef", 128),
        label: text(item.label, "label", 300),
        position: connectOffsetPosition(item.position),
      };
    }),
    observedAt: time(p.observedAt),
    detail: text(p.detail, "detail", 512),
  };
  if (
    new Set(snapshot.positions.map((item) => item.partitionRef)).size !==
      snapshot.positions.length ||
    (snapshot.status === "available"
      ? !snapshot.snapshotId ||
        !snapshot.expiresAt ||
        !snapshot.clusterId ||
        !snapshot.workerVersion ||
        !snapshot.mapping
      : snapshot.snapshotId !== null ||
        snapshot.expiresAt !== null ||
        snapshot.mapping !== null ||
        snapshot.positions.length > 0)
  )
    throw new Error("Inconsistent offset snapshot.");
  return snapshot;
}
export function parseConnectOffsetsReview(value: unknown): ConnectOffsetsReview {
  const p = record(value, "offset review");
  exactKeys(
    p,
    [
      "planId",
      "expiresAt",
      "name",
      "connectionName",
      "clusterId",
      "mapping",
      "input",
      "changes",
      "confirmation",
    ],
    "offset review",
  );
  if (!Array.isArray(p.changes) || !p.changes.length || p.changes.length > 128)
    throw new Error("Choose a bounded, observed offset change.");
  const review = {
    planId: text(p.planId, "planId", 128),
    expiresAt: time(p.expiresAt),
    name: connectName(p.name),
    connectionName: text(p.connectionName, "connectionName", 200),
    clusterId: text(p.clusterId, "clusterId", 128),
    mapping: declaredValue(p.mapping, ["kafka-sink", "file-source"], "mapping"),
    input: parseConnectOffsetsInput(p.input),
    changes: p.changes.map((value: unknown) => {
      const item = record(value, "change");
      exactKeys(item, ["label", "before", "after"], "change");
      return {
        label: text(item.label, "label", 300),
        before: connectOffsetPosition(item.before),
        after: item.after === null ? null : connectOffsetPosition(item.after),
      };
    }),
    confirmation: text(p.confirmation, "confirmation", 512),
  };
  if (
    review.confirmation !== `${review.input.action} OFFSETS ${review.name}` ||
    (review.input.action !== "reset" && review.changes.length !== 1) ||
    review.changes.some((item) => item.after !== review.input.position)
  )
    throw new Error("Inconsistent offset review.");
  return review;
}
export function parseConnectOffsetsOutcome(value: unknown): ConnectOffsetsOutcome {
  const p = record(value, "offset outcome");
  exactKeys(
    p,
    [
      "planId",
      "confirmation",
      "state",
      "dispatch",
      "verification",
      "cleanup",
      "detail",
      "observed",
    ],
    "offset outcome",
  );
  const planId = text(p.planId, "planId", 128),
    confirmation = text(p.confirmation, "confirmation", 512);
  if (!/^(set|remove|reset) OFFSETS /u.test(confirmation))
    throw new Error("Invalid offset confirmation.");
  const name = connectName(confirmation.split(" ").slice(2).join(" "));
  const receipt = parseConnectOutcome({
    state: p.state,
    dispatch: p.dispatch,
    verification: p.verification,
    cleanup: p.cleanup,
    detail: p.detail,
    observed: null,
  });
  const observed = p.observed === null ? null : parseConnectOffsetsSnapshot(p.observed);
  if (
    (receipt.verification === "verified" || receipt.verification === "different") &&
    observed?.status !== "available"
  )
    throw new Error("Offset verification needs an available observation.");
  if (observed !== null && observed.name !== name) throw new Error("Foreign offset observation.");
  return { ...receipt, observed, planId, confirmation };
}
