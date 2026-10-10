import { HOST_ERROR_CODES } from "./host-errors";
import { parseObservationInput } from "./observation-validation";
import type { ObservationInput, ObservationIssue } from "./observations";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  record,
  text,
  truth,
} from "./validation-primitives";

export interface ObservationWatchSnapshot {
  readonly revision: number;
  readonly id: string | null;
  readonly phase: "stopped" | "capturing" | "waiting" | "stopping" | "failed";
  readonly repeated: boolean;
  readonly input: ObservationInput | null;
  readonly connectionName: string | null;
  readonly clusterId: string | null;
  readonly topicId: string | null;
  readonly current: boolean;
  readonly nextCaptureAt: number | null;
  readonly lastSampleId: string | null;
  readonly lastSeriesId: string | null;
  readonly error: Pick<ObservationIssue, "code" | "summary" | "recovery" | "retryable"> | null;
}

export function emptyObservationWatch(): ObservationWatchSnapshot {
  return {
    revision: 0,
    id: null,
    phase: "stopped",
    repeated: false,
    input: null,
    connectionName: null,
    clusterId: null,
    topicId: null,
    current: false,
    nextCaptureAt: null,
    lastSampleId: null,
    lastSeriesId: null,
    error: null,
  };
}

/** Compact attachment evidence; never serializes credentials or original authority. */
export function parseObservationWatch(value: unknown): ObservationWatchSnapshot {
  const p = record(value, "observation watch");
  exactKeys(p, Object.keys(emptyObservationWatch()), "observation watch");
  let error: ObservationWatchSnapshot["error"] = null;
  if (p.error !== null) {
    const e = record(p.error, "watch error");
    exactKeys(e, ["code", "summary", "recovery", "retryable"], "watch error");
    error = {
      code: declaredValue(e.code, HOST_ERROR_CODES, "watch error code"),
      summary: text(e.summary, "watch error summary", 512),
      recovery: text(e.recovery, "watch error recovery", 1024),
      retryable: truth(e.retryable, "watch error retryable"),
    };
  }
  const snapshot: ObservationWatchSnapshot = {
    revision: nonNegativeInteger(p.revision, "watch revision"),
    id: nullableText(p.id, "watch id", 128),
    phase: declaredValue(
      p.phase,
      ["stopped", "capturing", "waiting", "stopping", "failed"],
      "watch phase",
    ),
    repeated: truth(p.repeated, "watch repeated"),
    input: p.input === null ? null : parseObservationInput(p.input),
    connectionName: nullableText(p.connectionName, "watch connection", 256),
    clusterId: nullableText(p.clusterId, "watch cluster", 512),
    topicId: nullableText(p.topicId, "watch topic", 128),
    current: truth(p.current, "watch current"),
    nextCaptureAt:
      p.nextCaptureAt === null ? null : nonNegativeInteger(p.nextCaptureAt, "watch next capture"),
    lastSampleId: nullableText(p.lastSampleId, "watch last sample", 128),
    lastSeriesId: nullableText(p.lastSeriesId, "watch last series", 2048),
    error,
  };
  const active = ["capturing", "waiting"].includes(snapshot.phase);
  if (
    (active && (snapshot.id === null || snapshot.input === null)) ||
    (snapshot.current &&
      (snapshot.lastSampleId === null ||
        snapshot.lastSeriesId === null ||
        snapshot.clusterId === null ||
        snapshot.topicId === null)) ||
    (snapshot.lastSampleId === null) !== (snapshot.lastSeriesId === null) ||
    (snapshot.phase === "waiting" && (!snapshot.repeated || snapshot.nextCaptureAt === null)) ||
    (snapshot.phase === "failed" && snapshot.error === null)
  )
    throw new Error("Inconsistent observation watch evidence.");
  return snapshot;
}
