import {
  NATS_FAILURE_CODES,
  NATS_COMMANDS,
  NATS_LIMITS,
  type NatsSafeFailure,
  type NatsHostError,
  type NatsConnectionSnapshot,
  type NatsSubscriptionSnapshot,
  type NatsRecordsBatch,
} from "./types";
import { parseNatsName } from "./profile-validation";
import {
  parseNatsRecord,
  parseNatsSubject,
  parseNatsSubscriptionCounters,
} from "./record-validation";
import {
  NatsContractValidationError,
  natsArray,
  natsEnum,
  natsExactKeys,
  natsIdentifier,
  natsInteger,
  natsObject,
  natsText,
  natsUtf8Bytes,
} from "./validation-primitives";

export function parseNatsSafeFailure(value: unknown): NatsSafeFailure {
  const input = natsObject(value);
  natsExactKeys(input, ["code", "summary"], ["recovery"]);
  return {
    code: natsEnum(input.code, NATS_FAILURE_CODES),
    summary: natsText(input.summary, 512),
    ...(Object.hasOwn(input, "recovery") ? { recovery: natsText(input.recovery, 1024) } : {}),
  };
}
export function parseNatsHostError(value: unknown): NatsHostError {
  const input = natsObject(value);
  natsExactKeys(input, ["code", "summary", "stage", "operation", "correlationId"], ["recovery"]);
  return {
    ...parseNatsSafeFailure({
      code: input.code,
      summary: input.summary,
      ...(Object.hasOwn(input, "recovery") ? { recovery: input.recovery } : {}),
    }),
    stage: natsEnum(input.stage, [
      "validation",
      "profiles",
      "connection",
      "subscription",
      "lifecycle",
    ] as const),
    operation: natsEnum(input.operation, NATS_COMMANDS),
    correlationId: natsIdentifier(input.correlationId),
  };
}
export function parseNatsConnectionSnapshot(value: unknown): NatsConnectionSnapshot {
  const input = natsObject(value);
  natsExactKeys(input, ["state", "profile"], ["failure"]);
  const state = natsEnum(input.state, [
    "disconnected",
    "connecting",
    "connected",
    "disconnecting",
    "failed",
  ] as const);
  let profile: NatsConnectionSnapshot["profile"] = null;
  if (input.profile !== null) {
    const identity = natsObject(input.profile);
    natsExactKeys(identity, ["id", "revision", "name"]);
    profile = {
      id: natsIdentifier(identity.id),
      revision: natsInteger(identity.revision, 1),
      name: parseNatsName(identity.name),
    };
  }
  if (state === "connected" && profile === null) throw new NatsContractValidationError();
  return {
    state,
    profile,
    ...(Object.hasOwn(input, "failure") ? { failure: parseNatsSafeFailure(input.failure) } : {}),
  };
}
export function parseNatsSubscriptionSnapshot(value: unknown): NatsSubscriptionSnapshot {
  const input = natsObject(value);
  natsExactKeys(input, ["state", "generation", "subject", "counters"], ["failure"]);
  const state = natsEnum(input.state, [
    "idle",
    "loading",
    "streaming",
    "stopping",
    "stopped",
    "failed",
  ] as const);
  const generation = input.generation === null ? null : natsIdentifier(input.generation);
  const subject = input.subject === null ? null : parseNatsSubject(input.subject);
  const counters = parseNatsSubscriptionCounters(input.counters);
  if (
    (generation === null) !== (subject === null) ||
    (state === "idle" &&
      (generation !== null || Object.values(counters).some((count) => count !== 0))) ||
    (state !== "idle" && generation === null)
  )
    throw new NatsContractValidationError();
  return {
    state,
    generation,
    subject,
    counters,
    ...(Object.hasOwn(input, "failure") ? { failure: parseNatsSafeFailure(input.failure) } : {}),
  };
}
export function parseNatsRecordsBatch(value: unknown): NatsRecordsBatch {
  const input = natsObject(value);
  natsExactKeys(input, ["generation", "records", "counters"]);
  const generation = natsIdentifier(input.generation);
  const records = natsArray(input.records, NATS_LIMITS.batchRecords).map(parseNatsRecord);
  if (
    records.some((record) => record.generation !== generation) ||
    new Set(records.map((record) => record.id)).size !== records.length
  )
    throw new NatsContractValidationError();
  const parsed = { generation, records, counters: parseNatsSubscriptionCounters(input.counters) };
  if (natsUtf8Bytes(JSON.stringify(parsed)) > NATS_LIMITS.batchBytes)
    throw new NatsContractValidationError();
  return parsed;
}
