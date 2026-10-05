import {
  NATS_COMMANDS,
  NATS_PROTOCOL_VERSION,
  NATS_LIMITS,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsEvent,
} from "./types";
import {
  parseNatsProfileCreateInput,
  parseNatsProfileUpdateInput,
  parseNatsProfilesSnapshot,
} from "./profile-validation";
import { parseNatsSubject } from "./record-validation";
import {
  parseNatsConnectionSnapshot,
  parseNatsHostError,
  parseNatsRecordsBatch,
  parseNatsSubscriptionSnapshot,
} from "./state-validation";
import {
  NatsContractValidationError,
  natsBoolean,
  natsEnum,
  natsExactKeys,
  natsIdentifier,
  natsInteger,
  natsObject,
  natsText,
  natsUtf8Bytes,
  type NatsUnknownRecord,
} from "./validation-primitives";

function version(value: unknown): typeof NATS_PROTOCOL_VERSION {
  if (value !== NATS_PROTOCOL_VERSION) throw new NatsContractValidationError();
  return NATS_PROTOCOL_VERSION;
}
function empty(value: NatsUnknownRecord): Readonly<Record<string, never>> {
  natsExactKeys(value, []);
  return {};
}
function identity(value: NatsUnknownRecord): {
  readonly profileId: string;
  readonly expectedRevision: number;
} {
  natsExactKeys(value, ["profileId", "expectedRevision"]);
  return {
    profileId: natsIdentifier(value.profileId),
    expectedRevision: natsInteger(value.expectedRevision, 1),
  };
}
export function parseNatsCommand(value: unknown): NatsCommand {
  const input = natsObject(value);
  natsExactKeys(input, ["version", "id", "command", "payload"]);
  const base = { version: version(input.version), id: natsIdentifier(input.id) };
  const command = natsEnum(input.command, NATS_COMMANDS);
  const payload = natsObject(input.payload);
  switch (command) {
    case "profiles.list":
      return { ...base, command, payload: empty(payload) };
    case "profiles.create":
      natsExactKeys(payload, ["profile"]);
      return {
        ...base,
        command,
        payload: { profile: parseNatsProfileCreateInput(payload.profile) },
      };
    case "profiles.update":
      natsExactKeys(payload, ["profileId", "expectedRevision", "profile"]);
      return {
        ...base,
        command,
        payload: {
          profileId: natsIdentifier(payload.profileId),
          expectedRevision: natsInteger(payload.expectedRevision, 1),
          profile: parseNatsProfileUpdateInput(payload.profile),
        },
      };
    case "profiles.delete":
      return { ...base, command, payload: identity(payload) };
    case "profiles.connect":
      return { ...base, command, payload: identity(payload) };
    case "connection.disconnect":
      return { ...base, command, payload: empty(payload) };
    case "subscription.start":
      natsExactKeys(payload, ["subject"]);
      return { ...base, command, payload: { subject: parseNatsSubject(payload.subject) } };
    case "subscription.stop":
      return { ...base, command, payload: empty(payload) };
  }
}
export function parseNatsResponse(value: unknown): NatsCommandResponse {
  const input = natsObject(value);
  const ok = natsBoolean(input.ok);
  natsExactKeys(input, ["version", "id", "command", "ok", ok ? "result" : "error"]);
  const base = { version: version(input.version), id: natsIdentifier(input.id) };
  const command = natsEnum(input.command, NATS_COMMANDS);
  if (!ok) {
    const error = parseNatsHostError(input.error);
    if (error.operation !== command) throw new NatsContractValidationError();
    return { ...base, command, ok: false, error };
  }
  const result = natsObject(input.result);
  const correlationId = natsIdentifier(result.correlationId);
  switch (command) {
    case "profiles.list":
      natsExactKeys(result, ["correlationId", "profiles", "connection", "subscription"]);
      return {
        ...base,
        command,
        ok: true,
        result: {
          correlationId,
          profiles: parseNatsProfilesSnapshot(result.profiles),
          connection: parseNatsConnectionSnapshot(result.connection),
          subscription: parseNatsSubscriptionSnapshot(result.subscription),
        },
      };
    case "profiles.create":
      natsExactKeys(result, ["correlationId", "profiles"]);
      return {
        ...base,
        command,
        ok: true,
        result: { correlationId, profiles: parseNatsProfilesSnapshot(result.profiles) },
      };
    case "profiles.update":
      natsExactKeys(result, ["correlationId", "profiles"]);
      return {
        ...base,
        command,
        ok: true,
        result: { correlationId, profiles: parseNatsProfilesSnapshot(result.profiles) },
      };
    case "profiles.delete":
      natsExactKeys(result, ["correlationId", "profiles"]);
      return {
        ...base,
        command,
        ok: true,
        result: { correlationId, profiles: parseNatsProfilesSnapshot(result.profiles) },
      };
    case "profiles.connect":
      natsExactKeys(result, ["correlationId", "connection"]);
      return {
        ...base,
        command,
        ok: true,
        result: { correlationId, connection: parseNatsConnectionSnapshot(result.connection) },
      };
    case "connection.disconnect":
      natsExactKeys(result, ["correlationId", "connection", "subscription"]);
      return {
        ...base,
        command,
        ok: true,
        result: {
          correlationId,
          connection: parseNatsConnectionSnapshot(result.connection),
          subscription: parseNatsSubscriptionSnapshot(result.subscription),
        },
      };
    case "subscription.start":
      natsExactKeys(result, ["correlationId", "subscription"]);
      return {
        ...base,
        command,
        ok: true,
        result: { correlationId, subscription: parseNatsSubscriptionSnapshot(result.subscription) },
      };
    case "subscription.stop":
      natsExactKeys(result, ["correlationId", "subscription"]);
      return {
        ...base,
        command,
        ok: true,
        result: { correlationId, subscription: parseNatsSubscriptionSnapshot(result.subscription) },
      };
  }
}
export function parseCorrelatedNatsResponse<Command extends NatsCommand>(
  value: unknown,
  submitted: Command,
): NatsCommandResponse<Command["command"]> {
  const response = parseNatsResponse(value);
  if (response.command !== submitted.command || response.id !== submitted.id)
    throw new NatsContractValidationError();
  // The complete discriminated response has been parsed and matched to this command above.
  return response as NatsCommandResponse<Command["command"]>;
}
export function parseNatsEvent(value: unknown): NatsEvent {
  const input = natsObject(value);
  const event = natsEnum(input.event, [
    "backend.availability",
    "profiles.changed",
    "connection.state",
    "subscription.changed",
    "records.batch",
  ] as const);
  natsExactKeys(
    input,
    event === "backend.availability"
      ? ["version", "sequence", "event", "payload"]
      : ["version", "sequence", "event", "payload", "operation", "correlationId"],
  );
  const base = { version: version(input.version), sequence: natsInteger(input.sequence) };
  if (event === "backend.availability") {
    const payload = natsObject(input.payload);
    const state = natsEnum(payload.state, ["ready", "unavailable"] as const);
    natsExactKeys(payload, state === "ready" ? ["state"] : ["state", "recovery"]);
    return {
      ...base,
      event,
      payload:
        state === "ready" ? { state } : { state, recovery: natsText(payload.recovery, 1024) },
    };
  }
  const context = {
    operation: natsEnum(input.operation, NATS_COMMANDS),
    correlationId: natsIdentifier(input.correlationId),
  };
  switch (event) {
    case "profiles.changed":
      return { ...base, ...context, event, payload: parseNatsProfilesSnapshot(input.payload) };
    case "connection.state":
      return { ...base, ...context, event, payload: parseNatsConnectionSnapshot(input.payload) };
    case "subscription.changed":
      return { ...base, ...context, event, payload: parseNatsSubscriptionSnapshot(input.payload) };
    case "records.batch": {
      const parsed = { ...base, ...context, event, payload: parseNatsRecordsBatch(input.payload) };
      if (natsUtf8Bytes(JSON.stringify(parsed)) > NATS_LIMITS.batchBytes)
        throw new NatsContractValidationError();
      return parsed;
    }
  }
}
