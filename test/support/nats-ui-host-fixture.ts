import {
  NATS_PROTOCOL_VERSION,
  parseCorrelatedNatsResponse,
  parseNatsEvent,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsHost,
  type NatsProfileSummary,
  type NatsProfilesSnapshot,
  type NatsRecord,
  type NatsSubscriptionSnapshot,
} from "../../src/features/nats/contracts";
import { initialNatsWorkspaceSnapshot } from "../../src/features/nats/ui/workspace-state";

type EventInput<Event> = Event extends NatsEvent ? Omit<Event, "version" | "sequence"> : never;
export interface NatsUiRequest {
  readonly command: NatsCommand;
  readonly answer: (result: unknown) => void;
  readonly reject: (error: unknown) => void;
}
export const uiNatsProfile: NatsProfileSummary = {
  id: "fixture-profile",
  revision: 1,
  name: "Fixture NATS",
  servers: ["nats://127.0.0.1:4222"],
  authentication: { mode: "none" },
  tls: { mode: "plaintext" },
  createdAt: "2026-10-05T00:00:00.000Z",
  updatedAt: "2026-10-05T00:00:00.000Z",
};
export function uiNatsProfiles(
  profiles: readonly NatsProfileSummary[] = [uiNatsProfile],
  revision = 0,
): NatsProfilesSnapshot {
  return {
    revision,
    capability: { durability: "session", protection: "memory", state: "ready" },
    profiles,
  };
}
export function uiNatsSubscription(
  state: NatsSubscriptionSnapshot["state"] = "streaming",
  generation = "generation-1",
  total = 0,
  omitted = 0,
  revision = { idle: 0, loading: 1, streaming: 2, stopping: 3, stopped: 4, failed: 5 }[state],
): NatsSubscriptionSnapshot {
  return {
    revision,
    state,
    generation,
    subject: "qualification.*",
    counters: {
      receivedRecords: total,
      publishedRecords: total,
      applicationOmittedRecords: 0,
      queuedRecords: 0,
      queuedBytes: 0,
      transportOmittedRecords: omitted,
    },
  };
}
export function uiNatsRecord(
  id = "record-1",
  generation = "generation-1",
  data = "event",
): NatsRecord {
  return {
    id,
    generation,
    subject: "qualification.event",
    headers: [],
    headersTruncated: false,
    payload: { encoding: "utf8", data },
    payloadBytes: new TextEncoder().encode(data).length,
    preview: "event",
    receivedAt: "2026-10-05T00:00:00.000Z",
    timestampProvenance: "host-received",
  };
}
export function natsUiHostFixture(): {
  readonly host: NatsHost;
  readonly requests: NatsUiRequest[];
  readonly calls: string[];
  readonly emit: (event: EventInput<NatsEvent>) => void;
  readonly bootstrap: (request?: NatsUiRequest) => void;
  readonly listenerCount: () => number;
} {
  const requests: NatsUiRequest[] = [];
  const calls: string[] = [];
  const listeners = new Set<(event: NatsEvent) => void>();
  let sequence = 0;
  const host: NatsHost = {
    execute: async <Command extends NatsCommand>(
      command: Command,
    ): Promise<NatsCommandResponse<Command["command"]>> => {
      calls.push(command.command);
      let resolve!: (value: unknown) => void;
      let reject!: (reason: unknown) => void;
      const response = new Promise<unknown>((complete, fail) => {
        resolve = complete;
        reject = fail;
      });
      requests.push({
        command,
        answer: (result): void =>
          resolve({
            version: NATS_PROTOCOL_VERSION,
            id: command.id,
            command: command.command,
            ok: true,
            result: { correlationId: command.id, ...(result as object) },
          }),
        reject,
      });
      return parseCorrelatedNatsResponse(await response, command);
    },
    subscribe: (listener): (() => void) => {
      calls.push("subscribe");
      listeners.add(listener);
      return (): void => {
        calls.push("unsubscribe");
        listeners.delete(listener);
      };
    },
  };
  return {
    host,
    requests,
    calls,
    listenerCount: (): number => listeners.size,
    emit: (event): void => {
      const parsed = parseNatsEvent({
        ...event,
        version: NATS_PROTOCOL_VERSION,
        sequence: ++sequence,
      });
      for (const listener of listeners) listener(parsed);
    },
    bootstrap: (request = requests[0]!): void => {
      const initial = initialNatsWorkspaceSnapshot();
      request.answer({
        profiles: uiNatsProfiles(),
        connection: initial.connection,
        subscription: initial.subscription,
      });
    },
  };
}
