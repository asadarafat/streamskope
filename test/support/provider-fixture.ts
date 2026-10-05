import type { ProviderEventCodec } from "../../src/platform/providers/host";
import {
  createProviderEndpoint,
  type ProviderWireEndpoint,
} from "../../src/platform/node/provider-host";

export type FixtureCommand =
  | {
      readonly provider: string;
      readonly requestId: string;
      readonly version: number;
      readonly action: "read";
    }
  | {
      readonly provider: string;
      readonly requestId: string;
      readonly version: number;
      readonly action: "set";
      readonly value: string;
    };

export interface FixtureResponse {
  readonly provider: string;
  readonly requestId: string;
  readonly version: number;
  readonly action: FixtureCommand["action"];
  readonly accepted: true;
  readonly value: string;
}

export type FixtureEvent =
  | {
      readonly provider: string;
      readonly sequence: number;
      readonly version: number;
      readonly type: "fixture.value";
      readonly value: string;
    }
  | {
      readonly provider: string;
      readonly sequence: number;
      readonly version: number;
      readonly type: "fixture.availability";
      readonly state: "ready" | "unavailable";
      readonly recovery?: string;
    };

export interface ProviderFixture {
  readonly endpoint: ProviderWireEndpoint;
  readonly codec: ProviderEventCodec<FixtureEvent>;
  readonly requests: FixtureCommand[];
  readonly subscribeCalls: number;
  readonly shutdownCalls: number;
  nextResponse: FixtureResponse | undefined;
  subscribeFailure: Error | undefined;
  shutdownOperation: (() => Promise<void>) | undefined;
  readonly command: (action?: "read" | "set", value?: string, requestId?: string) => FixtureCommand;
  readonly response: (command: FixtureCommand, value?: string) => FixtureResponse;
  readonly event: (value?: string, sequence?: number) => FixtureEvent;
  readonly parseCommand: (wire: unknown) => FixtureCommand;
  readonly correlateResponse: (wire: unknown, command: FixtureCommand) => FixtureResponse;
  readonly emit: (wire: unknown) => void;
  readonly listenerCount: () => number;
}

function record(wire: unknown): Record<string, unknown> {
  if (wire === null || typeof wire !== "object" || Array.isArray(wire))
    throw new Error("Invalid fixture protocol object.");
  return wire as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): void {
  const actual = Object.keys(value);
  if (actual.length !== keys.length || actual.some((key) => !keys.includes(key)))
    throw new Error("Unexpected fixture protocol fields.");
}

function text(value: unknown, maximum: number): string {
  if (typeof value !== "string" || value.length > maximum)
    throw new Error("Invalid fixture protocol text.");
  return value;
}

/** A deliberately different protocol proves that transport routing is not Kafka dispatch. */
export function createProviderFixture(options: {
  readonly id: string;
  readonly version: number;
}): ProviderFixture {
  const { id, version } = options;
  const listeners = new Set<(wire: unknown) => void>();
  const requests: FixtureCommand[] = [];
  let value = "fixture-initial";
  let nextId = 0;
  let nextResponse: FixtureResponse | undefined;
  let subscribeFailure: Error | undefined;
  let shutdownOperation: (() => Promise<void>) | undefined;
  let subscribeCalls = 0;
  let shutdownCalls = 0;

  const identity = (value: Record<string, unknown>): void => {
    if (value.provider !== id || value.version !== version)
      throw new Error("Fixture provider or protocol version does not match.");
  };
  const parseCommand = (wire: unknown): FixtureCommand => {
    const command = record(wire);
    identity(command);
    const requestId = text(command.requestId, 64);
    if (requestId.length === 0) throw new Error("A fixture request ID is required.");
    if (command.action === "read") {
      exactKeys(command, ["provider", "requestId", "version", "action"]);
      return { provider: id, requestId, version, action: "read" };
    }
    if (command.action === "set") {
      exactKeys(command, ["provider", "requestId", "version", "action", "value"]);
      return { provider: id, requestId, version, action: "set", value: text(command.value, 256) };
    }
    throw new Error("Invalid fixture action.");
  };
  const response = (command: FixtureCommand, result = value): FixtureResponse => ({
    provider: command.provider,
    requestId: command.requestId,
    version: command.version,
    action: command.action,
    accepted: true,
    value: result,
  });
  const correlateResponse = (wire: unknown, command: FixtureCommand): FixtureResponse => {
    const result = record(wire);
    exactKeys(result, ["provider", "requestId", "version", "action", "accepted", "value"]);
    identity(result);
    if (
      result.requestId !== command.requestId ||
      result.action !== command.action ||
      result.accepted !== true
    )
      throw new Error("Fixture response does not match the submitted request.");
    return response(command, text(result.value, 256));
  };
  const parseEvent = (wire: unknown): FixtureEvent => {
    const event = record(wire);
    identity(event);
    if (!Number.isSafeInteger(event.sequence) || (event.sequence as number) < 0)
      throw new Error("Invalid fixture sequence.");
    const sequence = event.sequence as number;
    if (event.type === "fixture.value") {
      exactKeys(event, ["provider", "sequence", "version", "type", "value"]);
      return { provider: id, sequence, version, type: event.type, value: text(event.value, 256) };
    }
    if (
      event.type === "fixture.availability" &&
      (event.state === "ready" || event.state === "unavailable")
    ) {
      exactKeys(event, [
        "provider",
        "sequence",
        "version",
        "type",
        "state",
        ...(event.recovery === undefined ? [] : ["recovery"]),
      ]);
      return {
        provider: id,
        sequence,
        version,
        type: event.type,
        state: event.state,
        ...(event.recovery === undefined ? {} : { recovery: text(event.recovery, 1024) }),
      };
    }
    throw new Error("Invalid fixture event type.");
  };
  const codec: ProviderEventCodec<FixtureEvent> = {
    version,
    parseEvent,
    availability: (sequence, state, recovery): FixtureEvent => ({
      provider: id,
      sequence,
      version,
      type: "fixture.availability",
      state,
      ...(recovery === undefined ? {} : { recovery }),
    }),
    isAvailability: (event): boolean => event.type === "fixture.availability",
  };
  const endpoint = createProviderEndpoint({
    id,
    version,
    parseCommand,
    execute: (command: FixtureCommand): Promise<FixtureResponse> => {
      requests.push(command);
      if (command.action === "set") value = command.value;
      const override = nextResponse;
      nextResponse = undefined;
      return Promise.resolve(override ?? response(command));
    },
    correlateResponse,
    parseEvent,
    subscribe: (listener: (wire: unknown) => void): (() => void) => {
      subscribeCalls += 1;
      if (subscribeFailure !== undefined) throw subscribeFailure;
      listeners.add(listener);
      return (): void => {
        listeners.delete(listener);
      };
    },
    availability: codec.availability,
    shutdown: (): Promise<void> => {
      shutdownCalls += 1;
      return shutdownOperation?.() ?? Promise.resolve();
    },
  });
  return {
    endpoint,
    codec,
    requests,
    get subscribeCalls(): number {
      return subscribeCalls;
    },
    get shutdownCalls(): number {
      return shutdownCalls;
    },
    get nextResponse(): FixtureResponse | undefined {
      return nextResponse;
    },
    set nextResponse(response: FixtureResponse | undefined) {
      nextResponse = response;
    },
    get subscribeFailure(): Error | undefined {
      return subscribeFailure;
    },
    set subscribeFailure(error: Error | undefined) {
      subscribeFailure = error;
    },
    get shutdownOperation(): (() => Promise<void>) | undefined {
      return shutdownOperation;
    },
    set shutdownOperation(operation: (() => Promise<void>) | undefined) {
      shutdownOperation = operation;
    },
    command: (
      action = "read",
      text = "",
      requestId = `${id}-${String(++nextId)}`,
    ): FixtureCommand =>
      action === "read"
        ? { provider: id, requestId, version, action }
        : { provider: id, requestId, version, action, value: text },
    response,
    event: (text = value, sequence = 1): FixtureEvent => ({
      provider: id,
      sequence,
      version,
      type: "fixture.value",
      value: text,
    }),
    parseCommand,
    correlateResponse,
    emit: (wire: unknown): void => {
      for (const listener of listeners) listener(wire);
    },
    listenerCount: (): number => listeners.size,
  };
}
