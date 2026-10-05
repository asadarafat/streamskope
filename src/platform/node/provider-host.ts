import type { ProviderWireEvent } from "../providers/host";

export type { ProviderWireEvent } from "../providers/host";

export interface ProviderEventQueueOptions {
  readonly maxEvents: number;
  readonly maxMessageBytes: number;
  readonly maxMessages: number;
}

interface TypedEventQueue<Event extends ProviderWireEvent> {
  readonly length: number;
  enqueue(event: Event): boolean;
  dequeue(): Event | undefined;
}

export type ProviderEventQueue = TypedEventQueue<ProviderWireEvent>;

/** Sealed host-only serialization boundary; feature UIs use typed provider hosts. */
export interface ProviderWireEndpoint {
  readonly id: string;
  readonly version: number;
  dispatch(wire: unknown): Promise<unknown>;
  parseEvent(wire: unknown): ProviderWireEvent;
  subscribe(listener: (wire: unknown) => void): () => void;
  createEventQueue(options: ProviderEventQueueOptions): ProviderEventQueue;
  availability(
    sequence: number,
    state: "ready" | "unavailable",
    recovery?: string,
  ): ProviderWireEvent;
  shutdown(): Promise<void>;
}

export interface ProviderEndpointOptions<Command, Response, Event extends ProviderWireEvent> {
  readonly id: string;
  readonly version: number;
  readonly parseCommand: (wire: unknown) => Command;
  readonly commandErrorSummary?: (error: unknown) => string;
  readonly execute: (command: Command) => Promise<Response>;
  readonly correlateResponse: (wire: unknown, command: Command) => Response;
  readonly parseEvent: (wire: unknown) => Event;
  readonly subscribe: (listener: (wire: unknown) => void) => () => void;
  readonly availability: (
    sequence: number,
    state: "ready" | "unavailable",
    recovery?: string,
  ) => Event;
  readonly shutdown: () => Promise<void>;
  readonly createEventQueue?: (options: ProviderEventQueueOptions) => TypedEventQueue<Event>;
}

export class ProviderWireValidationError extends Error {
  constructor(
    readonly stage: "command" | "response",
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ProviderWireValidationError";
  }
}

export class ProviderHostClosedError extends Error {
  constructor() {
    super("The application provider host is shutting down.");
    this.name = "ProviderHostClosedError";
  }
}

function validateProviderIdentity(id: string, version: number): void {
  if (!/^[a-z][a-z0-9-]{0,31}$/u.test(id)) throw new Error("Invalid provider identifier.");
  if (!Number.isSafeInteger(version) || version <= 0)
    throw new Error("Invalid provider protocol version.");
}

function positiveInteger(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0)
    throw new Error("Provider event queue bounds must be positive safe integers.");
  return value;
}

/** Control-only fallback; record retention is supplied by the provider's policy. */
class ControlEventQueue<Event extends ProviderWireEvent> implements TypedEventQueue<Event> {
  private readonly events: { readonly event: Event; readonly bytes: number }[] = [];
  private bytes = 0;
  private readonly maxEvents: number;
  private readonly maxBytes: number;

  constructor(options: ProviderEventQueueOptions) {
    this.maxEvents = positiveInteger(options.maxEvents);
    this.maxBytes = positiveInteger(options.maxMessageBytes);
    positiveInteger(options.maxMessages);
  }

  get length(): number {
    return this.events.length;
  }

  enqueue(event: Event): boolean {
    const bytes = Buffer.byteLength(JSON.stringify(event));
    if (this.events.length >= this.maxEvents || this.bytes + bytes > this.maxBytes) return false;
    this.events.push({ event, bytes });
    this.bytes += bytes;
    return true;
  }

  dequeue(): Event | undefined {
    const next = this.events.shift();
    if (next === undefined) return undefined;
    this.bytes -= next.bytes;
    return next.event;
  }
}

export function createProviderEndpoint<Command, Response, Event extends ProviderWireEvent>(
  options: ProviderEndpointOptions<Command, Response, Event>,
): ProviderWireEndpoint {
  validateProviderIdentity(options.id, options.version);
  return {
    id: options.id,
    version: options.version,
    dispatch: async (wire): Promise<Response> => {
      let command: Command;
      try {
        command = options.parseCommand(wire);
      } catch (error) {
        throw new ProviderWireValidationError(
          "command",
          options.commandErrorSummary?.(error) ?? "Provider command is invalid.",
          { cause: error },
        );
      }
      const response = await options.execute(command);
      try {
        return options.correlateResponse(response, command);
      } catch (error) {
        throw new ProviderWireValidationError(
          "response",
          "Backend response did not correlate to the submitted command.",
          { cause: error },
        );
      }
    },
    parseEvent: (wire): Event => options.parseEvent(wire),
    subscribe: (listener): (() => void) => options.subscribe(listener),
    createEventQueue: (queueOptions): ProviderEventQueue => {
      const queue =
        options.createEventQueue?.(queueOptions) ?? new ControlEventQueue<Event>(queueOptions);
      return {
        get length(): number {
          return queue.length;
        },
        enqueue: (wire): boolean => queue.enqueue(options.parseEvent(wire)),
        dequeue: (): Event | undefined => queue.dequeue(),
      };
    },
    availability: (sequence, state, recovery): Event =>
      options.availability(sequence, state, recovery),
    shutdown: (): Promise<void> => options.shutdown(),
  };
}

/** Fixed composition, independent admission and one completion barrier for every owner. */
export class ProviderHostRegistry {
  private readonly providers = new Map<string, ProviderWireEndpoint>();
  private readonly owned: readonly ProviderWireEndpoint[];
  private closing = false;
  private shutdownPromise: Promise<void> | undefined;

  constructor(endpoints: readonly ProviderWireEndpoint[]) {
    this.owned = [...endpoints];
    for (const endpoint of endpoints) {
      validateProviderIdentity(endpoint.id, endpoint.version);
      if (this.providers.has(endpoint.id)) throw new Error("Duplicate provider identifier.");
      this.providers.set(
        endpoint.id,
        Object.freeze({
          id: endpoint.id,
          version: endpoint.version,
          dispatch: (wire: unknown): Promise<unknown> => {
            if (this.closing) return Promise.reject(new ProviderHostClosedError());
            return endpoint.dispatch(wire);
          },
          parseEvent: (wire: unknown): ProviderWireEvent => endpoint.parseEvent(wire),
          subscribe: (listener: (wire: unknown) => void): (() => void) => {
            if (this.closing) throw new ProviderHostClosedError();
            return endpoint.subscribe(listener);
          },
          createEventQueue: (options: ProviderEventQueueOptions): ProviderEventQueue =>
            endpoint.createEventQueue(options),
          availability: (
            sequence: number,
            state: "ready" | "unavailable",
            recovery?: string,
          ): ProviderWireEvent => endpoint.availability(sequence, state, recovery),
          shutdown: (): Promise<void> => this.shutdown(),
        }),
      );
    }
  }

  endpoints(): readonly ProviderWireEndpoint[] {
    return [...this.providers.values()];
  }

  get(id: string): ProviderWireEndpoint | undefined {
    return this.providers.get(id);
  }

  shutdown(): Promise<void> {
    if (this.shutdownPromise !== undefined) return this.shutdownPromise;
    this.closing = true;
    let complete = (): void => undefined;
    let fail = (_error: unknown): void => undefined;
    this.shutdownPromise = new Promise<void>((resolve, reject) => {
      complete = resolve;
      fail = reject;
    });
    void Promise.allSettled(
      this.owned.map((endpoint) => Promise.resolve().then(() => endpoint.shutdown())),
    ).then((results) => {
      const failures = results.flatMap((result, index) =>
        result.status === "rejected"
          ? [
              new Error("Provider " + this.owned[index]!.id + " failed to shut down.", {
                cause: result.reason as unknown,
              }),
            ]
          : [],
      );
      if (failures.length > 0)
        fail(new AggregateError(failures, "Application providers did not stop cleanly."));
      else complete();
    });
    return this.shutdownPromise;
  }
}
