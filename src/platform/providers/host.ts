/** Feature hosts retain their own command-specific execute callable. */
export interface ProviderHostPort<Execute, Event> {
  execute: Execute;
  subscribe(listener: (event: Event) => void): () => void;
}

export interface ProviderWireEvent {
  readonly sequence: number;
  readonly version: number;
}

/** Domain codecs sit above the private IPC/HTTP wire transports. */
export interface ProviderEventCodec<Event extends ProviderWireEvent> {
  readonly version: number;
  readonly parseEvent: (value: unknown) => Event;
  readonly availability: (
    sequence: number,
    state: "ready" | "unavailable",
    recovery?: string,
  ) => Event;
  readonly isAvailability: (event: Event) => boolean;
}
