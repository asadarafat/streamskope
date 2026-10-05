import type { NodeConnectionOptions, ServerInfo, Status } from "@nats-io/transport-node";

/** Only supported SDK operations used by the engine; no protocol/socket internals. */
export interface NatsSdkMessage {
  readonly subject: string;
  readonly reply?: string;
  readonly data: Uint8Array;
  readonly headers?: Iterable<[string, string[]]>;
}

export interface NatsSdkSubscription {
  readonly closed: Promise<void | Error>;
  unsubscribe(): void;
  isClosed(): boolean;
}

export interface NatsSdkConnection {
  readonly info?: Pick<ServerInfo, "tls_required">;
  subscribe(
    subject: string,
    options: { readonly callback: (error: Error | null, message: NatsSdkMessage) => void },
  ): NatsSdkSubscription;
  flush(): Promise<void>;
  close(): Promise<void>;
  closed(): Promise<void | Error>;
  isClosed(): boolean;
  status(): AsyncIterable<Status>;
}

export type NatsSdkConnect = (options: NodeConnectionOptions) => Promise<NatsSdkConnection>;
