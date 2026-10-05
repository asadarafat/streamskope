import type { NatsFailureCode, NatsProfileStoreCapability, NatsSafeFailure } from "../contracts";

/** Host-only resolved credentials. This type is never part of a command result/event. */
export interface NatsConnectionInput {
  readonly servers: readonly string[];
  readonly authentication:
    { readonly mode: "none" } | { readonly mode: "token"; readonly token: string };
  readonly tls: { readonly mode: "plaintext" } | { readonly mode: "tls"; readonly caPem?: string };
}
export interface NatsProfileRecord extends NatsConnectionInput {
  readonly id: string;
  readonly revision: number;
  readonly name: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}
export interface NatsProfileStore {
  readonly capability: NatsProfileStoreCapability;
  load(signal?: AbortSignal): Promise<readonly NatsProfileRecord[]>;
  save(records: readonly NatsProfileRecord[], signal?: AbortSignal): Promise<void>;
}
export interface NatsResolvedProfile {
  readonly identity: { readonly id: string; readonly revision: number; readonly name: string };
  readonly connection: NatsConnectionInput;
}
export class NatsProfileError extends Error implements NatsSafeFailure {
  readonly recovery?: string;
  constructor(
    readonly code: NatsFailureCode,
    readonly summary: string,
    recovery?: string,
    options?: ErrorOptions,
  ) {
    super(summary, options);
    this.name = "NatsProfileError";
    if (recovery !== undefined) this.recovery = recovery;
  }
}
