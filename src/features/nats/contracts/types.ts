import type { ProviderHostPort } from "../../../platform/providers/host";

export const NATS_PROVIDER_ID = "nats" as const;
export const NATS_PROTOCOL_VERSION = 1 as const;
export const NATS_LIMITS = {
  profiles: 100,
  servers: 8,
  serverBytes: 2048,
  nameBytes: 256,
  identifierCharacters: 128,
  tokenBytes: 16 * 1024,
  caPemBytes: 256 * 1024,
  profilePlaintextBytes: 320 * 1024,
  profileCiphertextBytes: 384 * 1024,
  profileFileBytes: 52 * 1024 * 1024,
  payloadBytes: 256 * 1024,
  previewBytes: 8 * 1024,
  subjectBytes: 512,
  replyBytes: 512,
  headerNameBytes: 512,
  headerValueBytes: 8 * 1024,
  headerValues: 128,
  headerEntries: 128,
  headerBytes: 64 * 1024,
  queuedRecords: 1000,
  queuedBytes: 8 * 1024 * 1024,
  batchRecords: 200,
  batchBytes: 1024 * 1024,
} as const;

export const NATS_COMMANDS = [
  "profiles.list",
  "profiles.create",
  "profiles.update",
  "profiles.delete",
  "profiles.connect",
  "connection.disconnect",
  "subscription.start",
  "subscription.stop",
] as const;
export type NatsCommandName = (typeof NATS_COMMANDS)[number];

export const NATS_FAILURE_CODES = [
  "validation",
  "not-connected",
  "not-found",
  "revision-conflict",
  "profile-in-use",
  "profile-capacity",
  "storage-unavailable",
  "authentication",
  "tls",
  "permission",
  "connection",
  "timeout",
  "cancelled",
  "cleanup",
  "unavailable",
] as const;
export type NatsFailureCode = (typeof NATS_FAILURE_CODES)[number];
export interface NatsSafeFailure {
  readonly code: NatsFailureCode;
  readonly summary: string;
  readonly recovery?: string;
}
export interface NatsHostError extends NatsSafeFailure {
  readonly stage: "validation" | "profiles" | "connection" | "subscription" | "lifecycle";
  readonly operation: NatsCommandName;
  readonly correlationId: string;
}

export type NatsCreateSecret =
  { readonly mode: "clear" } | { readonly mode: "replace"; readonly value: string };
export type NatsUpdateSecret = NatsCreateSecret | { readonly mode: "retain" };
export type NatsProfileAuthenticationInput<Secret> =
  { readonly mode: "none" } | { readonly mode: "token"; readonly token: Secret };
export type NatsProfileTlsInput<Secret> =
  { readonly mode: "plaintext" } | { readonly mode: "tls"; readonly caPem: Secret };
export interface NatsProfileCreateInput {
  readonly name: string;
  readonly servers: readonly string[];
  readonly authentication: NatsProfileAuthenticationInput<NatsCreateSecret>;
  readonly tls: NatsProfileTlsInput<NatsCreateSecret>;
}
export interface NatsProfileUpdateInput {
  readonly name: string;
  readonly servers: readonly string[];
  readonly authentication: NatsProfileAuthenticationInput<NatsUpdateSecret>;
  readonly tls: NatsProfileTlsInput<NatsUpdateSecret>;
}
export interface NatsProfileStoreCapability {
  readonly durability: "durable" | "session";
  readonly protection: "memory" | "os-protected" | "unavailable";
  readonly state: "ready" | "unavailable";
  readonly recovery?: string;
}
export interface NatsProfileSummary {
  readonly id: string;
  readonly revision: number;
  readonly name: string;
  readonly servers: readonly string[];
  readonly authentication:
    { readonly mode: "none" } | { readonly mode: "token"; readonly tokenPresent: boolean };
  readonly tls:
    { readonly mode: "plaintext" } | { readonly mode: "tls"; readonly caPresent: boolean };
  readonly createdAt: string;
  readonly updatedAt: string;
}
export interface NatsProfilesSnapshot {
  readonly capability: NatsProfileStoreCapability;
  readonly profiles: readonly NatsProfileSummary[];
}

export interface NatsRecord {
  readonly id: string;
  readonly generation: string;
  readonly subject: string;
  readonly reply?: string;
  readonly headers: readonly { readonly name: string; readonly values: readonly string[] }[];
  readonly headersTruncated: boolean;
  readonly payload: { readonly encoding: "utf8" | "base64"; readonly data: string };
  readonly payloadBytes: number;
  readonly preview: string;
  readonly receivedAt: string;
  readonly timestampProvenance: "host-received";
}
export interface NatsSubscriptionCounters {
  readonly receivedRecords: number;
  readonly applicationOmittedRecords: number;
  readonly publishedRecords: number;
  readonly queuedRecords: number;
  readonly queuedBytes: number;
  readonly transportOmittedRecords: number;
}
export interface NatsConnectionSnapshot {
  readonly state: "disconnected" | "connecting" | "connected" | "disconnecting" | "failed";
  readonly profile: {
    readonly id: string;
    readonly revision: number;
    readonly name: string;
  } | null;
  readonly failure?: NatsSafeFailure;
}
export interface NatsSubscriptionSnapshot {
  readonly state: "idle" | "loading" | "streaming" | "stopping" | "stopped" | "failed";
  readonly generation: string | null;
  readonly subject: string | null;
  readonly counters: NatsSubscriptionCounters;
  readonly failure?: NatsSafeFailure;
}
export interface NatsRecordsBatch {
  readonly generation: string;
  readonly records: readonly NatsRecord[];
  readonly counters: NatsSubscriptionCounters;
}

interface NatsCommandBase {
  readonly version: typeof NATS_PROTOCOL_VERSION;
  readonly id: string;
}
export type NatsCommand =
  | (NatsCommandBase & {
      readonly command: "profiles.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (NatsCommandBase & {
      readonly command: "profiles.create";
      readonly payload: { readonly profile: NatsProfileCreateInput };
    })
  | (NatsCommandBase & {
      readonly command: "profiles.update";
      readonly payload: {
        readonly profileId: string;
        readonly expectedRevision: number;
        readonly profile: NatsProfileUpdateInput;
      };
    })
  | (NatsCommandBase & {
      readonly command: "profiles.delete";
      readonly payload: { readonly profileId: string; readonly expectedRevision: number };
    })
  | (NatsCommandBase & {
      readonly command: "profiles.connect";
      readonly payload: { readonly profileId: string; readonly expectedRevision: number };
    })
  | (NatsCommandBase & {
      readonly command: "connection.disconnect";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (NatsCommandBase & {
      readonly command: "subscription.start";
      readonly payload: { readonly subject: string };
    })
  | (NatsCommandBase & {
      readonly command: "subscription.stop";
      readonly payload: Readonly<Record<string, never>>;
    });

export interface NatsCommandResultMap {
  readonly "profiles.list": {
    readonly correlationId: string;
    readonly profiles: NatsProfilesSnapshot;
    readonly connection: NatsConnectionSnapshot;
    readonly subscription: NatsSubscriptionSnapshot;
  };
  readonly "profiles.create": {
    readonly correlationId: string;
    readonly profiles: NatsProfilesSnapshot;
  };
  readonly "profiles.update": {
    readonly correlationId: string;
    readonly profiles: NatsProfilesSnapshot;
  };
  readonly "profiles.delete": {
    readonly correlationId: string;
    readonly profiles: NatsProfilesSnapshot;
  };
  readonly "profiles.connect": {
    readonly correlationId: string;
    readonly connection: NatsConnectionSnapshot;
  };
  readonly "connection.disconnect": {
    readonly correlationId: string;
    readonly connection: NatsConnectionSnapshot;
    readonly subscription: NatsSubscriptionSnapshot;
  };
  readonly "subscription.start": {
    readonly correlationId: string;
    readonly subscription: NatsSubscriptionSnapshot;
  };
  readonly "subscription.stop": {
    readonly correlationId: string;
    readonly subscription: NatsSubscriptionSnapshot;
  };
}
export type NatsCommandSuccess<Name extends NatsCommandName = NatsCommandName> = {
  [Command in Name]: {
    readonly version: typeof NATS_PROTOCOL_VERSION;
    readonly id: string;
    readonly command: Command;
    readonly ok: true;
    readonly result: NatsCommandResultMap[Command];
  };
}[Name];
export interface NatsCommandFailure<Name extends NatsCommandName = NatsCommandName> {
  readonly version: typeof NATS_PROTOCOL_VERSION;
  readonly id: string;
  readonly command: Name;
  readonly ok: false;
  readonly error: NatsHostError;
}
export type NatsCommandResponse<Name extends NatsCommandName = NatsCommandName> =
  NatsCommandSuccess<Name> | NatsCommandFailure<Name>;

interface NatsEventBase {
  readonly version: typeof NATS_PROTOCOL_VERSION;
  readonly sequence: number;
}
interface NatsOperationContext {
  readonly operation: NatsCommandName;
  readonly correlationId: string;
}
export type NatsEvent =
  | (NatsEventBase & {
      readonly event: "backend.availability";
      readonly payload:
        { readonly state: "ready" } | { readonly state: "unavailable"; readonly recovery: string };
    })
  | (NatsEventBase &
      NatsOperationContext & {
        readonly event: "profiles.changed";
        readonly payload: NatsProfilesSnapshot;
      })
  | (NatsEventBase &
      NatsOperationContext & {
        readonly event: "connection.state";
        readonly payload: NatsConnectionSnapshot;
      })
  | (NatsEventBase &
      NatsOperationContext & {
        readonly event: "subscription.changed";
        readonly payload: NatsSubscriptionSnapshot;
      })
  | (NatsEventBase &
      NatsOperationContext & {
        readonly event: "records.batch";
        readonly payload: NatsRecordsBatch;
      });
export type NatsHostExecute = <Command extends NatsCommand>(
  command: Command,
) => Promise<NatsCommandResponse<Command["command"]>>;
export type NatsHost = ProviderHostPort<NatsHostExecute, NatsEvent>;
