import type { PluginProfileSource } from "../../../plugins/contracts";

import type { ProfileBindingInput } from "./profile-binding";

export const PROFILE_LIMITS = {
  brokers: 32,
  brokerCharacters: 512,
  clientIdCharacters: 512,
  clientSecretCharacters: 4_096,
  idCharacters: 128,
  nameCharacters: 256,
  profiles: 100,
  scopeCharacters: 1_024,
  tokenEndpointCharacters: 2_048,
  trustBinaryBytes: 8 * 1_048_576,
  trustEncodedCharacters: 11_184_812,
  trustLabelCharacters: 512,
} as const;

export const PROFILE_TRUST_KINDS = ["pem", "jks", "pkcs12"] as const;
export const PROFILE_STORE_DURABILITIES = ["durable", "session"] as const;
export const PROFILE_STORE_PROTECTIONS = [
  "memory",
  "os-protected",
  "passphrase-protected",
  "unavailable",
] as const;
export const PROFILE_STORE_STATES = ["ready", "unavailable"] as const;
export const CLUSTER_SERVICE_AUTHENTICATION_MODES = [
  "none",
  "oauth",
  "basic",
  "bearer",
  "oauth-client",
] as const;
export const KAFKA_SASL_MECHANISMS = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] as const;
export type KafkaSaslMechanism = (typeof KAFKA_SASL_MECHANISMS)[number];
export const KAFKA_PROFILE_TRANSPORTS = ["tls", "plaintext"] as const;

export type ProfileTrustKind = (typeof PROFILE_TRUST_KINDS)[number];
export type ProfileStoreDurability = (typeof PROFILE_STORE_DURABILITIES)[number];
export type ProfileStoreProtection = (typeof PROFILE_STORE_PROTECTIONS)[number];
export type ProfileStoreState = (typeof PROFILE_STORE_STATES)[number];
export type ClusterServiceAuthenticationMode =
  (typeof CLUSTER_SERVICE_AUTHENTICATION_MODES)[number];
export type KafkaProfileTransport = (typeof KAFKA_PROFILE_TRANSPORTS)[number];

export type ProfileSource = PluginProfileSource;

export interface ProfileSaslInput<T = ProtectedValueUpdateInput> {
  readonly mechanism: KafkaSaslMechanism;
  readonly username: string;
  readonly password: T;
}

export interface ProfileClientIdentityInput<T = ProtectedValueUpdateInput> {
  readonly certificatePem: T;
  readonly privateKeyPem: T;
  readonly passphrase: T;
}

export type ProfileServiceTrustInput<T = ProtectedValueUpdateInput> =
  { readonly mode: "broker" | "system" } | ({ readonly mode: "custom" } & ProfileTrustInput<T>);

export interface ClusterServiceEndpointInput<T = ProtectedValueUpdateInput> {
  readonly authentication: ClusterServiceAuthenticationMode;
  readonly baseUrl: string;
  readonly basic?: { readonly username: string; readonly password: T };
  readonly bearer?: T;
  readonly oauth?: ProfileOAuthInput<T>;
  readonly trust?: ProfileServiceTrustInput<T>;
  readonly clientIdentity?: ProfileClientIdentityInput<T>;
}

export interface ClusterServiceEndpointsInput<T = ProtectedValueUpdateInput> {
  readonly connect?: ClusterServiceEndpointInput<T>;
  readonly redpandaAdmin?: ClusterServiceEndpointInput<T>;
  readonly schemaRegistry?: ClusterServiceEndpointInput<T>;
}

export type ProtectedValueCreateInput =
  | {
      readonly mode: "clear";
    }
  | {
      readonly mode: "replace";
      readonly value: string;
    };

export type ProtectedValueUpdateInput =
  | ProtectedValueCreateInput
  | {
      readonly mode: "retain";
    };

export interface AcquiredProtectedValueInput {
  readonly editorId?: string;
  readonly acquisitionId: string;
  readonly mode: "acquired";
}

export type ProfileTrustCreateValueInput = AcquiredProtectedValueInput | ProtectedValueCreateInput;

export type ProfileTrustUpdateValueInput = AcquiredProtectedValueInput | ProtectedValueUpdateInput;

export interface ProfileTrustInput<TProtectedValue> {
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly material: TProtectedValue;
  readonly password: TProtectedValue;
}

export interface ProfileOAuthInput<TProtectedValue> {
  readonly clientId: string;
  readonly clientSecret: TProtectedValue;
  readonly scope: string;
  readonly tokenEndpoint: string;
}

interface ProfileCreateInputBase {
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: ProfileOAuthInput<ProtectedValueCreateInput>;
  readonly sasl?: ProfileSaslInput<ProtectedValueCreateInput>;
  readonly services?: ClusterServiceEndpointsInput<ProtectedValueCreateInput>;
  readonly source?: ProfileSource;
}

export interface ProfileTlsCreateInput extends ProfileCreateInputBase {
  readonly clientIdentity?: ProfileClientIdentityInput<ProtectedValueCreateInput>;
  readonly apiCa?: ProtectedValueCreateInput;
  readonly binding?: ProfileBindingInput;
  readonly transport?: "tls";
  readonly trust: ProfileTrustInput<ProfileTrustCreateValueInput>;
}

export interface ProfilePlaintextCreateInput extends ProfileCreateInputBase {
  readonly clientIdentity?: never;
  readonly apiCa?: never;
  readonly binding?: never;
  readonly transport: "plaintext";
  readonly trust?: never;
}

export type ProfileCreateInput = ProfilePlaintextCreateInput | ProfileTlsCreateInput;

interface ProfileUpdateInputBase {
  readonly expectedRevision?: number;
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: ProfileOAuthInput<ProtectedValueUpdateInput>;
  readonly sasl?: ProfileSaslInput<ProtectedValueUpdateInput>;
  readonly services?: ClusterServiceEndpointsInput;
  readonly source?: ProfileSource;
}

export interface ProfileTlsUpdateInput extends ProfileUpdateInputBase {
  readonly clientIdentity?: ProfileClientIdentityInput<ProtectedValueUpdateInput>;
  readonly apiCa?: ProtectedValueUpdateInput;
  readonly binding?: ProfileBindingInput;
  readonly transport?: "tls";
  readonly trust: ProfileTrustInput<ProfileTrustUpdateValueInput>;
}

export interface ProfilePlaintextUpdateInput extends ProfileUpdateInputBase {
  readonly clientIdentity?: never;
  readonly apiCa?: never;
  readonly binding?: never;
  readonly transport: "plaintext";
  readonly trust?: never;
}

export type ProfileUpdateInput = ProfilePlaintextUpdateInput | ProfileTlsUpdateInput;

export type ProfileTestInput =
  | {
      readonly mode: "create";
      readonly profile: ProfileCreateInput;
    }
  | {
      readonly mode: "update";
      readonly profile: ProfileUpdateInput;
      readonly profileId: string;
    };

export interface ProfileSummaryTrust {
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly materialPresent: boolean;
  readonly passwordPresent: boolean;
}

export interface ProfileSummaryOAuth {
  readonly clientId: string;
  readonly clientSecretPresent: boolean;
  readonly scope: string;
  readonly tokenEndpoint: string;
}

export interface ProfileSummarySasl {
  readonly mechanism: KafkaSaslMechanism;
  readonly username: string;
  readonly passwordPresent: boolean;
}

export interface ProfileSummaryClientIdentity {
  readonly certificatePresent: boolean;
  readonly privateKeyPresent: boolean;
  readonly passphrasePresent: boolean;
}

export interface ClusterServiceEndpointSummary {
  readonly authentication: ClusterServiceAuthenticationMode;
  readonly baseUrl: string;
  readonly basic?: { readonly username: string; readonly passwordPresent: boolean };
  readonly bearerPresent?: boolean;
  readonly oauth?: ProfileSummaryOAuth;
  readonly trust?:
    { readonly mode: "broker" | "system" } | ({ readonly mode: "custom" } & ProfileSummaryTrust);
  readonly clientIdentity?: ProfileSummaryClientIdentity;
}

export interface ClusterServiceEndpointsSummary {
  readonly connect?: ClusterServiceEndpointSummary;
  readonly redpandaAdmin?: ClusterServiceEndpointSummary;
  readonly schemaRegistry?: ClusterServiceEndpointSummary;
}

interface ProfileSummaryBase {
  readonly revision?: number;
  readonly active: boolean;
  readonly brokers: readonly string[];
  readonly createdAt: string;
  readonly id: string;
  readonly name: string;
  readonly oauth?: ProfileSummaryOAuth;
  readonly sasl?: ProfileSummarySasl;
  readonly services?: ClusterServiceEndpointsSummary;
  readonly source?: ProfileSource;
  readonly updatedAt: string;
}

export interface ProfileTlsSummary extends ProfileSummaryBase {
  readonly clientIdentity?: ProfileSummaryClientIdentity;
  readonly transport?: "tls";
  readonly trust: ProfileSummaryTrust;
}

export interface ProfilePlaintextSummary extends ProfileSummaryBase {
  readonly clientIdentity?: never;
  readonly transport: "plaintext";
  readonly trust?: never;
}

export type ProfileSummary = ProfilePlaintextSummary | ProfileTlsSummary;

export interface ProfileStoreCapability {
  readonly durability: ProfileStoreDurability;
  readonly protection: ProfileStoreProtection;
  readonly recovery?: string;
  readonly state: ProfileStoreState;
}
