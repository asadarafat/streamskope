import type {
  ProfileSaslInput,
  ProfileClientIdentityInput,
  ResolvedClusterServiceEndpoints,
  ClusterServiceEndpointsInput,
  HostErrorCode,
  HostErrorStage,
  KafkaProfileTransport,
  ProfileCreateInput,
  ProfileStoreCapability,
  ProfileSource,
  ProfileSummary,
  ProfileTrustKind,
  ProfileAcquisitionBinding,
  TrustAcquisitionRecipe,
  SecureConnectionInput,
} from "../contracts";

import {
  connectionIdentity,
  securityValidationInput,
  summarizeIdentity,
  summarizeServices,
} from "./profile-security";
import type { KafkaTrustAcquisitionResolver } from "./trust-acquisition-types";

interface KafkaProfileRecordBase {
  readonly sasl?: ProfileSaslInput<string>;
  readonly revision?: number;
  readonly brokers: readonly string[];
  readonly createdAt: string;
  readonly id: string;
  readonly name: string;
  readonly oauth?: {
    readonly clientId: string;
    readonly clientSecret: string;
    readonly scope: string;
    readonly tokenEndpoint: string;
  };
  readonly services?: ClusterServiceEndpointsInput<string>;
  readonly source?: ProfileSource;
  readonly updatedAt: string;
}

export interface KafkaTlsProfileRecord extends KafkaProfileRecordBase {
  readonly clientIdentity?: ProfileClientIdentityInput<string>;
  readonly apiCaPem?: string;
  readonly binding?: ProfileAcquisitionBinding;
  readonly transport?: Extract<KafkaProfileTransport, "tls">;
  readonly trust: {
    readonly kind: ProfileTrustKind;
    readonly label: string;
    readonly material: string;
    readonly password?: string;
  };
}

export interface KafkaPlaintextProfileRecord extends KafkaProfileRecordBase {
  readonly clientIdentity?: never;
  readonly apiCaPem?: never;
  readonly binding?: never;
  readonly transport: Extract<KafkaProfileTransport, "plaintext">;
  readonly trust?: never;
}

export type KafkaProfileRecord = KafkaPlaintextProfileRecord | KafkaTlsProfileRecord;

interface KafkaResolvedProfileDraftBase {
  readonly sasl?: ProfileSaslInput<string>;
  readonly resolvedServices?: ResolvedClusterServiceEndpoints;
  readonly brokers: readonly string[];
  readonly lifetimeSignal?: AbortSignal;
  readonly name: string;
  readonly oauth?: NonNullable<SecureConnectionInput["oauth"]>;
  readonly services?: ClusterServiceEndpointsInput<string>;
  readonly source?: ProfileSource;
}

interface KafkaResolvedTlsProfileDraft extends KafkaResolvedProfileDraftBase {
  readonly clientIdentity?: ProfileClientIdentityInput<string>;
  readonly apiCaPem?: string;
  readonly binding?: ProfileAcquisitionBinding;
  readonly acquisitionId?: string;
  readonly transport: "tls";
  readonly trust: KafkaTlsProfileRecord["trust"] & {
    readonly caPem: string;
  };
}

interface KafkaResolvedPlaintextProfileDraft extends KafkaResolvedProfileDraftBase {
  readonly transport: "plaintext";
}

export type KafkaResolvedProfileDraft =
  KafkaResolvedPlaintextProfileDraft | KafkaResolvedTlsProfileDraft;

export function kafkaProfileDraftConnection(
  draft: KafkaResolvedProfileDraft,
): SecureConnectionInput {
  const base = {
    ...(draft.sasl === undefined ? {} : { sasl: draft.sasl }),
    brokers: draft.brokers,
    name: draft.name,
    ...(draft.oauth === undefined ? {} : { oauth: draft.oauth }),
    ...(draft.resolvedServices === undefined ? {} : { services: draft.resolvedServices }),
  };
  return draft.transport === "plaintext"
    ? { ...base, tls: { enabled: false } }
    : {
        ...base,
        tls: {
          caPem: draft.trust.caPem,
          ...(draft.clientIdentity === undefined
            ? {}
            : { clientIdentity: connectionIdentity(draft.clientIdentity) }),
          enabled: true,
        },
      };
}

export function kafkaProfileRecordFromDraft(
  draft: KafkaResolvedProfileDraft,
  identity: {
    readonly createdAt: string;
    readonly id: string;
    readonly revision: number;
    readonly updatedAt: string;
  },
): KafkaProfileRecord {
  const base = {
    revision: identity.revision,
    ...(draft.sasl === undefined ? {} : { sasl: draft.sasl }),
    brokers: draft.brokers,
    createdAt: identity.createdAt,
    id: identity.id,
    name: draft.name,
    ...(draft.oauth === undefined ? {} : { oauth: draft.oauth }),
    ...(draft.services === undefined ? {} : { services: draft.services }),
    ...(draft.source === undefined ? {} : { source: draft.source }),
    updatedAt: identity.updatedAt,
  };
  if (draft.transport === "plaintext") {
    return { ...base, transport: "plaintext" };
  }
  return {
    ...base,
    ...(draft.clientIdentity === undefined ? {} : { clientIdentity: draft.clientIdentity }),
    ...(draft.apiCaPem === undefined ? {} : { apiCaPem: draft.apiCaPem }),
    ...(draft.binding === undefined ? {} : { binding: draft.binding }),
    transport: "tls",
    trust: {
      kind: draft.trust.kind,
      label: draft.trust.label,
      material: draft.trust.material,
      ...(draft.trust.password === undefined ? {} : { password: draft.trust.password }),
    },
  };
}

export function kafkaProfileSummary(
  record: KafkaProfileRecord,
  activeProfileId?: string,
): ProfileSummary {
  const base = {
    ...(record.revision === undefined ? {} : { revision: record.revision }),
    active: record.id === activeProfileId,
    ...(record.sasl === undefined
      ? {}
      : {
          sasl: {
            mechanism: record.sasl.mechanism,
            username: record.sasl.username,
            passwordPresent: record.sasl.password.length > 0,
          },
        }),
    brokers: [...record.brokers],
    createdAt: record.createdAt,
    id: record.id,
    name: record.name,
    ...(record.services === undefined ? {} : { services: summarizeServices(record.services) }),
    ...(record.source === undefined ? {} : { source: record.source }),
    transport: record.transport ?? "tls",
    updatedAt: record.updatedAt,
  };
  const withOAuth =
    record.oauth === undefined
      ? base
      : {
          ...base,
          oauth: {
            clientId: record.oauth.clientId,
            clientSecretPresent: record.oauth.clientSecret.length > 0,
            scope: record.oauth.scope,
            tokenEndpoint: record.oauth.tokenEndpoint,
          },
        };
  if (record.transport === "plaintext") {
    return { ...withOAuth, transport: "plaintext" };
  }
  return {
    ...withOAuth,
    ...(record.clientIdentity === undefined
      ? {}
      : { clientIdentity: summarizeIdentity(record.clientIdentity) }),
    transport: "tls",
    trust: {
      kind: record.trust.kind,
      label: record.trust.label,
      materialPresent: record.trust.material.length > 0,
      passwordPresent: record.trust.password !== undefined,
    },
  };
}

export function kafkaProfileValidationInput(record: KafkaProfileRecord): ProfileCreateInput {
  const security = securityValidationInput(record);
  const base = {
    ...(security.sasl === undefined ? {} : { sasl: security.sasl }),
    brokers: record.brokers,
    name: record.name,
    ...(security.services === undefined ? {} : { services: security.services }),
    ...(record.source === undefined ? {} : { source: record.source }),
    ...(record.oauth === undefined
      ? {}
      : {
          oauth: {
            clientId: record.oauth.clientId,
            clientSecret:
              record.oauth.clientSecret.length === 0
                ? ({ mode: "clear" } as const)
                : ({ mode: "replace", value: record.oauth.clientSecret } as const),
            scope: record.oauth.scope,
            tokenEndpoint: record.oauth.tokenEndpoint,
          },
        }),
  };
  if (record.transport === "plaintext") {
    return { ...base, transport: "plaintext" };
  }
  return {
    ...base,
    ...(security.clientIdentity === undefined ? {} : { clientIdentity: security.clientIdentity }),
    transport: "tls",
    trust: {
      kind: record.trust.kind,
      label: record.trust.label,
      material:
        record.trust.material.length === 0
          ? { mode: "clear" }
          : { mode: "replace", value: record.trust.material },
      password:
        record.trust.password === undefined
          ? { mode: "clear" }
          : { mode: "replace", value: record.trust.password },
    },
  };
}

export interface KafkaProfileSnapshot {
  readonly profiles: readonly ProfileSummary[];
  readonly store: ProfileStoreCapability;
}

export interface KafkaProfileStore {
  capability(): ProfileStoreCapability;
  commit(records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void>;
  load(signal?: AbortSignal): Promise<readonly KafkaProfileRecord[]>;
}

export interface KafkaProfileTrustDecoderInput {
  readonly kind: ProfileTrustKind;
  readonly material: string;
  readonly password?: string;
}

export interface KafkaProfileTrustDecoderResult {
  readonly evidence?: import("../contracts/remote-trust-types").TrustCertificateEvidence;
  readonly caPem: string;
  readonly kind: ProfileTrustKind;
}

export interface KafkaProfileTrustDecoder {
  decode(
    input: KafkaProfileTrustDecoderInput,
    signal?: AbortSignal,
  ): Promise<KafkaProfileTrustDecoderResult>;
}

export interface KafkaProfileServiceOptions {
  readonly resolveRecipe?: (
    id: string,
    revision: number,
    signal?: AbortSignal,
  ) => Promise<TrustAcquisitionRecipe>;
  readonly createId?: () => string;
  readonly now?: () => Date;
  readonly trustAcquisitions?: KafkaTrustAcquisitionResolver;
}

export interface KafkaProfileIssue {
  readonly field: string;
  readonly message: string;
}

export interface KafkaProfileStructuredError extends Error {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target: string | undefined;
}
