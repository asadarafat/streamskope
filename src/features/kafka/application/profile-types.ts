import type {
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

import type { KafkaTrustAcquisitionResolver } from "./trust-acquisition-types";

interface KafkaProfileRecordBase {
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
  readonly services?: ClusterServiceEndpointsInput;
  readonly source?: ProfileSource;
  readonly updatedAt: string;
}

export interface KafkaTlsProfileRecord extends KafkaProfileRecordBase {
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
  readonly apiCaPem?: never;
  readonly binding?: never;
  readonly transport: Extract<KafkaProfileTransport, "plaintext">;
  readonly trust?: never;
}

export type KafkaProfileRecord = KafkaPlaintextProfileRecord | KafkaTlsProfileRecord;

interface KafkaResolvedProfileDraftBase {
  readonly brokers: readonly string[];
  readonly lifetimeSignal?: AbortSignal;
  readonly name: string;
  readonly oauth?: NonNullable<SecureConnectionInput["oauth"]>;
  readonly services?: ClusterServiceEndpointsInput;
  readonly source?: ProfileSource;
}

interface KafkaResolvedTlsProfileDraft extends KafkaResolvedProfileDraftBase {
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
    brokers: draft.brokers,
    name: draft.name,
    ...(draft.oauth === undefined ? {} : { oauth: draft.oauth }),
    ...(draft.services === undefined ? {} : { services: draft.services }),
  };
  return draft.transport === "plaintext"
    ? { ...base, tls: { enabled: false } }
    : {
        ...base,
        tls: {
          caPem: draft.trust.caPem,
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
    brokers: [...record.brokers],
    createdAt: record.createdAt,
    id: record.id,
    name: record.name,
    ...(record.services === undefined ? {} : { services: record.services }),
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
  const base = {
    brokers: record.brokers,
    name: record.name,
    ...(record.services === undefined ? {} : { services: record.services }),
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
