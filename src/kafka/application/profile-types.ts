import type {
  ClusterServiceEndpointsInput,
  HostErrorCode,
  HostErrorStage,
  ProfileStoreCapability,
  ProfileSummary,
  ProfileTrustKind,
  ProfileAcquisitionBinding,
  TrustAcquisitionRecipe,
} from "../contracts";

import type { KafkaTrustAcquisitionResolver } from "./trust-acquisition-types";

export interface KafkaProfileRecord {
  readonly apiCaPem?: string;
  readonly binding?: ProfileAcquisitionBinding;
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
  readonly trust: {
    readonly kind: ProfileTrustKind;
    readonly label: string;
    readonly material: string;
    readonly password?: string;
  };
  readonly updatedAt: string;
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
