import { PROFILE_LIMITS, type ProfileTrustKind } from "./profile-types";
import type { ProfileBindingInput } from "./profile-binding";
import type { TrustRecipeOAuth } from "./trust-recipe-types";
import type { HttpsTrustAuthentication } from "./https-trust-types";

export interface TrustAcquisitionCapabilities {
  readonly sshAgent: "configured" | "unavailable";
  readonly methods?: readonly ("ssh" | "https")[];
}

export interface HttpsTrustMaterialFetchInput {
  readonly editor: TrustAcquisitionEditor;
  readonly profile?: { readonly id: string; readonly revision: number };
  readonly recipe: Extract<ProfileBindingInput, { readonly mode: "replace" }>;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly secretParameters?: Readonly<Record<string, string>>;
  readonly truststorePassword?: string;
  readonly api: {
    readonly host: string;
    readonly authentication: HttpsTrustAuthentication;
    readonly tls:
      | { readonly mode: "system" }
      | { readonly mode: "custom"; readonly caPem: string }
      | { readonly mode: "retain" };
  };
}

export const REMOTE_TRUST_ACQUISITION_LIMITS = {
  acquisitions: 8,
  commandOutputBytes: 16 * 1_024,
  diagnosticBytes: 8_192,
  fingerprintCharacters: 50,
  hostCharacters: 255,
  materialBytes: PROFILE_LIMITS.trustBinaryBytes,
  operationMs: 60_000,
  passwordCharacters: PROFILE_LIMITS.clientSecretCharacters,
  privateKeyCharacters: 65_536,
  portMaximum: 65_535,
  readyMs: 20_000,
  ttlMs: 10 * 60_000,
  usernameCharacters: 256,
} as const;

export type RemoteSshAuthentication =
  | { readonly mode: "password"; readonly password: string }
  | { readonly mode: "private-key"; readonly privateKey: string; readonly passphrase?: string }
  | { readonly mode: "agent" };

export type RemoteSshTargetInput = {
  readonly host: string;
  readonly hostKeyFingerprint: string;
  readonly port: number;
  readonly username: string;
} & (
  | { readonly password: string; readonly authentication?: never }
  | { readonly authentication: RemoteSshAuthentication; readonly password?: never }
);

export interface RemoteSshEndpointInput {
  readonly host: string;
  readonly port: number;
}

export interface RemoteTrustHostKeyDiscoveryInput {
  readonly editor?: TrustAcquisitionEditor;
  readonly target: RemoteSshEndpointInput;
}

export interface RemoteSshHostKeySummary {
  readonly review?: {
    readonly id: string;
    readonly expiresAt: string;
    readonly confirmationRequired: boolean;
  };
  readonly fingerprint: string;
  readonly target: RemoteSshEndpointInput;
}

export interface RemoteTrustHostKeyDiscoveryResult {
  readonly correlationId: string;
  readonly hostKey: RemoteSshHostKeySummary;
}

export interface RemoteTrustPasswordFetchInput {
  readonly target: RemoteSshTargetInput;
}

export interface RemoteTrustMaterialFetchInput {
  readonly identityId?: string;
  readonly acceptIdentity?: boolean;
  readonly editor?: TrustAcquisitionEditor;
  readonly profile?: { readonly id: string; readonly revision: number };
  readonly recipe?: Extract<ProfileBindingInput, { readonly mode: "replace" }>;
  readonly secretParameters?: Readonly<Record<string, string>>;
  readonly truststorePassword?: string;
  readonly acquisitionId?: string;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly target: RemoteSshTargetInput;
}

export interface AcceptedSshIdentity {
  readonly host: string;
  readonly port: number;
  readonly fingerprint: string;
}

export interface RemoteTrustAcquisitionIdentity {
  readonly editorId?: string;
  readonly acquisitionId: string;
}

export interface AcquiredTlsConnectionInput {
  readonly editorId?: string;
  readonly acquisitionId: string;
  readonly enabled: true;
  readonly kind: ProfileTrustKind;
}

export interface RemoteTrustAcquisitionMaterialSummary {
  readonly evidence?: TrustCertificateEvidence;
  readonly expiredCertificates?: boolean;
  readonly notYetValidCertificates?: boolean;
  readonly byteCount: number;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly templateName: string;
}

export const TRUST_CERTIFICATE_EVIDENCE_LIMITS = { entries: 16, nameCharacters: 512 } as const;

export interface TrustCertificateEvidence {
  readonly validity?: { readonly earliestExpiry: string; readonly latestStart: string };
  readonly count: number;
  readonly truncated: boolean;
  readonly certificates: readonly {
    readonly subject: string;
    readonly issuer: string;
    readonly validFrom: string;
    readonly validTo: string;
    readonly fingerprint: string;
    readonly truncated: boolean;
  }[];
}

export interface RemoteTrustAcquisitionPasswordSummary {
  readonly present: boolean;
  readonly templateName: string | null;
}

export interface RemoteTrustAcquisitionSummary {
  readonly createdAt?: string;
  readonly recipe?: {
    readonly id: string;
    readonly revision: number;
    readonly source: "file" | "stdout" | "legacy-tempfile" | "https";
  };
  readonly oauth?: TrustRecipeOAuth;
  readonly editor?: TrustAcquisitionEditor;
  readonly expiresAt: string;
  readonly id: string;
  readonly material: RemoteTrustAcquisitionMaterialSummary | null;
  readonly password: RemoteTrustAcquisitionPasswordSummary;
  readonly target: {
    readonly host: string;
    readonly port: number;
  } & (
    | { readonly hostKeyFingerprint: string; readonly origin?: never }
    | { readonly origin: string; readonly hostKeyFingerprint?: never }
  );
}

export interface TrustAcquisitionEditor {
  readonly id: string;
  readonly generation: number;
}

export interface RemoteTrustAcquisitionResult {
  readonly acquisition: RemoteTrustAcquisitionSummary;
  readonly correlationId: string;
}
