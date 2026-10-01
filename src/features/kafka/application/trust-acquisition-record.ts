import type { ProfileTrustKind, RemoteTrustAcquisitionSummary } from "../contracts";
import type { TrustAcquisitionEditor } from "../contracts/remote-trust-types";

export interface TrustAcquisitionRecord {
  readonly access?: import("../contracts/remote-ssh-access").RemoteSshAccess;
  readonly recipe?: RemoteTrustAcquisitionSummary["recipe"];
  readonly oauth?: RemoteTrustAcquisitionSummary["oauth"];
  readonly editor?: TrustAcquisitionEditor;
  readonly applied?: boolean;
  readonly createdAtMs: number;
  readonly expiresAtMs: number;
  readonly id: string;
  readonly target: RemoteTrustAcquisitionSummary["target"];
  readonly password?: string;
  readonly passwordTemplateName?: string;
  readonly material?: {
    readonly evidence?: import("../contracts/remote-trust-types").TrustCertificateEvidence;
    readonly byteCount: number;
    readonly caPem: string;
    readonly encoded: string;
    readonly kind: ProfileTrustKind;
    readonly label: string;
    readonly templateName: string;
  };
}
