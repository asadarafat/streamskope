import type {
  ProfileTrustKind,
  RemoteSshEndpointInput,
  RemoteSshHostKeySummary,
  RemoteSshTargetInput,
  RemoteTrustAcquisitionSummary,
  RemoteTrustMaterialFetchInput,
  RemoteTrustHostKeyDiscoveryInput,
  RemoteTrustPasswordFetchInput,
  ProfileAcquisitionBinding,
  ProfileBindingInput,
} from "../contracts";

export interface KafkaRemoteHostKeyRequest {
  readonly target: RemoteSshEndpointInput;
}

export interface KafkaRemotePasswordRequest {
  readonly command: string;
  readonly target: RemoteSshTargetInput;
}

export type KafkaRemoteMaterialRequest = {
  readonly maximumBytes: number;
  readonly target: RemoteSshTargetInput;
} & (
  | { readonly source?: "legacy-tempfile"; readonly command: string; readonly remotePath: string }
  | { readonly source: "file"; readonly remotePath: string; readonly command?: never }
  | { readonly source: "command"; readonly command: string; readonly remotePath?: never }
);

export interface KafkaRemoteTrustPort {
  agentStatus?(): "configured" | "unavailable";
  discoverHostKey(request: KafkaRemoteHostKeyRequest, signal?: AbortSignal): Promise<string>;
  fetchMaterial(request: KafkaRemoteMaterialRequest, signal?: AbortSignal): Promise<Uint8Array>;
  fetchPassword(request: KafkaRemotePasswordRequest, signal?: AbortSignal): Promise<string>;
}

export interface KafkaTrustAcquisitionServiceOptions {
  readonly resolveProfileApiCa?: (
    profileId: string,
    revision: number,
    signal?: AbortSignal,
  ) => Promise<string>;
  readonly https?: import("./https-trust-port").HttpsTrustAcquisitionPort;
  readonly resolveProfileBinding?: (
    profileId: string,
    revision: number,
    reference: Extract<ProfileBindingInput, { readonly mode: "replace" }>,
    signal?: AbortSignal,
  ) => Promise<ProfileAcquisitionBinding>;
  readonly createId?: () => string;
  readonly createRemotePath?: () => string;
  readonly now?: () => Date;
}

export interface KafkaResolvedTrustAcquisition {
  readonly access?: import("../contracts/remote-ssh-access").RemoteSshAccess;
  readonly identity?: import("../contracts/remote-trust-types").AcceptedSshIdentity;
  readonly lifetimeSignal?: AbortSignal;
  readonly caPem: string;
  readonly id: string;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly material: string;
  readonly password?: string;
}

export interface KafkaTrustAcquisitionResolver {
  consume(acquisitionId: string): void;
  resolve(
    acquisitionId: string,
    expectedKind: ProfileTrustKind,
    editorId?: string,
  ): KafkaResolvedTrustAcquisition;
}

export interface KafkaTrustAcquisitionServicePort extends KafkaTrustAcquisitionResolver {
  openEditor(
    identity?: import("../contracts/remote-trust-types").AcceptedSshIdentity,
  ): import("../contracts/remote-trust-types").TrustAcquisitionEditor;
  closeEditor(editorId: string): void;
  advanceEditor(editorId: string, generation: number): void;
  apply(acquisitionId: string, editorId: string): void;
  capabilities(): import("../contracts/remote-trust-types").TrustAcquisitionCapabilities;
  cancel(requestId: string, editorId?: string): void;
  clear(): void;
  discoverHostKey(
    input: RemoteTrustHostKeyDiscoveryInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteSshHostKeySummary>;
  discard(acquisitionId: string, editorId?: string): void;
  fetchHttpsMaterial(
    input: import("../contracts/remote-trust-types").HttpsTrustMaterialFetchInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteTrustAcquisitionSummary>;
  fetchMaterial(
    input: RemoteTrustMaterialFetchInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteTrustAcquisitionSummary>;
  fetchPassword(
    input: RemoteTrustPasswordFetchInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteTrustAcquisitionSummary>;
}

export type KafkaTrustAcquisitionSnapshot = RemoteTrustAcquisitionSummary;
