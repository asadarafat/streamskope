import type {
  ClusterServiceAuthenticationMode,
  ProfileOAuthInput,
  ProfileSaslInput,
} from "./profile-types";

/** Host-resolved credentials never appear in profile summaries or persisted metadata. */
export interface ConnectionClientIdentity {
  readonly certificatePem: string;
  readonly privateKeyPem: string;
  readonly passphrase?: string;
}

export type ConnectionSasl = ProfileSaslInput<string>;

export interface ResolvedClusterServiceEndpoint {
  readonly baseUrl: string;
  /** oauth retains the legacy broker token; oauth-client owns its token lifecycle. */
  readonly authentication: ClusterServiceAuthenticationMode;
  readonly basic?: { readonly username: string; readonly password: string };
  readonly bearer?: string;
  readonly oauth?: ProfileOAuthInput<string>;
  /** Omitted inherits broker CA only; an empty object selects system roots. */
  readonly tls?: {
    readonly caPem?: string;
    readonly clientIdentity?: ConnectionClientIdentity;
  };
}

export interface ResolvedClusterServiceEndpoints {
  readonly connect?: ResolvedClusterServiceEndpoint;
  readonly redpandaAdmin?: ResolvedClusterServiceEndpoint;
  readonly schemaRegistry?: ResolvedClusterServiceEndpoint;
}
