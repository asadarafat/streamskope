import type {
  ClusterServiceEndpointInput,
  ClusterServiceEndpointSummary,
  ClusterServiceEndpointsInput,
  ClusterServiceEndpointsSummary,
  ProfileClientIdentityInput,
  ProfileSummaryClientIdentity,
} from "./profile-types";

/** Reuse host-owned values without requesting or fabricating their secret contents. */
export function retainedClientIdentity(
  summary: ProfileSummaryClientIdentity,
): ProfileClientIdentityInput {
  return {
    certificatePem: { mode: summary.certificatePresent ? "retain" : "clear" },
    privateKeyPem: { mode: summary.privateKeyPresent ? "retain" : "clear" },
    passphrase: { mode: summary.passphrasePresent ? "retain" : "clear" },
  };
}

function retainedEndpoint(summary: ClusterServiceEndpointSummary): ClusterServiceEndpointInput {
  return {
    baseUrl: summary.baseUrl,
    authentication: summary.authentication,
    ...(summary.basic === undefined
      ? {}
      : {
          basic: {
            username: summary.basic.username,
            password: {
              mode: summary.basic.passwordPresent ? ("retain" as const) : ("clear" as const),
            },
          },
        }),
    ...(summary.bearerPresent === undefined
      ? {}
      : { bearer: { mode: summary.bearerPresent ? ("retain" as const) : ("clear" as const) } }),
    ...(summary.oauth === undefined
      ? {}
      : {
          oauth: {
            clientId: summary.oauth.clientId,
            tokenEndpoint: summary.oauth.tokenEndpoint,
            scope: summary.oauth.scope,
            clientSecret: {
              mode: summary.oauth.clientSecretPresent ? ("retain" as const) : ("clear" as const),
            },
          },
        }),
    ...(summary.trust === undefined
      ? {}
      : {
          trust:
            summary.trust.mode === "custom"
              ? {
                  mode: "custom" as const,
                  kind: summary.trust.kind,
                  label: summary.trust.label,
                  material: {
                    mode: summary.trust.materialPresent ? ("retain" as const) : ("clear" as const),
                  },
                  password: {
                    mode: summary.trust.passwordPresent ? ("retain" as const) : ("clear" as const),
                  },
                }
              : summary.trust,
        }),
    ...(summary.clientIdentity === undefined
      ? {}
      : { clientIdentity: retainedClientIdentity(summary.clientIdentity) }),
  };
}

export function retainedServiceEndpoints(
  summary: ClusterServiceEndpointsSummary,
): ClusterServiceEndpointsInput {
  return {
    ...(summary.connect === undefined ? {} : { connect: retainedEndpoint(summary.connect) }),
    ...(summary.schemaRegistry === undefined
      ? {}
      : { schemaRegistry: retainedEndpoint(summary.schemaRegistry) }),
    ...(summary.redpandaAdmin === undefined
      ? {}
      : { redpandaAdmin: retainedEndpoint(summary.redpandaAdmin) }),
  };
}
