import {
  CLUSTER_SERVICE_AUTHENTICATION_MODES,
  KAFKA_SASL_MECHANISMS,
  PROFILE_LIMITS,
  PROFILE_TRUST_KINDS,
  type ClusterServiceEndpointsInput,
  type ClusterServiceEndpointInput,
  type ClusterServiceEndpointsSummary,
  type ClusterServiceEndpointSummary,
  type ProfileSaslInput,
  type ProfileClientIdentityInput,
  type ProfileOAuthInput,
  type ProfileSummaryClientIdentity,
  type ProfileSummaryOAuth,
  type ProfileSummarySasl,
} from "./profile-types";
import type {
  ConnectionClientIdentity,
  ResolvedClusterServiceEndpoints,
  ResolvedClusterServiceEndpoint,
} from "./connection-security";
import {
  boundedText,
  declaredValue,
  exactKeys,
  record,
  text,
  truth,
} from "./validation-primitives";
import { HostContractValidationError } from "./validation-error";

export type CredentialParser<T> = (value: unknown, path: string, maximum: number) => T;

export function parseSecurityOAuth<T>(
  value: unknown,
  path: string,
  secret: CredentialParser<T>,
): ProfileOAuthInput<T> {
  const v = record(value, path);
  exactKeys(v, ["clientId", "clientSecret", "scope", "tokenEndpoint"], path);
  return {
    clientId: text(v.clientId, `${path}.clientId`, PROFILE_LIMITS.clientIdCharacters),
    clientSecret: secret(
      v.clientSecret,
      `${path}.clientSecret`,
      PROFILE_LIMITS.clientSecretCharacters,
    ),
    scope: boundedText(v.scope, `${path}.scope`, PROFILE_LIMITS.scopeCharacters),
    tokenEndpoint: text(
      v.tokenEndpoint,
      `${path}.tokenEndpoint`,
      PROFILE_LIMITS.tokenEndpointCharacters,
    ),
  };
}

export function parseProfileSasl<T>(
  value: unknown,
  path: string,
  secret: CredentialParser<T>,
): ProfileSaslInput<T> {
  const v = record(value, path);
  exactKeys(v, ["mechanism", "username", "password"], path);
  return {
    mechanism: declaredValue(v.mechanism, KAFKA_SASL_MECHANISMS, `${path}.mechanism`),
    username: text(v.username, `${path}.username`, PROFILE_LIMITS.clientIdCharacters),
    password: secret(v.password, `${path}.password`, PROFILE_LIMITS.clientSecretCharacters),
  };
}

export function parseProfileIdentity<T>(
  value: unknown,
  path: string,
  secret: CredentialParser<T>,
): ProfileClientIdentityInput<T> {
  const v = record(value, path);
  exactKeys(v, ["certificatePem", "privateKeyPem", "passphrase"], path);
  return {
    certificatePem: secret(
      v.certificatePem,
      `${path}.certificatePem`,
      PROFILE_LIMITS.trustBinaryBytes,
    ),
    privateKeyPem: secret(
      v.privateKeyPem,
      `${path}.privateKeyPem`,
      PROFILE_LIMITS.trustBinaryBytes,
    ),
    passphrase: secret(v.passphrase, `${path}.passphrase`, PROFILE_LIMITS.clientSecretCharacters),
  };
}

function assertAuthenticationFields(v: Record<string, unknown>, path: string): void {
  for (const [mode, key] of [
    ["basic", "basic"],
    ["bearer", "bearer"],
    ["oauth-client", "oauth"],
  ] as const) {
    if ((v.authentication === mode) !== Object.hasOwn(v, key))
      throw new HostContractValidationError(
        `${path}.${key}`,
        `is required only for ${mode} authentication`,
      );
  }
}

function endpoints<T>(
  value: unknown,
  path: string,
  parse: (value: unknown, path: string) => T,
): { readonly connect?: T; readonly schemaRegistry?: T; readonly redpandaAdmin?: T } {
  const v = record(value, path);
  exactKeys(v, ["connect", "schemaRegistry", "redpandaAdmin"], path);
  return Object.fromEntries(
    Object.entries(v).map(([key, endpoint]) => [key, parse(endpoint, `${path}.${key}`)]),
  );
}

export function parseProfileServices<T>(
  value: unknown,
  path: string,
  secret: CredentialParser<T>,
): ClusterServiceEndpointsInput<T> {
  return endpoints(value, path, (value, path): ClusterServiceEndpointInput<T> => {
    const v = record(value, path);
    exactKeys(
      v,
      ["baseUrl", "authentication", "basic", "bearer", "oauth", "trust", "clientIdentity"],
      path,
    );
    assertAuthenticationFields(v, path);
    const base = {
      baseUrl: text(v.baseUrl, `${path}.baseUrl`, PROFILE_LIMITS.tokenEndpointCharacters),
      authentication: declaredValue(
        v.authentication,
        CLUSTER_SERVICE_AUTHENTICATION_MODES,
        `${path}.authentication`,
      ),
    };
    let basic: ClusterServiceEndpointInput<T>["basic"];
    if (v.basic !== undefined) {
      const b = record(v.basic, `${path}.basic`);
      exactKeys(b, ["username", "password"], `${path}.basic`);
      basic = {
        username: text(b.username, `${path}.basic.username`, PROFILE_LIMITS.clientIdCharacters),
        password: secret(
          b.password,
          `${path}.basic.password`,
          PROFILE_LIMITS.clientSecretCharacters,
        ),
      };
    }
    let trust: ClusterServiceEndpointInput<T>["trust"];
    if (v.trust !== undefined) {
      const t = record(v.trust, `${path}.trust`);
      const mode = declaredValue(t.mode, ["broker", "system", "custom"], `${path}.trust.mode`);
      exactKeys(
        t,
        mode === "custom" ? ["mode", "kind", "label", "material", "password"] : ["mode"],
        `${path}.trust`,
      );
      trust =
        mode === "custom"
          ? {
              mode,
              kind: declaredValue(t.kind, PROFILE_TRUST_KINDS, `${path}.trust.kind`),
              label: text(t.label, `${path}.trust.label`, PROFILE_LIMITS.trustLabelCharacters),
              material: secret(
                t.material,
                `${path}.trust.material`,
                PROFILE_LIMITS.trustEncodedCharacters,
              ),
              password: secret(
                t.password,
                `${path}.trust.password`,
                PROFILE_LIMITS.clientSecretCharacters,
              ),
            }
          : { mode };
    }
    return {
      ...base,
      ...(basic === undefined ? {} : { basic }),
      ...(v.bearer === undefined
        ? {}
        : { bearer: secret(v.bearer, `${path}.bearer`, PROFILE_LIMITS.clientSecretCharacters) }),
      ...(v.oauth === undefined
        ? {}
        : { oauth: parseSecurityOAuth(v.oauth, `${path}.oauth`, secret) }),
      ...(trust === undefined ? {} : { trust }),
      ...(v.clientIdentity === undefined
        ? {}
        : {
            clientIdentity: parseProfileIdentity(
              v.clientIdentity,
              `${path}.clientIdentity`,
              secret,
            ),
          }),
    };
  });
}

export function parseSummaryIdentity(value: unknown, path: string): ProfileSummaryClientIdentity {
  const v = record(value, path);
  exactKeys(v, ["certificatePresent", "privateKeyPresent", "passphrasePresent"], path);
  return {
    certificatePresent: truth(v.certificatePresent, `${path}.certificatePresent`),
    privateKeyPresent: truth(v.privateKeyPresent, `${path}.privateKeyPresent`),
    passphrasePresent: truth(v.passphrasePresent, `${path}.passphrasePresent`),
  };
}

export function parseSummarySasl(value: unknown, path: string): ProfileSummarySasl {
  const v = record(value, path);
  exactKeys(v, ["mechanism", "username", "passwordPresent"], path);
  return {
    mechanism: declaredValue(v.mechanism, KAFKA_SASL_MECHANISMS, `${path}.mechanism`),
    username: text(v.username, `${path}.username`, PROFILE_LIMITS.clientIdCharacters),
    passwordPresent: truth(v.passwordPresent, `${path}.passwordPresent`),
  };
}

export function parseServiceSummaries(
  value: unknown,
  path: string,
  oauth: (value: unknown, path: string) => ProfileSummaryOAuth,
): ClusterServiceEndpointsSummary {
  return endpoints(value, path, (value, path): ClusterServiceEndpointSummary => {
    const v = record(value, path);
    exactKeys(
      v,
      ["baseUrl", "authentication", "basic", "bearerPresent", "oauth", "trust", "clientIdentity"],
      path,
    );
    const authentication = declaredValue(
      v.authentication,
      CLUSTER_SERVICE_AUTHENTICATION_MODES,
      `${path}.authentication`,
    );
    for (const [mode, key] of [
      ["basic", "basic"],
      ["bearer", "bearerPresent"],
      ["oauth-client", "oauth"],
    ] as const) {
      if ((authentication === mode) !== Object.hasOwn(v, key))
        throw new HostContractValidationError(path, "inconsistent authentication summary");
    }
    let basic: ClusterServiceEndpointSummary["basic"];
    if (v.basic !== undefined) {
      const b = record(v.basic, `${path}.basic`);
      exactKeys(b, ["username", "passwordPresent"], `${path}.basic`);
      basic = {
        username: text(b.username, `${path}.basic.username`, PROFILE_LIMITS.clientIdCharacters),
        passwordPresent: truth(b.passwordPresent, `${path}.basic.passwordPresent`),
      };
    }
    let trust: ClusterServiceEndpointSummary["trust"];
    if (v.trust !== undefined) {
      const t = record(v.trust, `${path}.trust`);
      const mode = declaredValue(t.mode, ["broker", "system", "custom"], `${path}.trust.mode`);
      exactKeys(
        t,
        mode === "custom"
          ? ["mode", "kind", "label", "materialPresent", "passwordPresent"]
          : ["mode"],
        `${path}.trust`,
      );
      trust =
        mode === "custom"
          ? {
              mode,
              kind: declaredValue(t.kind, PROFILE_TRUST_KINDS, `${path}.trust.kind`),
              label: text(t.label, `${path}.trust.label`, PROFILE_LIMITS.trustLabelCharacters),
              materialPresent: truth(t.materialPresent, `${path}.trust.materialPresent`),
              passwordPresent: truth(t.passwordPresent, `${path}.trust.passwordPresent`),
            }
          : { mode };
    }
    return {
      authentication,
      baseUrl: text(v.baseUrl, `${path}.baseUrl`, PROFILE_LIMITS.tokenEndpointCharacters),
      ...(basic === undefined ? {} : { basic }),
      ...(v.bearerPresent === undefined
        ? {}
        : { bearerPresent: truth(v.bearerPresent, `${path}.bearerPresent`) }),
      ...(v.oauth === undefined ? {} : { oauth: oauth(v.oauth, `${path}.oauth`) }),
      ...(trust === undefined ? {} : { trust }),
      ...(v.clientIdentity === undefined
        ? {}
        : { clientIdentity: parseSummaryIdentity(v.clientIdentity, `${path}.clientIdentity`) }),
    };
  });
}

export function parseConnectionIdentity(value: unknown, path: string): ConnectionClientIdentity {
  const v = record(value, path);
  exactKeys(v, ["certificatePem", "privateKeyPem", "passphrase"], path);
  return {
    certificatePem: text(
      v.certificatePem,
      `${path}.certificatePem`,
      PROFILE_LIMITS.trustBinaryBytes,
    ),
    privateKeyPem: text(v.privateKeyPem, `${path}.privateKeyPem`, PROFILE_LIMITS.trustBinaryBytes),
    ...(v.passphrase === undefined
      ? {}
      : {
          passphrase: text(
            v.passphrase,
            `${path}.passphrase`,
            PROFILE_LIMITS.clientSecretCharacters,
          ),
        }),
  };
}

export function parseResolvedServices(
  value: unknown,
  path: string,
): ResolvedClusterServiceEndpoints {
  return endpoints(value, path, (value, path): ResolvedClusterServiceEndpoint => {
    const v = record(value, path);
    exactKeys(v, ["baseUrl", "authentication", "basic", "bearer", "oauth", "tls"], path);
    assertAuthenticationFields(v, path);
    const authentication = declaredValue(
      v.authentication,
      CLUSTER_SERVICE_AUTHENTICATION_MODES,
      `${path}.authentication`,
    );
    let basic: ResolvedClusterServiceEndpoint["basic"];
    if (v.basic !== undefined) {
      const b = record(v.basic, `${path}.basic`);
      exactKeys(b, ["username", "password"], `${path}.basic`);
      basic = {
        username: text(b.username, `${path}.basic.username`, PROFILE_LIMITS.clientIdCharacters),
        password: text(b.password, `${path}.basic.password`, PROFILE_LIMITS.clientSecretCharacters),
      };
    }
    let tls: ResolvedClusterServiceEndpoint["tls"];
    if (v.tls !== undefined) {
      const t = record(v.tls, `${path}.tls`);
      exactKeys(t, ["caPem", "clientIdentity"], `${path}.tls`);
      tls = {
        ...(t.caPem === undefined
          ? {}
          : { caPem: text(t.caPem, `${path}.tls.caPem`, PROFILE_LIMITS.trustBinaryBytes) }),
        ...(t.clientIdentity === undefined
          ? {}
          : {
              clientIdentity: parseConnectionIdentity(
                t.clientIdentity,
                `${path}.tls.clientIdentity`,
              ),
            }),
      };
    }
    return {
      authentication,
      baseUrl: text(v.baseUrl, `${path}.baseUrl`, PROFILE_LIMITS.tokenEndpointCharacters),
      ...(basic === undefined ? {} : { basic }),
      ...(v.bearer === undefined
        ? {}
        : { bearer: text(v.bearer, `${path}.bearer`, PROFILE_LIMITS.clientSecretCharacters) }),
      ...(v.oauth === undefined
        ? {}
        : { oauth: parseSecurityOAuth(v.oauth, `${path}.oauth`, text) }),
      ...(tls === undefined ? {} : { tls }),
    };
  });
}
