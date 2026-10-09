import {
  KAFKA_SASL_MECHANISMS,
  type ClusterServiceEndpointsInput,
  type ClusterServiceEndpointInput,
  type ClusterServiceEndpointsSummary,
  type ClusterServiceEndpointSummary,
  type ProfileClientIdentityInput,
  type ProfileCreateInput,
  type ProfileUpdateInput,
  type ProfileSaslInput,
  type ProfileSummaryClientIdentity,
  type ProfileOAuthInput,
  type ConnectionClientIdentity,
  type ProtectedValueUpdateInput,
  type ProtectedValueCreateInput,
  type ResolvedClusterServiceEndpoints,
  type ResolvedClusterServiceEndpoint,
} from "../contracts";
import {
  parseProfileServices,
  parseProfileSasl,
  parseProfileIdentity,
} from "../contracts/profile-security-validation";

import { KafkaProfileValidationError } from "./profile-errors";
import type { KafkaProfileTrustDecoder } from "./profile-types";

export interface StoredProfileSecurity {
  readonly sasl?: ProfileSaslInput<string>;
  readonly services?: ClusterServiceEndpointsInput<string>;
  readonly clientIdentity?: ProfileClientIdentityInput<string>;
}

function invalid(field: string, message: string): never {
  throw new KafkaProfileValidationError([{ field, message }]);
}

/** All credential kinds use the same retain/replace/clear semantics. */
export function resolveProtectedCredential(
  input: ProtectedValueUpdateInput,
  existing: string | undefined,
  field: string,
  required = true,
): string {
  const value =
    input.mode === "replace" ? input.value : input.mode === "retain" ? existing : undefined;
  if (required && (value === undefined || value.length === 0))
    invalid(field, "Supply a value or retain an existing protected value.");
  return value ?? "";
}

function protectedInput(value: unknown, field: string, maximum: number): ProtectedValueUpdateInput {
  if (value === null || typeof value !== "object" || !("mode" in value))
    invalid(field, "Use a protected credential value.");
  const input = value as Record<string, unknown>;
  if (input.mode === "clear" || input.mode === "retain") {
    if (Object.keys(input).length !== 1) invalid(field, "Invalid protected credential fields.");
    return { mode: input.mode };
  }
  if (
    input.mode !== "replace" ||
    typeof input.value !== "string" ||
    input.value.length === 0 ||
    input.value.length > maximum ||
    Object.keys(input).some((key) => key !== "mode" && key !== "value")
  )
    invalid(field, "Supply a non-empty value within the supported size.");
  return { mode: "replace", value: input.value };
}

function checkUrl(value: string, field: string): void {
  try {
    const url = new URL(value);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      !url.hostname ||
      url.username ||
      url.password ||
      url.hash ||
      url.search
    )
      throw new Error();
  } catch {
    invalid(field, "Enter an HTTP or HTTPS URL without credentials, query or fragment.");
  }
}

function identity(
  input: ProfileClientIdentityInput,
  previous: ProfileClientIdentityInput<string> | undefined,
  field: string,
): ProfileClientIdentityInput<string> {
  return {
    certificatePem: resolveProtectedCredential(
      input.certificatePem,
      previous?.certificatePem,
      `${field}.certificatePem`,
    ),
    privateKeyPem: resolveProtectedCredential(
      input.privateKeyPem,
      previous?.privateKeyPem,
      `${field}.privateKeyPem`,
    ),
    passphrase: resolveProtectedCredential(
      input.passphrase,
      previous?.passphrase,
      `${field}.passphrase`,
      false,
    ),
  };
}

function oauth(
  input: ProfileOAuthInput<ProtectedValueUpdateInput>,
  previous: ProfileOAuthInput<string> | undefined,
  field: string,
): ProfileOAuthInput<string> {
  checkUrl(input.tokenEndpoint, `${field}.tokenEndpoint`);
  if (!input.clientId.trim()) invalid(`${field}.clientId`, "Enter the OAuth client ID.");
  return {
    clientId: input.clientId.trim(),
    clientSecret: resolveProtectedCredential(
      input.clientSecret,
      previous?.clientSecret,
      `${field}.clientSecret`,
    ),
    scope: input.scope.trim(),
    tokenEndpoint: input.tokenEndpoint.trim(),
  };
}

function bearerCredential(
  input: ProtectedValueUpdateInput,
  previous: string | undefined,
  field: string,
): string {
  const value = resolveProtectedCredential(input, previous, field);
  // eslint-disable-next-line no-control-regex -- Reject control bytes in an HTTP authorization credential.
  if (/[\u0000-\u0020\u007f]/u.test(value))
    invalid(field, "Bearer tokens cannot contain whitespace or control characters.");
  return value;
}

function service(
  input: ClusterServiceEndpointInput,
  previous: ClusterServiceEndpointInput<string> | undefined,
  field: string,
): ClusterServiceEndpointInput<string> {
  checkUrl(input.baseUrl, `${field}.baseUrl`);
  const url = new URL(input.baseUrl.trim());
  if (input.clientIdentity !== undefined && url.protocol !== "https:")
    invalid(`${field}.clientIdentity`, "A client certificate requires an HTTPS service URL.");
  // eslint-disable-next-line no-control-regex -- Reject controls and the Basic username separator.
  if (input.basic !== undefined && /[:\u0000-\u001f\u007f]/u.test(input.basic.username))
    invalid(
      `${field}.basic.username`,
      "HTTP Basic usernames cannot contain a colon or control characters.",
    );
  const baseUrl = url.toString().replace(/\/+$/u, "");
  let trust: ClusterServiceEndpointInput<string>["trust"];
  if (input.trust?.mode === "custom") {
    const prior = previous?.trust?.mode === "custom" ? previous.trust : undefined;
    if (
      prior !== undefined &&
      prior.kind !== input.trust.kind &&
      input.trust.material.mode === "retain"
    )
      invalid(`${field}.trust.material`, "Changing trust format requires new trust material.");
    trust = {
      mode: "custom",
      kind: input.trust.kind,
      label: input.trust.label.replaceAll("\\", "/").split("/").at(-1)!.trim(),
      material: resolveProtectedCredential(
        input.trust.material,
        prior?.material,
        `${field}.trust.material`,
      ),
      password: resolveProtectedCredential(
        input.trust.password,
        prior?.password,
        `${field}.trust.password`,
        input.trust.kind !== "pem",
      ),
    };
    if (trust.kind === "pem" && trust.password !== "")
      invalid(`${field}.trust.password`, "PEM trust does not use a password.");
  } else trust = input.trust;
  return {
    baseUrl,
    authentication: input.authentication,
    ...(input.basic === undefined
      ? {}
      : {
          basic: {
            username: input.basic.username,
            password: resolveProtectedCredential(
              input.basic.password,
              previous?.basic?.password,
              `${field}.basic.password`,
            ),
          },
        }),
    ...(input.bearer === undefined
      ? {}
      : { bearer: bearerCredential(input.bearer, previous?.bearer, `${field}.bearer`) }),
    ...(input.oauth === undefined
      ? {}
      : { oauth: oauth(input.oauth, previous?.oauth, `${field}.oauth`) }),
    ...(trust === undefined ? {} : { trust }),
    ...(input.clientIdentity === undefined
      ? {}
      : {
          clientIdentity: identity(
            input.clientIdentity,
            previous?.clientIdentity,
            `${field}.clientIdentity`,
          ),
        }),
  };
}

export function resolveProfileSecurity(
  input: ProfileCreateInput | ProfileUpdateInput,
  previous: StoredProfileSecurity | undefined,
): StoredProfileSecurity {
  if (input.sasl !== undefined && input.oauth !== undefined)
    invalid("sasl", "Choose one broker authentication method.");
  if (input.transport === "plaintext" && input.clientIdentity !== undefined)
    invalid("clientIdentity", "A client certificate requires Kafka TLS.");
  let sasl: StoredProfileSecurity["sasl"];
  let services: StoredProfileSecurity["services"];
  try {
    if (input.sasl !== undefined) {
      const parsed = parseProfileSasl(input.sasl, "sasl", protectedInput);
      if (
        !KAFKA_SASL_MECHANISMS.includes(parsed.mechanism) ||
        !parsed.username.trim() ||
        // eslint-disable-next-line no-control-regex -- NUL cannot delimit SASL username fields.
        /[\u0000\r\n]/u.test(parsed.username)
      )
        invalid("sasl.username", "Enter a valid SASL username.");
      sasl = {
        mechanism: parsed.mechanism,
        username: parsed.username,
        password: resolveProtectedCredential(
          parsed.password,
          previous?.sasl?.password,
          "sasl.password",
        ),
      };
    }
    if (input.services !== undefined) {
      const parsed = parseProfileServices(input.services, "services", protectedInput);
      services = Object.fromEntries(
        Object.entries<ClusterServiceEndpointInput>({ ...parsed }).map(
          ([key, value]): [string, ClusterServiceEndpointInput<string>] => [
            key,
            service(
              value,
              previous?.services?.[key as keyof ClusterServiceEndpointsInput],
              `services.${key}`,
            ),
          ],
        ),
      );
    }
    return {
      ...(sasl === undefined ? {} : { sasl }),
      ...(services === undefined ? {} : { services }),
      ...(input.clientIdentity === undefined
        ? {}
        : {
            clientIdentity: identity(
              parseProfileIdentity(input.clientIdentity, "clientIdentity", protectedInput),
              previous?.clientIdentity,
              "clientIdentity",
            ),
          }),
    };
  } catch (error) {
    if (error instanceof KafkaProfileValidationError) throw error;
    return invalid("authentication", "Invalid authentication, trust or client identity settings.");
  }
}

export function connectionIdentity(
  value: ProfileClientIdentityInput<string>,
): ConnectionClientIdentity {
  return {
    certificatePem: value.certificatePem,
    privateKeyPem: value.privateKeyPem,
    ...(value.passphrase.length === 0 ? {} : { passphrase: value.passphrase }),
  };
}

export async function resolveServiceConnections(
  services: ClusterServiceEndpointsInput<string> | undefined,
  decoder: KafkaProfileTrustDecoder,
  signal?: AbortSignal,
  brokerCaPem?: string,
): Promise<ResolvedClusterServiceEndpoints | undefined> {
  if (services === undefined) return undefined;
  const entries = await Promise.all(
    Object.entries<ClusterServiceEndpointInput<string>>({ ...services }).map(
      async ([key, service]): Promise<[string, ResolvedClusterServiceEndpoint]> => {
        const t = service.trust;
        const caPem =
          t?.mode === "custom"
            ? (
                await decoder.decode(
                  {
                    kind: t.kind,
                    material: t.material,
                    ...(t.password ? { password: t.password } : {}),
                  },
                  signal,
                )
              ).caPem
            : t?.mode === "system"
              ? undefined
              : brokerCaPem;
        signal?.throwIfAborted();
        const explicitTls =
          t?.mode === "system" || t?.mode === "custom" || service.clientIdentity !== undefined;
        return [
          key,
          {
            baseUrl: service.baseUrl,
            authentication: service.authentication,
            ...(service.basic === undefined ? {} : { basic: { ...service.basic } }),
            ...(service.bearer === undefined ? {} : { bearer: service.bearer }),
            ...(service.oauth === undefined ? {} : { oauth: { ...service.oauth } }),
            ...(explicitTls
              ? {
                  tls: {
                    ...(caPem === undefined ? {} : { caPem }),
                    ...(service.clientIdentity === undefined
                      ? {}
                      : { clientIdentity: connectionIdentity(service.clientIdentity) }),
                  },
                }
              : {}),
          },
        ];
      },
    ),
  );
  signal?.throwIfAborted();
  return Object.fromEntries(entries);
}

export function summarizeIdentity(
  value: ProfileClientIdentityInput<string>,
): ProfileSummaryClientIdentity {
  return {
    certificatePresent: value.certificatePem.length > 0,
    privateKeyPresent: value.privateKeyPem.length > 0,
    passphrasePresent: value.passphrase.length > 0,
  };
}

export function summarizeServices(
  services: ClusterServiceEndpointsInput<string>,
): ClusterServiceEndpointsSummary {
  return Object.fromEntries(
    Object.entries<ClusterServiceEndpointInput<string>>({ ...services }).map(
      ([key, value]): [string, ClusterServiceEndpointSummary] => [
        key,
        {
          authentication: value.authentication,
          baseUrl: value.baseUrl,
          ...(value.basic === undefined
            ? {}
            : {
                basic: {
                  username: value.basic.username,
                  passwordPresent: value.basic.password.length > 0,
                },
              }),
          ...(value.bearer === undefined ? {} : { bearerPresent: value.bearer.length > 0 }),
          ...(value.oauth === undefined
            ? {}
            : {
                oauth: {
                  clientId: value.oauth.clientId,
                  clientSecretPresent: value.oauth.clientSecret.length > 0,
                  scope: value.oauth.scope,
                  tokenEndpoint: value.oauth.tokenEndpoint,
                },
              }),
          ...(value.trust === undefined
            ? {}
            : {
                trust:
                  value.trust.mode === "custom"
                    ? {
                        mode: "custom",
                        kind: value.trust.kind,
                        label: value.trust.label,
                        materialPresent: value.trust.material.length > 0,
                        passwordPresent: value.trust.password.length > 0,
                      }
                    : { ...value.trust },
              }),
          ...(value.clientIdentity === undefined
            ? {}
            : { clientIdentity: summarizeIdentity(value.clientIdentity) }),
        },
      ],
    ),
  );
}

/** Converts host-only stored values to the same validated draft shape used by manual profiles. */
export function securityValidationInput(value: StoredProfileSecurity): Pick<
  ProfileCreateInput,
  "sasl" | "services"
> & {
  readonly clientIdentity?: ProfileClientIdentityInput<ProtectedValueCreateInput>;
} {
  const protect = (value: string): ProtectedValueCreateInput =>
    value.length === 0 ? { mode: "clear" } : { mode: "replace", value };
  const identityInput = (
    value: ProfileClientIdentityInput<string>,
  ): ProfileClientIdentityInput<ProtectedValueCreateInput> => ({
    certificatePem: protect(value.certificatePem),
    privateKeyPem: protect(value.privateKeyPem),
    passphrase: protect(value.passphrase),
  });
  return {
    ...(value.sasl === undefined
      ? {}
      : { sasl: { ...value.sasl, password: protect(value.sasl.password) } }),
    ...(value.clientIdentity === undefined
      ? {}
      : { clientIdentity: identityInput(value.clientIdentity) }),
    ...(value.services === undefined
      ? {}
      : {
          services: Object.fromEntries(
            Object.entries<ClusterServiceEndpointInput<string>>({ ...value.services }).map(
              ([key, v]): [string, ClusterServiceEndpointInput<ProtectedValueCreateInput>] => [
                key,
                {
                  authentication: v.authentication,
                  baseUrl: v.baseUrl,
                  ...(v.basic === undefined
                    ? {}
                    : {
                        basic: { username: v.basic.username, password: protect(v.basic.password) },
                      }),
                  ...(v.bearer === undefined ? {} : { bearer: protect(v.bearer) }),
                  ...(v.oauth === undefined
                    ? {}
                    : { oauth: { ...v.oauth, clientSecret: protect(v.oauth.clientSecret) } }),
                  ...(v.trust === undefined
                    ? {}
                    : {
                        trust:
                          v.trust.mode === "custom"
                            ? {
                                ...v.trust,
                                material: protect(v.trust.material),
                                password: protect(v.trust.password),
                              }
                            : v.trust,
                      }),
                  ...(v.clientIdentity === undefined
                    ? {}
                    : { clientIdentity: identityInput(v.clientIdentity) }),
                },
              ],
            ),
          ),
        }),
  };
}
