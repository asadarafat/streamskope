import {
  PROFILE_LIMITS,
  PROFILE_STORE_DURABILITIES,
  PROFILE_STORE_PROTECTIONS,
  PROFILE_STORE_STATES,
  type ProfileStoreCapability,
  type ProfileSummaryOAuth,
  PROFILE_TRUST_KINDS,
  CLUSTER_SERVICE_AUTHENTICATION_MODES,
  type AcquiredProtectedValueInput,
  type ClusterServiceEndpointInput,
  type ClusterServiceEndpointsInput,
  type ProfileCreateInput,
  type ProfileOAuthInput,
  type ProfileTrustCreateValueInput,
  type ProfileTrustInput,
  type ProfileTrustUpdateValueInput,
  type ProfileTestInput,
  type ProfileUpdateInput,
  type ProtectedValueCreateInput,
  type ProtectedValueUpdateInput,
} from "./profile-types";
import { HostContractValidationError } from "./validation-error";
import { parseProfileBindingInput, type ProfileBindingInput } from "./profile-binding";
import {
  declaredValue,
  exactKeys,
  optionalText,
  parseBoundedBrokers,
  positiveBoundedInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export function parseProfileSummaryOAuth(value: unknown, path: string): ProfileSummaryOAuth {
  const oauth = record(value, path);
  exactKeys(oauth, ["clientId", "clientSecretPresent", "scope", "tokenEndpoint"], path);
  return {
    clientId: text(oauth.clientId, `${path}.clientId`, PROFILE_LIMITS.clientIdCharacters),
    clientSecretPresent: truth(oauth.clientSecretPresent, `${path}.clientSecretPresent`),
    scope: text(oauth.scope, `${path}.scope`, PROFILE_LIMITS.scopeCharacters),
    tokenEndpoint: text(
      oauth.tokenEndpoint,
      `${path}.tokenEndpoint`,
      PROFILE_LIMITS.tokenEndpointCharacters,
    ),
  };
}

export function parseProfileStoreCapability(value: unknown, path: string): ProfileStoreCapability {
  const store = record(value, path);
  exactKeys(store, ["durability", "protection", "recovery", "state"], path);
  const recovery = optionalText(store, "recovery", path, 2_048);
  const base = {
    durability: declaredValue(store.durability, PROFILE_STORE_DURABILITIES, `${path}.durability`),
    protection: declaredValue(store.protection, PROFILE_STORE_PROTECTIONS, `${path}.protection`),
    state: declaredValue(store.state, PROFILE_STORE_STATES, `${path}.state`),
  };
  if (
    (base.durability === "session" && base.protection !== "memory") ||
    (base.durability === "durable" && base.protection === "memory") ||
    (base.state === "ready" && base.protection === "unavailable") ||
    (base.state === "unavailable" && base.protection !== "unavailable")
  ) {
    throw new HostContractValidationError(path, "contains inconsistent store capability");
  }
  return recovery === undefined ? base : { ...base, recovery };
}

function parseAcquiredProtectedValue(value: unknown, path: string): AcquiredProtectedValueInput {
  const protectedValue = record(value, path);
  exactKeys(protectedValue, ["acquisitionId", "mode", "editorId"], path);
  if (protectedValue.mode !== "acquired") {
    throw new HostContractValidationError(`${path}.mode`, "must equal acquired");
  }
  return {
    acquisitionId: text(
      protectedValue.acquisitionId,
      `${path}.acquisitionId`,
      PROFILE_LIMITS.idCharacters,
    ),
    mode: "acquired",
    ...(protectedValue.editorId === undefined
      ? {}
      : {
          editorId: text(protectedValue.editorId, `${path}.editorId`, PROFILE_LIMITS.idCharacters),
        }),
  };
}

function parseProtectedCreate(
  value: unknown,
  path: string,
  maximumCharacters: number,
): ProtectedValueCreateInput {
  const protectedValue = record(value, path);
  const mode = declaredValue(protectedValue.mode, ["clear", "replace"], `${path}.mode`);
  if (mode === "clear") {
    exactKeys(protectedValue, ["mode"], path);
    return { mode };
  }
  exactKeys(protectedValue, ["mode", "value"], path);
  return {
    mode,
    value: text(protectedValue.value, `${path}.value`, maximumCharacters),
  };
}

function parseProtectedUpdate(
  value: unknown,
  path: string,
  maximumCharacters: number,
): ProtectedValueUpdateInput {
  const protectedValue = record(value, path);
  const mode = declaredValue(protectedValue.mode, ["clear", "replace", "retain"], `${path}.mode`);
  if (mode === "clear" || mode === "retain") {
    exactKeys(protectedValue, ["mode"], path);
    return { mode };
  }
  exactKeys(protectedValue, ["mode", "value"], path);
  return {
    mode,
    value: text(protectedValue.value, `${path}.value`, maximumCharacters),
  };
}

function parseTrustProtectedCreate(
  value: unknown,
  path: string,
  maximumCharacters: number,
): ProfileTrustCreateValueInput {
  const protectedValue = record(value, path);
  return protectedValue.mode === "acquired"
    ? parseAcquiredProtectedValue(value, path)
    : parseProtectedCreate(value, path, maximumCharacters);
}

function parseTrustProtectedUpdate(
  value: unknown,
  path: string,
  maximumCharacters: number,
): ProfileTrustUpdateValueInput {
  const protectedValue = record(value, path);
  return protectedValue.mode === "acquired"
    ? parseAcquiredProtectedValue(value, path)
    : parseProtectedUpdate(value, path, maximumCharacters);
}

function parseProfileInput<
  TTrustValue extends ProfileTrustCreateValueInput | ProfileTrustUpdateValueInput,
  TOAuthValue extends ProtectedValueCreateInput | ProtectedValueUpdateInput,
>(
  value: unknown,
  path: string,
  parseTrustProtected: (value: unknown, path: string, maximumCharacters: number) => TTrustValue,
  parseOAuthProtected: (value: unknown, path: string, maximumCharacters: number) => TOAuthValue,
): {
  readonly apiCa?: TOAuthValue;
  readonly binding?: ProfileBindingInput;
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: ProfileOAuthInput<TOAuthValue>;
  readonly services?: ClusterServiceEndpointsInput;
  readonly trust: ProfileTrustInput<TTrustValue>;
} {
  const profile = record(value, path);
  exactKeys(profile, ["brokers", "name", "oauth", "services", "trust", "binding", "apiCa"], path);
  const trust = record(profile.trust, `${path}.trust`);
  exactKeys(trust, ["kind", "label", "material", "password"], `${path}.trust`);
  const parsedTrust: ProfileTrustInput<TTrustValue> = {
    kind: declaredValue(trust.kind, PROFILE_TRUST_KINDS, `${path}.trust.kind`),
    label: text(trust.label, `${path}.trust.label`, PROFILE_LIMITS.trustLabelCharacters),
    material: parseTrustProtected(
      trust.material,
      `${path}.trust.material`,
      PROFILE_LIMITS.trustEncodedCharacters,
    ),
    password: parseTrustProtected(
      trust.password,
      `${path}.trust.password`,
      PROFILE_LIMITS.clientSecretCharacters,
    ),
  };
  const services = Object.hasOwn(profile, "services")
    ? parseClusterServiceEndpoints(profile.services, `${path}.services`)
    : undefined;
  const base = {
    ...(profile.apiCa === undefined
      ? {}
      : {
          apiCa: parseOAuthProtected(
            profile.apiCa,
            `${path}.apiCa`,
            PROFILE_LIMITS.trustBinaryBytes,
          ),
        }),
    brokers: parseBoundedBrokers(
      profile.brokers,
      `${path}.brokers`,
      PROFILE_LIMITS.brokers,
      PROFILE_LIMITS.brokerCharacters,
    ),
    name: text(profile.name, `${path}.name`, PROFILE_LIMITS.nameCharacters),
    ...(Object.hasOwn(profile, "binding")
      ? { binding: parseProfileBindingInput(profile.binding, `${path}.binding`) }
      : {}),
    ...(services === undefined ? {} : { services }),
    trust: parsedTrust,
  };
  if (!Object.hasOwn(profile, "oauth")) {
    return base;
  }
  const oauth = record(profile.oauth, `${path}.oauth`);
  exactKeys(oauth, ["clientId", "clientSecret", "scope", "tokenEndpoint"], `${path}.oauth`);
  return {
    ...base,
    oauth: {
      clientId: text(oauth.clientId, `${path}.oauth.clientId`, PROFILE_LIMITS.clientIdCharacters),
      clientSecret: parseOAuthProtected(
        oauth.clientSecret,
        `${path}.oauth.clientSecret`,
        PROFILE_LIMITS.clientSecretCharacters,
      ),
      scope: text(oauth.scope, `${path}.oauth.scope`, PROFILE_LIMITS.scopeCharacters),
      tokenEndpoint: text(
        oauth.tokenEndpoint,
        `${path}.oauth.tokenEndpoint`,
        PROFILE_LIMITS.tokenEndpointCharacters,
      ),
    },
  };
}

export function parseClusterServiceEndpoints(
  value: unknown,
  path: string,
): ClusterServiceEndpointsInput {
  const services = record(value, path);
  exactKeys(services, ["redpandaAdmin", "schemaRegistry"], path);
  const parseEndpoint = (
    endpointValue: unknown,
    endpointPath: string,
  ): ClusterServiceEndpointInput => {
    const endpoint = record(endpointValue, endpointPath);
    exactKeys(endpoint, ["authentication", "baseUrl"], endpointPath);
    return {
      authentication: declaredValue(
        endpoint.authentication,
        CLUSTER_SERVICE_AUTHENTICATION_MODES,
        `${endpointPath}.authentication`,
      ),
      baseUrl: text(
        endpoint.baseUrl,
        `${endpointPath}.baseUrl`,
        PROFILE_LIMITS.tokenEndpointCharacters,
      ),
    };
  };
  return {
    ...(Object.hasOwn(services, "redpandaAdmin")
      ? { redpandaAdmin: parseEndpoint(services.redpandaAdmin, `${path}.redpandaAdmin`) }
      : {}),
    ...(Object.hasOwn(services, "schemaRegistry")
      ? { schemaRegistry: parseEndpoint(services.schemaRegistry, `${path}.schemaRegistry`) }
      : {}),
  };
}

export function parseProfileCreateInput(value: unknown, path: string): ProfileCreateInput {
  return parseProfileInput(value, path, parseTrustProtectedCreate, parseProtectedCreate);
}

export function parseProfileUpdateInput(value: unknown, path: string): ProfileUpdateInput {
  const profile = record(value, path);
  const { expectedRevision, ...fields } = profile;
  const parsed = parseProfileInput(fields, path, parseTrustProtectedUpdate, parseProtectedUpdate);
  return Object.hasOwn(profile, "expectedRevision")
    ? {
        ...parsed,
        expectedRevision: positiveBoundedInteger(
          expectedRevision,
          `${path}.expectedRevision`,
          Number.MAX_SAFE_INTEGER,
        ),
      }
    : parsed;
}

export function parseProfileTestInput(value: unknown, path: string): ProfileTestInput {
  const payload = record(value, path);
  const mode = declaredValue(payload.mode, ["create", "update"], `${path}.mode`);
  if (mode === "create") {
    exactKeys(payload, ["mode", "profile"], path);
    return {
      mode,
      profile: parseProfileCreateInput(payload.profile, `${path}.profile`),
    };
  }
  exactKeys(payload, ["mode", "profile", "profileId"], path);
  return {
    mode,
    profile: parseProfileUpdateInput(payload.profile, `${path}.profile`),
    profileId: text(payload.profileId, `${path}.profileId`, PROFILE_LIMITS.idCharacters),
  };
}

export function parseProfileIdPayload(
  value: unknown,
  path: string,
): {
  readonly profileId: string;
} {
  const payload = record(value, path);
  exactKeys(payload, ["profileId"], path);
  return {
    profileId: text(payload.profileId, `${path}.profileId`, PROFILE_LIMITS.idCharacters),
  };
}
