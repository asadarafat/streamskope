import { isDeepStrictEqual } from "node:util";
import { chmod, open, unlink } from "node:fs/promises";
import { basename, dirname } from "node:path";

import {
  summarizeIdentity,
  summarizeServices,
  type StoredProfileSecurity,
} from "../../features/kafka/application/profile-security";
import {
  parseSummaryIdentity,
  parseSummarySasl,
  parseServiceSummaries,
} from "../../features/kafka/contracts/profile-security-validation";
import { parseProfileSummaryOAuth } from "../../features/kafka/contracts/profile-validation";
import { boundedText as possiblyEmptyText } from "../../features/kafka/contracts/validation-primitives";
import { parsePluginJson } from "../../plugins/validation";
import {
  KAFKA_PROFILE_TRANSPORTS,
  PROFILE_LIMITS,
  PROFILE_TRUST_KINDS,
  parseProfileAcquisitionBinding,
  parseProfileSource,
  type ProfileSource,
  type ProfileAcquisitionBinding,
  type HostErrorCode,
  type HostErrorStage,
  type KafkaProfileTransport,
  type ProfileStoreCapability,
  type ClusterServiceEndpointSummary,
  type ClusterServiceEndpointsSummary,
  type ProfileSummarySasl,
  type ProfileSummaryClientIdentity,
  type ProfileTrustKind,
} from "../../features/kafka/contracts";
import type {
  KafkaProfileRecord,
  KafkaProfileStore,
  KafkaProfileStructuredError,
} from "../../features/kafka/application";

import {
  hasExpandedProfileSecurity,
  storedProfileSecurity,
  parseStoredProfileSecurity,
} from "./kafka-profile-security";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";
import { readBoundedFile } from "./bounded-file";
import type { ProfileProtector } from "./profile-protector";

const LEGACY_PROFILE_FILE_VERSION = 1 as const;
const SERVICES_PROFILE_FILE_VERSION = 2 as const;
const PROFILE_FILE_VERSION = 3 as const;
const SECURITY_PROFILE_FILE_VERSION = 4 as const;
const SECURITY_PROTECTED_PROFILE_VERSION = 6 as const;
type ProfileFileVersion = 1 | 2 | 3 | 4;
const ACQUISITION_PROTECTED_PROFILE_VERSION = 4 as const;
const TRANSPORT_PROTECTED_PROFILE_VERSION = 5 as const;
export const KAFKA_PROFILE_FILE_MAX_BYTES = 64 * 1_048_576;
const DEFAULT_MAXIMUM_FILE_BYTES = KAFKA_PROFILE_FILE_MAX_BYTES;
const MAXIMUM_PROTECTED_VALUE_BYTES = 32 * 1_048_576;
const PROFILE_ROLLBACK_GENERATIONS = 100;

type UnknownRecord = Record<string, unknown>;

export type KafkaProfileProtector = ProfileProtector;

export interface KafkaProfileFileStoreOptions {
  readonly createTempId?: () => string;
  readonly maximumFileBytes?: number;
}

abstract class KafkaProfileFileError extends Error implements KafkaProfileStructuredError {
  abstract readonly code: HostErrorCode;
  abstract readonly recovery: string;
  readonly retryable = false;
  readonly stage: HostErrorStage = "storage";
  readonly target = undefined;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class KafkaProfileFileCorruptError extends KafkaProfileFileError {
  readonly code = "PROFILE_CORRUPT" as const;
  readonly recovery =
    "Preserve the profile file, restore a known-good copy, or remove it manually after confirming a backup.";

  constructor() {
    super("The Kafka profile file is corrupt or uses an unsupported schema.");
  }
}

export class KafkaProfileFileWriteError extends KafkaProfileFileError {
  readonly code = "PROFILE_STORE_UNAVAILABLE" as const;
  readonly recovery: string;

  constructor(rollbackGeneration?: string, recovery?: string) {
    super("Kafka profile storage could not commit the requested change.");
    this.recovery =
      recovery ??
      (rollbackGeneration === undefined
        ? "Check application-data permissions and operating-system credential access, then retry."
        : `The original profile data remains in ${rollbackGeneration}. Restore that exact rollback generation before launching an older StreamSkope build.`);
  }
}

function valueRecord(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new KafkaProfileFileCorruptError();
  }
  return value as UnknownRecord;
}

function exactKeys(value: UnknownRecord, allowed: readonly string[]): void {
  const keys = new Set(allowed);
  if (Object.keys(value).some((key) => !keys.has(key))) {
    throw new KafkaProfileFileCorruptError();
  }
}

function boundedText(value: unknown, maximum: number): string {
  if (typeof value !== "string" || value.length === 0 || value.length > maximum) {
    throw new KafkaProfileFileCorruptError();
  }
  return value;
}

function booleanValue(value: unknown): boolean {
  if (typeof value !== "boolean") {
    throw new KafkaProfileFileCorruptError();
  }
  return value;
}

function positiveRevision(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new KafkaProfileFileCorruptError();
  }
  return value;
}

function profileTransport(value: unknown): KafkaProfileTransport {
  if (
    typeof value !== "string" ||
    !KAFKA_PROFILE_TRANSPORTS.includes(value as KafkaProfileTransport)
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  return value as KafkaProfileTransport;
}

function trustKind(value: unknown): ProfileTrustKind {
  if (typeof value !== "string" || !PROFILE_TRUST_KINDS.includes(value as ProfileTrustKind)) {
    throw new KafkaProfileFileCorruptError();
  }
  return value as ProfileTrustKind;
}

function brokers(value: unknown): readonly string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > PROFILE_LIMITS.brokers) {
    throw new KafkaProfileFileCorruptError();
  }
  return value.map((broker) => boundedText(broker, PROFILE_LIMITS.brokerCharacters));
}

function optionalText(value: UnknownRecord, key: string, maximum: number): string | undefined {
  return Object.hasOwn(value, key) ? boundedText(value[key], maximum) : undefined;
}

function base64Buffer(value: unknown): Buffer {
  const encoded = boundedText(value, MAXIMUM_PROTECTED_VALUE_BYTES * 2);
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) {
    throw new KafkaProfileFileCorruptError();
  }
  const decoded = Buffer.from(encoded, "base64");
  if (
    decoded.length === 0 ||
    decoded.length > MAXIMUM_PROTECTED_VALUE_BYTES ||
    decoded.toString("base64") !== encoded
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  return decoded;
}

interface SafeStoredProfile {
  readonly brokers: readonly string[];
  readonly createdAt: string;
  readonly id: string;
  readonly name: string;
  readonly oauth?: {
    readonly clientId: string;
    readonly clientSecretPresent: boolean;
    readonly scope: string;
    readonly tokenEndpoint: string;
  };
  readonly protectedValue: string;
  readonly revision?: number;
  readonly services?: ClusterServiceEndpointsSummary;
  readonly sasl?: ProfileSummarySasl;
  readonly clientIdentity?: ProfileSummaryClientIdentity;
  readonly source?: ProfileSource;
  readonly transport: KafkaProfileTransport;
  readonly trust?: {
    readonly kind: ProfileTrustKind;
    readonly label: string;
    readonly materialPresent: boolean;
    readonly passwordPresent: boolean;
  };
  readonly updatedAt: string;
}

/** Preserve pre-plugin capture records without loading plugin implementation code. */
function parseStoredProfileSource(value: unknown): ProfileSource {
  const input = valueRecord(value);
  if (input.kind === "eda-capture") {
    return parseProfileSource(
      { kind: "plugin", pluginId: "streamskope.eda", version: 1, data: parsePluginJson(input) },
      "profile.source",
    );
  }
  return parseProfileSource(input, "profile.source");
}

function parseServiceEndpoint(value: unknown): ClusterServiceEndpointSummary {
  const endpoint = valueRecord(value);
  exactKeys(endpoint, ["authentication", "baseUrl"]);
  const authentication = boundedText(endpoint.authentication, 16);
  if (!["none", "oauth"].includes(authentication)) {
    throw new KafkaProfileFileCorruptError();
  }
  return {
    authentication: authentication as ClusterServiceEndpointSummary["authentication"],
    baseUrl: boundedText(endpoint.baseUrl, PROFILE_LIMITS.tokenEndpointCharacters),
  };
}

function parseServices(value: unknown): ClusterServiceEndpointsSummary {
  const services = valueRecord(value);
  exactKeys(services, ["connect", "redpandaAdmin", "schemaRegistry"]);
  return {
    ...(Object.hasOwn(services, "connect")
      ? { connect: parseServiceEndpoint(services.connect) }
      : {}),
    ...(Object.hasOwn(services, "redpandaAdmin")
      ? { redpandaAdmin: parseServiceEndpoint(services.redpandaAdmin) }
      : {}),
    ...(Object.hasOwn(services, "schemaRegistry")
      ? { schemaRegistry: parseServiceEndpoint(services.schemaRegistry) }
      : {}),
  };
}

function parseSafeProfile(value: unknown, version: ProfileFileVersion): SafeStoredProfile {
  const profile = valueRecord(value);
  const transport =
    version >= PROFILE_FILE_VERSION ? profileTransport(profile.transport) : ("tls" as const);
  exactKeys(profile, [
    "brokers",
    "createdAt",
    "id",
    "name",
    "oauth",
    "protectedValue",
    ...(version === SECURITY_PROFILE_FILE_VERSION ? ["sasl", "clientIdentity"] : []),
    ...(version >= PROFILE_FILE_VERSION ? ["revision", "transport", "source"] : []),
    ...(version === LEGACY_PROFILE_FILE_VERSION ? [] : ["services"]),
    ...(transport === "tls" ? ["trust"] : []),
    "updatedAt",
  ]);
  const base: SafeStoredProfile = {
    brokers: brokers(profile.brokers),
    createdAt: boundedText(profile.createdAt, 128),
    id: boundedText(profile.id, PROFILE_LIMITS.idCharacters),
    name: boundedText(profile.name, PROFILE_LIMITS.nameCharacters),
    protectedValue: boundedText(profile.protectedValue, MAXIMUM_PROTECTED_VALUE_BYTES * 2),
    ...(Object.hasOwn(profile, "revision") ? { revision: positiveRevision(profile.revision) } : {}),
    ...(Object.hasOwn(profile, "services")
      ? {
          services:
            version === SECURITY_PROFILE_FILE_VERSION
              ? parseServiceSummaries(
                  profile.services,
                  "profile.services",
                  parseProfileSummaryOAuth,
                )
              : parseServices(profile.services),
        }
      : {}),
    ...(profile.sasl === undefined ? {} : { sasl: parseSummarySasl(profile.sasl, "profile.sasl") }),
    ...(profile.clientIdentity === undefined
      ? {}
      : { clientIdentity: parseSummaryIdentity(profile.clientIdentity, "profile.clientIdentity") }),
    ...(Object.hasOwn(profile, "source")
      ? { source: parseStoredProfileSource(profile.source) }
      : {}),
    transport,
    updatedAt: boundedText(profile.updatedAt, 128),
  };
  let parsedOAuth: NonNullable<SafeStoredProfile["oauth"]> | undefined;
  if (Object.hasOwn(profile, "oauth")) {
    const oauth = valueRecord(profile.oauth);
    exactKeys(oauth, ["clientId", "clientSecretPresent", "scope", "tokenEndpoint"]);
    parsedOAuth = {
      clientId: boundedText(oauth.clientId, PROFILE_LIMITS.clientIdCharacters),
      clientSecretPresent: booleanValue(oauth.clientSecretPresent),
      scope: possiblyEmptyText(oauth.scope, "oauth.scope", PROFILE_LIMITS.scopeCharacters),
      tokenEndpoint: boundedText(oauth.tokenEndpoint, PROFILE_LIMITS.tokenEndpointCharacters),
    };
  }
  const withOAuth = parsedOAuth === undefined ? base : { ...base, oauth: parsedOAuth };
  if (transport === "plaintext") {
    return withOAuth;
  }
  const trust = valueRecord(profile.trust);
  exactKeys(trust, ["kind", "label", "materialPresent", "passwordPresent"]);
  return {
    ...withOAuth,
    trust: {
      kind: trustKind(trust.kind),
      label: boundedText(trust.label, PROFILE_LIMITS.trustLabelCharacters),
      materialPresent: booleanValue(trust.materialPresent),
      passwordPresent: booleanValue(trust.passwordPresent),
    },
  };
}

interface StoredProfileDocument {
  readonly profiles: readonly SafeStoredProfile[];
  readonly rollbackGeneration?: string;
  readonly version: ProfileFileVersion;
}

interface ProfileRollback {
  readonly created: boolean;
  readonly generation: string;
}

function parseDocument(value: unknown): StoredProfileDocument {
  const document = valueRecord(value);
  if (
    document.version !== LEGACY_PROFILE_FILE_VERSION &&
    document.version !== SERVICES_PROFILE_FILE_VERSION &&
    document.version !== PROFILE_FILE_VERSION &&
    document.version !== SECURITY_PROFILE_FILE_VERSION
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  exactKeys(document, [
    "profiles",
    ...(document.version >= PROFILE_FILE_VERSION ? ["rollbackGeneration"] : []),
    "version",
  ]);
  if (!Array.isArray(document.profiles) || document.profiles.length > PROFILE_LIMITS.profiles) {
    throw new KafkaProfileFileCorruptError();
  }
  const version = document.version;
  let rollbackGeneration: string | undefined;
  if (Object.hasOwn(document, "rollbackGeneration")) {
    rollbackGeneration = boundedText(document.rollbackGeneration, 512);
    if (
      rollbackGeneration.includes("/") ||
      rollbackGeneration.includes("\\") ||
      !/\.pre-(?:transport-v2|security-v3)(?:\.[1-9]\d?)?$/u.test(rollbackGeneration)
    ) {
      throw new KafkaProfileFileCorruptError();
    }
  }
  return {
    profiles: document.profiles.map((profile) => parseSafeProfile(profile, version)),
    ...(rollbackGeneration === undefined ? {} : { rollbackGeneration }),
    version,
  };
}

export interface KafkaProfileEnvelope extends Omit<StoredProfileDocument, "profiles"> {
  readonly profiles: readonly (SafeStoredProfile & { readonly protectedBytes: Buffer })[];
}

/** Parse only persisted metadata/ciphertext; never decrypt, migrate or re-encrypt. */
export function inspectKafkaProfileEnvelope(
  contents: Uint8Array,
  maximumFileBytes = KAFKA_PROFILE_FILE_MAX_BYTES,
): KafkaProfileEnvelope {
  if (contents.byteLength > maximumFileBytes) throw new KafkaProfileFileCorruptError();
  const document = parseDocument(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(contents)) as unknown,
  );
  return {
    ...document,
    profiles: document.profiles.map((profile) => ({
      ...profile,
      protectedBytes: base64Buffer(profile.protectedValue),
    })),
  };
}

interface ProtectedProfileValues {
  readonly security?: StoredProfileSecurity;
  readonly apiCaPem?: string;
  readonly binding?: ProfileAcquisitionBinding;
  readonly revision?: number;
  readonly oauth?: {
    readonly clientSecret: string;
  };
  readonly profileId: string;
  readonly transport: KafkaProfileTransport;
  readonly trust?: {
    readonly material: string;
    readonly password?: string;
  };
}

function parseProtectedValues(
  value: string,
  expectedProfileId: string,
  documentVersion: ProfileFileVersion,
): ProtectedProfileValues {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value) as unknown;
  } catch {
    throw new KafkaProfileFileCorruptError();
  }
  const protectedValues = valueRecord(parsed);
  const version = protectedValues.version;
  if (
    version !== 1 &&
    version !== 2 &&
    version !== 3 &&
    version !== ACQUISITION_PROTECTED_PROFILE_VERSION &&
    version !== TRANSPORT_PROTECTED_PROFILE_VERSION &&
    version !== SECURITY_PROTECTED_PROFILE_VERSION
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  if (
    documentVersion >= PROFILE_FILE_VERSION !== version >= TRANSPORT_PROTECTED_PROFILE_VERSION ||
    (documentVersion === SECURITY_PROFILE_FILE_VERSION) !==
      (version === SECURITY_PROTECTED_PROFILE_VERSION)
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  const transport =
    version >= TRANSPORT_PROTECTED_PROFILE_VERSION
      ? profileTransport(protectedValues.transport)
      : ("tls" as const);
  exactKeys(protectedValues, [
    ...(transport === "tls" &&
    (version === ACQUISITION_PROTECTED_PROFILE_VERSION ||
      version >= TRANSPORT_PROTECTED_PROFILE_VERSION)
      ? ["apiCaPem"]
      : []),
    ...(transport === "tls" && version >= 3 ? ["binding"] : []),
    "oauth",
    "profileId",
    ...(version === SECURITY_PROTECTED_PROFILE_VERSION ? ["security"] : []),
    ...(version === 1 ? [] : ["revision"]),
    ...(transport === "tls" ? ["trust"] : []),
    ...(version >= TRANSPORT_PROTECTED_PROFILE_VERSION ? ["transport"] : []),
    "version",
  ]);
  const revision =
    version === 1 ||
    (version >= TRANSPORT_PROTECTED_PROFILE_VERSION && protectedValues.revision === undefined)
      ? undefined
      : positiveRevision(protectedValues.revision);
  const profileId = boundedText(protectedValues.profileId, PROFILE_LIMITS.idCharacters);
  if (profileId !== expectedProfileId) {
    throw new KafkaProfileFileCorruptError();
  }
  const base: ProtectedProfileValues = {
    ...(version === SECURITY_PROTECTED_PROFILE_VERSION
      ? {
          security: parseStoredProfileSecurity(protectedValues.security, transport === "plaintext"),
        }
      : {}),
    ...(Object.hasOwn(protectedValues, "binding")
      ? { binding: parseProfileAcquisitionBinding(protectedValues.binding) }
      : {}),
    ...(protectedValues.apiCaPem === undefined
      ? {}
      : { apiCaPem: boundedText(protectedValues.apiCaPem, PROFILE_LIMITS.trustBinaryBytes) }),
    ...(typeof revision === "number" ? { revision } : {}),
    profileId,
    transport,
  };
  let parsedOAuth: NonNullable<ProtectedProfileValues["oauth"]> | undefined;
  if (Object.hasOwn(protectedValues, "oauth")) {
    const oauth = valueRecord(protectedValues.oauth);
    exactKeys(oauth, ["clientSecret"]);
    parsedOAuth = {
      clientSecret: boundedText(oauth.clientSecret, PROFILE_LIMITS.clientSecretCharacters),
    };
  }
  const withOAuth = parsedOAuth === undefined ? base : { ...base, oauth: parsedOAuth };
  if (transport === "plaintext") {
    return withOAuth;
  }
  const trust = valueRecord(protectedValues.trust);
  exactKeys(trust, ["material", "password"]);
  const password = optionalText(trust, "password", PROFILE_LIMITS.clientSecretCharacters);
  return {
    ...withOAuth,
    trust: {
      material: boundedText(trust.material, PROFILE_LIMITS.trustEncodedCharacters),
      ...(password === undefined ? {} : { password }),
    },
  };
}

function safeProfile(
  record: KafkaProfileRecord,
  protectedValue: Buffer,
  version: ProfileFileVersion,
): SafeStoredProfile {
  const transport = record.transport ?? "tls";
  const base = {
    brokers: [...record.brokers],
    createdAt: record.createdAt,
    id: record.id,
    name: record.name,
    protectedValue: protectedValue.toString("base64"),
    ...(record.revision === undefined ? {} : { revision: record.revision }),
    ...(record.services === undefined ? {} : { services: summarizeServices(record.services) }),
    ...(version !== SECURITY_PROFILE_FILE_VERSION || record.sasl === undefined
      ? {}
      : {
          sasl: {
            mechanism: record.sasl.mechanism,
            username: record.sasl.username,
            passwordPresent: record.sasl.password.length > 0,
          },
        }),
    ...(version !== SECURITY_PROFILE_FILE_VERSION || record.clientIdentity === undefined
      ? {}
      : { clientIdentity: summarizeIdentity(record.clientIdentity) }),
    ...(record.source === undefined ? {} : { source: record.source }),
    transport,
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
  if (transport === "plaintext") {
    return withOAuth;
  }
  if (record.trust === undefined) {
    throw new KafkaProfileFileWriteError();
  }
  return {
    ...withOAuth,
    trust: {
      kind: record.trust.kind,
      label: record.trust.label,
      materialPresent: record.trust.material.length > 0,
      passwordPresent: record.trust.password !== undefined,
    },
  };
}

function protectedProfile(record: KafkaProfileRecord, documentVersion: ProfileFileVersion): string {
  if (
    record.revision !== undefined &&
    (!Number.isSafeInteger(record.revision) || record.revision < 1)
  ) {
    throw new KafkaProfileFileWriteError();
  }
  const transport = record.transport ?? "tls";
  const base = {
    ...(record.revision === undefined ? {} : { revision: record.revision }),
    ...(record.oauth === undefined ? {} : { oauth: { clientSecret: record.oauth.clientSecret } }),
    profileId: record.id,
    transport,
    version:
      documentVersion === SECURITY_PROFILE_FILE_VERSION
        ? SECURITY_PROTECTED_PROFILE_VERSION
        : TRANSPORT_PROTECTED_PROFILE_VERSION,
    ...(documentVersion === SECURITY_PROFILE_FILE_VERSION
      ? { security: storedProfileSecurity(record) }
      : {}),
  };
  if (transport === "plaintext") {
    if (
      Object.hasOwn(record, "apiCaPem") ||
      Object.hasOwn(record, "binding") ||
      Object.hasOwn(record, "trust") ||
      Object.hasOwn(record, "clientIdentity")
    ) {
      throw new KafkaProfileFileWriteError();
    }
    return JSON.stringify(base);
  }
  if (record.trust === undefined) {
    throw new KafkaProfileFileWriteError();
  }
  return JSON.stringify({
    ...base,
    ...(record.apiCaPem === undefined
      ? {}
      : { apiCaPem: boundedText(record.apiCaPem, PROFILE_LIMITS.trustBinaryBytes) }),
    ...(record.binding === undefined
      ? {}
      : { binding: parseProfileAcquisitionBinding(record.binding) }),
    trust: {
      material: record.trust.material,
      ...(record.trust.password === undefined ? {} : { password: record.trust.password }),
    },
  });
}

function restoredRecord(
  safe: SafeStoredProfile,
  values: ProtectedProfileValues,
  documentVersion: ProfileFileVersion,
): KafkaProfileRecord {
  if (
    safe.transport !== values.transport ||
    (documentVersion >= PROFILE_FILE_VERSION && safe.revision !== values.revision) ||
    (safe.oauth === undefined) !== (values.oauth === undefined) ||
    (safe.oauth !== undefined &&
      safe.oauth.clientSecretPresent !== (values.oauth?.clientSecret.length ?? 0) > 0)
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  const security = values.security;
  if (
    documentVersion === SECURITY_PROFILE_FILE_VERSION &&
    (security === undefined ||
      !isDeepStrictEqual(
        safe.services,
        security.services === undefined ? undefined : summarizeServices(security.services),
      ) ||
      !isDeepStrictEqual(
        safe.clientIdentity,
        security.clientIdentity === undefined
          ? undefined
          : summarizeIdentity(security.clientIdentity),
      ) ||
      !isDeepStrictEqual(
        safe.sasl,
        security.sasl === undefined
          ? undefined
          : {
              mechanism: security.sasl.mechanism,
              username: security.sasl.username,
              passwordPresent: security.sasl.password.length > 0,
            },
      ))
  )
    throw new KafkaProfileFileCorruptError();
  const legacyServices =
    safe.services === undefined
      ? undefined
      : Object.fromEntries(
          Object.entries<ClusterServiceEndpointSummary>({ ...safe.services }).map(([key, v]) => [
            key,
            { authentication: v.authentication, baseUrl: v.baseUrl },
          ]),
        );
  const services =
    documentVersion === SECURITY_PROFILE_FILE_VERSION ? security?.services : legacyServices;
  const base = {
    ...(security?.sasl === undefined ? {} : { sasl: security.sasl }),
    brokers: safe.brokers,
    ...(values.revision === undefined ? {} : { revision: values.revision }),
    createdAt: safe.createdAt,
    id: safe.id,
    name: safe.name,
    ...(safe.oauth === undefined || values.oauth === undefined
      ? {}
      : {
          oauth: {
            clientId: safe.oauth.clientId,
            clientSecret: values.oauth.clientSecret,
            scope: safe.oauth.scope,
            tokenEndpoint: safe.oauth.tokenEndpoint,
          },
        }),
    ...(services === undefined ? {} : { services }),
    ...(safe.source === undefined ? {} : { source: safe.source }),
    transport: safe.transport,
    updatedAt: safe.updatedAt,
  };
  if (safe.transport === "plaintext") {
    if (
      safe.trust !== undefined ||
      values.trust !== undefined ||
      values.binding !== undefined ||
      values.apiCaPem !== undefined ||
      security?.clientIdentity !== undefined ||
      safe.clientIdentity !== undefined
    ) {
      throw new KafkaProfileFileCorruptError();
    }
    return { ...base, transport: "plaintext" };
  }
  if (
    safe.trust === undefined ||
    values.trust === undefined ||
    safe.trust.materialPresent !== values.trust.material.length > 0 ||
    safe.trust.passwordPresent !== (values.trust.password !== undefined)
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  return {
    ...base,
    ...(security?.clientIdentity === undefined ? {} : { clientIdentity: security.clientIdentity }),
    ...(values.apiCaPem === undefined ? {} : { apiCaPem: values.apiCaPem }),
    ...(values.binding === undefined ? {} : { binding: values.binding }),
    transport: "tls",
    trust: {
      kind: safe.trust.kind,
      label: safe.trust.label,
      material: values.trust.material,
      ...(values.trust.password === undefined ? {} : { password: values.trust.password }),
    },
  };
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function isExistingPath(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "EEXIST";
}

export class AtomicKafkaProfileFileStore implements KafkaProfileStore {
  private readonly createTempId;
  private currentCapability: ProfileStoreCapability;
  private readonly maximumFileBytes;

  constructor(
    private readonly path: string,
    private readonly protector: KafkaProfileProtector,
    storeCapability: ProfileStoreCapability,
    options: KafkaProfileFileStoreOptions = {},
  ) {
    this.createTempId = options.createTempId ?? createAtomicPrivateFileTempId;
    this.currentCapability = storeCapability;
    this.maximumFileBytes = options.maximumFileBytes ?? DEFAULT_MAXIMUM_FILE_BYTES;
  }

  capability(): ProfileStoreCapability {
    return this.currentCapability;
  }

  async commit(records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void> {
    let rollback: ProfileRollback | undefined;
    try {
      signal?.throwIfAborted();
      if (records.length > PROFILE_LIMITS.profiles) {
        throw new KafkaProfileFileWriteError();
      }
      const version = await this.writeVersion(records, signal);
      const profiles = await Promise.all(
        records.map(async (record) => {
          const plaintext = protectedProfile(record, version);
          if (Buffer.byteLength(plaintext, "utf8") > MAXIMUM_PROTECTED_VALUE_BYTES)
            throw new KafkaProfileFileWriteError();
          const protectedValue = await this.protector.protect(plaintext);
          if (protectedValue.length === 0 || protectedValue.length > MAXIMUM_PROTECTED_VALUE_BYTES)
            throw new KafkaProfileFileWriteError();
          return safeProfile(record, protectedValue, version);
        }),
      );
      signal?.throwIfAborted();
      rollback = await this.preserveProfileRollback(signal, version);
      const rollbackGeneration = rollback?.generation;
      const serialized = `${JSON.stringify({
        profiles,
        ...(rollbackGeneration === undefined ? {} : { rollbackGeneration }),
        version,
      })}\n`;
      if (Buffer.byteLength(serialized, "utf8") > this.maximumFileBytes) {
        throw new KafkaProfileFileWriteError();
      }
      if (records.some((record) => record.revision !== undefined))
        await this.preserveUpgradeBackup(signal);
      await writeAtomicPrivateTextFile({
        path: this.path,
        contents: serialized,
        createTempId: this.createTempId,
        ...(signal === undefined ? {} : { signal }),
      });
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        throw error;
      }
      throw rollback?.created !== true && error instanceof KafkaProfileFileWriteError
        ? error
        : new KafkaProfileFileWriteError(rollback?.generation);
    }
  }

  async load(signal?: AbortSignal): Promise<readonly KafkaProfileRecord[]> {
    let contents: Buffer;
    try {
      signal?.throwIfAborted();
      contents = await readBoundedFile(this.path, this.maximumFileBytes, {
        rejectSymlinks: true,
        signal,
      });
    } catch (error) {
      if (isMissingFile(error)) {
        return [];
      }
      if (error instanceof DOMException && error.name === "AbortError") {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaProfileFileCorruptError
        ? error
        : new KafkaProfileFileCorruptError();
    }

    try {
      const document = inspectKafkaProfileEnvelope(contents, this.maximumFileBytes);
      const restored: KafkaProfileRecord[] = [];
      let shouldReEncrypt = false;
      for (const profile of document.profiles) {
        signal?.throwIfAborted();
        const result = await this.protector.unprotect(profile.protectedBytes);
        shouldReEncrypt ||= result.shouldReEncrypt;
        restored.push(
          restoredRecord(
            profile,
            parseProtectedValues(result.plaintext, profile.id, document.version),
            document.version,
          ),
        );
      }
      if (shouldReEncrypt && document.version >= PROFILE_FILE_VERSION) {
        await this.commit(restored, signal);
      }
      return restored;
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        throw error;
      }
      this.markUnavailable();
      throw error instanceof KafkaProfileFileCorruptError
        ? error
        : new KafkaProfileFileCorruptError();
    }
  }

  private async writeVersion(
    records: readonly KafkaProfileRecord[],
    signal?: AbortSignal,
  ): Promise<3 | 4> {
    if (records.some(hasExpandedProfileSecurity)) return SECURITY_PROFILE_FILE_VERSION;
    try {
      const original = await readBoundedFile(this.path, this.maximumFileBytes, {
        rejectSymlinks: true,
        signal,
      });
      const version = parseDocument(JSON.parse(original.toString("utf8")) as unknown).version;
      return version === SECURITY_PROFILE_FILE_VERSION
        ? SECURITY_PROFILE_FILE_VERSION
        : PROFILE_FILE_VERSION;
    } catch (error) {
      if (isMissingFile(error)) return PROFILE_FILE_VERSION;
      throw error;
    }
  }

  private async preserveProfileRollback(
    signal: AbortSignal | undefined,
    targetVersion: ProfileFileVersion,
  ): Promise<ProfileRollback | undefined> {
    let original: Buffer;
    try {
      original = await readBoundedFile(this.path, this.maximumFileBytes, {
        rejectSymlinks: true,
        signal,
      });
    } catch (error) {
      if (isMissingFile(error)) return undefined;
      throw error;
    }
    await chmod(dirname(this.path), 0o700);
    if (original.length > this.maximumFileBytes) throw new KafkaProfileFileWriteError();
    const document = parseDocument(JSON.parse(original.toString("utf8")) as unknown);
    if (document.version >= targetVersion)
      return document.rollbackGeneration === undefined
        ? undefined
        : { created: false, generation: document.rollbackGeneration };
    for (const profile of document.profiles) {
      signal?.throwIfAborted();
      const protectedValues = await this.protector.unprotect(base64Buffer(profile.protectedValue));
      parseProtectedValues(protectedValues.plaintext, profile.id, document.version);
    }

    const rollbackBase = `${this.path}.${targetVersion === SECURITY_PROFILE_FILE_VERSION ? "pre-security-v3" : "pre-transport-v2"}`;
    for (let generation = 0; generation < PROFILE_ROLLBACK_GENERATIONS; generation += 1) {
      const rollbackPath =
        generation === 0 ? rollbackBase : `${rollbackBase}.${String(generation)}`;
      let handle: Awaited<ReturnType<typeof open>> | undefined;
      try {
        handle = await open(rollbackPath, "wx", 0o600);
      } catch (error) {
        if (isExistingPath(error)) continue;
        throw new KafkaProfileFileWriteError();
      }
      try {
        signal?.throwIfAborted();
        await handle.writeFile(original);
        await handle.chmod(0o600);
        await handle.sync();
        await handle.close();
        return { created: true, generation: basename(rollbackPath) };
      } catch {
        await handle.close().catch(() => undefined);
        await unlink(rollbackPath).catch(() => undefined);
        throw new KafkaProfileFileWriteError();
      }
    }
    throw new KafkaProfileFileWriteError(
      undefined,
      "All 100 profile rollback generations already exist. Preserve them, remove or relocate only generations no longer required for recovery, then retry.",
    );
  }

  private async preserveUpgradeBackup(signal?: AbortSignal): Promise<void> {
    let original: Buffer;
    try {
      original = await readBoundedFile(this.path, this.maximumFileBytes, {
        rejectSymlinks: true,
        signal,
      });
    } catch (error) {
      if (isMissingFile(error)) return;
      throw error;
    }
    const validate = async (contents: Buffer): Promise<void> => {
      if (contents.length > this.maximumFileBytes) throw new KafkaProfileFileWriteError();
      const document = parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
      for (const profile of document.profiles) {
        signal?.throwIfAborted();
        const protectedValues = await this.protector.unprotect(
          base64Buffer(profile.protectedValue),
        );
        parseProtectedValues(protectedValues.plaintext, profile.id, document.version);
      }
    };
    const backupPath = `${this.path}.pre-upgrade.bak`;
    let handle: Awaited<ReturnType<typeof open>>;
    try {
      handle = await open(backupPath, "wx", 0o600);
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") throw error;
      await validate(
        await readBoundedFile(backupPath, this.maximumFileBytes, { rejectSymlinks: true, signal }),
      );
      return;
    }
    try {
      await validate(original);
      signal?.throwIfAborted();
      await handle.writeFile(original);
      await handle.sync();
    } finally {
      await handle.close();
    }
  }

  private markUnavailable(): void {
    this.currentCapability = {
      durability: "durable",
      protection: "unavailable",
      recovery:
        "Preserve the profile file, restore a known-good copy, or remove it manually after confirming a backup.",
      state: "unavailable",
    };
  }
}
