import { chmod, mkdir, open, readFile, rename, stat, lstat, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

import {
  CLUSTER_SERVICE_AUTHENTICATION_MODES,
  PROFILE_LIMITS,
  PROFILE_TRUST_KINDS,
  parseProfileAcquisitionBinding,
  type ProfileAcquisitionBinding,
  type HostErrorCode,
  type HostErrorStage,
  type ProfileStoreCapability,
  type ClusterServiceEndpointInput,
  type ClusterServiceEndpointsInput,
  type ProfileTrustKind,
} from "../../../features/kafka/contracts";
import type {
  KafkaProfileRecord,
  KafkaProfileStore,
  KafkaProfileStructuredError,
} from "../../../features/kafka/application";

const LEGACY_PROFILE_FILE_VERSION = 1 as const;
const PROFILE_FILE_VERSION = 2 as const;
const PROTECTED_PROFILE_VERSION = 4 as const;
const DEFAULT_MAXIMUM_FILE_BYTES = 64 * 1_048_576;
const MAXIMUM_PROTECTED_VALUE_BYTES = 32 * 1_048_576;

type UnknownRecord = Record<string, unknown>;

export interface KafkaProfileProtector {
  protect(plaintext: string): Promise<Buffer>;
  unprotect(
    protectedValue: Buffer,
  ): Promise<{ readonly plaintext: string; readonly shouldReEncrypt: boolean }>;
}

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
  readonly recovery =
    "Check application-data permissions and operating-system credential access, then retry.";

  constructor() {
    super("Kafka profile storage could not commit the requested change.");
  }
}

function defaultCreateTempId(): string {
  return globalThis.crypto.randomUUID();
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
  readonly services?: ClusterServiceEndpointsInput;
  readonly trust: {
    readonly kind: ProfileTrustKind;
    readonly label: string;
    readonly materialPresent: boolean;
    readonly passwordPresent: boolean;
  };
  readonly updatedAt: string;
}

function parseServiceEndpoint(value: unknown): ClusterServiceEndpointInput {
  const endpoint = valueRecord(value);
  exactKeys(endpoint, ["authentication", "baseUrl"]);
  const authentication = boundedText(endpoint.authentication, 16);
  if (
    !CLUSTER_SERVICE_AUTHENTICATION_MODES.includes(
      authentication as ClusterServiceEndpointInput["authentication"],
    )
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  return {
    authentication: authentication as ClusterServiceEndpointInput["authentication"],
    baseUrl: boundedText(endpoint.baseUrl, PROFILE_LIMITS.tokenEndpointCharacters),
  };
}

function parseServices(value: unknown): ClusterServiceEndpointsInput {
  const services = valueRecord(value);
  exactKeys(services, ["redpandaAdmin", "schemaRegistry"]);
  return {
    ...(Object.hasOwn(services, "redpandaAdmin")
      ? { redpandaAdmin: parseServiceEndpoint(services.redpandaAdmin) }
      : {}),
    ...(Object.hasOwn(services, "schemaRegistry")
      ? { schemaRegistry: parseServiceEndpoint(services.schemaRegistry) }
      : {}),
  };
}

function parseSafeProfile(value: unknown, version: 1 | 2): SafeStoredProfile {
  const profile = valueRecord(value);
  exactKeys(profile, [
    "brokers",
    "createdAt",
    "id",
    "name",
    "oauth",
    "protectedValue",
    ...(version === PROFILE_FILE_VERSION ? ["services"] : []),
    "trust",
    "updatedAt",
  ]);
  const trust = valueRecord(profile.trust);
  exactKeys(trust, ["kind", "label", "materialPresent", "passwordPresent"]);
  const base: SafeStoredProfile = {
    brokers: brokers(profile.brokers),
    createdAt: boundedText(profile.createdAt, 128),
    id: boundedText(profile.id, PROFILE_LIMITS.idCharacters),
    name: boundedText(profile.name, PROFILE_LIMITS.nameCharacters),
    protectedValue: boundedText(profile.protectedValue, MAXIMUM_PROTECTED_VALUE_BYTES * 2),
    ...(Object.hasOwn(profile, "services") ? { services: parseServices(profile.services) } : {}),
    trust: {
      kind: trustKind(trust.kind),
      label: boundedText(trust.label, PROFILE_LIMITS.trustLabelCharacters),
      materialPresent: booleanValue(trust.materialPresent),
      passwordPresent: booleanValue(trust.passwordPresent),
    },
    updatedAt: boundedText(profile.updatedAt, 128),
  };
  if (!Object.hasOwn(profile, "oauth")) {
    return base;
  }
  const oauth = valueRecord(profile.oauth);
  exactKeys(oauth, ["clientId", "clientSecretPresent", "scope", "tokenEndpoint"]);
  return {
    ...base,
    oauth: {
      clientId: boundedText(oauth.clientId, PROFILE_LIMITS.clientIdCharacters),
      clientSecretPresent: booleanValue(oauth.clientSecretPresent),
      scope: boundedText(oauth.scope, PROFILE_LIMITS.scopeCharacters),
      tokenEndpoint: boundedText(oauth.tokenEndpoint, PROFILE_LIMITS.tokenEndpointCharacters),
    },
  };
}

function parseDocument(value: unknown): readonly SafeStoredProfile[] {
  const document = valueRecord(value);
  exactKeys(document, ["profiles", "version"]);
  if (
    document.version !== LEGACY_PROFILE_FILE_VERSION &&
    document.version !== PROFILE_FILE_VERSION
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  if (!Array.isArray(document.profiles) || document.profiles.length > PROFILE_LIMITS.profiles) {
    throw new KafkaProfileFileCorruptError();
  }
  return document.profiles.map((profile) => parseSafeProfile(profile, document.version as 1 | 2));
}

interface ProtectedProfileValues {
  readonly apiCaPem?: string;
  readonly binding?: ProfileAcquisitionBinding;
  readonly revision?: number;
  readonly oauth?: {
    readonly clientSecret: string;
  };
  readonly profileId: string;
  readonly trust: {
    readonly material: string;
    readonly password?: string;
  };
}

function parseProtectedValues(value: string, expectedProfileId: string): ProtectedProfileValues {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value) as unknown;
  } catch {
    throw new KafkaProfileFileCorruptError();
  }
  const protectedValues = valueRecord(parsed);
  exactKeys(protectedValues, [
    "oauth",
    "profileId",
    "trust",
    "version",
    ...(protectedValues.version === 2 ||
    protectedValues.version === 3 ||
    protectedValues.version === PROTECTED_PROFILE_VERSION
      ? ["revision"]
      : []),
    ...(protectedValues.version === 3 || protectedValues.version === PROTECTED_PROFILE_VERSION
      ? ["binding"]
      : []),
    ...(protectedValues.version === PROTECTED_PROFILE_VERSION ? ["apiCaPem"] : []),
  ]);
  if (
    protectedValues.version !== 1 &&
    protectedValues.version !== 2 &&
    protectedValues.version !== 3 &&
    protectedValues.version !== PROTECTED_PROFILE_VERSION
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  const revision = protectedValues.version !== 1 ? protectedValues.revision : undefined;
  if (
    protectedValues.version !== 1 &&
    (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1)
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  const profileId = boundedText(protectedValues.profileId, PROFILE_LIMITS.idCharacters);
  if (profileId !== expectedProfileId) {
    throw new KafkaProfileFileCorruptError();
  }
  const trust = valueRecord(protectedValues.trust);
  exactKeys(trust, ["material", "password"]);
  const password = optionalText(trust, "password", PROFILE_LIMITS.clientSecretCharacters);
  const base: ProtectedProfileValues = {
    ...(protectedValues.version === 3 ||
    (protectedValues.binding !== undefined && protectedValues.version === PROTECTED_PROFILE_VERSION)
      ? { binding: parseProfileAcquisitionBinding(protectedValues.binding) }
      : {}),
    ...(protectedValues.apiCaPem === undefined
      ? {}
      : { apiCaPem: boundedText(protectedValues.apiCaPem, PROFILE_LIMITS.trustBinaryBytes) }),
    ...(typeof revision === "number" ? { revision } : {}),
    profileId,
    trust: {
      material: boundedText(trust.material, PROFILE_LIMITS.trustEncodedCharacters),
      ...(password === undefined ? {} : { password }),
    },
  };
  if (!Object.hasOwn(protectedValues, "oauth")) {
    return base;
  }
  const oauth = valueRecord(protectedValues.oauth);
  exactKeys(oauth, ["clientSecret"]);
  return {
    ...base,
    oauth: {
      clientSecret: boundedText(oauth.clientSecret, PROFILE_LIMITS.clientSecretCharacters),
    },
  };
}

function safeProfile(record: KafkaProfileRecord, protectedValue: Buffer): SafeStoredProfile {
  const base = {
    brokers: [...record.brokers],
    createdAt: record.createdAt,
    id: record.id,
    name: record.name,
    protectedValue: protectedValue.toString("base64"),
    ...(record.services === undefined ? {} : { services: record.services }),
    trust: {
      kind: record.trust.kind,
      label: record.trust.label,
      materialPresent: record.trust.material.length > 0,
      passwordPresent: record.trust.password !== undefined,
    },
    updatedAt: record.updatedAt,
  };
  return record.oauth === undefined
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
}

function protectedProfile(record: KafkaProfileRecord): string {
  if (
    (record.binding !== undefined || record.apiCaPem !== undefined) &&
    record.revision === undefined
  )
    throw new KafkaProfileFileWriteError();
  if (
    record.revision !== undefined &&
    (!Number.isSafeInteger(record.revision) || record.revision < 1)
  ) {
    throw new KafkaProfileFileWriteError();
  }
  return JSON.stringify({
    ...(record.apiCaPem === undefined
      ? {}
      : { apiCaPem: boundedText(record.apiCaPem, PROFILE_LIMITS.trustBinaryBytes) }),
    ...(record.binding === undefined
      ? {}
      : { binding: parseProfileAcquisitionBinding(record.binding) }),
    ...(record.revision === undefined ? {} : { revision: record.revision }),
    ...(record.oauth === undefined ? {} : { oauth: { clientSecret: record.oauth.clientSecret } }),
    profileId: record.id,
    trust: {
      material: record.trust.material,
      ...(record.trust.password === undefined ? {} : { password: record.trust.password }),
    },
    version:
      record.binding !== undefined || record.apiCaPem !== undefined
        ? PROTECTED_PROFILE_VERSION
        : record.revision === undefined
          ? 1
          : 2,
  });
}

function restoredRecord(
  safe: SafeStoredProfile,
  values: ProtectedProfileValues,
): KafkaProfileRecord {
  if (
    safe.trust.materialPresent !== values.trust.material.length > 0 ||
    safe.trust.passwordPresent !== (values.trust.password !== undefined) ||
    (safe.oauth === undefined) !== (values.oauth === undefined) ||
    (safe.oauth !== undefined &&
      safe.oauth.clientSecretPresent !== (values.oauth?.clientSecret.length ?? 0) > 0)
  ) {
    throw new KafkaProfileFileCorruptError();
  }
  return {
    brokers: safe.brokers,
    ...(values.apiCaPem === undefined ? {} : { apiCaPem: values.apiCaPem }),
    ...(values.binding === undefined ? {} : { binding: values.binding }),
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
    ...(safe.services === undefined ? {} : { services: safe.services }),
    trust: {
      kind: safe.trust.kind,
      label: safe.trust.label,
      material: values.trust.material,
      ...(values.trust.password === undefined ? {} : { password: values.trust.password }),
    },
    updatedAt: safe.updatedAt,
  };
}

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
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
    this.createTempId = options.createTempId ?? defaultCreateTempId;
    this.currentCapability = storeCapability;
    this.maximumFileBytes = options.maximumFileBytes ?? DEFAULT_MAXIMUM_FILE_BYTES;
  }

  capability(): ProfileStoreCapability {
    return this.currentCapability;
  }

  async commit(records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void> {
    try {
      signal?.throwIfAborted();
      if (records.length > PROFILE_LIMITS.profiles) {
        throw new KafkaProfileFileWriteError();
      }
      const profiles = await Promise.all(
        records.map(async (record) =>
          safeProfile(record, await this.protector.protect(protectedProfile(record))),
        ),
      );
      signal?.throwIfAborted();
      const serialized = `${JSON.stringify({
        profiles,
        version: PROFILE_FILE_VERSION,
      })}\n`;
      if (Buffer.byteLength(serialized, "utf8") > this.maximumFileBytes) {
        throw new KafkaProfileFileWriteError();
      }
      if (records.some((record) => record.revision !== undefined))
        await this.preserveUpgradeBackup(signal);
      await this.writeAtomically(serialized, signal);
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        throw error;
      }
      throw error instanceof KafkaProfileFileWriteError ? error : new KafkaProfileFileWriteError();
    }
  }

  async load(signal?: AbortSignal): Promise<readonly KafkaProfileRecord[]> {
    let contents: Buffer;
    try {
      signal?.throwIfAborted();
      const metadata = await stat(this.path);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes) {
        throw new KafkaProfileFileCorruptError();
      }
      contents = await readFile(this.path);
      signal?.throwIfAborted();
      if (contents.length > this.maximumFileBytes) {
        throw new KafkaProfileFileCorruptError();
      }
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
      const profiles = parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
      const restored: KafkaProfileRecord[] = [];
      let shouldReEncrypt = false;
      for (const profile of profiles) {
        signal?.throwIfAborted();
        const result = await this.protector.unprotect(base64Buffer(profile.protectedValue));
        shouldReEncrypt ||= result.shouldReEncrypt;
        restored.push(restoredRecord(profile, parseProtectedValues(result.plaintext, profile.id)));
      }
      if (shouldReEncrypt) {
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

  private async writeAtomically(serialized: string, signal?: AbortSignal): Promise<void> {
    const directory = dirname(this.path);
    await mkdir(directory, { mode: 0o700, recursive: true });
    await chmod(directory, 0o700);
    const temporaryPath = join(directory, `.${basename(this.path)}.${this.createTempId()}.tmp`);
    let handle: Awaited<ReturnType<typeof open>> | undefined;
    try {
      handle = await open(temporaryPath, "wx", 0o600);
      await handle.writeFile(serialized, "utf8");
      await handle.sync();
      await handle.close();
      handle = undefined;
      signal?.throwIfAborted();
      await rename(temporaryPath, this.path);
      await chmod(this.path, 0o600);
    } catch (error) {
      await handle?.close().catch(() => undefined);
      await unlink(temporaryPath).catch(() => undefined);
      throw error;
    }
  }

  private async preserveUpgradeBackup(signal?: AbortSignal): Promise<void> {
    let original: Buffer;
    try {
      const metadata = await lstat(this.path);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes)
        throw new KafkaProfileFileWriteError();
      original = await readFile(this.path);
    } catch (error) {
      if (isMissingFile(error)) return;
      throw error;
    }
    const validate = async (contents: Buffer): Promise<void> => {
      if (contents.length > this.maximumFileBytes) throw new KafkaProfileFileWriteError();
      const profiles = parseDocument(JSON.parse(contents.toString("utf8")) as unknown);
      for (const profile of profiles) {
        signal?.throwIfAborted();
        const protectedValues = await this.protector.unprotect(
          base64Buffer(profile.protectedValue),
        );
        parseProtectedValues(protectedValues.plaintext, profile.id);
      }
    };
    const backupPath = `${this.path}.pre-upgrade.bak`;
    let handle: Awaited<ReturnType<typeof open>>;
    try {
      handle = await open(backupPath, "wx", 0o600);
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") throw error;
      const metadata = await lstat(backupPath);
      if (!metadata.isFile() || metadata.size > this.maximumFileBytes)
        throw new KafkaProfileFileWriteError();
      await validate(await readFile(backupPath));
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
