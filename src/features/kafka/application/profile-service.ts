import {
  PROFILE_LIMITS,
  parseProfileAcquisitionBinding,
  parseProfileBindingInput,
  type ProfileBindingInput,
  type ProfileBindingDetail,
  type ProfileAcquisitionBinding,
  type ClusterServiceEndpointInput,
  type ClusterServiceEndpointsInput,
  type ProfileCreateInput,
  type ProfileStoreCapability,
  type ProfileSummary,
  type ProfileTestInput,
  type ProfileUpdateInput,
  type ProtectedValueUpdateInput,
  type SecureConnectionInput,
} from "../contracts";

import {
  ActiveKafkaProfileMutationError,
  DuplicateKafkaProfileError,
  KafkaProfileCapacityError,
  KafkaProfileNotFoundError,
  KafkaProfileStoreUnavailableError,
  KafkaProfileValidationError,
  KafkaProfileRevisionError,
} from "./profile-errors";
import { resolveCreateProfileTrust, resolveUpdateProfileTrust } from "./profile-trust-resolution";
import type {
  KafkaProfileIssue,
  KafkaProfileRecord,
  KafkaProfileServiceOptions,
  KafkaProfileSnapshot,
  KafkaProfileStore,
  KafkaProfileTrustDecoder,
} from "./profile-types";

interface ResolvedProfileDraft {
  readonly apiCaPem?: string;
  readonly lifetimeSignal?: AbortSignal;
  readonly binding?: ProfileAcquisitionBinding;
  readonly acquisitionId?: string;
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: NonNullable<SecureConnectionInput["oauth"]>;
  readonly services?: ClusterServiceEndpointsInput;
  readonly trust: KafkaProfileRecord["trust"] & {
    readonly caPem: string;
  };
}

function acquiredDraftSignal(
  signal: AbortSignal | undefined,
  lifetime: AbortSignal | undefined,
): AbortSignal | undefined {
  return lifetime === undefined
    ? signal
    : signal === undefined
      ? lifetime
      : AbortSignal.any([signal, lifetime]);
}

function draftConnection(draft: ResolvedProfileDraft): SecureConnectionInput {
  return {
    brokers: draft.brokers,
    name: draft.name,
    ...(draft.oauth === undefined ? {} : { oauth: draft.oauth }),
    ...(draft.services === undefined ? {} : { services: draft.services }),
    tls: {
      caPem: draft.trust.caPem,
      enabled: true,
    },
  };
}

function defaultCreateId(): string {
  return globalThis.crypto.randomUUID();
}

function defaultNow(): Date {
  return new Date();
}

function normalizedName(name: string): string {
  return name.normalize("NFKC").trim().toLocaleLowerCase("en-US");
}

function trustLabel(label: string): string {
  const segments = label.replaceAll("\\", "/").split("/");
  return segments.at(-1)?.trim() ?? "";
}

function validBroker(value: string): boolean {
  try {
    const url = new URL(`tcp://${value}`);
    return (
      url.hostname.length > 0 &&
      url.port.length > 0 &&
      Number(url.port) > 0 &&
      Number(url.port) <= 65_535 &&
      url.pathname === "" &&
      url.username.length === 0 &&
      url.password.length === 0
    );
  } catch {
    return false;
  }
}

function validHttpEndpoint(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.hostname.length > 0 &&
      url.username.length === 0 &&
      url.password.length === 0 &&
      url.hash.length === 0 &&
      url.search.length === 0
    );
  } catch {
    return false;
  }
}

function canonicalServiceEndpoint(
  endpoint: ClusterServiceEndpointInput,
): ClusterServiceEndpointInput {
  const url = new URL(endpoint.baseUrl.trim());
  url.pathname = url.pathname.replace(/\/+$/u, "") || "/";
  const serialized = url.toString();
  return {
    authentication: endpoint.authentication,
    baseUrl: url.pathname === "/" ? serialized.slice(0, -1) : serialized,
  };
}

function canonicalServices(
  services: ClusterServiceEndpointsInput | undefined,
): ClusterServiceEndpointsInput | undefined {
  if (services === undefined) {
    return undefined;
  }
  return {
    ...(services.redpandaAdmin === undefined
      ? {}
      : { redpandaAdmin: canonicalServiceEndpoint(services.redpandaAdmin) }),
    ...(services.schemaRegistry === undefined
      ? {}
      : { schemaRegistry: canonicalServiceEndpoint(services.schemaRegistry) }),
  };
}

function createIssues(
  input: ProfileCreateInput | ProfileUpdateInput,
  retainAllowed = false,
): readonly KafkaProfileIssue[] {
  const issues: KafkaProfileIssue[] = [];
  if (input.name.trim().length === 0 || input.name.length > PROFILE_LIMITS.nameCharacters) {
    issues.push({
      field: "name",
      message: `Enter a name no longer than ${PROFILE_LIMITS.nameCharacters} characters.`,
    });
  }
  if (input.brokers.length === 0 || input.brokers.length > PROFILE_LIMITS.brokers) {
    issues.push({
      field: "brokers",
      message: `Enter between 1 and ${PROFILE_LIMITS.brokers} bootstrap brokers.`,
    });
  }
  input.brokers.forEach((broker, index) => {
    if (broker.length > PROFILE_LIMITS.brokerCharacters || !validBroker(broker.trim())) {
      issues.push({
        field: `brokers[${index}]`,
        message: "Enter a host and port, for example broker.example.test:9093.",
      });
    }
  });
  if (trustLabel(input.trust.label).length === 0) {
    issues.push({ field: "trust.label", message: "Trust material must have a file label." });
  }
  if (
    input.trust.material.mode === "clear" ||
    (input.trust.material.mode === "retain" && !retainAllowed)
  ) {
    issues.push({
      field: "trust.material",
      message: "New profiles require trust material.",
    });
  }
  if (
    input.trust.kind === "pem" &&
    input.trust.password.mode !== "clear" &&
    !(retainAllowed && input.trust.password.mode === "retain")
  ) {
    issues.push({
      field: "trust.password",
      message: "PEM CA files do not use a truststore password.",
    });
  }
  if (
    input.trust.kind !== "pem" &&
    input.trust.password.mode !== "replace" &&
    input.trust.password.mode !== "acquired" &&
    !(retainAllowed && input.trust.password.mode === "retain")
  ) {
    issues.push({
      field: "trust.password",
      message: "JKS and PKCS12 truststores require a password.",
    });
  }
  if (input.oauth !== undefined) {
    if (
      input.oauth.clientId.trim().length === 0 ||
      input.oauth.clientId.length > PROFILE_LIMITS.clientIdCharacters
    ) {
      issues.push({ field: "oauth.clientId", message: "OAuth client ID is required." });
    }
    if (
      input.oauth.clientSecret.mode !== "replace" &&
      !(retainAllowed && input.oauth.clientSecret.mode === "retain")
    ) {
      issues.push({
        field: "oauth.clientSecret",
        message: "New OAuth profiles require a client secret.",
      });
    }
    if (
      input.oauth.scope.trim().length === 0 ||
      input.oauth.scope.length > PROFILE_LIMITS.scopeCharacters
    ) {
      issues.push({ field: "oauth.scope", message: "OAuth scope is required." });
    }
    if (
      input.oauth.tokenEndpoint.length > PROFILE_LIMITS.tokenEndpointCharacters ||
      !validHttpEndpoint(input.oauth.tokenEndpoint.trim())
    ) {
      issues.push({
        field: "oauth.tokenEndpoint",
        message: "Enter an HTTP or HTTPS OAuth token endpoint.",
      });
    }
  }
  const services = [
    ["redpandaAdmin", input.services?.redpandaAdmin],
    ["schemaRegistry", input.services?.schemaRegistry],
  ] as const;
  for (const [serviceName, service] of services) {
    if (service === undefined) {
      continue;
    }
    if (
      service.baseUrl.length > PROFILE_LIMITS.tokenEndpointCharacters ||
      !validHttpEndpoint(service.baseUrl.trim())
    ) {
      issues.push({
        field: `services.${serviceName}.baseUrl`,
        message: "Enter an HTTP or HTTPS service base URL without credentials, query, or fragment.",
      });
    }
    if (service.authentication === "oauth" && input.oauth === undefined) {
      issues.push({
        field: `services.${serviceName}.authentication`,
        message: "OAuth service authentication requires complete profile OAuth settings.",
      });
    }
  }
  return issues;
}

function toSummary(record: KafkaProfileRecord, activeProfileId?: string): ProfileSummary {
  const base = {
    ...(record.revision === undefined ? {} : { revision: record.revision }),
    active: record.id === activeProfileId,
    brokers: [...record.brokers],
    createdAt: record.createdAt,
    id: record.id,
    name: record.name,
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

function resolveProtectedValue(
  input: ProtectedValueUpdateInput,
  existing: string | undefined,
): string | undefined {
  switch (input.mode) {
    case "clear":
      return undefined;
    case "replace":
      return input.value;
    case "retain":
      return existing;
  }
}

function storedRecordInput(record: KafkaProfileRecord): ProfileCreateInput {
  return {
    brokers: record.brokers,
    name: record.name,
    ...(record.services === undefined ? {} : { services: record.services }),
    ...(record.oauth === undefined
      ? {}
      : {
          oauth: {
            clientId: record.oauth.clientId,
            clientSecret:
              record.oauth.clientSecret.length === 0
                ? ({ mode: "clear" } as const)
                : ({ mode: "replace", value: record.oauth.clientSecret } as const),
            scope: record.oauth.scope,
            tokenEndpoint: record.oauth.tokenEndpoint,
          },
        }),
    trust: {
      kind: record.trust.kind,
      label: record.trust.label,
      material:
        record.trust.material.length === 0
          ? { mode: "clear" }
          : { mode: "replace", value: record.trust.material },
      password:
        record.trust.password === undefined
          ? { mode: "clear" }
          : { mode: "replace", value: record.trust.password },
    },
  };
}

function canonicalTimestamp(value: string): boolean {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value;
}

function validStoredRecords(records: readonly KafkaProfileRecord[]): boolean {
  const ids = new Set<string>();
  const names = new Set<string>();
  for (const record of records) {
    const name = normalizedName(record.name);
    if (
      (record.revision !== undefined &&
        (!Number.isSafeInteger(record.revision) || record.revision < 1)) ||
      record.id.trim().length === 0 ||
      record.id !== record.id.trim() ||
      ids.has(record.id) ||
      names.has(name) ||
      record.name !== record.name.normalize("NFKC").trim() ||
      record.trust.label !== trustLabel(record.trust.label) ||
      record.brokers.some((broker) => broker !== broker.trim()) ||
      !canonicalTimestamp(record.createdAt) ||
      !canonicalTimestamp(record.updatedAt) ||
      record.createdAt > record.updatedAt ||
      createIssues(storedRecordInput(record)).length > 0
    ) {
      return false;
    }
    ids.add(record.id);
    names.add(name);
  }
  return true;
}

export class KafkaProfileService {
  private activeProfileId: string | undefined;
  private readonly createId;
  private loadPromise: Promise<void> | undefined;
  private mutationTail: Promise<void> = Promise.resolve();
  private readonly now;
  private profileDataUnavailable = false;
  private records: readonly KafkaProfileRecord[] = [];
  private readonly trustAcquisitions;
  private readonly resolveRecipe;

  constructor(
    private readonly store: KafkaProfileStore,
    private readonly trustDecoder: KafkaProfileTrustDecoder,
    options: KafkaProfileServiceOptions = {},
  ) {
    this.createId = options.createId ?? defaultCreateId;
    this.now = options.now ?? defaultNow;
    this.trustAcquisitions = options.trustAcquisitions;
    this.resolveRecipe = options.resolveRecipe;
  }

  create(input: ProfileCreateInput, signal?: AbortSignal): Promise<KafkaProfileSnapshot> {
    return this.mutate(() => this.completeCreate(input, signal), signal);
  }

  clearActive(): KafkaProfileSnapshot {
    this.activeProfileId = undefined;
    return this.snapshot();
  }

  currentSnapshot(): KafkaProfileSnapshot {
    return this.snapshot();
  }

  delete(profileId: string, signal?: AbortSignal): Promise<KafkaProfileSnapshot> {
    return this.mutate(() => this.completeDelete(profileId, signal), signal);
  }

  async list(signal?: AbortSignal): Promise<KafkaProfileSnapshot> {
    await this.ensureLoaded(signal);
    return this.snapshot();
  }

  async assertAcquisitionUsageReadable(signal?: AbortSignal): Promise<void> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
  }

  async recipeUsage(
    recipeId: string,
    signal?: AbortSignal,
  ): Promise<readonly { id: string; name: string; revision: number }[]> {
    await this.assertAcquisitionUsageReadable(signal);
    return this.records
      .filter((profile) => profile.binding?.recipe.id === recipeId)
      .map((profile) => ({ id: profile.id, name: profile.name, revision: profile.revision ?? 1 }));
  }

  withConfirmedRecipeUsage<T>(
    recipeId: string,
    confirmedProfileIds: readonly string[],
    work: () => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    return this.mutate(async () => {
      const actual = (await this.recipeUsage(recipeId, signal)).map((profile) => profile.id).sort();
      if (JSON.stringify(actual) !== JSON.stringify([...confirmedProfileIds].sort()))
        throw new KafkaProfileValidationError([
          {
            field: "binding",
            message: "Template usage changed. Review affected profiles before deleting.",
          },
        ]);
      return work();
    }, signal);
  }

  async bindingDetail(profileId: string, signal?: AbortSignal): Promise<ProfileBindingDetail> {
    const profile = await this.existingProfile(profileId, signal);
    return {
      profileId: profile.id,
      ...(profile.apiCaPem === undefined ? {} : { apiCaPresent: true }),
      revision: profile.revision ?? 1,
      binding:
        profile.binding === undefined ? null : parseProfileAcquisitionBinding(profile.binding),
    };
  }

  async resolveAcquisitionApiCa(
    profileId: string,
    revision: number,
    signal?: AbortSignal,
  ): Promise<string> {
    const profile = await this.existingProfile(profileId, signal);
    this.assertRevision(profile, revision);
    if (profile.apiCaPem === undefined) throw new KafkaProfileRevisionError();
    return profile.apiCaPem;
  }

  async resolveAcquisitionBinding(
    profileId: string,
    revision: number,
    reference: Extract<ProfileBindingInput, { readonly mode: "replace" }>,
    signal?: AbortSignal,
  ): Promise<ProfileAcquisitionBinding> {
    const profile = await this.existingProfile(profileId, signal);
    if (this.activeProfileId === profileId) throw new ActiveKafkaProfileMutationError(profile.name);
    this.assertRevision(profile, revision);
    const binding = await this.resolveBinding(reference, profile.binding, signal);
    if (binding === undefined) throw new KafkaProfileRevisionError();
    return binding;
  }

  update(
    profileId: string,
    input: ProfileUpdateInput,
    signal?: AbortSignal,
  ): Promise<KafkaProfileSnapshot> {
    return this.mutate(() => this.completeUpdate(profileId, input, signal), signal);
  }

  markActive(profileId: string, signal?: AbortSignal): Promise<KafkaProfileSnapshot> {
    return this.mutate(() => this.completeMarkActive(profileId, signal), signal);
  }

  async resolveConnection(profileId: string, signal?: AbortSignal): Promise<SecureConnectionInput> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const profile = this.records.find((record) => record.id === profileId);
    if (profile === undefined) {
      throw new KafkaProfileNotFoundError(profileId);
    }
    const trust = await this.trustDecoder.decode(
      {
        kind: profile.trust.kind,
        material: profile.trust.material,
        ...(profile.trust.password === undefined ? {} : { password: profile.trust.password }),
      },
      signal,
    );
    signal?.throwIfAborted();
    return {
      brokers: [...profile.brokers],
      name: profile.name,
      ...(profile.oauth === undefined ? {} : { oauth: { ...profile.oauth } }),
      ...(profile.services === undefined ? {} : { services: profile.services }),
      tls: {
        caPem: trust.caPem,
        enabled: true,
      },
    };
  }

  async resolveTestContext(
    input: ProfileTestInput,
    signal?: AbortSignal,
  ): Promise<{
    readonly connection: SecureConnectionInput;
    readonly lifetimeSignal?: AbortSignal;
  }> {
    const draft =
      input.mode === "create"
        ? await this.resolveCreateDraft(input.profile, signal)
        : await this.resolveUpdateDraft(
            input.profile,
            await this.existingProfile(input.profileId, signal),
            signal,
          );
    return {
      connection: draftConnection(draft),
      ...(draft.lifetimeSignal === undefined ? {} : { lifetimeSignal: draft.lifetimeSignal }),
    };
  }

  private async completeCreate(
    input: ProfileCreateInput,
    signal?: AbortSignal,
  ): Promise<KafkaProfileSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    if (this.records.length >= PROFILE_LIMITS.profiles) {
      throw new KafkaProfileCapacityError(PROFILE_LIMITS.profiles);
    }
    const issues = createIssues(input);
    if (issues.length > 0) {
      throw new KafkaProfileValidationError(issues);
    }
    const name = input.name.normalize("NFKC").trim();
    if (this.records.some((record) => normalizedName(record.name) === normalizedName(name))) {
      throw new DuplicateKafkaProfileError(name);
    }
    const draft = await this.resolveCreateDraft(input, signal);
    signal = draft.lifetimeSignal ?? signal;
    signal?.throwIfAborted();
    const timestamp = this.now().toISOString();
    const record: KafkaProfileRecord = {
      ...(draft.apiCaPem === undefined ? {} : { apiCaPem: draft.apiCaPem }),
      ...(draft.binding === undefined ? {} : { binding: draft.binding }),
      revision: 1,
      brokers: draft.brokers,
      createdAt: timestamp,
      id: this.createId(),
      name: draft.name,
      ...(draft.oauth === undefined ? {} : { oauth: draft.oauth }),
      ...(draft.services === undefined ? {} : { services: draft.services }),
      trust: {
        kind: draft.trust.kind,
        label: draft.trust.label,
        material: draft.trust.material,
        ...(draft.trust.password === undefined ? {} : { password: draft.trust.password }),
      },
      updatedAt: timestamp,
    };
    const nextRecords = [...this.records, record];
    await this.store.commit(nextRecords, signal);
    if (draft.acquisitionId !== undefined) {
      this.trustAcquisitions?.consume(draft.acquisitionId);
    }
    this.records = nextRecords;
    return this.snapshot();
  }

  private async completeDelete(
    profileId: string,
    signal?: AbortSignal,
  ): Promise<KafkaProfileSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const existing = this.records.find((record) => record.id === profileId);
    if (existing === undefined) {
      throw new KafkaProfileNotFoundError(profileId);
    }
    if (this.activeProfileId === profileId) {
      throw new ActiveKafkaProfileMutationError(existing.name);
    }
    const nextRecords = this.records.filter((record) => record.id !== profileId);
    await this.store.commit(nextRecords, signal);
    this.records = nextRecords;
    return this.snapshot();
  }

  private async completeMarkActive(
    profileId: string,
    signal?: AbortSignal,
  ): Promise<KafkaProfileSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    if (!this.records.some((record) => record.id === profileId)) {
      throw new KafkaProfileNotFoundError(profileId);
    }
    this.activeProfileId = profileId;
    return this.snapshot();
  }

  private async completeUpdate(
    profileId: string,
    input: ProfileUpdateInput,
    signal?: AbortSignal,
  ): Promise<KafkaProfileSnapshot> {
    const existing = await this.existingProfile(profileId, signal);
    const recordIndex = this.records.findIndex((record) => record.id === profileId);
    if (this.activeProfileId === profileId) {
      throw new ActiveKafkaProfileMutationError(existing.name);
    }
    const draft = await this.resolveUpdateDraft(input, existing, signal);
    signal = draft.lifetimeSignal ?? signal;
    signal?.throwIfAborted();
    if (
      this.records.some(
        (record) =>
          record.id !== profileId && normalizedName(record.name) === normalizedName(draft.name),
      )
    ) {
      throw new DuplicateKafkaProfileError(draft.name);
    }
    const updated: KafkaProfileRecord = {
      ...(draft.apiCaPem === undefined ? {} : { apiCaPem: draft.apiCaPem }),
      ...(draft.binding === undefined ? {} : { binding: draft.binding }),
      revision: (existing.revision ?? 1) + 1,
      brokers: draft.brokers,
      createdAt: existing.createdAt,
      id: existing.id,
      name: draft.name,
      ...(draft.oauth === undefined ? {} : { oauth: draft.oauth }),
      ...(draft.services === undefined ? {} : { services: draft.services }),
      trust: {
        kind: draft.trust.kind,
        label: draft.trust.label,
        material: draft.trust.material,
        ...(draft.trust.password === undefined ? {} : { password: draft.trust.password }),
      },
      updatedAt: this.now().toISOString(),
    };
    const nextRecords = this.records.map((record, index) =>
      index === recordIndex ? updated : record,
    );
    await this.store.commit(nextRecords, signal);
    if (draft.acquisitionId !== undefined) {
      this.trustAcquisitions?.consume(draft.acquisitionId);
    }
    this.records = nextRecords;
    return this.snapshot();
  }

  private async existingProfile(
    profileId: string,
    signal?: AbortSignal,
  ): Promise<KafkaProfileRecord> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const existing = this.records.find((record) => record.id === profileId);
    if (existing === undefined) {
      throw new KafkaProfileNotFoundError(profileId);
    }
    return existing;
  }

  private async resolveBinding(
    input: ProfileBindingInput | undefined,
    existing: ProfileAcquisitionBinding | undefined,
    signal?: AbortSignal,
  ): Promise<ProfileAcquisitionBinding | undefined> {
    if (input === undefined)
      return existing === undefined ? undefined : parseProfileAcquisitionBinding(existing);
    const parsed = parseProfileBindingInput(input);
    if (parsed.mode === "clear") return undefined;
    const access = parsed.access === undefined ? existing?.access : parsed.access;
    const apiAccess = parsed.apiAccess === undefined ? existing?.apiAccess : parsed.apiAccess;
    const withIdentity = (
      recipe: ProfileAcquisitionBinding["recipe"],
    ): ProfileAcquisitionBinding => {
      const candidate =
        parsed.identity?.mode === "acquired"
          ? this.trustAcquisitions?.resolve(
              parsed.identity.acquisitionId,
              recipe.kind,
              parsed.identity.editorId,
            )
          : undefined;
      const identity =
        parsed.identity === undefined
          ? existing?.identity
          : parsed.identity.mode === "reset"
            ? undefined
            : candidate?.identity;
      const retrievalAccess = parsed.access === undefined ? (candidate?.access ?? access) : access;
      if (parsed.identity?.mode === "acquired" && identity === undefined)
        throw new KafkaProfileValidationError([
          {
            field: "binding",
            message:
              "Acquire and apply a complete trust candidate before retaining its SSH identity.",
          },
        ]);
      if (
        parsed.identity?.mode === "acquired" &&
        existing?.identity !== undefined &&
        identity !== undefined &&
        existing.identity.host === identity.host &&
        existing.identity.port === identity.port &&
        existing.identity.fingerprint !== identity.fingerprint
      )
        throw new KafkaProfileValidationError([
          {
            field: "binding",
            message:
              "The saved SSH identity changed. Verify it independently and explicitly reset the saved identity before acquisition.",
          },
        ]);
      return parseProfileAcquisitionBinding({
        recipe,
        ...(apiAccess == null ? {} : { apiAccess }),
        overrides: parsed.overrides,
        ...(retrievalAccess == null ? {} : { access: retrievalAccess }),
        ...(identity === undefined ? {} : { identity }),
      });
    };
    if (
      existing?.recipe.id === parsed.recipeId &&
      existing.recipe.revision === parsed.recipeRevision
    ) {
      return withIdentity(existing.recipe);
    }
    if (this.resolveRecipe === undefined)
      throw new KafkaProfileValidationError([
        {
          field: "binding",
          message: "Template resolution is unavailable. Refresh the application host.",
        },
      ]);
    const recipe = await this.resolveRecipe(parsed.recipeId, parsed.recipeRevision, signal);
    signal?.throwIfAborted();
    return withIdentity(recipe);
  }

  private async resolveApiCa(
    input: ProfileUpdateInput["apiCa"],
    existing: string | undefined,
    signal?: AbortSignal,
  ): Promise<string | undefined> {
    if (input === undefined || input.mode === "retain") return existing;
    if (input.mode === "clear") return undefined;
    if (
      input.value.length === 0 ||
      new TextEncoder().encode(input.value).byteLength > PROFILE_LIMITS.trustBinaryBytes
    )
      throw new KafkaProfileValidationError([
        { field: "apiCa", message: "API CA must be a non-empty bounded PEM certificate bundle." },
      ]);
    const decoded = await this.trustDecoder.decode({ kind: "pem", material: input.value }, signal);
    signal?.throwIfAborted();
    return decoded.caPem;
  }

  private async resolveCreateDraft(
    input: ProfileCreateInput,
    signal?: AbortSignal,
  ): Promise<ResolvedProfileDraft> {
    const issues = createIssues(input);
    if (issues.length > 0) {
      throw new KafkaProfileValidationError(issues);
    }
    const binding = await this.resolveBinding(input.binding, undefined, signal);
    const apiCaPem = await this.resolveApiCa(input.apiCa, undefined, signal);
    signal?.throwIfAborted();
    const trust = resolveCreateProfileTrust(input.trust, this.trustAcquisitions);
    const effectiveSignal = acquiredDraftSignal(signal, trust.lifetimeSignal);
    const material = trust.material ?? "";
    const decoded = await this.trustDecoder.decode(
      {
        kind: input.trust.kind,
        material,
        ...(trust.password === undefined ? {} : { password: trust.password }),
      },
      effectiveSignal,
    );
    effectiveSignal?.throwIfAborted();
    const oauth =
      input.oauth === undefined || input.oauth.clientSecret.mode !== "replace"
        ? undefined
        : {
            clientId: input.oauth.clientId.trim(),
            clientSecret: input.oauth.clientSecret.value,
            scope: input.oauth.scope.trim(),
            tokenEndpoint: input.oauth.tokenEndpoint.trim(),
          };
    return {
      ...(trust.acquisitionId === undefined ? {} : { acquisitionId: trust.acquisitionId }),
      ...(apiCaPem === undefined ? {} : { apiCaPem }),
      ...(effectiveSignal === undefined ? {} : { lifetimeSignal: effectiveSignal }),
      ...(binding === undefined ? {} : { binding }),
      brokers: input.brokers.map((broker) => broker.trim()),
      name: input.name.normalize("NFKC").trim(),
      ...(oauth === undefined ? {} : { oauth }),
      ...(input.services === undefined ? {} : { services: canonicalServices(input.services)! }),
      trust: {
        caPem: decoded.caPem,
        kind: decoded.kind,
        label: trustLabel(input.trust.label),
        material,
        ...(trust.password === undefined ? {} : { password: trust.password }),
      },
    };
  }

  private async resolveUpdateDraft(
    input: ProfileUpdateInput,
    existing: KafkaProfileRecord,
    signal?: AbortSignal,
  ): Promise<ResolvedProfileDraft> {
    this.assertRevision(existing, input.expectedRevision);
    const preliminaryIssues = createIssues(input, true);
    if (preliminaryIssues.length > 0) {
      throw new KafkaProfileValidationError(preliminaryIssues);
    }
    const binding = await this.resolveBinding(input.binding, existing.binding, signal);
    const apiCaPem = await this.resolveApiCa(input.apiCa, existing.apiCaPem, signal);
    const trust = resolveUpdateProfileTrust(input.trust, existing.trust, this.trustAcquisitions);
    const effectiveSignal = acquiredDraftSignal(signal, trust.lifetimeSignal);
    const material = trust.material;
    const password = trust.password;
    const clientSecret =
      input.oauth === undefined
        ? undefined
        : resolveProtectedValue(input.oauth.clientSecret, existing.oauth?.clientSecret);
    const validationInput: ProfileCreateInput = {
      brokers: input.brokers,
      name: input.name,
      ...(input.services === undefined ? {} : { services: input.services }),
      ...(input.oauth === undefined
        ? {}
        : {
            oauth: {
              clientId: input.oauth.clientId,
              clientSecret:
                clientSecret === undefined
                  ? { mode: "clear" }
                  : { mode: "replace", value: clientSecret },
              scope: input.oauth.scope,
              tokenEndpoint: input.oauth.tokenEndpoint,
            },
          }),
      trust: {
        kind: input.trust.kind,
        label: input.trust.label,
        material: material === undefined ? { mode: "clear" } : { mode: "replace", value: material },
        password: password === undefined ? { mode: "clear" } : { mode: "replace", value: password },
      },
    };
    const issues = createIssues(validationInput);
    if (issues.length > 0) {
      throw new KafkaProfileValidationError(issues);
    }
    signal?.throwIfAborted();
    const decoded = await this.trustDecoder.decode(
      {
        kind: input.trust.kind,
        material: material ?? "",
        ...(password === undefined ? {} : { password }),
      },
      effectiveSignal,
    );
    effectiveSignal?.throwIfAborted();
    const oauth =
      input.oauth === undefined || clientSecret === undefined
        ? undefined
        : {
            clientId: input.oauth.clientId.trim(),
            clientSecret,
            scope: input.oauth.scope.trim(),
            tokenEndpoint: input.oauth.tokenEndpoint.trim(),
          };
    return {
      ...(trust.acquisitionId === undefined ? {} : { acquisitionId: trust.acquisitionId }),
      ...(apiCaPem === undefined ? {} : { apiCaPem }),
      ...(effectiveSignal === undefined ? {} : { lifetimeSignal: effectiveSignal }),
      ...(binding === undefined ? {} : { binding }),
      brokers: input.brokers.map((broker) => broker.trim()),
      name: input.name.normalize("NFKC").trim(),
      ...(oauth === undefined ? {} : { oauth }),
      ...(input.services === undefined ? {} : { services: canonicalServices(input.services)! }),
      trust: {
        caPem: decoded.caPem,
        kind: decoded.kind,
        label: trustLabel(input.trust.label),
        material: material ?? "",
        ...(password === undefined ? {} : { password }),
      },
    };
  }

  private async ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.loadPromise ??= this.load();
    await this.loadPromise;
    signal?.throwIfAborted();
  }

  private assertRevision(existing: KafkaProfileRecord, expected: number | undefined): void {
    const revision = existing.revision ?? 1;
    if (
      revision >= Number.MAX_SAFE_INTEGER ||
      (existing.revision !== undefined && expected === undefined) ||
      (expected !== undefined && expected !== revision)
    )
      throw new KafkaProfileRevisionError();
  }

  private assertStoreAvailable(): void {
    if (this.storeCapability().state === "unavailable") {
      throw new KafkaProfileStoreUnavailableError();
    }
  }

  private async load(): Promise<void> {
    try {
      const records = await this.store.load();
      for (const record of records) {
        if (record.binding !== undefined) parseProfileAcquisitionBinding(record.binding);
      }
      if (!validStoredRecords(records)) {
        throw new KafkaProfileStoreUnavailableError();
      }
      this.records = [...records].sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      );
    } catch (error) {
      this.profileDataUnavailable = true;
      this.records = [];
      throw error;
    }
  }

  private mutate<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const result = this.mutationTail.then(async () => {
      signal?.throwIfAborted();
      return operation();
    });
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private snapshot(): KafkaProfileSnapshot {
    return {
      profiles: this.records.map((record) => toSummary(record, this.activeProfileId)),
      store: this.storeCapability(),
    };
  }

  private storeCapability(): ProfileStoreCapability {
    const capability = this.store.capability();
    if (!this.profileDataUnavailable || capability.state === "unavailable") {
      return capability;
    }
    return {
      durability: capability.durability,
      protection: "unavailable",
      recovery:
        "Preserve the profile data, correct it outside the running application, then restart StreamSkope.",
      state: "unavailable",
    };
  }
}
