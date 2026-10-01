import {
  PROFILE_LIMITS,
  parseProfileAcquisitionBinding,
  type ProfileBindingInput,
  type ProfileBindingDetail,
  type ProfileAcquisitionBinding,
  type ProfileCreateInput,
  type ProfileStoreCapability,
  type ProfileSource,
  type ProfileTestInput,
  type ProfileUpdateInput,
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
import { KafkaProfileDraftResolver } from "./profile-draft-resolver";
import {
  assertProfileRevision,
  createIssues,
  normalizedName,
  validStoredRecords,
} from "./profile-validation";
import {
  kafkaProfileDraftConnection,
  kafkaProfileRecordFromDraft,
  kafkaProfileSummary,
  type KafkaProfileRecord,
  type KafkaProfileServiceOptions,
  type KafkaProfileSnapshot,
  type KafkaProfileStore,
  type KafkaProfileTrustDecoder,
} from "./profile-types";

function defaultCreateId(): string {
  return globalThis.crypto.randomUUID();
}

function defaultNow(): Date {
  return new Date();
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
  private readonly drafts: KafkaProfileDraftResolver;

  constructor(
    private readonly store: KafkaProfileStore,
    private readonly trustDecoder: KafkaProfileTrustDecoder,
    options: KafkaProfileServiceOptions = {},
  ) {
    this.createId = options.createId ?? defaultCreateId;
    this.now = options.now ?? defaultNow;
    this.trustAcquisitions = options.trustAcquisitions;
    this.drafts = new KafkaProfileDraftResolver(trustDecoder, options);
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
    assertProfileRevision(profile, revision);
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
    assertProfileRevision(profile, revision);
    if (profile.transport === "plaintext")
      throw new KafkaProfileValidationError([
        {
          field: "transport",
          message: "Plaintext profiles do not own broker trust retrieval.",
        },
      ]);
    const binding = await this.drafts.resolveBinding(reference, profile.binding, signal);
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
    const base = {
      brokers: [...profile.brokers],
      name: profile.name,
      ...(profile.oauth === undefined ? {} : { oauth: { ...profile.oauth } }),
      ...(profile.services === undefined ? {} : { services: profile.services }),
    };
    if (profile.transport === "plaintext") {
      return { ...base, tls: { enabled: false } };
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
      ...base,
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
    readonly source?: ProfileSource;
  }> {
    const draft =
      input.mode === "create"
        ? await this.drafts.resolveCreateDraft(input.profile, signal)
        : await this.drafts.resolveUpdateDraft(
            input.profile,
            await this.existingProfile(input.profileId, signal),
            signal,
          );
    return {
      connection: kafkaProfileDraftConnection(draft),
      ...(draft.lifetimeSignal === undefined ? {} : { lifetimeSignal: draft.lifetimeSignal }),
      ...(draft.source === undefined ? {} : { source: draft.source }),
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
    const draft = await this.drafts.resolveCreateDraft(input, signal);
    signal = draft.lifetimeSignal ?? signal;
    signal?.throwIfAborted();
    const timestamp = this.now().toISOString();
    const record = kafkaProfileRecordFromDraft(draft, {
      createdAt: timestamp,
      id: this.createId(),
      revision: 1,
      updatedAt: timestamp,
    });
    const nextRecords = [...this.records, record];
    await this.store.commit(nextRecords, signal);
    if (draft.transport === "tls" && draft.acquisitionId !== undefined) {
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
    signal?.throwIfAborted();
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
    const draft = await this.drafts.resolveUpdateDraft(input, existing, signal);
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
    const updated = kafkaProfileRecordFromDraft(draft, {
      createdAt: existing.createdAt,
      id: existing.id,
      revision: (existing.revision ?? 1) + 1,
      updatedAt: this.now().toISOString(),
    });
    const nextRecords = this.records.map((record, index) =>
      index === recordIndex ? updated : record,
    );
    await this.store.commit(nextRecords, signal);
    if (draft.transport === "tls" && draft.acquisitionId !== undefined) {
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

  private async ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.loadPromise ??= this.load();
    await this.loadPromise;
    signal?.throwIfAborted();
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
      const normalized = records.map((record) =>
        record.transport === undefined ? { ...record, transport: "tls" as const } : record,
      );
      this.records = normalized.sort(
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
      profiles: this.records.map((record) => kafkaProfileSummary(record, this.activeProfileId)),
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
