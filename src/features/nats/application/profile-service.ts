import { SerialMutationQueue } from "../../../platform/providers/operation-ownership";
import {
  NATS_LIMITS,
  parseNatsProfileCreateInput,
  parseNatsProfileUpdateInput,
  parseNatsProfileStoreCapability,
  type NatsProfileCreateInput,
  type NatsProfileUpdateInput,
  type NatsProfileSummary,
  type NatsProfileStoreCapability,
  type NatsProfilesSnapshot,
  type NatsUpdateSecret,
} from "../contracts";
import { natsIdentifier, natsInteger } from "../contracts/validation-primitives";

import { parseNatsProfileRecord, parseNatsProfileRecords } from "./profile-record-validation";
import {
  NatsProfileError,
  type NatsConnectionInput,
  type NatsProfileRecord,
  type NatsProfileStore,
  type NatsResolvedProfile,
} from "./profile-types";

export interface NatsProfileServiceOptions {
  readonly createId?: () => string;
  readonly now?: () => Date;
  readonly isProfileInUse?: (id: string) => boolean;
}
function storageFailure(cause?: unknown): NatsProfileError {
  return new NatsProfileError(
    "storage-unavailable",
    "Protected NATS profile storage is unavailable.",
    "Unlock or configure protected profile storage, then restart StreamSkope.",
    { cause },
  );
}
function validationFailure(cause?: unknown): NatsProfileError {
  return new NatsProfileError(
    "validation",
    "Correct the NATS profile fields before saving.",
    "Use a unique name, supported server URLs and the declared authentication and TLS options.",
    { cause },
  );
}
function summary(record: NatsProfileRecord): NatsProfileSummary {
  return {
    id: record.id,
    revision: record.revision,
    name: record.name,
    servers: [...record.servers],
    authentication:
      record.authentication.mode === "none"
        ? { mode: "none" }
        : { mode: "token", tokenPresent: true },
    tls:
      record.tls.mode === "plaintext"
        ? { mode: "plaintext" }
        : { mode: "tls", caPresent: record.tls.caPem !== undefined },
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}
function secret(input: NatsUpdateSecret, previous: string | undefined): string | undefined {
  switch (input.mode) {
    case "clear":
      return undefined;
    case "replace":
      return input.value;
    case "retain":
      return previous;
  }
}
function connection(
  input: NatsProfileCreateInput | NatsProfileUpdateInput,
  previous?: NatsProfileRecord,
): NatsConnectionInput {
  const token =
    input.authentication.mode === "token"
      ? secret(
          input.authentication.token,
          previous?.authentication.mode === "token" ? previous.authentication.token : undefined,
        )
      : undefined;
  if (input.authentication.mode === "token" && token === undefined) throw validationFailure();
  const caPem =
    input.tls.mode === "tls"
      ? secret(input.tls.caPem, previous?.tls.mode === "tls" ? previous.tls.caPem : undefined)
      : undefined;
  return {
    servers: [...input.servers],
    authentication: token === undefined ? { mode: "none" } : { mode: "token", token },
    tls:
      input.tls.mode === "plaintext"
        ? { mode: "plaintext" }
        : { mode: "tls", ...(caPem === undefined ? {} : { caPem }) },
  };
}

/** Committed memory and revision authority sit above the private encrypted document adapter. */
export class NatsProfileService {
  private readonly mutations = new SerialMutationQueue();
  private readonly createId;
  private readonly now;
  private readonly isProfileInUse;
  private loadPromise: Promise<void> | undefined;
  private records: readonly NatsProfileRecord[] = [];
  private dataUnavailable = false;
  private snapshotRevision = 0;
  private snapshotCapability: NatsProfileStoreCapability | undefined;

  constructor(
    private readonly store: NatsProfileStore,
    options: NatsProfileServiceOptions = {},
  ) {
    this.createId = options.createId ?? ((): string => globalThis.crypto.randomUUID());
    this.now = options.now ?? ((): Date => new Date());
    this.isProfileInUse = options.isProfileInUse ?? ((): boolean => false);
  }

  async list(signal?: AbortSignal): Promise<NatsProfilesSnapshot> {
    signal?.throwIfAborted();
    try {
      await this.ensureLoaded(signal);
    } catch (cause) {
      if (signal?.aborted) throw signal.reason;
      if (!this.dataUnavailable) throw cause;
    }
    signal?.throwIfAborted();
    return this.snapshot();
  }

  create(input: NatsProfileCreateInput, signal?: AbortSignal): Promise<NatsProfilesSnapshot> {
    return this.mutations.enqueue(async () => {
      await this.ensureLoaded(signal);
      signal?.throwIfAborted();
      this.assertAvailable();
      let parsed: NatsProfileCreateInput;
      try {
        parsed = parseNatsProfileCreateInput(input);
      } catch (cause) {
        throw validationFailure(cause);
      }
      if (this.records.length >= NATS_LIMITS.profiles)
        throw new NatsProfileError(
          "profile-capacity",
          "The NATS profile limit has been reached.",
          "Delete an unused profile before creating another.",
        );
      this.assertUniqueName(parsed.name);
      const timestamp = this.now().toISOString();
      let created: NatsProfileRecord;
      try {
        created = parseNatsProfileRecord({
          ...connection(parsed),
          id: natsIdentifier(this.createId()),
          revision: 1,
          name: parsed.name,
          createdAt: timestamp,
          updatedAt: timestamp,
        });
      } catch (cause) {
        throw validationFailure(cause);
      }
      if (this.records.some((record) => record.id === created.id)) throw validationFailure();
      return this.commit([...this.records, created], signal);
    }, signal);
  }

  update(
    profileId: string,
    expectedRevision: number,
    input: NatsProfileUpdateInput,
    signal?: AbortSignal,
  ): Promise<NatsProfilesSnapshot> {
    return this.mutations.enqueue(async () => {
      await this.ensureLoaded(signal);
      signal?.throwIfAborted();
      this.assertAvailable();
      const existing = this.find(profileId, expectedRevision);
      this.assertMutable(existing.id);
      let parsed: NatsProfileUpdateInput;
      let updated: NatsProfileRecord;
      try {
        parsed = parseNatsProfileUpdateInput(input);
        const timestamp = new Date(
          Math.max(this.now().getTime(), Date.parse(existing.updatedAt)),
        ).toISOString();
        updated = parseNatsProfileRecord({
          ...connection(parsed, existing),
          id: existing.id,
          revision: natsInteger(existing.revision + 1, 1),
          name: parsed.name,
          createdAt: existing.createdAt,
          updatedAt: timestamp,
        });
      } catch (cause) {
        throw validationFailure(cause);
      }
      this.assertUniqueName(parsed.name, existing.id);
      this.assertMutable(existing.id);
      return this.commit(
        this.records.map((record) => (record.id === existing.id ? updated : record)),
        signal,
      );
    }, signal);
  }

  delete(
    profileId: string,
    expectedRevision: number,
    signal?: AbortSignal,
  ): Promise<NatsProfilesSnapshot> {
    return this.mutations.enqueue(async () => {
      await this.ensureLoaded(signal);
      signal?.throwIfAborted();
      this.assertAvailable();
      const existing = this.find(profileId, expectedRevision);
      this.assertMutable(existing.id);
      return this.commit(
        this.records.filter((record) => record.id !== existing.id),
        signal,
      );
    }, signal);
  }

  /** Resolving waits for prior mutations so connecting cannot use a precommit revision. */
  resolve(
    profileId: string,
    expectedRevision: number,
    signal?: AbortSignal,
  ): Promise<NatsResolvedProfile> {
    return this.mutations.enqueue(async () => {
      await this.ensureLoaded(signal);
      signal?.throwIfAborted();
      this.assertAvailable();
      const record = parseNatsProfileRecord(this.find(profileId, expectedRevision));
      return {
        identity: { id: record.id, revision: record.revision, name: record.name },
        connection: {
          servers: record.servers,
          authentication: record.authentication,
          tls: record.tls,
        },
      };
    }, signal);
  }

  private snapshot(): NatsProfilesSnapshot {
    const storeCapability = parseNatsProfileStoreCapability(this.store.capability);
    const capability = this.dataUnavailable
      ? {
          durability: storeCapability.durability,
          protection: "unavailable" as const,
          state: "unavailable" as const,
          recovery:
            storeCapability.recovery ??
            "Unlock or configure protected profile storage, then restart StreamSkope.",
        }
      : storeCapability;
    if (
      this.snapshotCapability !== undefined &&
      JSON.stringify(this.snapshotCapability) !== JSON.stringify(capability)
    )
      this.snapshotRevision = natsInteger(this.snapshotRevision + 1);
    this.snapshotCapability = capability;
    return { revision: this.snapshotRevision, capability, profiles: this.records.map(summary) };
  }
  private assertAvailable(): void {
    if (this.dataUnavailable || this.store.capability.state !== "ready") throw storageFailure();
  }
  private assertMutable(profileId: string): void {
    if (this.isProfileInUse(profileId))
      throw new NatsProfileError(
        "profile-in-use",
        "This NATS profile is in use and cannot be changed.",
        "Disconnect the profile before editing or deleting it.",
      );
  }
  private assertUniqueName(name: string, existingId?: string): void {
    if (
      this.records.some(
        (record) => record.id !== existingId && record.name.toLowerCase() === name.toLowerCase(),
      )
    )
      throw validationFailure();
  }
  private find(profileId: string, expectedRevision: number): NatsProfileRecord {
    try {
      natsIdentifier(profileId);
      natsInteger(expectedRevision, 1);
    } catch (cause) {
      throw validationFailure(cause);
    }
    const record = this.records.find((profile) => profile.id === profileId);
    if (record === undefined)
      throw new NatsProfileError(
        "not-found",
        "The selected NATS profile no longer exists.",
        "Refresh profiles and choose an existing profile.",
      );
    if (record.revision !== expectedRevision)
      throw new NatsProfileError(
        "revision-conflict",
        "This NATS profile changed after it was opened.",
        "Refresh profiles and reopen the profile before retrying.",
      );
    return record;
  }
  private ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (this.loadPromise !== undefined) return this.loadPromise;
    if (this.store.capability.state !== "ready") return Promise.resolve();
    const loading = Promise.resolve().then(async () => {
      try {
        const records = await this.store.load(signal);
        signal?.throwIfAborted();
        const parsed = parseNatsProfileRecords(records);
        this.snapshotRevision = natsInteger(this.snapshotRevision + 1);
        this.records = parsed;
      } catch (cause) {
        if (signal?.aborted) {
          this.loadPromise = undefined;
          throw signal.reason;
        }
        this.dataUnavailable = true;
        throw storageFailure(cause);
      }
    });
    this.loadPromise = loading;
    return loading;
  }
  private async commit(
    records: readonly NatsProfileRecord[],
    signal?: AbortSignal,
  ): Promise<NatsProfilesSnapshot> {
    signal?.throwIfAborted();
    natsInteger(this.snapshotRevision + 1);
    try {
      await this.store.save(records, signal);
    } catch (cause) {
      if (signal?.aborted) throw signal.reason;
      this.dataUnavailable = true;
      throw storageFailure(cause);
    }
    // The store returned its actual commit receipt. Cancellation after commit cannot undo it.
    this.records = records;
    this.snapshotRevision = natsInteger(this.snapshotRevision + 1);
    return this.snapshot();
  }
}

export class InMemoryNatsProfileStore implements NatsProfileStore {
  readonly capability: NatsProfileStoreCapability = Object.freeze({
    durability: "session",
    protection: "memory",
    state: "ready",
  });
  private records: readonly NatsProfileRecord[];
  constructor(records: readonly NatsProfileRecord[] = []) {
    this.records = parseNatsProfileRecords(records);
  }
  load(signal?: AbortSignal): Promise<readonly NatsProfileRecord[]> {
    signal?.throwIfAborted();
    return Promise.resolve(parseNatsProfileRecords(this.records));
  }
  save(records: readonly NatsProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const parsed = parseNatsProfileRecords(records);
    signal?.throwIfAborted();
    this.records = parsed;
    return Promise.resolve();
  }
}
export class UnavailableNatsProfileStore implements NatsProfileStore {
  readonly capability: NatsProfileStoreCapability;
  constructor(capability: NatsProfileStoreCapability) {
    const parsed = parseNatsProfileStoreCapability(capability);
    if (parsed.state !== "unavailable") throw storageFailure();
    this.capability = parsed;
  }
  load(signal?: AbortSignal): Promise<readonly NatsProfileRecord[]> {
    signal?.throwIfAborted();
    return Promise.resolve([]);
  }
  save(_records: readonly NatsProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    return Promise.reject(storageFailure());
  }
}
