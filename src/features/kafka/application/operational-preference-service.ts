import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  HostContractValidationError,
  parseKafkaOperationalPreferencePatch,
  parseKafkaOperationalPreferences,
  type KafkaOperationalPreferencePatch,
  type KafkaOperationalPreferences,
  type KafkaOperationalPreferenceSnapshot,
  type KafkaOperationalPreferenceStoreCapability,
} from "../contracts";

import {
  KafkaOperationalPreferenceCorruptError,
  KafkaOperationalPreferenceStoreUnavailableError,
  KafkaOperationalPreferenceValidationError,
} from "./operational-preference-errors";
import { cloneKafkaOperationalPreferences } from "./in-memory-operational-preference-store";
import type {
  KafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceStructuredError,
} from "./operational-preference-types";

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function isPreferenceStorageFailure(
  error: unknown,
): error is KafkaOperationalPreferenceStructuredError {
  return (
    error instanceof Error &&
    "code" in error &&
    (error.code === "PREFERENCE_CORRUPT" || error.code === "PREFERENCE_STORE_UNAVAILABLE") &&
    "recovery" in error &&
    typeof error.recovery === "string"
  );
}

function immutablePreferences(
  preferences: KafkaOperationalPreferences,
): KafkaOperationalPreferences {
  const clone = cloneKafkaOperationalPreferences(preferences);
  Object.freeze(clone.fetch);
  Object.freeze(clone.latency);
  Object.freeze(clone.rules);
  Object.freeze(clone.stream);
  return Object.freeze(clone);
}

function immutableSnapshot(
  preferences: KafkaOperationalPreferences,
  store: KafkaOperationalPreferenceStoreCapability,
): KafkaOperationalPreferenceSnapshot {
  return Object.freeze({
    preferences: immutablePreferences(preferences),
    store: Object.freeze({ ...store }),
  });
}

function mergePreferences(
  current: KafkaOperationalPreferences,
  patch: KafkaOperationalPreferencePatch,
): KafkaOperationalPreferences {
  return {
    fetch: { ...current.fetch, ...patch.fetch },
    latency: { ...current.latency, ...patch.latency },
    rules: { ...current.rules, ...patch.rules },
    stream: { ...current.stream, ...patch.stream },
  };
}

export class KafkaOperationalPreferenceService {
  private loadPromise: Promise<void> | undefined;
  private loaded = false;
  private mutationTail: Promise<void> = Promise.resolve();
  private preferences = cloneKafkaOperationalPreferences(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
  private unavailableRecovery: string | undefined;

  constructor(private readonly store: KafkaOperationalPreferenceStore) {}

  currentSnapshot(): KafkaOperationalPreferenceSnapshot {
    return immutableSnapshot(this.preferences, this.capability());
  }

  async get(signal?: AbortSignal): Promise<KafkaOperationalPreferenceSnapshot> {
    await this.ensureLoaded(signal);
    return this.currentSnapshot();
  }

  reset(signal?: AbortSignal): Promise<KafkaOperationalPreferenceSnapshot> {
    return this.mutate(() => this.completeReset(signal), signal);
  }

  update(
    input: KafkaOperationalPreferencePatch,
    signal?: AbortSignal,
  ): Promise<KafkaOperationalPreferenceSnapshot> {
    return this.mutate(() => this.completeUpdate(input, signal), signal);
  }

  private capability(): KafkaOperationalPreferenceStoreCapability {
    const capability = this.store.capability();
    if (this.unavailableRecovery !== undefined || capability.state === "unavailable") {
      return {
        durability: capability.durability,
        recovery:
          this.unavailableRecovery ??
          capability.recovery ??
          new KafkaOperationalPreferenceStoreUnavailableError().recovery,
        state: "unavailable",
      };
    }
    return {
      durability: capability.durability,
      state: "ready",
    };
  }

  private async commit(
    next: KafkaOperationalPreferences,
    signal?: AbortSignal,
  ): Promise<KafkaOperationalPreferenceSnapshot> {
    try {
      await this.store.commit(next, signal);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw new KafkaOperationalPreferenceStoreUnavailableError();
    }
    const capability = this.store.capability();
    if (capability.state !== "ready") {
      throw new KafkaOperationalPreferenceStoreUnavailableError();
    }
    this.preferences = cloneKafkaOperationalPreferences(next);
    this.unavailableRecovery = undefined;
    return this.currentSnapshot();
  }

  private async completeReset(signal?: AbortSignal): Promise<KafkaOperationalPreferenceSnapshot> {
    await this.ensureLoaded(signal);
    return this.commit(
      cloneKafkaOperationalPreferences(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS),
      signal,
    );
  }

  private async completeUpdate(
    input: KafkaOperationalPreferencePatch,
    signal?: AbortSignal,
  ): Promise<KafkaOperationalPreferenceSnapshot> {
    await this.ensureLoaded(signal);
    if (this.capability().state === "unavailable") {
      throw new KafkaOperationalPreferenceStoreUnavailableError();
    }
    let patch: KafkaOperationalPreferencePatch;
    let next: KafkaOperationalPreferences;
    try {
      patch = parseKafkaOperationalPreferencePatch(input);
      next = parseKafkaOperationalPreferences(mergePreferences(this.preferences, patch));
    } catch (error) {
      if (error instanceof HostContractValidationError) {
        throw new KafkaOperationalPreferenceValidationError(error.message);
      }
      throw error;
    }
    return this.commit(next, signal);
  }

  private async ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (!this.loaded) {
      this.loadPromise ??= this.load();
      try {
        await this.loadPromise;
      } catch (error) {
        this.loadPromise = undefined;
        throw error;
      }
    }
    signal?.throwIfAborted();
  }

  private async load(): Promise<void> {
    try {
      const stored = await this.store.load();
      this.preferences =
        stored === undefined
          ? cloneKafkaOperationalPreferences(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS)
          : parseKafkaOperationalPreferences(stored, "storedPreferences");
      const capability = this.store.capability();
      this.unavailableRecovery =
        capability.state === "unavailable"
          ? (capability.recovery ?? new KafkaOperationalPreferenceStoreUnavailableError().recovery)
          : undefined;
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.preferences = cloneKafkaOperationalPreferences(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
      this.unavailableRecovery = isPreferenceStorageFailure(error)
        ? error.recovery
        : error instanceof HostContractValidationError
          ? new KafkaOperationalPreferenceCorruptError().recovery
          : new KafkaOperationalPreferenceStoreUnavailableError().recovery;
    }
    this.loaded = true;
  }

  private mutate<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const result = this.mutationTail.then(() => {
      signal?.throwIfAborted();
      return operation();
    });
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}
