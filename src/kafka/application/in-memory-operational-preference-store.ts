import type {
  KafkaOperationalPreferences,
  KafkaOperationalPreferenceStoreCapability,
} from "../contracts";

import type { KafkaOperationalPreferenceStore } from "./operational-preference-types";

export function cloneKafkaOperationalPreferences(
  preferences: KafkaOperationalPreferences,
): KafkaOperationalPreferences {
  return {
    fetch: { ...preferences.fetch },
    latency: { ...preferences.latency },
    rules: { ...preferences.rules },
    stream: { ...preferences.stream },
  };
}

export class InMemoryKafkaOperationalPreferenceStore implements KafkaOperationalPreferenceStore {
  commitCount = 0;
  private current: KafkaOperationalPreferences | undefined;
  private storeCapability: KafkaOperationalPreferenceStoreCapability;

  constructor(
    capability: KafkaOperationalPreferenceStoreCapability,
    initialPreferences?: KafkaOperationalPreferences,
  ) {
    this.storeCapability = { ...capability };
    this.current =
      initialPreferences === undefined
        ? undefined
        : cloneKafkaOperationalPreferences(initialPreferences);
  }

  capability(): KafkaOperationalPreferenceStoreCapability {
    return { ...this.storeCapability };
  }

  commit(preferences: KafkaOperationalPreferences, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.current = cloneKafkaOperationalPreferences(preferences);
    this.commitCount += 1;
    this.storeCapability = {
      durability: this.storeCapability.durability,
      state: "ready",
    };
    return Promise.resolve();
  }

  document(): KafkaOperationalPreferences | undefined {
    return this.current === undefined ? undefined : cloneKafkaOperationalPreferences(this.current);
  }

  load(signal?: AbortSignal): Promise<KafkaOperationalPreferences | undefined> {
    signal?.throwIfAborted();
    return Promise.resolve(this.document());
  }
}
