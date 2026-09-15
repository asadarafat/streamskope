import type { ProfileStoreCapability } from "../contracts";

import { KafkaProfileStoreUnavailableError } from "./profile-errors";
import type { KafkaProfileRecord, KafkaProfileStore } from "./profile-types";

export class UnavailableKafkaProfileStore implements KafkaProfileStore {
  constructor(private readonly storeCapability: ProfileStoreCapability) {
    if (storeCapability.state !== "unavailable") {
      throw new Error("Unavailable profile store requires an unavailable capability.");
    }
  }

  capability(): ProfileStoreCapability {
    return this.storeCapability;
  }

  commit(_records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    return Promise.reject(new KafkaProfileStoreUnavailableError());
  }

  load(signal?: AbortSignal): Promise<readonly KafkaProfileRecord[]> {
    signal?.throwIfAborted();
    return Promise.resolve([]);
  }
}
