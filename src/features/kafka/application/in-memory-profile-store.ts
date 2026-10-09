import { parseProfileAcquisitionBinding, type ProfileStoreCapability } from "../contracts";

import type { KafkaProfileRecord, KafkaProfileStore } from "./profile-types";

function cloneRecord(record: KafkaProfileRecord): KafkaProfileRecord {
  // A profile now includes nested service secrets, trust and client identities. Copy the
  // complete value graph so drafts and loaded snapshots cannot mutate committed state.
  const cloned = structuredClone(record);
  if (cloned.transport === "plaintext") return { ...cloned, transport: "plaintext" };
  return {
    ...cloned,
    ...(cloned.binding === undefined
      ? {}
      : { binding: parseProfileAcquisitionBinding(cloned.binding) }),
  };
}

export class InMemoryKafkaProfileStore implements KafkaProfileStore {
  commitCount = 0;
  private currentRecords: readonly KafkaProfileRecord[];

  constructor(
    private readonly storeCapability: ProfileStoreCapability,
    initialRecords: readonly KafkaProfileRecord[] = [],
  ) {
    this.currentRecords = initialRecords.map(cloneRecord);
  }

  capability(): ProfileStoreCapability {
    return this.storeCapability;
  }

  commit(records: readonly KafkaProfileRecord[], signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.currentRecords = records.map(cloneRecord);
    this.commitCount += 1;
    return Promise.resolve();
  }

  load(signal?: AbortSignal): Promise<readonly KafkaProfileRecord[]> {
    signal?.throwIfAborted();
    return Promise.resolve(this.records());
  }

  records(): readonly KafkaProfileRecord[] {
    return this.currentRecords.map(cloneRecord);
  }
}
