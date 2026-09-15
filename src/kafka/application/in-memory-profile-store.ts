import { parseProfileAcquisitionBinding, type ProfileStoreCapability } from "../contracts";

import type { KafkaProfileRecord, KafkaProfileStore } from "./profile-types";

function cloneRecord(record: KafkaProfileRecord): KafkaProfileRecord {
  return {
    ...record,
    ...(record.binding === undefined
      ? {}
      : { binding: parseProfileAcquisitionBinding(record.binding) }),
    brokers: [...record.brokers],
    ...(record.oauth === undefined ? {} : { oauth: { ...record.oauth } }),
    ...(record.services === undefined
      ? {}
      : {
          services: {
            ...(record.services.redpandaAdmin === undefined
              ? {}
              : { redpandaAdmin: { ...record.services.redpandaAdmin } }),
            ...(record.services.schemaRegistry === undefined
              ? {}
              : { schemaRegistry: { ...record.services.schemaRegistry } }),
          },
        }),
    trust: { ...record.trust },
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
