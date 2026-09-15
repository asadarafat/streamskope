import type { KafkaTopicConfigurationHistoryStoreCapability } from "../contracts";

import type {
  KafkaTopicConfigurationHistoryDocument,
  KafkaTopicConfigurationHistoryStore,
} from "./topic-configuration-types";

export function cloneKafkaTopicConfigurationHistoryDocument(
  document: KafkaTopicConfigurationHistoryDocument,
): KafkaTopicConfigurationHistoryDocument {
  return {
    entries: document.entries.map((entry) => ({
      ...entry,
      changes: entry.changes.map((change) => ({ ...change })),
    })),
  };
}

export class InMemoryKafkaTopicConfigurationHistoryStore implements KafkaTopicConfigurationHistoryStore {
  commitCount = 0;
  private current: KafkaTopicConfigurationHistoryDocument | undefined;

  constructor(
    private readonly storeCapability: KafkaTopicConfigurationHistoryStoreCapability,
    initialDocument?: KafkaTopicConfigurationHistoryDocument,
  ) {
    this.current =
      initialDocument === undefined
        ? undefined
        : cloneKafkaTopicConfigurationHistoryDocument(initialDocument);
  }

  capability(): KafkaTopicConfigurationHistoryStoreCapability {
    return { ...this.storeCapability };
  }

  commit(document: KafkaTopicConfigurationHistoryDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.current = cloneKafkaTopicConfigurationHistoryDocument(document);
    this.commitCount += 1;
    return Promise.resolve();
  }

  document(): KafkaTopicConfigurationHistoryDocument | undefined {
    return this.current === undefined
      ? undefined
      : cloneKafkaTopicConfigurationHistoryDocument(this.current);
  }

  load(signal?: AbortSignal): Promise<KafkaTopicConfigurationHistoryDocument | undefined> {
    signal?.throwIfAborted();
    return Promise.resolve(this.document());
  }
}
