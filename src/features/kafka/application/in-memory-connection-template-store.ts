import type { ConnectionTemplateStoreCapability } from "../contracts";

import type {
  KafkaConnectionTemplateDocument,
  KafkaConnectionTemplateStore,
} from "./connection-template-types";

export function cloneConnectionTemplateDocument(
  document: KafkaConnectionTemplateDocument,
): KafkaConnectionTemplateDocument {
  return {
    catalogs: document.catalogs.map((catalog) => ({
      catalog: catalog.catalog,
      entries: catalog.entries.map((entry) => ({ ...entry })),
      selectedName: catalog.selectedName,
    })),
  };
}

export class InMemoryKafkaConnectionTemplateStore implements KafkaConnectionTemplateStore {
  commitCount = 0;
  private current: KafkaConnectionTemplateDocument | undefined;

  constructor(
    private readonly storeCapability: ConnectionTemplateStoreCapability,
    initialDocument?: KafkaConnectionTemplateDocument,
  ) {
    this.current =
      initialDocument === undefined ? undefined : cloneConnectionTemplateDocument(initialDocument);
  }

  capability(): ConnectionTemplateStoreCapability {
    return this.storeCapability;
  }

  commit(document: KafkaConnectionTemplateDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.current = cloneConnectionTemplateDocument(document);
    this.commitCount += 1;
    return Promise.resolve();
  }

  document(): KafkaConnectionTemplateDocument | undefined {
    return this.current === undefined ? undefined : cloneConnectionTemplateDocument(this.current);
  }

  load(signal?: AbortSignal): Promise<KafkaConnectionTemplateDocument | undefined> {
    signal?.throwIfAborted();
    return Promise.resolve(this.document());
  }
}
