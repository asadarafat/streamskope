import type { KafkaRuleStoreCapability } from "../contracts";

import type { KafkaRuleDocument, KafkaRuleStore } from "./rule-types";

export function cloneKafkaRuleDocument(document: KafkaRuleDocument): KafkaRuleDocument {
  return {
    rules: document.rules.map((rule) => ({ ...rule })),
  };
}

export class InMemoryKafkaRuleStore implements KafkaRuleStore {
  commitCount = 0;
  private current: KafkaRuleDocument | undefined;

  constructor(
    private readonly storeCapability: KafkaRuleStoreCapability,
    initialDocument?: KafkaRuleDocument,
  ) {
    this.current =
      initialDocument === undefined ? undefined : cloneKafkaRuleDocument(initialDocument);
  }

  capability(): KafkaRuleStoreCapability {
    return { ...this.storeCapability };
  }

  commit(document: KafkaRuleDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.current = cloneKafkaRuleDocument(document);
    this.commitCount += 1;
    return Promise.resolve();
  }

  document(): KafkaRuleDocument | undefined {
    return this.current === undefined ? undefined : cloneKafkaRuleDocument(this.current);
  }

  load(signal?: AbortSignal): Promise<KafkaRuleDocument | undefined> {
    signal?.throwIfAborted();
    return Promise.resolve(this.document());
  }
}
