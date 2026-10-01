import type {
  KafkaConnectionTemplateDocument,
  KafkaLegacyTemplateSource,
} from "../../src/features/kafka/application/connection-template-types";
import { InMemoryKafkaTrustRecipeStore } from "../../src/features/kafka/application/trust-recipe-store";
import {
  KafkaTrustRecipeLibrary,
  type KafkaTrustRecipeLibraryOptions,
} from "../../src/features/kafka/application/trust-recipe-library";
import type { ConnectionTemplateStoreCapability } from "../../src/features/kafka/contracts";

export function createRecipeLibrary(
  legacy?: KafkaLegacyTemplateSource,
  options?: KafkaTrustRecipeLibraryOptions,
): KafkaTrustRecipeLibrary {
  return new KafkaTrustRecipeLibrary(
    options ?? {
      store: new InMemoryKafkaTrustRecipeStore(
        { durability: "session", state: "ready" },
        { version: 1, recipes: [] },
      ),
    },
    legacy,
  );
}

/** Simulates external edits to a preserved legacy source, never a production writer. */
export class LegacyTemplateFixture implements KafkaLegacyTemplateSource {
  commitCount = 0;
  constructor(
    private readonly storeCapability: ConnectionTemplateStoreCapability,
    private current?: KafkaConnectionTemplateDocument,
  ) {}
  capability(): ConnectionTemplateStoreCapability {
    return this.storeCapability;
  }
  document(): KafkaConnectionTemplateDocument | undefined {
    return this.current === undefined ? undefined : structuredClone(this.current);
  }
  load(signal?: AbortSignal): Promise<KafkaConnectionTemplateDocument | undefined> {
    signal?.throwIfAborted();
    return Promise.resolve(this.document());
  }
  commit(document: KafkaConnectionTemplateDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.current = structuredClone(document);
    this.commitCount += 1;
    return Promise.resolve();
  }
}
