import {
  parseTrustAcquisitionRecipeDocument,
  type ConnectionTemplateStoreCapability,
  type TrustAcquisitionRecipeDocument,
} from "../contracts";

export interface KafkaTrustRecipeStore {
  capability(): ConnectionTemplateStoreCapability;
  load(signal?: AbortSignal): Promise<TrustAcquisitionRecipeDocument | undefined>;
  commit(document: TrustAcquisitionRecipeDocument, signal?: AbortSignal): Promise<void>;
}

export class InMemoryKafkaTrustRecipeStore implements KafkaTrustRecipeStore {
  private current: TrustAcquisitionRecipeDocument | undefined;

  constructor(
    private readonly storeCapability: ConnectionTemplateStoreCapability,
    initial?: TrustAcquisitionRecipeDocument,
  ) {
    this.current = initial === undefined ? undefined : parseTrustAcquisitionRecipeDocument(initial);
  }

  capability(): ConnectionTemplateStoreCapability {
    return { ...this.storeCapability };
  }

  load(signal?: AbortSignal): Promise<TrustAcquisitionRecipeDocument | undefined> {
    signal?.throwIfAborted();
    return Promise.resolve(
      this.current === undefined ? undefined : parseTrustAcquisitionRecipeDocument(this.current),
    );
  }

  commit(document: TrustAcquisitionRecipeDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.current = parseTrustAcquisitionRecipeDocument(document);
    return Promise.resolve();
  }
}
