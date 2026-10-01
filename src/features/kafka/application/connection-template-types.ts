import type { ConnectionTemplateCatalogSnapshot } from "../contracts";

/** Preserved catalog schema, used only for explicit recipe conversion. */
export interface KafkaConnectionTemplateDocument {
  readonly catalogs: readonly ConnectionTemplateCatalogSnapshot[];
}

export interface KafkaLegacyTemplateSource {
  load(signal?: AbortSignal): Promise<KafkaConnectionTemplateDocument | undefined>;
}
