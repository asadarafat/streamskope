export const CONNECTION_TEMPLATE_CATALOGS = [
  "truststore-fetch",
  "truststore-password",
  "oauth-endpoint",
] as const;

export const CONNECTION_TEMPLATE_LIMITS = {
  entriesPerCatalog: 100,
  nameCharacters: 128,
  commandCharacters: 8_192,
  endpointCharacters: 2_048,
} as const;

export const CONNECTION_TEMPLATE_STORE_DURABILITIES = ["durable", "session"] as const;
export const CONNECTION_TEMPLATE_STORE_STATES = ["ready", "unavailable"] as const;

export type ConnectionTemplateCatalog = (typeof CONNECTION_TEMPLATE_CATALOGS)[number];
export type CommandTemplateCatalog = Exclude<ConnectionTemplateCatalog, "oauth-endpoint">;
export type ConnectionTemplateStoreDurability =
  (typeof CONNECTION_TEMPLATE_STORE_DURABILITIES)[number];
export type ConnectionTemplateStoreState = (typeof CONNECTION_TEMPLATE_STORE_STATES)[number];

export interface ConnectionTemplateInput {
  readonly catalog: ConnectionTemplateCatalog;
  readonly name: string;
  readonly template: string;
}

export interface ConnectionTemplateEntry {
  readonly name: string;
  readonly template: string;
}

export interface ConnectionTemplateCatalogSnapshot {
  readonly catalog: ConnectionTemplateCatalog;
  readonly entries: readonly ConnectionTemplateEntry[];
  readonly selectedName: string | null;
}

export interface ConnectionTemplateStoreCapability {
  readonly durability: ConnectionTemplateStoreDurability;
  readonly recovery?: string;
  readonly state: ConnectionTemplateStoreState;
}

export interface ConnectionTemplateSnapshot {
  readonly catalogs: readonly ConnectionTemplateCatalogSnapshot[];
  readonly store: ConnectionTemplateStoreCapability;
}

export interface ConnectionTemplateIssue {
  readonly field: "name" | "template";
  readonly message: string;
}
