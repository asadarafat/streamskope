import {
  CONNECTION_TEMPLATE_CATALOGS,
  CONNECTION_TEMPLATE_LIMITS,
  canonicalConnectionTemplateName,
  validateConnectionTemplateInput,
  type ConnectionTemplateCatalog,
  type ConnectionTemplateCatalogSnapshot,
  type CommandTemplateCatalog,
  type ConnectionTemplateEntry,
  type ConnectionTemplateInput,
  type ConnectionTemplateStoreCapability,
} from "../contracts";

import {
  DuplicateKafkaConnectionTemplateError,
  KafkaConnectionTemplateCapacityError,
  KafkaConnectionTemplateNotFoundError,
  KafkaConnectionTemplateStoreUnavailableError,
  KafkaConnectionTemplateValidationError,
} from "./connection-template-errors";
import { DEFAULT_CONNECTION_TEMPLATE_DOCUMENT } from "./connection-template-defaults";
import type {
  KafkaConnectionTemplateDocument,
  KafkaConnectionTemplateSnapshot,
  KafkaConnectionTemplateStore,
  KafkaConnectionTemplateStructuredError,
} from "./connection-template-types";
import { cloneConnectionTemplateDocument } from "./in-memory-connection-template-store";
import { InMemoryKafkaTrustRecipeStore } from "./trust-recipe-store";
import {
  KafkaTrustRecipeLibrary,
  type KafkaTrustRecipeLibraryOptions,
} from "./trust-recipe-library";

function emptyDocument(): KafkaConnectionTemplateDocument {
  return {
    catalogs: CONNECTION_TEMPLATE_CATALOGS.map((catalog) => ({
      catalog,
      entries: [],
      selectedName: null,
    })),
  };
}

function comparableName(name: string): string {
  return canonicalConnectionTemplateName(name).toLocaleLowerCase("en-US");
}

function validDocument(document: KafkaConnectionTemplateDocument): boolean {
  if (document.catalogs.length !== CONNECTION_TEMPLATE_CATALOGS.length) {
    return false;
  }
  return CONNECTION_TEMPLATE_CATALOGS.every((expectedCatalog, catalogIndex) => {
    const catalog = document.catalogs[catalogIndex];
    if (
      catalog === undefined ||
      catalog.catalog !== expectedCatalog ||
      catalog.entries.length > CONNECTION_TEMPLATE_LIMITS.entriesPerCatalog
    ) {
      return false;
    }
    const names = new Set<string>();
    for (const entry of catalog.entries) {
      const name = comparableName(entry.name);
      if (
        entry.name !== canonicalConnectionTemplateName(entry.name) ||
        names.has(name) ||
        validateConnectionTemplateInput({
          catalog: expectedCatalog,
          name: entry.name,
          template: entry.template,
        }).length > 0
      ) {
        return false;
      }
      names.add(name);
    }
    return (
      catalog.selectedName === null ||
      (catalog.selectedName === canonicalConnectionTemplateName(catalog.selectedName) &&
        catalog.entries.some((entry) => entry.name === catalog.selectedName))
    );
  });
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function isStructuredTemplateError(
  error: unknown,
): error is KafkaConnectionTemplateStructuredError {
  return (
    error instanceof Error &&
    "code" in error &&
    (error.code === "TEMPLATE_CORRUPT" || error.code === "TEMPLATE_STORE_UNAVAILABLE") &&
    "recovery" in error &&
    typeof error.recovery === "string"
  );
}

export class KafkaConnectionTemplateService {
  readonly recipes: KafkaTrustRecipeLibrary;
  private originalDocument: Promise<KafkaConnectionTemplateDocument | undefined> | undefined;
  private document: KafkaConnectionTemplateDocument = emptyDocument();
  private loadPromise: Promise<void> | undefined;
  private mutationTail: Promise<void> = Promise.resolve();
  private templateDataUnavailable = false;

  constructor(
    private readonly store: KafkaConnectionTemplateStore,
    recipeOptions?: KafkaTrustRecipeLibraryOptions,
  ) {
    this.recipes = new KafkaTrustRecipeLibrary(
      recipeOptions ?? {
        store: new InMemoryKafkaTrustRecipeStore({ durability: "session", state: "ready" }),
      },
      () => this.loadOriginalDocument(),
      () => this.readLegacyDocument(),
    );
  }

  private loadOriginalDocument(): Promise<KafkaConnectionTemplateDocument | undefined> {
    this.originalDocument ??= this.store.load();
    return this.originalDocument;
  }

  private async readLegacyDocument(): Promise<KafkaConnectionTemplateDocument | undefined> {
    await this.mutationTail;
    if (this.loadPromise === undefined) return this.loadOriginalDocument();
    await this.loadPromise;
    this.assertStoreAvailable();
    return cloneConnectionTemplateDocument(this.document);
  }

  create(
    input: ConnectionTemplateInput,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    return this.mutate(() => this.completeCreate(input, signal), signal);
  }

  currentSnapshot(): KafkaConnectionTemplateSnapshot {
    return this.snapshot();
  }

  delete(
    catalog: ConnectionTemplateCatalog,
    name: string,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    return this.mutate(() => this.completeDelete(catalog, name, signal), signal);
  }

  async list(signal?: AbortSignal): Promise<KafkaConnectionTemplateSnapshot> {
    await this.ensureLoaded(signal);
    return this.snapshot();
  }

  async selected(
    catalogName: CommandTemplateCatalog,
    signal?: AbortSignal,
  ): Promise<ConnectionTemplateEntry> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const catalog = this.catalog(catalogName);
    const entry = catalog.entries.find((candidate) => candidate.name === catalog.selectedName);
    if (entry === undefined) {
      throw new KafkaConnectionTemplateNotFoundError(catalogName, "selected template");
    }
    return { ...entry };
  }

  select(
    catalog: ConnectionTemplateCatalog,
    name: string,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    return this.mutate(() => this.completeSelect(catalog, name, signal), signal);
  }

  update(
    catalog: ConnectionTemplateCatalog,
    originalName: string,
    input: Omit<ConnectionTemplateInput, "catalog">,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    return this.mutate(
      () => this.completeUpdate(catalog, originalName, { ...input, catalog }, signal),
      signal,
    );
  }

  private async commit(
    next: KafkaConnectionTemplateDocument,
    signal: AbortSignal | undefined,
    target: string,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    try {
      await this.store.commit(next, signal);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw new KafkaConnectionTemplateStoreUnavailableError(target);
    }
    this.document = cloneConnectionTemplateDocument(next);
    return this.snapshot();
  }

  private async completeCreate(
    input: ConnectionTemplateInput,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const issues = validateConnectionTemplateInput(input);
    if (issues.length > 0) {
      throw new KafkaConnectionTemplateValidationError(issues);
    }
    const catalog = this.catalog(input.catalog);
    if (catalog.entries.length >= CONNECTION_TEMPLATE_LIMITS.entriesPerCatalog) {
      throw new KafkaConnectionTemplateCapacityError(
        input.catalog,
        CONNECTION_TEMPLATE_LIMITS.entriesPerCatalog,
      );
    }
    const name = canonicalConnectionTemplateName(input.name);
    if (this.entryIndex(catalog, name) >= 0) {
      throw new DuplicateKafkaConnectionTemplateError(input.catalog, name);
    }
    const next = this.replaceCatalog({
      ...catalog,
      entries: [...catalog.entries, { name, template: input.template }],
    });
    return this.commit(next, signal, `${input.catalog} · ${name}`);
  }

  private async completeDelete(
    catalogName: ConnectionTemplateCatalog,
    name: string,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const catalog = this.catalog(catalogName);
    const entryIndex = this.entryIndex(catalog, name);
    if (entryIndex < 0) {
      throw new KafkaConnectionTemplateNotFoundError(catalogName, name);
    }
    const entry = catalog.entries[entryIndex];
    if (entry === undefined) {
      throw new KafkaConnectionTemplateNotFoundError(catalogName, name);
    }
    const entries = catalog.entries.filter((_entry, index) => index !== entryIndex);
    const selectedName =
      catalog.selectedName === entry.name ? (entries[0]?.name ?? null) : catalog.selectedName;
    return this.commit(
      this.replaceCatalog({ ...catalog, entries, selectedName }),
      signal,
      `${catalogName} · ${entry.name}`,
    );
  }

  private async completeSelect(
    catalogName: ConnectionTemplateCatalog,
    name: string,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const catalog = this.catalog(catalogName);
    const entry = catalog.entries[this.entryIndex(catalog, name)];
    if (entry === undefined) {
      throw new KafkaConnectionTemplateNotFoundError(catalogName, name);
    }
    if (catalog.selectedName === entry.name) {
      return this.snapshot();
    }
    return this.commit(
      this.replaceCatalog({ ...catalog, selectedName: entry.name }),
      signal,
      `${catalogName} · ${entry.name}`,
    );
  }

  private async completeUpdate(
    catalogName: ConnectionTemplateCatalog,
    originalName: string,
    input: ConnectionTemplateInput,
    signal?: AbortSignal,
  ): Promise<KafkaConnectionTemplateSnapshot> {
    await this.ensureLoaded(signal);
    this.assertStoreAvailable();
    const issues = validateConnectionTemplateInput(input);
    if (issues.length > 0) {
      throw new KafkaConnectionTemplateValidationError(issues);
    }
    const catalog = this.catalog(catalogName);
    const entryIndex = this.entryIndex(catalog, originalName);
    const existing = catalog.entries[entryIndex];
    if (existing === undefined) {
      throw new KafkaConnectionTemplateNotFoundError(catalogName, originalName);
    }
    const name = canonicalConnectionTemplateName(input.name);
    if (
      catalog.entries.some(
        (entry, index) =>
          index !== entryIndex && comparableName(entry.name) === comparableName(name),
      )
    ) {
      throw new DuplicateKafkaConnectionTemplateError(catalogName, name);
    }
    const entries = catalog.entries.map((entry, index) =>
      index === entryIndex ? { name, template: input.template } : entry,
    );
    const selectedName = catalog.selectedName === existing.name ? name : catalog.selectedName;
    return this.commit(
      this.replaceCatalog({ ...catalog, entries, selectedName }),
      signal,
      `${catalogName} · ${existing.name}`,
    );
  }

  private catalog(catalog: ConnectionTemplateCatalog): ConnectionTemplateCatalogSnapshot {
    const found = this.document.catalogs.find((candidate) => candidate.catalog === catalog);
    if (found === undefined) {
      throw new KafkaConnectionTemplateStoreUnavailableError(catalog);
    }
    return found;
  }

  private entryIndex(catalog: ConnectionTemplateCatalogSnapshot, name: string): number {
    const comparable = comparableName(name);
    return catalog.entries.findIndex((entry) => comparableName(entry.name) === comparable);
  }

  private async ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.loadPromise ??= this.load();
    await this.loadPromise;
    signal?.throwIfAborted();
  }

  private assertStoreAvailable(): void {
    if (this.storeCapability().state === "unavailable") {
      throw new KafkaConnectionTemplateStoreUnavailableError();
    }
  }

  private async load(): Promise<void> {
    try {
      const stored = await this.loadOriginalDocument();
      if (stored === undefined) {
        const defaults = cloneConnectionTemplateDocument(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
        await this.store.commit(defaults);
        this.document = defaults;
        return;
      }
      if (!validDocument(stored)) {
        throw new KafkaConnectionTemplateStoreUnavailableError();
      }
      this.document = cloneConnectionTemplateDocument(stored);
    } catch (error) {
      this.templateDataUnavailable = true;
      this.document = emptyDocument();
      throw isAbort(error) || isStructuredTemplateError(error)
        ? error
        : new KafkaConnectionTemplateStoreUnavailableError();
    }
  }

  private mutate<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const result = this.mutationTail.then(async () => {
      signal?.throwIfAborted();
      return operation();
    });
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private replaceCatalog(
    replacement: ConnectionTemplateCatalogSnapshot,
  ): KafkaConnectionTemplateDocument {
    return {
      catalogs: this.document.catalogs.map((catalog) =>
        catalog.catalog === replacement.catalog ? replacement : catalog,
      ),
    };
  }

  private snapshot(): KafkaConnectionTemplateSnapshot {
    const document = cloneConnectionTemplateDocument(this.document);
    return {
      catalogs: document.catalogs,
      store: this.storeCapability(),
    };
  }

  private storeCapability(): ConnectionTemplateStoreCapability {
    const capability = this.store.capability();
    if (!this.templateDataUnavailable || capability.state === "unavailable") {
      return capability;
    }
    return {
      durability: capability.durability,
      recovery:
        "Preserve the template data, correct it outside the running application, then restart StreamSkope.",
      state: "unavailable",
    };
  }
}
