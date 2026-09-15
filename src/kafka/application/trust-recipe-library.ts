import {
  BUILT_IN_TRUST_RECIPES,
  TRUST_RECIPE_LIMITS,
  parseTrustAcquisitionRecipe,
  parseTrustAcquisitionRecipeDocument,
  parseTrustAcquisitionRecipeInput,
  trustRecipeComparableName,
  trustAcquisitionRecipeDefinition,
  type HostErrorCode,
  type LegacyTrustRecipeSelection,
  type LegacyTrustRecipeSource,
  type ConnectionTemplateCatalog,
  type TrustAcquisitionRecipe,
  type TrustAcquisitionRecipeDocument,
  type TrustAcquisitionRecipeInput,
  type TrustAcquisitionRecipeSnapshot,
} from "../contracts";
import { parseConnectionTemplateSnapshotPayload } from "../contracts/connection-template-validation";

import type { KafkaConnectionTemplateDocument } from "./connection-template-types";
import type { KafkaTrustRecipeStore } from "./trust-recipe-store";

export class KafkaTrustRecipeError extends Error {
  readonly stage = "template" as const;
  readonly retryable = false;
  readonly target = undefined;

  constructor(
    readonly code: HostErrorCode,
    message: string,
    readonly recovery: string,
  ) {
    super(message);
    this.name = "KafkaTrustRecipeError";
  }
}

export interface KafkaTrustRecipeLibraryOptions {
  readonly store: KafkaTrustRecipeStore;
  readonly createId?: () => string;
}

async function digestDefinition(value: unknown): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(value)),
  );
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function storeUnavailable(): KafkaTrustRecipeError {
  return new KafkaTrustRecipeError(
    "TEMPLATE_STORE_UNAVAILABLE",
    "Trust acquisition template storage is unavailable.",
    "Preserve the template file, check its format and permissions, then restart or retry.",
  );
}

function initialRecipes(): readonly TrustAcquisitionRecipeInput[] {
  const common = {
    syntax: "named-v1",
    method: "ssh",
    timeoutSeconds: TRUST_RECIPE_LIMITS.defaultTimeoutSeconds,
    parameters: [
      { key: "certificate_path", label: "Certificate path", type: "path", required: true },
    ],
  } as const;
  return [
    {
      ...common,
      name: "SSH certificate file",
      kind: "pem",
      ssh: { source: "file", value: "{{certificate_path}}", password: { source: "none" } },
    },
    {
      ...common,
      name: "SSH truststore",
      kind: "jks",
      ssh: { source: "file", value: "{{certificate_path}}", password: { source: "ask" } },
    },
    ...BUILT_IN_TRUST_RECIPES,
  ];
}

export class KafkaTrustRecipeLibrary {
  private document: TrustAcquisitionRecipeDocument = { version: 1, recipes: [] };
  private loading: Promise<void> | undefined;
  private tail: Promise<void> = Promise.resolve();
  private unavailable = false;
  private readonly createId: () => string;

  constructor(
    private readonly options: KafkaTrustRecipeLibraryOptions,
    private readonly loadLegacy: () => Promise<KafkaConnectionTemplateDocument | undefined>,
    private readonly readLegacy: () => Promise<KafkaConnectionTemplateDocument | undefined>,
  ) {
    this.createId = options.createId ?? ((): string => crypto.randomUUID());
  }

  currentSnapshot(): TrustAcquisitionRecipeSnapshot {
    const capability = this.options.store.capability();
    const unavailable = this.unavailable || capability.state === "unavailable";
    return {
      recipes: unavailable ? [] : parseTrustAcquisitionRecipeDocument(this.document).recipes,
      store: unavailable
        ? { ...capability, state: "unavailable", recovery: storeUnavailable().recovery }
        : { ...capability },
    };
  }

  async list(signal?: AbortSignal): Promise<TrustAcquisitionRecipeSnapshot> {
    await this.ensureLoaded(signal);
    return this.currentSnapshot();
  }

  async resolve(
    id: string,
    revision: number,
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipe> {
    await this.ensureLoaded(signal);
    this.assertAvailable();
    return parseTrustAcquisitionRecipe(this.find(id, revision));
  }

  async legacy(): Promise<KafkaConnectionTemplateDocument | undefined> {
    const original = await this.readLegacy();
    if (original === undefined) return undefined;
    const snapshot = parseConnectionTemplateSnapshotPayload(
      { catalogs: original.catalogs, store: { durability: "session", state: "ready" } },
      "legacy",
    );
    return { catalogs: snapshot.catalogs };
  }

  async previewLegacy(): Promise<LegacyTrustRecipeSource | null> {
    const legacy = await this.legacy();
    return legacy === undefined
      ? null
      : { ...legacy, sourceRevision: await digestDefinition(legacy) };
  }

  convertLegacy(
    selection: LegacyTrustRecipeSelection,
    signal?: AbortSignal,
    expectedSourceRevision?: string,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    return this.mutate(async () => {
      const legacy = await this.legacy();
      if (
        expectedSourceRevision !== undefined &&
        (legacy === undefined || (await digestDefinition(legacy)) !== expectedSourceRevision)
      ) {
        throw new KafkaTrustRecipeError(
          "VALIDATION",
          "The legacy source changed after review.",
          "Refresh legacy entries and review the selected combination again.",
        );
      }
      const entry = (catalog: ConnectionTemplateCatalog, name: string): string => {
        const found = legacy?.catalogs
          .find((candidate) => candidate.catalog === catalog)
          ?.entries.find((candidate) => candidate.name === name);
        if (!found)
          throw new KafkaTrustRecipeError(
            "TEMPLATE_NOT_FOUND",
            "A selected legacy entry is unavailable.",
            "Review the exact preserved entries before converting.",
          );
        return found.template;
      };
      const definition = parseTrustAcquisitionRecipeInput({
        name: selection.name,
        kind: selection.kind,
        syntax: "legacy-v1",
        method: "ssh",
        parameters: [],
        timeoutSeconds: TRUST_RECIPE_LIMITS.defaultTimeoutSeconds,
        ssh: {
          source: "legacy-tempfile",
          value: entry("truststore-fetch", selection.materialName),
          password:
            selection.passwordName === null
              ? { source: selection.kind === "pem" ? "none" : "ask" }
              : {
                  source: "command",
                  command: entry("truststore-password", selection.passwordName),
                },
        },
        ...(selection.oauthName === null
          ? {}
          : {
              oauth: {
                endpoint: entry("oauth-endpoint", selection.oauthName),
                clientId: "",
                scope: "",
              },
            }),
      });
      const legacySourceId = await digestDefinition(definition);
      if (this.document.recipes.some((recipe) => recipe.legacySourceId === legacySourceId))
        return this.currentSnapshot();
      this.assertUnique(definition.name);
      const recipe = parseTrustAcquisitionRecipe({
        ...definition,
        id: this.createId(),
        revision: 1,
        legacySourceId,
      });
      return this.commit([...this.document.recipes, recipe], signal);
    }, signal);
  }

  create(
    input: TrustAcquisitionRecipeInput,
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    const parsed = parseTrustAcquisitionRecipeInput(input);
    return this.mutate(async () => {
      this.assertUnique(parsed.name);
      const recipe = parseTrustAcquisitionRecipe({ ...parsed, id: this.createId(), revision: 1 });
      return this.commit([...this.document.recipes, recipe], signal);
    }, signal);
  }

  update(
    id: string,
    revision: number,
    input: TrustAcquisitionRecipeInput,
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    const parsed = parseTrustAcquisitionRecipeInput(input);
    return this.mutate(async () => {
      const current = this.find(id, revision);
      this.assertUnique(parsed.name, id);
      const candidate = parseTrustAcquisitionRecipe({
        ...parsed,
        id,
        revision,
        ...(current.legacySourceId === undefined ? {} : { legacySourceId: current.legacySourceId }),
      });
      if (JSON.stringify(candidate) === JSON.stringify(current)) return this.currentSnapshot();
      const next = parseTrustAcquisitionRecipe({ ...candidate, revision: revision + 1 });
      return this.commit(
        this.document.recipes.map((entry) => (entry.id === id ? next : entry)),
        signal,
      );
    }, signal);
  }

  duplicate(
    id: string,
    revision: number,
    name: string,
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    return this.mutate(async () => {
      const definition = trustAcquisitionRecipeDefinition(this.find(id, revision));
      const parsed = parseTrustAcquisitionRecipeInput({ ...definition, name });
      this.assertUnique(parsed.name);
      const copy = parseTrustAcquisitionRecipe({ ...parsed, id: this.createId(), revision: 1 });
      return this.commit([...this.document.recipes, copy], signal);
    }, signal);
  }

  delete(
    id: string,
    revision: number,
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    return this.mutate(async () => {
      this.find(id, revision);
      return this.commit(
        this.document.recipes.filter((entry) => entry.id !== id),
        signal,
      );
    }, signal);
  }

  private assertAvailable(): void {
    if (this.unavailable || this.options.store.capability().state !== "ready")
      throw storeUnavailable();
  }

  private find(id: string, revision: number): TrustAcquisitionRecipe {
    const recipe = this.document.recipes.find((entry) => entry.id === id);
    if (!recipe)
      throw new KafkaTrustRecipeError(
        "TEMPLATE_NOT_FOUND",
        "The trust acquisition template is no longer available.",
        "Refresh templates or use a retained profile revision.",
      );
    if (recipe.revision !== revision)
      throw new KafkaTrustRecipeError(
        "VALIDATION",
        "The trust acquisition template revision changed.",
        "Refresh and review the current revision before retrying.",
      );
    return recipe;
  }

  private assertUnique(name: string, excludingId?: string): void {
    if (
      this.document.recipes.some(
        (entry) =>
          entry.id !== excludingId &&
          trustRecipeComparableName(entry.name) === trustRecipeComparableName(name),
      )
    ) {
      throw new KafkaTrustRecipeError(
        "TEMPLATE_DUPLICATE",
        "A template with this name already exists.",
        "Choose a unique name or edit the existing template.",
      );
    }
  }

  private async ensureLoaded(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.loading ??= this.load();
    await this.loading;
    signal?.throwIfAborted();
  }

  private async load(): Promise<void> {
    if (this.options.store.capability().state !== "ready") {
      this.unavailable = true;
      return;
    }
    try {
      const stored = await this.options.store.load();
      if (stored !== undefined) {
        this.document = parseTrustAcquisitionRecipeDocument(stored);
        return;
      }
      const legacy = await this.loadLegacy();
      const recipes =
        legacy === undefined
          ? initialRecipes().map((definition) => ({
              ...definition,
              id: this.createId(),
              revision: 1,
            }))
          : [];
      await this.commit(recipes);
    } catch {
      this.unavailable = true;
      this.document = { version: 1, recipes: [] };
    }
  }

  private async commit(
    recipes: readonly TrustAcquisitionRecipe[],
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    const next = parseTrustAcquisitionRecipeDocument({ version: 1, recipes });
    signal?.throwIfAborted();
    try {
      await this.options.store.commit(next, signal);
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") throw error;
      throw storeUnavailable();
    }
    this.document = next;
    return this.currentSnapshot();
  }

  private mutate(
    operation: () => Promise<TrustAcquisitionRecipeSnapshot>,
    signal?: AbortSignal,
  ): Promise<TrustAcquisitionRecipeSnapshot> {
    const result = this.tail.then(async () => {
      await this.ensureLoaded(signal);
      this.assertAvailable();
      return operation();
    });
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}
