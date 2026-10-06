import type { PluginCatalogSnapshot, PluginPackageReference } from "../../../plugins/contracts";
import { isPluginCompatibleWithHost, isPrereleaseVersion } from "../../../plugins/validation";

import type { OfficialPluginCatalog, OfficialPluginEntry } from "./catalog";
import type { PluginCatalogCache, StoredPluginCatalog } from "./catalog-cache";

export interface PluginCatalogDiscoveryOptions {
  readonly source: Pick<OfficialPluginCatalog, "list">;
  readonly cache: Pick<PluginCatalogCache, "read" | "save">;
  readonly hostRelease: string;
  readonly assertOpen: () => void;
  readonly persist?: boolean;
}

function summary(error: unknown): string {
  return error instanceof Error ? error.message.slice(0, 1_024) : "The plugin could not be loaded.";
}

/** Discovery has its own queue: a slow remote catalog never blocks installed plugin lifecycle work. */
export class PluginCatalogDiscovery {
  private refreshing: Promise<PluginCatalogSnapshot> | undefined;
  private memory: StoredPluginCatalog | undefined;

  constructor(private readonly options: PluginCatalogDiscoveryOptions) {}

  private snapshot(catalog: StoredPluginCatalog, source: "live" | "cache"): PluginCatalogSnapshot {
    const entries = catalog.entries.filter(
      ({ manifest }) =>
        isPluginCompatibleWithHost(manifest, this.options.hostRelease) &&
        !(
          manifest.apiVersion === 4 &&
          isPrereleaseVersion(manifest.version) &&
          !isPrereleaseVersion(this.options.hostRelease.replace(/^v/u, ""))
        ),
    );
    return {
      plugins: entries.map((entry) => entry.manifest),
      packages: entries.map((entry) => ({
        pluginId: entry.manifest.id,
        version: entry.manifest.version,
        sha256: entry.sha256,
      })),
      source,
      checkedAt: catalog.checkedAt,
    };
  }

  async resolve(reference: PluginPackageReference): Promise<OfficialPluginEntry> {
    this.options.assertOpen();
    const catalog =
      this.memory ?? (this.options.persist === false ? undefined : await this.options.cache.read());
    const entry = catalog?.entries.find(
      (value) =>
        value.manifest.id === reference.pluginId &&
        value.manifest.version === reference.version &&
        value.sha256 === reference.sha256,
    );
    if (entry === undefined)
      throw new Error(
        "The selected catalog package changed or is unavailable. Refresh the catalog and review the package again.",
      );
    return entry;
  }

  private async cached(): Promise<PluginCatalogSnapshot> {
    try {
      if (this.memory !== undefined) return this.snapshot(this.memory, "cache");
      if (this.options.persist === false) return { plugins: [], source: "unavailable" };
      const cached = await this.options.cache.read();
      return cached === undefined
        ? { plugins: [], source: "unavailable" }
        : this.snapshot(cached, "cache");
    } catch (error) {
      return { plugins: [], source: "unavailable", error: summary(error) };
    }
  }

  async catalog(refresh = true): Promise<PluginCatalogSnapshot> {
    this.options.assertOpen();
    if (!refresh) return this.cached();
    if (this.refreshing !== undefined) return this.refreshing;
    const current = this.refresh();
    this.refreshing = current;
    try {
      return await current;
    } finally {
      if (this.refreshing === current) this.refreshing = undefined;
    }
  }

  private async refresh(): Promise<PluginCatalogSnapshot> {
    try {
      const entries = await this.options.source.list();
      this.options.assertOpen();
      const catalog = { entries, checkedAt: new Date().toISOString() };
      const snapshot = this.snapshot(catalog, "live");
      this.memory = catalog;
      if (this.options.persist === false) return snapshot;
      try {
        await this.options.cache.save(catalog);
        return snapshot;
      } catch (error) {
        return {
          ...snapshot,
          error: `Plugin catalog was refreshed but could not be saved. ${summary(error)}`,
        };
      }
    } catch (error) {
      return { ...(await this.cached()), error: summary(error) };
    }
  }
}
