import type { PluginCatalogSnapshot } from "../../../plugins/contracts";
import { isPluginCompatibleWithHost, isPrereleaseVersion } from "../../../plugins/validation";

import type { OfficialPluginCatalog } from "./catalog";
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
    return {
      plugins: catalog.entries
        .map((entry) => entry.manifest)
        .filter(
          (manifest) =>
            isPluginCompatibleWithHost(manifest, this.options.hostRelease) &&
            !(
              manifest.apiVersion === 4 &&
              isPrereleaseVersion(manifest.version) &&
              !isPrereleaseVersion(this.options.hostRelease.replace(/^v/u, ""))
            ),
        ),
      source,
      checkedAt: catalog.checkedAt,
    };
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
