import type { PluginCatalogSnapshot, PluginPackageReference } from "../../../plugins/contracts";
import { isPluginCompatibleWithHost, isPrereleaseVersion } from "../../../plugins/validation";

import type { OfficialPluginCatalog, OfficialPluginEntry, PluginCatalogRequest } from "./catalog";
import type { PluginCatalogCache, StoredPluginCatalog } from "./catalog-cache";
import { pluginNetworkProblem } from "./network-errors";

export interface PluginCatalogDiscoveryOptions {
  readonly source: Pick<OfficialPluginCatalog, "list">;
  readonly cache: Pick<PluginCatalogCache, "read" | "save">;
  readonly hostRelease: string;
  readonly assertOpen: () => void;
  readonly persist?: boolean;
}
interface Refresh {
  readonly controller: AbortController;
  readonly generation: number;
  readonly owners: Map<symbol, PluginCatalogRequest["onProgress"]>;
  readonly result: Promise<PluginCatalogSnapshot>;
}
interface DiscoveryRequest extends PluginCatalogRequest {
  readonly throwOnFailure?: boolean;
}

function summary(): string {
  return "Plugin catalog is unavailable. Check plugin download settings, refresh online, or install a signed file.";
}

/** Discovery has its own queue: a slow remote catalog never blocks installed plugin lifecycle work. */
export class PluginCatalogDiscovery {
  private refreshing: Refresh | undefined;
  private memory: StoredPluginCatalog | undefined;
  private generation = 0;

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
    } catch {
      return { plugins: [], source: "unavailable", error: summary() };
    }
  }

  invalidateRemote(): void {
    this.generation += 1;
    this.refreshing?.controller.abort();
    this.refreshing = undefined;
  }

  async catalog(refresh = true, request: DiscoveryRequest = {}): Promise<PluginCatalogSnapshot> {
    this.options.assertOpen();
    if (!refresh) return this.cached();
    request.signal?.throwIfAborted();
    if (this.refreshing === undefined || this.refreshing.controller.signal.aborted) {
      const controller = new AbortController();
      const generation = this.generation;
      const owners = new Map<symbol, PluginCatalogRequest["onProgress"]>();
      const result = Promise.resolve().then(() => this.refresh(controller, generation, owners));
      const current = { controller, generation, owners, result };
      this.refreshing = current;
      void result.then(
        () => {
          if (this.refreshing === current) this.refreshing = undefined;
        },
        () => {
          if (this.refreshing === current) this.refreshing = undefined;
        },
      );
    }
    const current = this.refreshing;
    const owner = Symbol();
    current.owners.set(owner, request.onProgress);
    try {
      return await new Promise<PluginCatalogSnapshot>((resolve, reject) => {
        const abort = (): void => {
          current.owners.delete(owner);
          if (current.owners.size === 0) current.controller.abort();
          reject(pluginNetworkProblem(request.signal?.reason, request.signal));
        };
        request.signal?.addEventListener("abort", abort, { once: true });
        void current.result.then(
          (value) => {
            request.signal?.removeEventListener("abort", abort);
            if (request.signal?.aborted) {
              reject(pluginNetworkProblem(request.signal.reason, request.signal));
              return;
            }
            resolve(value);
          },
          (error: unknown) => {
            request.signal?.removeEventListener("abort", abort);
            reject(pluginNetworkProblem(error));
          },
        );
        if (request.signal?.aborted) abort();
      });
    } catch (error) {
      if (request.throwOnFailure || request.signal?.aborted) throw error;
      return { ...(await this.cached()), error: summary() };
    } finally {
      current.owners.delete(owner);
    }
  }

  private async refresh(
    controller: AbortController,
    generation: number,
    owners: Map<symbol, PluginCatalogRequest["onProgress"]>,
  ): Promise<PluginCatalogSnapshot> {
    const assertCurrent = (): void => {
      this.options.assertOpen();
      controller.signal.throwIfAborted();
      if (generation !== this.generation) throw new Error("Plugin catalog refresh was superseded.");
    };
    const entries = await this.options.source.list({
      signal: controller.signal,
      onProgress: (phase, received, total): void => {
        for (const progress of owners.values()) progress?.(phase, received, total);
      },
    });
    assertCurrent();
    const catalog = { entries, checkedAt: new Date().toISOString() };
    const snapshot = this.snapshot(catalog, "live");
    if (this.options.persist === false) {
      this.memory = catalog;
      return snapshot;
    }
    try {
      await this.options.cache.save(catalog, controller.signal);
      assertCurrent();
      this.memory = catalog;
      return snapshot;
    } catch {
      assertCurrent();
      this.memory = catalog;
      return {
        ...snapshot,
        error: "Plugin catalog was refreshed but could not be saved. Check desktop storage access.",
      };
    }
  }
}
