import type {
  PluginAcquisitionProgress,
  PluginNetworkSnapshot,
  PluginNetworkTestResult,
  PluginNetworkUpdateInput,
} from "../../../plugins/contracts";
import type { ProfileProtector } from "../profile-protector";

import { PluginAcquisitions, type PluginAcquisitionContext } from "./acquisition";
import type { PluginCatalogSource } from "./catalog";
import type { PluginCatalogDiscovery } from "./catalog-discovery";
import { PluginNetworkSettings } from "./network-settings";
import type { PluginNetworkTransport } from "./network-transport";

interface Options {
  readonly path: string;
  readonly transport?: PluginNetworkTransport;
  readonly protector?: ProfileProtector;
  readonly durableSettings?: boolean;
  readonly changed: () => void;
}

/** Plugin download policy and command ownership are independent of provider connections. */
export class PluginNetworkController {
  private readonly acquisitions = new PluginAcquisitions();
  private readonly settings: PluginNetworkSettings;
  constructor(private readonly options: Options) {
    this.settings = new PluginNetworkSettings({
      path: options.path,
      ...(options.transport === undefined ? {} : { transport: options.transport }),
      ...(options.protector === undefined ? {} : { protector: options.protector }),
      ...(options.durableSettings === undefined
        ? {}
        : { durableSettings: options.durableSettings }),
      changing: (): void => {
        this.acquisitions.cancelRemote();
        options.changed();
      },
    });
  }
  snapshot(): Promise<PluginNetworkSnapshot> {
    return this.settings.snapshot();
  }
  update(input: PluginNetworkUpdateInput): Promise<PluginNetworkSnapshot> {
    return this.settings.update(input);
  }
  subscribe(listener: (progress: PluginAcquisitionProgress) => void): () => void {
    return this.acquisitions.subscribe(listener);
  }
  cancel(requestId: string): void {
    this.acquisitions.cancel(requestId);
  }
  async catalog(
    requestId: string | undefined,
    discovery: PluginCatalogDiscovery,
  ): ReturnType<PluginCatalogDiscovery["catalog"]> {
    try {
      return await this.acquire(requestId, "catalog", true, (context) =>
        discovery.catalog(true, {
          signal: context.signal,
          onProgress: context.progress,
          throwOnFailure: true,
        }),
      );
    } catch (error) {
      const cached = await discovery.catalog(false);
      const message =
        error instanceof Error ? error.message : "Plugin catalog could not be refreshed.";
      const recovery =
        error !== null &&
        typeof error === "object" &&
        "recovery" in error &&
        typeof error.recovery === "string"
          ? ` ${error.recovery}`
          : "";
      return { ...cached, error: `${message}${recovery}`.slice(0, 1024) };
    }
  }
  acquire<T>(
    requestId: string | undefined,
    operation: PluginAcquisitionProgress["operation"],
    remote: boolean,
    task: (context: PluginAcquisitionContext, settingsRevision?: number) => Promise<T>,
    discardLate?: (result: T) => void,
  ): Promise<T> {
    return this.acquisitions.run(
      requestId,
      operation,
      remote,
      async (context) => {
        const revision = remote ? await this.settings.remote() : undefined;
        const guarded: PluginAcquisitionContext = {
          ...context,
          assertCurrent: (): void => {
            context.assertCurrent();
            if (revision !== undefined) this.settings.assertCurrent(revision);
          },
        };
        guarded.assertCurrent();
        const result = await task(guarded, revision);
        try {
          guarded.assertCurrent();
        } catch (error) {
          discardLate?.(result);
          throw error;
        }
        return result;
      },
      discardLate,
    );
  }
  test(requestId: string, source: PluginCatalogSource): Promise<PluginNetworkTestResult> {
    return this.acquire(requestId, "test", true, async (context, settingsRevision) => {
      const entries = await source.list({ signal: context.signal, onProgress: context.progress });
      context.assertCurrent();
      const entry = entries[0];
      const scope =
        entry === undefined || source.probe === undefined ? "catalog-only" : "catalog-and-assets";
      if (entry !== undefined && source.probe !== undefined)
        await source.probe(entry, { signal: context.signal, onProgress: context.progress });
      context.assertCurrent();
      return {
        settingsRevision: settingsRevision!,
        checkedAt: new Date().toISOString(),
        scope,
        detail:
          entry === undefined
            ? "The official catalog is reachable. No compatible published package is available, so asset access could not be tested."
            : source.probe === undefined
              ? "The catalog is reachable. This catalog does not provide a bounded asset probe, so package download access has not been tested."
              : "The official catalog and a bounded sample of a published package asset are reachable. Package integrity is verified during installation.",
      };
    });
  }
  async close(): Promise<void> {
    this.acquisitions.close();
    this.settings.close();
    this.options.changed();
    await this.options.transport?.close();
  }
}
