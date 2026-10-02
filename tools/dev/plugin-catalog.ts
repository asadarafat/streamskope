import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import { DEVELOPMENT_VERSION, isPluginCompatibleWithHost } from "../../src/plugins/compatibility";
import { readBoundedFile } from "../../src/platform/node/bounded-file";
import type {
  OfficialPluginCatalog,
  OfficialPluginEntry,
} from "../../src/platform/node/plugins/catalog";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import {
  MAX_PLUGIN_PACKAGE_BYTES,
  parsePluginPackage,
} from "../../src/platform/node/plugins/package";

interface LocalPackage {
  readonly entry: OfficialPluginEntry;
  readonly bytes: Uint8Array;
}

/** Source development only: refresh reads local build outputs without contacting GitHub. */
export class DevelopmentPluginCatalog implements Pick<OfficialPluginCatalog, "list" | "download"> {
  constructor(private readonly directory: string) {}

  private async packages(): Promise<readonly LocalPackage[]> {
    let names: string[];
    try {
      names = await readdir(this.directory);
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return [];
      throw error;
    }
    const packages: LocalPackage[] = [];
    const identities = new Set<string>();
    for (const name of names.filter((entry) => entry.endsWith(".skope-plugin")).sort()) {
      const path = join(this.directory, name);
      const bytes = await readBoundedFile(path, MAX_PLUGIN_PACKAGE_BYTES, { rejectSymlinks: true });
      const { manifest, sha256 } = parsePluginPackage(bytes);
      const descriptor = OFFICIAL_PLUGINS.find((plugin) => plugin.id === manifest.id);
      if (
        descriptor === undefined ||
        !isPluginCompatibleWithHost(manifest, `v${DEVELOPMENT_VERSION}`) ||
        manifest.compatibility?.target.system !== descriptor.directory
      ) {
        throw new Error(
          "Local plugin packages must use an official identity, the current API and a development version.",
        );
      }
      if (identities.has(manifest.id)) {
        throw new Error(
          `Multiple local packages declare ${manifest.id}. Rebuild the plugin output directory.`,
        );
      }
      identities.add(manifest.id);
      if (name !== officialPluginAssets(descriptor, manifest.version).packageAsset) {
        throw new Error("Local plugin filename does not match its manifest identity and version.");
      }
      packages.push({
        entry: { manifest, sha256, downloadUrl: pathToFileURL(path).href },
        bytes,
      });
    }
    return packages;
  }

  async list(): Promise<readonly OfficialPluginEntry[]> {
    return (await this.packages()).map(({ entry }) => entry);
  }

  async download(pluginId: string): Promise<{ bytes: Uint8Array; sha256: string }> {
    const plugin = (await this.packages()).find(({ entry }) => entry.manifest.id === pluginId);
    if (plugin === undefined) {
      throw new Error(
        "Local plugin is unavailable. Run npm run package -- plugin, then refresh plugins.",
      );
    }
    return { bytes: plugin.bytes, sha256: plugin.entry.sha256 };
  }
}
