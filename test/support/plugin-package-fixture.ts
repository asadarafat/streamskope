import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import {
  OFFICIAL_PLUGINS,
  officialPluginAssets,
  type OfficialPlugin,
} from "../../src/platform/node/plugins/official";
import { parsePluginManifest } from "../../src/plugins/validation";
import { DEVELOPMENT_VERSION, isDevelopmentPluginVersion } from "../../src/plugins/compatibility";
import {
  encodePluginPackage,
  parsePluginPackage,
  pluginPackageSha256,
} from "../../src/platform/node/plugins/package";

export async function builtPluginAssets(
  directory: OfficialPlugin["directory"],
): Promise<ReturnType<typeof officialPluginAssets>> {
  const plugin = OFFICIAL_PLUGINS.find((item) => item.directory === directory)!;
  const manifests = (await readdir("dist/plugin-package")).filter(
    (name) => name.startsWith(`streamskope-${directory}-`) && name.endsWith("-plugin.json"),
  );
  if (manifests.length !== 1) throw new Error(`Expected one built ${directory} plugin manifest.`);
  const manifest = parsePluginManifest(
    JSON.parse(await readFile(join("dist/plugin-package", manifests[0]!), "utf8")),
  );
  if (manifest.id !== plugin.id)
    throw new Error("Built plugin identity does not match its assets.");
  return officialPluginAssets(plugin, manifest.version);
}

/** Fixture releases reuse actual built plugin code; no synthetic version is published. */
export async function pluginPackageFixtures(
  directory: OfficialPlugin["directory"] = "eda",
): Promise<{
  readonly current: ReturnType<typeof fixture>;
  readonly update: ReturnType<typeof fixture>;
  readonly broken: ReturnType<typeof fixture>;
}> {
  const assets = await builtPluginAssets(directory);
  const current = fixture(await readFile(join("dist/plugin-package", assets.packageAsset)));
  const parsed = parsePluginPackage(current.bytes, current.sha256);
  const revised = (offset: number): typeof parsed.manifest => {
    if (isDevelopmentPluginVersion(parsed.manifest.version)) {
      const build = BigInt(parsed.manifest.version.split(".")[3] ?? "0") + BigInt(offset);
      return { ...parsed.manifest, version: `${DEVELOPMENT_VERSION}.${build}` };
    }
    const [major, minor, patch] = parsed.manifest.version.split("-")[0]!.split(".").map(Number);
    return { ...parsed.manifest, version: `${major}.${minor}.${patch! + offset}` };
  };
  const update = fixture(encodePluginPackage(revised(1), parsed.files));
  const brokenFiles = new Map(parsed.files);
  brokenFiles.set(
    "renderer.js",
    Buffer.from('throw new Error("Fixture renderer activation failed"); export default {};'),
  );
  const broken = fixture(encodePluginPackage(revised(2), brokenFiles));
  return { current, update, broken };
}

function fixture(bytes: Uint8Array): {
  readonly bytes: Uint8Array;
  readonly sha256: string;
  readonly manifest: ReturnType<typeof parsePluginPackage>["manifest"];
} {
  const sha256 = pluginPackageSha256(bytes);
  return { bytes, sha256, manifest: parsePluginPackage(bytes, sha256).manifest };
}
