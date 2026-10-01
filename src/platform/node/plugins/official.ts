/** Explicit allowlist shared by the official catalog and package tooling. */
export const OFFICIAL_PLUGINS = [
  {
    directory: "eda",
    id: "streamskope.eda",
    packageAsset: "streamskope-eda.skope-plugin",
    manifestAsset: "streamskope-eda-plugin.json",
  },
  {
    directory: "nsp",
    id: "streamskope.nsp",
    packageAsset: "streamskope-nsp.skope-plugin",
    manifestAsset: "streamskope-nsp-plugin.json",
  },
] as const;

export type OfficialPlugin = (typeof OFFICIAL_PLUGINS)[number];

/** Published filenames carry compatibility; fixed names above remain for API 2 releases. */
export function officialPluginAssets(
  plugin: OfficialPlugin,
  version: string,
): { readonly packageAsset: string; readonly manifestAsset: string; readonly prefix: string } {
  const prefix = `streamskope-${plugin.directory}-${version}`;
  return {
    prefix,
    packageAsset: `${prefix}.skope-plugin`,
    manifestAsset: `${prefix}-plugin.json`,
  };
}
