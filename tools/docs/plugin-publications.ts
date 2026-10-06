import { pathToFileURL } from "node:url";

import {
  OfficialPluginCatalog,
  OFFICIAL_PLUGIN_REPOSITORY,
  type OfficialPluginEntry,
} from "../../src/platform/node/plugins/catalog";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import {
  MAX_PLUGIN_PACKAGE_BYTES,
  parsePortablePluginPackage,
  pluginPackagePayloadBytes,
  pluginPackageSha256,
} from "../../src/platform/node/plugins/package";
import {
  TRUSTED_PLUGIN_PUBLISHERS,
  type TrustedPluginPublisher,
} from "../../src/platform/node/plugins/publishers";
import type { PluginCompatibility } from "../../src/plugins/contracts";

const API_ROOT = `https://api.github.com/repos/${OFFICIAL_PLUGIN_REPOSITORY}`;
const RELEASE_ROOT = `https://github.com/${OFFICIAL_PLUGIN_REPOSITORY}/releases`;

export interface PluginPublication {
  readonly id: string;
  readonly version: string;
  readonly api: number;
  readonly sha256: string;
  readonly release_tag: string;
  readonly release_url: string;
  readonly compatibility: PluginCompatibility | null;
  readonly portable: {
    readonly name: string;
    readonly url: string;
    readonly sha256: string;
    readonly size: number;
    readonly publisher: string;
    readonly publisher_key_id: string;
  } | null;
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid plugin publication metadata.");
  return value as Record<string, unknown>;
}

function releaseUrl(value: unknown, route: "tag" | "download", tag: string, name?: string): string {
  if (typeof value !== "string") throw new Error("Missing official plugin release URL.");
  const url = new URL(value);
  const expected = `${RELEASE_ROOT}/${route}/${tag}${name === undefined ? "" : `/${name}`}`;
  if (
    url.origin !== "https://github.com" ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    decodeURIComponent(url.href) !== expected
  )
    throw new Error("Plugin publication URL is outside its official release.");
  return value;
}

async function publication(
  entry: OfficialPluginEntry,
  releases: readonly unknown[],
  catalog: Pick<OfficialPluginCatalog, "downloadPinned">,
  signal: AbortSignal,
  publishers: readonly TrustedPluginPublisher[],
): Promise<PluginPublication> {
  const assetId = Number(entry.downloadUrl.slice(entry.downloadUrl.lastIndexOf("/") + 1));
  const matches = releases
    .map(record)
    .filter(
      (release) =>
        Array.isArray(release.assets) &&
        release.assets.some((asset: unknown) => record(asset).id === assetId),
    );
  if (matches.length !== 1) throw new Error("Catalog package needs exactly one published release.");
  const release = matches[0]!;
  if (
    release.draft !== false ||
    typeof release.tag_name !== "string" ||
    typeof release.published_at !== "string" ||
    !Number.isFinite(Date.parse(release.published_at))
  )
    throw new Error("Catalog package release is not published.");
  const plugin = OFFICIAL_PLUGINS.find((item) => item.id === entry.manifest.id)!;
  const assets = release.assets as unknown[];
  const expected = officialPluginAssets(plugin, entry.manifest.version);
  const result: PluginPublication = {
    id: entry.manifest.id,
    version: entry.manifest.version,
    api: entry.manifest.apiVersion,
    sha256: entry.sha256,
    release_tag: release.tag_name,
    release_url: releaseUrl(release.html_url, "tag", release.tag_name),
    compatibility: entry.manifest.compatibility ?? null,
    portable: null,
  };
  const candidates = assets
    .map(record)
    .filter((asset) => asset.name === expected.portablePackageAsset);
  if (candidates.length === 0) return result;
  if (candidates.length !== 1) throw new Error("Plugin release has duplicate portable assets.");
  const asset = candidates[0]!;
  if (
    typeof asset.id !== "number" ||
    !Number.isSafeInteger(asset.id) ||
    asset.id <= 0 ||
    typeof asset.size !== "number" ||
    !Number.isSafeInteger(asset.size) ||
    asset.size <= 0 ||
    asset.size > MAX_PLUGIN_PACKAGE_BYTES ||
    asset.state !== "uploaded" ||
    typeof asset.digest !== "string" ||
    !/^sha256:[a-f0-9]{64}$/u.test(asset.digest)
  )
    throw new Error("Portable plugin asset needs an uploaded size and SHA256 digest.");
  const url = releaseUrl(
    asset.browser_download_url,
    "download",
    release.tag_name,
    expected.portablePackageAsset,
  );
  const primary = await catalog.downloadPinned(entry, signal);
  const { bytes } = await catalog.downloadPinned(
    {
      manifest: entry.manifest,
      sha256: asset.digest.slice(7),
      downloadUrl: `${API_ROOT}/releases/assets/${asset.id}`,
    },
    signal,
  );
  if (pluginPackageSha256(primary.bytes) !== entry.sha256)
    throw new Error("Catalog package digest differs from its publication.");
  if (bytes.byteLength !== asset.size)
    throw new Error("Portable plugin asset size differs from GitHub metadata.");
  const verified = parsePortablePluginPackage(bytes, asset.digest.slice(7), publishers);
  if (
    JSON.stringify(verified.manifest) !== JSON.stringify(entry.manifest) ||
    !Buffer.from(pluginPackagePayloadBytes(bytes, publishers)).equals(Buffer.from(primary.bytes))
  )
    throw new Error("Portable plugin differs from its catalog package and manifest.");
  return {
    ...result,
    portable: {
      name: expected.portablePackageAsset,
      url,
      sha256: pluginPackageSha256(bytes),
      size: bytes.byteLength,
      publisher: verified.publisher!.name,
      publisher_key_id: verified.publisher!.keyId,
    },
  };
}

/** Tests inject delivery and trust explicitly; production uses the existing catalog and shipped keys. */
export async function verifyPluginPublications(
  entries: readonly OfficialPluginEntry[],
  releases: readonly unknown[],
  catalog: Pick<OfficialPluginCatalog, "downloadPinned">,
  publishers: readonly TrustedPluginPublisher[] = TRUSTED_PLUGIN_PUBLISHERS,
): Promise<readonly PluginPublication[]> {
  const signal = AbortSignal.timeout(60_000);
  return Promise.all(
    entries.map((entry) => publication(entry, releases, catalog, signal, publishers)),
  );
}

export async function capturePluginPublications(
  desktopRelease: string,
  fetcher: typeof fetch = fetch,
): Promise<readonly PluginPublication[]> {
  const signal = AbortSignal.timeout(60_000);
  const catalog = new OfficialPluginCatalog(fetcher, desktopRelease);
  const entries = await catalog.list({ signal });
  if (entries.length === 0) return [];
  // Fixed API JSON endpoint: no redirects, bounded stream. Package transport belongs to the catalog.
  const response = await fetcher(`${API_ROOT}/releases?per_page=100`, {
    redirect: "error",
    signal,
    headers: {
      Accept: "application/vnd.github+json",
      "User-Agent": "StreamSkope-docs-qualification",
    },
  });
  if (!response.ok || !response.body)
    throw new Error(`Plugin publication lookup failed (HTTP ${response.status}).`);
  const reader = (response.body as ReadableStream<Uint8Array>).getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > 2 * 1024 * 1024)
        throw new Error("Plugin publication metadata exceeds its size limit.");
      chunks.push(chunk.value);
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
  const releases: unknown = JSON.parse(Buffer.concat(chunks, size).toString("utf8"));
  if (!Array.isArray(releases)) throw new Error("Invalid plugin publication release list.");
  return verifyPluginPublications(entries, releases, catalog);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const fetcher: typeof fetch = (input, options) => {
    const headers = new Headers(options?.headers);
    if (
      new URL(input instanceof Request ? input.url : input).hostname === "api.github.com" &&
      process.env.GH_TOKEN
    )
      headers.set("Authorization", `Bearer ${process.env.GH_TOKEN}`);
    return fetch(input, { ...options, headers });
  };
  capturePluginPublications(process.argv[2] ?? "", fetcher)
    .then((packages) => process.stdout.write(JSON.stringify(packages)))
    .catch((error: unknown) => {
      process.stderr.write(
        `Plugin availability failed: ${error instanceof Error ? error.message : "invalid publication"}\n`,
      );
      process.exitCode = 1;
    });
}
