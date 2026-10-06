import type { PluginManifest } from "../../../plugins/contracts";
import { STREAMSKOPE_RELEASE } from "../../../plugins/host-release";
import {
  comparePluginManifests,
  isPluginCompatibleWithHost,
  isPrereleaseVersion,
  isSupportedPluginApiVersion,
  parsePluginManifest,
} from "../../../plugins/validation";

import { MAX_PLUGIN_PACKAGE_BYTES, parsePluginPackage, pluginPackageSha256 } from "./package";
import { OFFICIAL_PLUGINS, type OfficialPlugin } from "./official";

export const OFFICIAL_PLUGIN_REPOSITORY = "asadarafat/streamskope";
export const EDA_PLUGIN_ID = OFFICIAL_PLUGINS[0].id;
export const PLUGIN_PACKAGE_ASSET = OFFICIAL_PLUGINS[0].packageAsset;
export const PLUGIN_MANIFEST_ASSET = OFFICIAL_PLUGINS[0].manifestAsset;
const MAX_RELEASE_CANDIDATES = 16;

export interface OfficialPluginEntry {
  readonly manifest: PluginManifest;
  readonly sha256: string;
  readonly downloadUrl: string;
}
export type PluginCatalogSource = Pick<OfficialPluginCatalog, "list" | "download"> &
  Partial<Pick<OfficialPluginCatalog, "downloadPinned">>;

interface ReleaseAsset {
  readonly id: number;
  readonly name: string;
  readonly digest: string;
  readonly size: number;
}

interface PluginRelease {
  readonly published: string;
  readonly packageAsset: ReleaseAsset;
  readonly manifestAsset: ReleaseAsset;
  readonly version?: string;
}

const API_ROOT = `https://api.github.com/repos/${OFFICIAL_PLUGIN_REPOSITORY}`;
const DOWNLOAD_HOSTS = new Set([
  "api.github.com",
  "github.com",
  "release-assets.githubusercontent.com",
  "objects.githubusercontent.com",
]);

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid official plugin release metadata.");
  return value as Record<string, unknown>;
}

function asset(value: unknown): ReleaseAsset {
  const input = record(value);
  if (
    typeof input.id !== "number" ||
    !Number.isSafeInteger(input.id) ||
    input.id <= 0 ||
    typeof input.name !== "string" ||
    typeof input.digest !== "string" ||
    !/^sha256:[a-f0-9]{64}$/u.test(input.digest) ||
    typeof input.size !== "number" ||
    !Number.isSafeInteger(input.size) ||
    input.size <= 0 ||
    input.size > MAX_PLUGIN_PACKAGE_BYTES ||
    input.state !== "uploaded"
  ) {
    throw new Error("Official plugin release has no valid SHA256 digest or asset size.");
  }
  return { id: input.id, name: input.name, digest: input.digest.slice(7), size: input.size };
}

function selectReleases(value: unknown, plugin: OfficialPlugin): readonly PluginRelease[] {
  if (!Array.isArray(value)) throw new Error("Invalid official plugin release list.");
  const candidates: { published: string; assets: unknown[] }[] = [];
  for (const item of value as unknown[]) {
    const release = record(item);
    if (
      release.draft !== false ||
      typeof release.tag_name !== "string" ||
      !(
        /^v\d/u.test(release.tag_name) ||
        release.tag_name.startsWith(`plugins/${plugin.directory}/v`)
      ) ||
      typeof release.published_at !== "string" ||
      !Number.isFinite(Date.parse(release.published_at)) ||
      !Array.isArray(release.assets)
    )
      continue;
    const assets = release.assets as unknown[];
    if (!assets.some((entry) => packageIdentity(record(entry).name, plugin) !== undefined))
      continue;
    candidates.push({ published: release.published_at, assets });
  }
  const selected: PluginRelease[] = [];
  for (const candidate of candidates.sort(
    (left, right) => Date.parse(right.published) - Date.parse(left.published),
  )) {
    const packages = candidate.assets.filter(
      (entry) => packageIdentity(record(entry).name, plugin) !== undefined,
    );
    for (const packaged of packages) {
      if (selected.length === MAX_RELEASE_CANDIDATES) return selected;
      const name = record(packaged).name as string;
      const identity = packageIdentity(name, plugin)!;
      const manifests = candidate.assets.filter(
        (entry) => record(entry).name === identity.manifestAsset,
      );
      if (
        manifests.length !== 1 ||
        packages.filter((entry) => record(entry).name === name).length !== 1
      )
        throw new Error("The official plugin release is incomplete.");
      selected.push({
        published: candidate.published,
        packageAsset: asset(packaged),
        manifestAsset: asset(manifests[0]),
        ...(identity.version === undefined ? {} : { version: identity.version }),
      });
    }
  }
  return selected;
}

function packageIdentity(
  name: unknown,
  plugin: OfficialPlugin,
): { readonly manifestAsset: string; readonly version?: string } | undefined {
  if (name === plugin.packageAsset) return { manifestAsset: plugin.manifestAsset };
  const prefix = `streamskope-${plugin.directory}-`;
  const suffix = ".skope-plugin";
  if (typeof name !== "string" || !name.startsWith(`${prefix}v`) || !name.endsWith(suffix))
    return undefined;
  return {
    manifestAsset: `${name.slice(0, -suffix.length)}-plugin.json`,
    version: name.slice(prefix.length, -suffix.length),
  };
}

/** Trust is the official GitHub repository over HTTPS, plus GitHub's SHA256 asset digests. */
export class OfficialPluginCatalog {
  readonly #fetch: typeof fetch;
  readonly #hostRelease: string;

  constructor(fetcher: typeof fetch = fetch, hostRelease = STREAMSKOPE_RELEASE) {
    this.#fetch = fetcher;
    this.#hostRelease = hostRelease;
  }

  async #download(
    url: string,
    limit: number,
    binary: boolean,
    signal: AbortSignal,
  ): Promise<Uint8Array> {
    let next = new URL(url);
    for (let redirects = 0; redirects <= 4; redirects += 1) {
      signal.throwIfAborted();
      if (
        next.protocol !== "https:" ||
        !DOWNLOAD_HOSTS.has(next.hostname) ||
        next.port ||
        next.username ||
        next.password ||
        next.hash
      )
        throw new Error("The official plugin download redirected outside GitHub.");
      const response = await this.#fetch(next, {
        redirect: "manual",
        signal,
        headers: {
          Accept: binary ? "application/octet-stream" : "application/vnd.github+json",
          "User-Agent": "StreamSkope-plugin-installer",
          ...(next.hostname === "api.github.com" ? { "X-GitHub-Api-Version": "2022-11-28" } : {}),
        },
      });
      if ([301, 302, 303, 307, 308].includes(response.status)) {
        await response.body?.cancel();
        const location = response.headers.get("location");
        if (!location) throw new Error("Official plugin download returned an invalid redirect.");
        next = new URL(location, next);
        continue;
      }
      if (!response.ok || !response.body)
        throw new Error(`Official plugin download failed (HTTP ${response.status}).`);
      const length = response.headers.get("content-length");
      if (length !== null && (!/^\d+$/u.test(length) || Number(length) > limit)) {
        await response.body.cancel();
        throw new Error("Official plugin download exceeds its size limit.");
      }
      const reader = (response.body as ReadableStream<Uint8Array>).getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) break;
          size += chunk.value.byteLength;
          if (size > limit) throw new Error("Official plugin download exceeds its size limit.");
          chunks.push(chunk.value);
        }
      } finally {
        await reader.cancel();
        reader.releaseLock();
      }
      return Buffer.concat(chunks, size);
    }
    throw new Error("Official plugin download redirected too many times.");
  }

  async #asset(assetInfo: ReleaseAsset, maximum: number, signal: AbortSignal): Promise<Uint8Array> {
    if (assetInfo.size > maximum) throw new Error("Official plugin asset exceeds its size limit.");
    const bytes = await this.#download(
      `${API_ROOT}/releases/assets/${assetInfo.id}`,
      maximum,
      true,
      signal,
    );
    if (bytes.byteLength !== assetInfo.size || pluginPackageSha256(bytes) !== assetInfo.digest)
      throw new Error(
        "Official plugin asset SHA256 or size does not match GitHub release metadata.",
      );
    return bytes;
  }

  async list(): Promise<readonly OfficialPluginEntry[]> {
    return this.#list(AbortSignal.timeout(30_000));
  }

  async #list(
    signal: AbortSignal,
    plugins: readonly OfficialPlugin[] = OFFICIAL_PLUGINS,
  ): Promise<readonly OfficialPluginEntry[]> {
    const releases: unknown = JSON.parse(
      Buffer.from(
        await this.#download(`${API_ROOT}/releases?per_page=100`, 2 * 1024 * 1024, false, signal),
      ).toString("utf8"),
    );
    const entries: OfficialPluginEntry[] = [];
    for (const plugin of plugins) {
      let selected: OfficialPluginEntry | undefined;
      const identities = new Map<string, string>();
      for (const release of selectReleases(releases, plugin)) {
        const metadata = record(
          JSON.parse(
            Buffer.from(await this.#asset(release.manifestAsset, 16 * 1024, signal)).toString(
              "utf8",
            ),
          ),
        );
        if (metadata.id !== plugin.id)
          throw new Error("The official plugin release has the wrong identity.");
        if (
          typeof metadata.apiVersion === "number" &&
          Number.isSafeInteger(metadata.apiVersion) &&
          metadata.apiVersion > 0 &&
          !isSupportedPluginApiVersion(metadata.apiVersion)
        )
          continue;
        const manifest = parsePluginManifest(metadata);
        const filenameVersion =
          manifest.apiVersion === 4 ? `v${manifest.version}` : manifest.version;
        if (release.version !== undefined && release.version !== filenameVersion)
          throw new Error("The plugin filename does not match its declared version.");
        if (manifest.apiVersion >= 3 && release.version === undefined)
          throw new Error("The plugin release filename must include its version.");
        if (manifest.apiVersion >= 3) {
          const previousDigest = identities.get(manifest.version);
          if (previousDigest !== undefined && previousDigest !== release.packageAsset.digest)
            throw new Error("The official plugin version has conflicting package content.");
          identities.set(manifest.version, release.packageAsset.digest);
        }
        if (!isPluginCompatibleWithHost(manifest, this.#hostRelease)) continue;
        if (
          manifest.apiVersion === 4 &&
          isPrereleaseVersion(manifest.version) &&
          !isPrereleaseVersion(this.#hostRelease.replace(/^v/u, ""))
        )
          continue;
        if (!selected || comparePluginManifests(manifest, selected.manifest) > 0) {
          selected = {
            manifest,
            sha256: release.packageAsset.digest,
            downloadUrl: `${API_ROOT}/releases/assets/${release.packageAsset.id}`,
          };
        }
      }
      if (selected) entries.push(selected);
    }
    return entries;
  }

  async download(id: string): Promise<{ readonly bytes: Uint8Array; readonly sha256: string }> {
    const pluginInfo = OFFICIAL_PLUGINS.find((plugin) => plugin.id === id);
    if (!pluginInfo) throw new Error("This plugin is not in the official catalog.");
    const signal = AbortSignal.timeout(30_000);
    const [entry] = await this.#list(signal, [pluginInfo]);
    if (!entry) throw new Error("This plugin has not been published yet.");
    return this.#downloadPinned(entry, signal);
  }

  async downloadPinned(
    entry: OfficialPluginEntry,
    callerSignal?: AbortSignal,
  ): Promise<{ readonly bytes: Uint8Array; readonly sha256: string }> {
    const deadline = AbortSignal.timeout(30_000);
    const signal =
      callerSignal === undefined ? deadline : AbortSignal.any([callerSignal, deadline]);
    return this.#downloadPinned(entry, signal);
  }

  async #downloadPinned(
    entry: OfficialPluginEntry,
    signal: AbortSignal,
  ): Promise<{ readonly bytes: Uint8Array; readonly sha256: string }> {
    if (
      !OFFICIAL_PLUGINS.some((plugin) => plugin.id === entry.manifest.id) ||
      !/^[a-f0-9]{64}$/u.test(entry.sha256) ||
      !new RegExp(`^${API_ROOT.replaceAll(".", "\\.")}/releases/assets/[1-9][0-9]*$`, "u").test(
        entry.downloadUrl,
      ) ||
      new URL(entry.downloadUrl).href !== entry.downloadUrl
    )
      throw new Error("The selected package is not a known official plugin asset.");
    const bytes = await this.#download(entry.downloadUrl, MAX_PLUGIN_PACKAGE_BYTES, true, signal);
    const plugin = parsePluginPackage(bytes, entry.sha256);
    if (JSON.stringify(plugin.manifest) !== JSON.stringify(entry.manifest))
      throw new Error("Plugin package does not match its published manifest.");
    return { bytes, sha256: entry.sha256 };
  }
}
