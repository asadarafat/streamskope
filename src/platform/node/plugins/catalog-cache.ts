import { lstat, mkdir } from "node:fs/promises";
import { join, resolve } from "node:path";

import { parsePluginManifest } from "../../../plugins/validation";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "../atomic-private-text-file";
import { readBoundedFile } from "../bounded-file";

import { OFFICIAL_PLUGIN_REPOSITORY, type OfficialPluginEntry } from "./catalog";
import { OFFICIAL_PLUGINS } from "./official";

const MAX_CATALOG_BYTES = 128 * 1024;
const ASSET_URL = new RegExp(
  `^https://api\\.github\\.com/repos/${OFFICIAL_PLUGIN_REPOSITORY}/releases/assets/[1-9][0-9]*$`,
  "u",
);

export interface StoredPluginCatalog {
  readonly checkedAt: string;
  readonly entries: readonly OfficialPluginEntry[];
}

function missing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid saved plugin catalog.");
  return value as Record<string, unknown>;
}

function parse(value: unknown): StoredPluginCatalog {
  const input = object(value);
  if (
    input.formatVersion !== 1 ||
    typeof input.checkedAt !== "string" ||
    !Number.isFinite(Date.parse(input.checkedAt)) ||
    new Date(input.checkedAt).toISOString() !== input.checkedAt ||
    !Array.isArray(input.entries) ||
    input.entries.length > OFFICIAL_PLUGINS.length ||
    Object.keys(input).some((key) => !["formatVersion", "checkedAt", "entries"].includes(key))
  )
    throw new Error("Invalid saved plugin catalog.");
  const ids = new Set<string>();
  const entries = (input.entries as unknown[]).map((value): OfficialPluginEntry => {
    const entry = object(value);
    const manifest = parsePluginManifest(entry.manifest);
    if (
      !OFFICIAL_PLUGINS.some((plugin) => plugin.id === manifest.id) ||
      ids.has(manifest.id) ||
      typeof entry.sha256 !== "string" ||
      !/^[a-f0-9]{64}$/u.test(entry.sha256) ||
      typeof entry.downloadUrl !== "string" ||
      !ASSET_URL.test(entry.downloadUrl) ||
      Object.keys(entry).some((key) => !["manifest", "sha256", "downloadUrl"].includes(key))
    )
      throw new Error("Invalid saved plugin catalog entry.");
    ids.add(manifest.id);
    return { manifest, sha256: entry.sha256, downloadUrl: entry.downloadUrl };
  });
  return { checkedAt: input.checkedAt, entries };
}

/** Last successful HTTPS discovery metadata; it never authorizes execution of package bytes. */
export class PluginCatalogCache {
  readonly #root: string;

  constructor(root: string) {
    this.#root = resolve(root);
  }

  async #directory(create: boolean): Promise<void> {
    if (create) await mkdir(this.#root, { recursive: true, mode: 0o700 });
    const metadata = await lstat(this.#root);
    if (!metadata.isDirectory() || metadata.isSymbolicLink())
      throw new Error("Saved plugin catalog must use a regular directory.");
  }

  async read(): Promise<StoredPluginCatalog | undefined> {
    try {
      await this.#directory(false);
      const bytes = await readBoundedFile(join(this.#root, "catalog.json"), MAX_CATALOG_BYTES, {
        rejectSymlinks: true,
      });
      return parse(JSON.parse(bytes.toString("utf8")));
    } catch (error) {
      if (missing(error)) return undefined;
      throw error;
    }
  }

  async save(catalog: StoredPluginCatalog, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const validated = parse({ formatVersion: 1, ...catalog });
    const contents = JSON.stringify({ formatVersion: 1, ...validated });
    if (Buffer.byteLength(contents) > MAX_CATALOG_BYTES)
      throw new Error("Saved plugin catalog exceeds its size limit.");
    await this.#directory(true);
    const path = join(this.#root, "catalog.json");
    try {
      const metadata = await lstat(path);
      if (!metadata.isFile() || metadata.isSymbolicLink())
        throw new Error("Saved plugin catalog must use a regular file.");
    } catch (error) {
      if (!missing(error)) throw error;
    }
    await writeAtomicPrivateTextFile({
      path,
      contents,
      createTempId: createAtomicPrivateFileTempId,
      ...(signal === undefined ? {} : { signal }),
    });
  }
}
