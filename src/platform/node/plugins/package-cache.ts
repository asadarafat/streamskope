import { randomUUID } from "node:crypto";
import { lstat, mkdir, open, readdir, rename, rm } from "node:fs/promises";
import { dirname, join } from "node:path";

import type {
  PluginCachedPackage,
  PluginPackageReference,
  PluginPackageTrust,
} from "../../../plugins/contracts";
import { parsePluginManifest } from "../../../plugins/validation";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "../atomic-private-text-file";
import { readBoundedFile } from "../bounded-file";

import { MAX_PLUGIN_ARCHIVE_BYTES, type VerifiedPluginPackage } from "./package";

export const MAX_CACHED_PLUGIN_PACKAGES = 4;
interface RecordEntry extends PluginCachedPackage {
  readonly size: number;
  readonly lastUsedAt: string;
}
export interface CachedPluginBytes {
  readonly bytes: Uint8Array;
  readonly verified: VerifiedPluginPackage;
  readonly record: PluginCachedPackage;
}
export type PluginPackageVerifier = (bytes: Uint8Array, sha256?: string) => VerifiedPluginPackage;
const DIGEST = /^[a-f0-9]{64}$/u;
const MAX_INDEX_BYTES = 128 * 1024;
function missing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}
function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid plugin package cache metadata.");
  return value as Record<string, unknown>;
}
function timestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString() === value
  );
}
function publicRecord(value: RecordEntry): PluginCachedPackage {
  return {
    manifest: value.manifest,
    sha256: value.sha256,
    cachedAt: value.cachedAt,
    trust: value.trust,
    ...(value.publisher === undefined ? {} : { publisher: value.publisher }),
  };
}

/** Private delivery copies preserve reviewed bytes independently of the user's original file. */
export class PluginPackageCache {
  private queue: Promise<unknown> = Promise.resolve();
  private readonly pins = new Map<string, number>();
  constructor(
    private readonly root: string,
    private readonly verify: PluginPackageVerifier,
  ) {}

  pin(sha256: string): () => void {
    if (!DIGEST.test(sha256)) throw new Error("Invalid plugin package cache digest.");
    this.pins.set(sha256, (this.pins.get(sha256) ?? 0) + 1);
    let released = false;
    return (): void => {
      if (released) return;
      released = true;
      const remaining = (this.pins.get(sha256) ?? 1) - 1;
      if (remaining === 0) this.pins.delete(sha256);
      else this.pins.set(sha256, remaining);
    };
  }
  private serial<T>(operation: () => Promise<T>): Promise<T> {
    const next = this.queue.then(operation);
    this.queue = next.catch(() => undefined);
    return next;
  }
  private async directory(create: boolean): Promise<void> {
    if (create) await mkdir(dirname(this.root), { mode: 0o700, recursive: true });
    const parent = await lstat(dirname(this.root));
    if (!parent.isDirectory() || parent.isSymbolicLink())
      throw new Error("Plugin package cache parent must use a regular directory.");
    if (create) await mkdir(this.root, { mode: 0o700, recursive: true });
    const metadata = await lstat(this.root);
    if (!metadata.isDirectory() || metadata.isSymbolicLink())
      throw new Error("Plugin package cache must use a regular directory.");
  }
  private async index(): Promise<RecordEntry[]> {
    const path = join(this.root, "index.json");
    try {
      await this.directory(false);
      const metadata = await lstat(path);
      if (!metadata.isFile() || metadata.isSymbolicLink())
        throw new Error("Plugin cache index must use a regular file.");
    } catch (error) {
      if (missing(error)) return [];
      throw error;
    }
    try {
      const raw = record(
        JSON.parse(
          (await readBoundedFile(path, MAX_INDEX_BYTES, { rejectSymlinks: true })).toString("utf8"),
        ),
      );
      if (
        raw.formatVersion !== 1 ||
        !Array.isArray(raw.packages) ||
        raw.packages.length > MAX_CACHED_PLUGIN_PACKAGES ||
        Object.keys(raw).some((key) => !["formatVersion", "packages"].includes(key))
      )
        throw new Error("Invalid plugin package cache index.");
      const identities = new Set<string>();
      return (raw.packages as unknown[]).map((value): RecordEntry => {
        const entry = record(value);
        if (
          typeof entry.sha256 !== "string" ||
          !DIGEST.test(entry.sha256) ||
          identities.has(entry.sha256) ||
          !timestamp(entry.cachedAt) ||
          !timestamp(entry.lastUsedAt) ||
          typeof entry.size !== "number" ||
          !Number.isSafeInteger(entry.size) ||
          entry.size <= 0 ||
          entry.size > MAX_PLUGIN_ARCHIVE_BYTES ||
          !["publisher", "official", "development"].includes(String(entry.trust)) ||
          Object.keys(entry).some(
            (key) =>
              ![
                "manifest",
                "sha256",
                "cachedAt",
                "lastUsedAt",
                "size",
                "trust",
                "publisher",
              ].includes(key),
          )
        )
          throw new Error("Invalid plugin package cache entry.");
        const publisher = entry.publisher === undefined ? undefined : record(entry.publisher);
        if (
          (entry.trust === "publisher") !== (publisher !== undefined) ||
          (publisher !== undefined &&
            (typeof publisher.keyId !== "string" ||
              typeof publisher.name !== "string" ||
              Object.keys(publisher).some((key) => !["keyId", "name"].includes(key))))
        )
          throw new Error("Invalid cached publisher provenance.");
        identities.add(entry.sha256);
        return {
          manifest: parsePluginManifest(entry.manifest),
          sha256: entry.sha256,
          cachedAt: entry.cachedAt,
          lastUsedAt: entry.lastUsedAt,
          size: entry.size,
          trust: entry.trust as PluginPackageTrust,
          ...(publisher === undefined
            ? {}
            : { publisher: { keyId: publisher.keyId as string, name: publisher.name as string } }),
        };
      });
    } catch (error) {
      // Invalid private metadata is recoverable. Unindexed archives remain untrusted:
      // never reconstruct provenance by scanning files in this directory.
      if (error !== null && typeof error === "object" && "code" in error) throw error;
      await this.saveIndex([]);
      return [];
    }
  }
  private async saveIndex(packages: readonly RecordEntry[]): Promise<void> {
    const path = join(this.root, "index.json");
    try {
      const metadata = await lstat(path);
      if (!metadata.isFile() || metadata.isSymbolicLink())
        throw new Error("Plugin cache index must use a regular file.");
    } catch (error) {
      if (!missing(error)) throw error;
    }
    const contents = JSON.stringify({ formatVersion: 1, packages });
    if (Buffer.byteLength(contents) > MAX_INDEX_BYTES)
      throw new Error("Plugin package cache metadata exceeds its storage bound.");
    await writeAtomicPrivateTextFile({
      path,
      contents,
      createTempId: createAtomicPrivateFileTempId,
    });
  }
  private async readEntry(
    entry: RecordEntry,
    allowDevelopment: boolean,
  ): Promise<CachedPluginBytes> {
    if (entry.trust === "development" && !allowDevelopment)
      throw new Error("Development packages cannot be installed as official cached plugins.");
    const bytes = await readBoundedFile(
      join(this.root, `${entry.sha256}.skope-plugin`),
      MAX_PLUGIN_ARCHIVE_BYTES,
      { rejectSymlinks: true },
    );
    const verified = this.verify(bytes, entry.sha256);
    if (
      bytes.byteLength !== entry.size ||
      JSON.stringify(verified.manifest) !== JSON.stringify(entry.manifest) ||
      JSON.stringify(verified.publisher) !== JSON.stringify(entry.publisher) ||
      (verified.publisher !== undefined) !== (entry.trust === "publisher")
    )
      throw new Error("Cached plugin package does not match its verified provenance.");
    return { bytes, verified, record: publicRecord(entry) };
  }
  list(allowDevelopment = false): Promise<readonly PluginCachedPackage[]> {
    return this.serial(async () => {
      const entries = await this.index();
      const packages: PluginCachedPackage[] = [];
      for (const entry of entries) {
        try {
          packages.push((await this.readEntry(entry, allowDevelopment)).record);
        } catch {
          /* Damaged copies are never advertised as available. */
        }
      }
      return packages;
    });
  }
  read(reference: PluginPackageReference, allowDevelopment = false): Promise<CachedPluginBytes> {
    return this.serial(async () => {
      const entries = await this.index();
      const entry = entries.find(
        (value) =>
          value.sha256 === reference.sha256 &&
          value.manifest.id === reference.pluginId &&
          value.manifest.version === reference.version,
      );
      if (entry === undefined)
        throw new Error(
          "The selected plugin package is not cached. Download it or choose its signed portable file.",
        );
      const result = await this.readEntry(entry, allowDevelopment);
      await this.saveIndex(
        entries.map((value) =>
          value.sha256 === entry.sha256
            ? { ...value, lastUsedAt: new Date().toISOString() }
            : value,
        ),
      );
      return result;
    });
  }
  put(
    bytes: Uint8Array,
    sha256: string,
    trust: "official" | "development" | "publisher",
  ): Promise<PluginCachedPackage> {
    const verified = this.verify(bytes, sha256);
    if (trust === "publisher" && verified.publisher === undefined)
      throw new Error("File installation requires a verified publisher signature.");
    return this.serial(async () => {
      await this.directory(true);
      const current = await this.index();
      const existing = current.find((entry) => entry.sha256 === sha256);
      const retained = current.filter((entry) => entry.sha256 !== sha256);
      while (retained.length >= MAX_CACHED_PLUGIN_PACKAGES) {
        const oldest = retained
          .filter((entry) => !this.pins.has(entry.sha256))
          .sort((left, right) => Date.parse(left.lastUsedAt) - Date.parse(right.lastUsedAt))[0];
        if (oldest === undefined)
          throw new Error(
            "The plugin cache is full of reviewed packages. Close a package review and retry.",
          );
        retained.splice(retained.indexOf(oldest), 1);
      }
      const timestamp = new Date().toISOString();
      const entry: RecordEntry = {
        manifest: verified.manifest,
        sha256,
        cachedAt: existing?.cachedAt ?? timestamp,
        lastUsedAt: timestamp,
        size: bytes.byteLength,
        trust: verified.publisher === undefined ? trust : "publisher",
        ...(verified.publisher === undefined ? {} : { publisher: verified.publisher }),
      };
      const path = join(this.root, `${sha256}.skope-plugin`);
      try {
        const metadata = await lstat(path);
        if (!metadata.isFile() || metadata.isSymbolicLink())
          throw new Error("Cached plugin packages must use regular files.");
      } catch (error) {
        if (!missing(error)) throw error;
      }
      const temporary = join(this.root, `.archive-${randomUUID()}.tmp`);
      try {
        const handle = await open(temporary, "wx", 0o600);
        try {
          await handle.writeFile(bytes);
          await handle.sync();
        } finally {
          await handle.close();
        }
        await rename(temporary, path);
        try {
          await this.saveIndex([...retained, entry]);
        } catch (error) {
          if (existing === undefined) await rm(path, { force: true });
          throw error;
        }
        const keep = new Set([...retained, entry].map((value) => `${value.sha256}.skope-plugin`));
        for (const name of await readdir(this.root)) {
          if (
            !/^[a-f0-9]{64}\.skope-plugin$/u.test(name) ||
            keep.has(name) ||
            this.pins.has(name.slice(0, 64))
          )
            continue;
          const obsolete = join(this.root, name);
          const metadata = await lstat(obsolete);
          if (!metadata.isFile() || metadata.isSymbolicLink())
            throw new Error("Obsolete plugin cache bytes must use regular files.");
          await rm(obsolete, { force: true });
        }
        return publicRecord(entry);
      } finally {
        await rm(temporary, { force: true });
      }
    });
  }
}
