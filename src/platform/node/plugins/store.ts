import { lstat, mkdir, mkdtemp, readdir, rename, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import type { JsonValue, PluginInstallation, PluginManifest } from "../../../plugins/contracts";
import { comparePluginManifests, parsePluginJson } from "../../../plugins/validation";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "../atomic-private-text-file";
import { readBoundedFile } from "../bounded-file";

import {
  MAX_PLUGIN_ARCHIVE_BYTES,
  parsePluginPackage,
  type VerifiedPluginPackage,
} from "./package";
import { PluginCatalogCache } from "./catalog-cache";
import { TRUSTED_PLUGIN_PUBLISHERS, type TrustedPluginPublisher } from "./publishers";

export interface PluginStoreOptions {
  /** Host composition only; package files and renderer requests cannot grant publisher trust. */
  readonly trustedPublishers?: readonly TrustedPluginPublisher[];
}

export interface ActivePlugin {
  readonly manifest: PluginManifest;
  readonly sha256: string;
  readonly contentSha256: string;
  readonly publisher?: VerifiedPluginPackage["publisher"];
  readonly directory: string;
  readonly backendPath: string;
  readonly rendererPath: string;
  readonly stylesPath?: string;
}

interface InstallationState {
  active?: string;
  inactive?: string;
  previous?: string;
  pending?: string | null;
  error?: string;
}

interface StoreState {
  readonly formatVersion: 1;
  readonly plugins: Record<string, InstallationState>;
}

const IDENTIFIER = /^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$/u;
const DIGEST = /^[a-f0-9]{64}$/u;

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : "Plugin could not be loaded.";
}

function validId(id: string): void {
  if (id.length > 80 || !IDENTIFIER.test(id)) throw new Error("Invalid plugin identifier.");
}

function isMissing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

async function directory(path: string, create: boolean): Promise<void> {
  if (create) await mkdir(path, { mode: 0o700, recursive: true });
  const metadata = await lstat(path);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error("Plugin storage must use regular directories.");
  }
}

function parseState(bytes: Uint8Array): StoreState {
  const value: unknown = JSON.parse(Buffer.from(bytes).toString("utf8"));
  if (
    value === null ||
    typeof value !== "object" ||
    !("formatVersion" in value) ||
    value.formatVersion !== 1 ||
    !("plugins" in value) ||
    value.plugins === null ||
    typeof value.plugins !== "object" ||
    Array.isArray(value.plugins)
  ) {
    throw new Error("Invalid plugin installation state.");
  }
  const entries = Object.entries(value.plugins);
  if (entries.length > 32) throw new Error("Too many plugin installations.");
  const plugins: Record<string, InstallationState> = Object.create(null) as Record<
    string,
    InstallationState
  >;
  for (const [id, raw] of entries) {
    validId(id);
    if (raw === null || typeof raw !== "object" || Array.isArray(raw))
      throw new Error("Invalid plugin installation record.");
    const entry = raw as Record<string, unknown>;
    if (
      Object.keys(entry).some(
        (key) => !["active", "inactive", "previous", "pending", "error"].includes(key),
      )
    )
      throw new Error("Unexpected plugin installation field.");
    for (const key of ["active", "inactive", "previous", "pending"] as const) {
      if (
        entry[key] !== undefined &&
        !(key === "pending" && entry[key] === null) &&
        (typeof entry[key] !== "string" || !DIGEST.test(entry[key]))
      )
        throw new Error("Invalid plugin installation digest.");
    }
    if (entry.error !== undefined && (typeof entry.error !== "string" || entry.error.length > 2048))
      throw new Error("Invalid plugin installation error.");
    plugins[id] = entry;
  }
  return { formatVersion: 1, plugins };
}

/** Verified packages are staged separately from the atomic active-version pointer. */
export class PluginStore {
  readonly #root: string;
  readonly catalogCache: PluginCatalogCache;
  readonly #trustedPublishers: readonly TrustedPluginPublisher[];
  #queue: Promise<unknown> = Promise.resolve();

  constructor(rootDirectory: string, options: PluginStoreOptions = {}) {
    this.#root = resolve(rootDirectory);
    this.catalogCache = new PluginCatalogCache(this.#root);
    this.#trustedPublishers = Object.freeze(
      (options.trustedPublishers ?? TRUSTED_PLUGIN_PUBLISHERS).map((publisher) =>
        Object.freeze({ ...publisher, pluginIds: Object.freeze([...publisher.pluginIds]) }),
      ),
    );
  }

  /** One sealed host verification authority is shared by downloads, storage and lifecycle changes. */
  verifyPackage(bytes: Uint8Array, expectedSha256?: string): VerifiedPluginPackage {
    return parsePluginPackage(bytes, expectedSha256, this.#trustedPublishers);
  }

  /** Recovery survives package replacement/removal; the plugin clears it after remote cleanup. */
  async readRecoveryState(id: string): Promise<JsonValue | null> {
    validId(id);
    try {
      await directory(this.#root, false);
      await directory(join(this.#root, ".recovery"), false);
      return parsePluginJson(
        JSON.parse(
          Buffer.from(
            await readBoundedFile(join(this.#root, ".recovery", `${id}.json`), 64 * 1024, {
              rejectSymlinks: true,
            }),
          ).toString("utf8"),
        ),
      );
    } catch (error) {
      if (isMissing(error)) return null;
      throw error;
    }
  }

  writeRecoveryState(id: string, value: JsonValue | null): Promise<void> {
    validId(id);
    const contents = JSON.stringify(parsePluginJson(value));
    if (Buffer.byteLength(contents) > 64 * 1024)
      throw new Error("Plugin recovery state is too large.");
    return this.#serial(async () => {
      await directory(this.#root, true);
      await directory(join(this.#root, ".recovery"), true);
      const path = join(this.#root, ".recovery", `${id}.json`);
      try {
        const metadata = await lstat(path);
        if (!metadata.isFile() || metadata.isSymbolicLink())
          throw new Error("Plugin recovery state must use a regular file.");
      } catch (error) {
        if (!isMissing(error)) throw error;
      }
      await writeAtomicPrivateTextFile({
        path,
        contents,
        createTempId: createAtomicPrivateFileTempId,
      });
    });
  }

  async #state(): Promise<StoreState> {
    try {
      await directory(this.#root, false);
      return parseState(
        await readBoundedFile(join(this.#root, "state.json"), 128 * 1024, { rejectSymlinks: true }),
      );
    } catch (error) {
      if (isMissing(error))
        return {
          formatVersion: 1,
          plugins: Object.create(null) as Record<string, InstallationState>,
        };
      throw error;
    }
  }

  async #save(state: StoreState): Promise<void> {
    await directory(this.#root, true);
    await writeAtomicPrivateTextFile({
      path: join(this.#root, "state.json"),
      contents: JSON.stringify(state),
      createTempId: createAtomicPrivateFileTempId,
    });
  }

  #serial<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#queue.then(operation);
    this.#queue = result.catch(() => undefined);
    return result;
  }

  async #verified(id: string, digest: string): Promise<ActivePlugin> {
    validId(id);
    if (!DIGEST.test(digest)) throw new Error("Invalid plugin package digest.");
    const parent = join(this.#root, id);
    const path = join(parent, digest);
    for (const item of [this.#root, parent, path]) await directory(item, false);
    const plugin = this.verifyPackage(
      await readBoundedFile(join(path, "package.skope-plugin"), MAX_PLUGIN_ARCHIVE_BYTES, {
        rejectSymlinks: true,
      }),
      digest,
    );
    if (plugin.manifest.id !== id)
      throw new Error("Installed plugin identity does not match its directory.");
    for (const [name, content] of plugin.files) {
      const stored = await readBoundedFile(join(path, name), content.byteLength, {
        rejectSymlinks: true,
      });
      if (!stored.equals(Buffer.from(content)))
        throw new Error("Installed plugin code does not match its verified package.");
    }
    return {
      manifest: plugin.manifest,
      sha256: digest,
      contentSha256: plugin.contentSha256,
      ...(plugin.publisher === undefined ? {} : { publisher: plugin.publisher }),
      directory: path,
      backendPath: join(path, plugin.manifest.backend),
      rendererPath: join(path, plugin.manifest.renderer),
      ...(plugin.manifest.styles ? { stylesPath: join(path, plugin.manifest.styles) } : {}),
    };
  }

  async #describe(id: string, entry: InstallationState): Promise<PluginInstallation> {
    let active: PluginManifest | undefined;
    let previous: PluginManifest | undefined;
    let installed: PluginManifest | undefined;
    let error = entry.error;
    try {
      if (entry.active) active = (await this.#verified(id, entry.active)).manifest;
      if (entry.previous) previous = (await this.#verified(id, entry.previous)).manifest;
      installed =
        typeof entry.pending === "string"
          ? (await this.#verified(id, entry.pending)).manifest
          : (active ??
            (entry.inactive ? (await this.#verified(id, entry.inactive)).manifest : undefined));
    } catch (failure) {
      error = errorText(failure);
    }
    return {
      id,
      ...(active ? { active } : {}),
      ...(previous ? { previous } : {}),
      ...(installed ? { installed } : {}),
      pending: entry.pending === undefined ? null : entry.pending === null ? "remove" : "install",
      restartRequired: entry.pending !== undefined,
      ...(error ? { error } : {}),
    };
  }

  async list(): Promise<readonly PluginInstallation[]> {
    const state = await this.#state();
    return Promise.all(
      Object.entries(state.plugins).map(([id, entry]) => this.#describe(id, entry)),
    );
  }

  async #stage(plugin: VerifiedPluginPackage, bytes: Uint8Array): Promise<void> {
    await directory(this.#root, true);
    const parent = join(this.#root, plugin.manifest.id);
    await directory(parent, true);
    const destination = join(parent, plugin.sha256);
    let repair = false;
    try {
      await this.#verified(plugin.manifest.id, plugin.sha256);
      return;
    } catch (error) {
      try {
        // Reinstallation may repair missing or corrupt contents, but never follows a replaced directory.
        await directory(destination, false);
        repair = true;
      } catch (directoryError) {
        if (!isMissing(directoryError)) throw directoryError;
        if (!isMissing(error)) throw error;
      }
    }
    const stage = await mkdtemp(join(parent, ".install-"));
    let rollbackDirectory: string | undefined;
    let retainRollback = false;
    try {
      await writeFile(join(stage, "package.skope-plugin"), bytes, { flag: "wx", mode: 0o600 });
      for (const [name, content] of plugin.files)
        await writeFile(join(stage, name), content, { flag: "wx", mode: 0o600 });
      if (repair) {
        await directory(destination, false);
        rollbackDirectory = await mkdtemp(join(parent, ".repair-"));
        const previous = join(rollbackDirectory, "original");
        await rename(destination, previous);
        try {
          await rename(stage, destination);
        } catch (error) {
          try {
            await rename(previous, destination);
          } catch (rollbackError) {
            retainRollback = true;
            throw new AggregateError(
              [error, rollbackError],
              `Plugin repair failed. Original files are retained at ${previous}.`,
              { cause: rollbackError },
            );
          }
          throw error;
        }
      } else await rename(stage, destination);
    } finally {
      await rm(stage, { force: true, recursive: true });
      if (rollbackDirectory !== undefined && !retainRollback)
        await rm(rollbackDirectory, { force: true, recursive: true });
    }
  }

  install(bytes: Uint8Array, expectedSha256: string): Promise<PluginInstallation> {
    return this.#serial(async () => {
      const plugin = this.verifyPackage(bytes, expectedSha256);
      validId(plugin.manifest.id);
      const state = await this.#state();
      const existing = await this.#assertUnchangedVersion(
        plugin,
        state.plugins[plugin.manifest.id],
      );
      if (
        !Object.hasOwn(state.plugins, plugin.manifest.id) &&
        Object.keys(state.plugins).length >= 32
      )
        throw new Error("At most 32 plugin installations can be retained.");
      const retained = existing ?? { plugin, bytes };
      await this.#stage(retained.plugin, retained.bytes);
      const entry = state.plugins[plugin.manifest.id] ?? {};
      if (entry.active === retained.plugin.sha256 && entry.pending === undefined)
        return this.#describe(plugin.manifest.id, entry);
      entry.pending = retained.plugin.sha256;
      delete entry.error;
      state.plugins[plugin.manifest.id] = entry;
      await this.#save(state);
      return this.#describe(plugin.manifest.id, entry);
    });
  }

  /** Prepare code without making it active, including after a host crash or restart. */
  prepareInstall(bytes: Uint8Array, expectedSha256: string): Promise<ActivePlugin> {
    return this.#serial(async () => {
      const plugin = this.verifyPackage(bytes, expectedSha256);
      validId(plugin.manifest.id);
      const state = await this.#state();
      const existing = await this.#assertUnchangedVersion(
        plugin,
        state.plugins[plugin.manifest.id],
      );
      if (
        !Object.hasOwn(state.plugins, plugin.manifest.id) &&
        Object.keys(state.plugins).length >= 32
      )
        throw new Error("At most 32 plugin installations can be retained.");
      const retained = existing ?? { plugin, bytes };
      await this.#stage(retained.plugin, retained.bytes);
      return this.#verified(plugin.manifest.id, retained.plugin.sha256);
    });
  }

  /** Protect retained identities even when their backend could not activate in this host. */
  async #assertUnchangedVersion(
    plugin: VerifiedPluginPackage,
    entry: InstallationState | undefined,
  ): Promise<{ readonly plugin: VerifiedPluginPackage; readonly bytes: Uint8Array } | undefined> {
    if (entry === undefined) return undefined;
    let equivalent:
      { readonly plugin: VerifiedPluginPackage; readonly bytes: Uint8Array } | undefined;
    for (const digest of new Set([entry.active, entry.inactive, entry.previous, entry.pending])) {
      if (!digest) continue;
      let retained: VerifiedPluginPackage;
      let bytes: Uint8Array;
      try {
        const parent = join(this.#root, plugin.manifest.id);
        const path = join(parent, digest);
        for (const item of [this.#root, parent, path]) await directory(item, false);
        bytes = await readBoundedFile(
          join(path, "package.skope-plugin"),
          MAX_PLUGIN_ARCHIVE_BYTES,
          {
            rejectSymlinks: true,
          },
        );
        retained = this.verifyPackage(bytes, digest);
      } catch {
        // Corrupt or missing cache bytes cannot establish a trusted version claim.
        // A verified catalog download may repair them; never execute the old bytes.
        continue;
      }
      if (comparePluginManifests(plugin.manifest, retained.manifest) === 0) {
        if (plugin.contentSha256 === retained.contentSha256)
          equivalent ??= { plugin: retained, bytes };
        else if (plugin.manifest.apiVersion >= 3)
          throw new Error(
            "This plugin version has different content from a retained package. Publish a new plugin version.",
          );
      }
    }
    return equivalent;
  }

  /** Publish only this installation after activation and previous-backend cleanup succeed. */
  commitInstall(id: string, digest: string): Promise<string | undefined> {
    return this.#serial(async () => {
      await this.#verified(id, digest);
      const state = await this.#state();
      const entry = state.plugins[id] ?? {};
      if (!Object.hasOwn(state.plugins, id) && Object.keys(state.plugins).length >= 32)
        throw new Error("At most 32 plugin installations can be retained.");
      const retained = new Set([entry.active, entry.previous]);
      delete entry.previous;
      for (const candidate of retained) {
        if (!candidate || candidate === digest) continue;
        try {
          await this.#verified(id, candidate);
          entry.previous = candidate;
          break;
        } catch {
          // An unreadable old cache must not become this working version's rollback package.
        }
      }
      entry.active = digest;
      delete entry.inactive;
      delete entry.pending;
      delete entry.error;
      state.plugins[id] = entry;
      await this.#save(state);
      try {
        // Keep the current and one previous verified version for UI rollback.
        const parent = join(this.#root, id);
        for (const name of await readdir(parent)) {
          if (!DIGEST.test(name) || [entry.active, entry.previous, entry.pending].includes(name))
            continue;
          const path = join(parent, name);
          await directory(path, false);
          await rm(path, { recursive: true, force: true });
        }
      } catch (error) {
        return `Plugin activated, but obsolete package files could not be deleted. ${errorText(error)}`;
      }
      return undefined;
    });
  }

  /** A failed candidate cannot be selected by a later startup. */
  discardInstall(id: string, digest: string): Promise<void> {
    return this.#serial(async () => {
      validId(id);
      if (!DIGEST.test(digest)) throw new Error("Invalid plugin package digest.");
      const entry = (await this.#state()).plugins[id];
      if ([entry?.active, entry?.inactive, entry?.previous, entry?.pending].includes(digest))
        return;
      await directory(this.#root, false);
      const parent = join(this.#root, id);
      await directory(parent, false);
      await rm(join(parent, digest), { recursive: true, force: true });
    });
  }

  /** Remove its state and every retained package, without following replaced directories. */
  uninstall(id: string): Promise<string | undefined> {
    return this.#serial(async () => {
      validId(id);
      const state = await this.#state();
      const parent = join(this.#root, id);
      let savedDirectory: string | undefined;
      try {
        await directory(parent, false);
        savedDirectory = await mkdtemp(join(this.#root, `.remove-${id}-`));
        await rename(parent, join(savedDirectory, "packages"));
      } catch (error) {
        if (!isMissing(error)) throw error;
      }
      try {
        delete state.plugins[id];
        await this.#save(state);
      } catch (error) {
        if (savedDirectory !== undefined) {
          await rename(join(savedDirectory, "packages"), parent);
          await rm(savedDirectory, { recursive: true, force: true });
        }
        throw error;
      }
      try {
        // Retry any earlier interrupted byte deletion for this plugin as well.
        for (const name of await readdir(this.#root)) {
          if (!name.startsWith(`.remove-${id}-`)) continue;
          const path = join(this.#root, name);
          await directory(path, false);
          await rm(path, { recursive: true, force: true });
        }
      } catch (error) {
        const warning = `Plugin removed, but retained package files could not be deleted. Retry removal to clean them up. ${errorText(error)}`;
        state.plugins[id] = { error: warning };
        try {
          await this.#save(state);
        } catch {
          // Removal was already committed. The running host still reports the cleanup warning.
        }
        return warning;
      }
      return undefined;
    });
  }

  remove(id: string): Promise<void> {
    return this.#serial(async () => {
      validId(id);
      const state = await this.#state();
      const entry = state.plugins[id];
      if (!entry) return;
      entry.pending = null;
      await this.#save(state);
    });
  }

  /** A broken package is isolated; other plugins and the Kafka host can still start. */
  activatePending(): Promise<readonly ActivePlugin[]> {
    return this.#serial(async () => {
      const state = await this.#state();
      const active: ActivePlugin[] = [];
      for (const [id, entry] of Object.entries(state.plugins)) {
        if (entry.pending !== undefined) {
          if (entry.active && entry.active !== entry.pending) entry.previous = entry.active;
          if (entry.pending === null) delete entry.active;
          else entry.active = entry.pending;
          delete entry.pending;
          delete entry.inactive;
          delete entry.error;
        }
        if (!entry.active) continue;
        try {
          active.push(await this.#verified(id, entry.active));
        } catch (error) {
          entry.error = errorText(error);
          delete entry.active;
          if (entry.previous) {
            try {
              const fallback = await this.#verified(id, entry.previous);
              entry.active = entry.previous;
              active.push(fallback);
            } catch {
              /* A corrupt previous version must also remain inactive. */
            }
            delete entry.previous;
          }
        }
      }
      await this.#save(state);
      return active;
    });
  }

  async getActive(id: string): Promise<ActivePlugin | undefined> {
    validId(id);
    const digest = (await this.#state()).plugins[id]?.active;
    return digest ? this.#verified(id, digest) : undefined;
  }

  /** Retry only explicitly retained, fully verified installed code; never scan arbitrary directories. */
  async getInstalled(id: string): Promise<ActivePlugin | undefined> {
    validId(id);
    const entry = (await this.#state()).plugins[id];
    const digest = entry?.active ?? entry?.inactive;
    return digest ? this.#verified(id, digest) : undefined;
  }

  rollback(id: string): Promise<ActivePlugin | undefined> {
    return this.#serial(async () => {
      validId(id);
      const state = await this.#state();
      const entry = state.plugins[id];
      if (!entry) return undefined;
      const failed = entry.active;
      delete entry.active;
      delete entry.inactive;
      entry.error =
        "Plugin failed to start. The previous working version was restored when available.";
      let fallback: ActivePlugin | undefined;
      if (entry.previous) {
        try {
          fallback = await this.#verified(id, entry.previous);
          entry.active = entry.previous;
        } catch {
          /* Keep a failed plugin disabled without stopping the desktop. */
        }
      }
      delete entry.previous;
      if (fallback === undefined && failed) {
        try {
          await this.#verified(id, failed);
          entry.inactive = failed;
        } catch {
          /* Corrupt code cannot become a retry candidate. */
        }
      }
      await this.#save(state);
      return fallback;
    });
  }
}
