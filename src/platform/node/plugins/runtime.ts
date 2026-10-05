import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
} from "../../../features/kafka/contracts";
import { PlatformaticAdminFactory } from "../../../features/kafka/engine/platformatic-admin";
import type {
  PluginBackend,
  PluginBackendHost,
  PluginBackendModule,
  PluginHostBindings,
  PluginRuntimePort,
} from "../../../plugins/api";
import type {
  JsonValue,
  PluginCatalogSnapshot,
  PluginChangeOperation,
  PluginChangePrompt,
  PluginEvent,
  PluginExitPrompt,
  PluginInstallation,
  PluginManifest,
  PluginProfileSource,
  PluginRequest,
  PluginSnapshot,
} from "../../../plugins/contracts";
import {
  comparePluginManifests,
  isPluginCompatibleWithHost,
  parsePluginId,
  parsePluginJson,
} from "../../../plugins/validation";
import { STREAMSKOPE_RELEASE } from "../../../plugins/host-release";
import { readBoundedFile } from "../bounded-file";

import { OfficialPluginCatalog } from "./catalog";
import { PluginCatalogDiscovery } from "./catalog-discovery";
import { PluginStore, type ActivePlugin } from "./store";

interface LoadedPlugin {
  readonly installation: ActivePlugin;
  readonly backend: PluginBackend;
  readonly activationId: string;
  readonly authority: { active: boolean; retired: boolean; draining: boolean };
  readonly requests: Set<Promise<unknown>>;
  readonly connections: Set<Promise<unknown>>;
}

interface ChangeConfirmation {
  readonly pluginId: string;
  readonly operation: PluginChangeOperation;
  readonly activationId: string;
  readonly fingerprint: string;
  readonly expires: number;
}

export interface PluginRuntimeOptions {
  readonly store: PluginStore;
  readonly catalog?: Pick<OfficialPluginCatalog, "list" | "download">;
  readonly loadModule?: (path: string) => Promise<PluginBackendModule>;
  readonly probeTopics?: PluginBackendHost["probeTopics"];
  readonly restart?: () => void | Promise<void>;
  readonly hostRelease?: string;
  /** Development catalog URLs have no official release provenance and stay in memory only. */
  readonly persistCatalog?: boolean;
}

export interface PluginRendererAsset {
  readonly content: Uint8Array;
  readonly contentType: string;
}

function problem(
  summary: string,
  recovery = "Open Preferences > Plugins to install or repair the plugin.",
): Error {
  return Object.assign(new Error(summary), {
    code: "BACKEND_UNAVAILABLE",
    stage: "backend",
    retryable: false,
    recovery,
  });
}

function summary(error: unknown): string {
  return error instanceof Error ? error.message.slice(0, 1_024) : "The plugin could not be loaded.";
}

function loadModule(path: string): Promise<PluginBackendModule> {
  const require = createRequire(path);
  delete require.cache[require.resolve(path)];
  const value: unknown = require(path);
  if (
    value === null ||
    typeof value !== "object" ||
    !("activate" in value) ||
    typeof value.activate !== "function"
  ) {
    throw new Error("The plugin does not export its backend activation function.");
  }
  return Promise.resolve(value as PluginBackendModule);
}

function assertBackend(value: PluginBackend): void {
  for (const name of [
    "execute",
    "validateProfile",
    "beforeExit",
    "resolveExit",
    "close",
    "beforeChange",
    "prepareUnload",
  ] as const) {
    if (typeof value?.[name] !== "function") throw new Error(`Plugin backend is missing ${name}.`);
  }
}

async function probeTopics(
  brokers: readonly string[],
  signal?: AbortSignal,
): Promise<readonly string[]> {
  signal?.throwIfAborted();
  const admin = new PlatformaticAdminFactory().create({
    brokers,
    tlsEnabled: false,
    operationTimeoutMs: 10_000,
  });
  const abort = (): void => {
    void admin.close().catch(() => undefined);
  };
  signal?.addEventListener("abort", abort, { once: true });
  try {
    const topics = await admin.listTopics();
    signal?.throwIfAborted();
    return topics;
  } finally {
    signal?.removeEventListener("abort", abort);
    await admin.close();
  }
}

/** Only explicitly installed, verified first-party packages are loaded into this trusted host. */
export class PluginRuntime implements PluginRuntimePort {
  private readonly modules = new Map<string, LoadedPlugin>();
  private readonly errors = new Map<string, string>();
  private readonly listeners = new Set<(event: PluginEvent) => void>();
  private readonly exitResolved = new Set<string>();
  private readonly exitPrompted = new Map<string, string>();
  private readonly changeListeners = new Set<(snapshot: PluginSnapshot) => void>();
  private readonly confirmations = new Map<string, ChangeConfirmation>();
  private readonly changing = new Set<string>();
  private readonly mutationIntents = new Map<string, symbol>();
  private mutations: Promise<unknown> = Promise.resolve();
  private revision = 0;
  private readonly catalogSource: Pick<OfficialPluginCatalog, "list" | "download">;
  private bindings: PluginHostBindings | undefined;
  private starting: Promise<void> | undefined;
  private closing: Promise<void> | undefined;
  private startupError: string | undefined;
  private readonly catalogDiscovery: PluginCatalogDiscovery;

  constructor(private readonly options: PluginRuntimeOptions) {
    this.catalogSource =
      options.catalog ??
      new OfficialPluginCatalog(undefined, options.hostRelease ?? STREAMSKOPE_RELEASE);
    this.catalogDiscovery = new PluginCatalogDiscovery({
      source: this.catalogSource,
      cache: options.store.catalogCache,
      hostRelease: options.hostRelease ?? STREAMSKOPE_RELEASE,
      assertOpen: (): void => this.assertOpen(),
      ...(options.persistCatalog === undefined ? {} : { persist: options.persistCatalog }),
    });
  }

  bindHost(host: PluginHostBindings): void {
    if (this.bindings !== undefined) throw new Error("The plugin runtime already has a host.");
    this.bindings = host;
  }

  private assertOpen(): void {
    if (this.closing !== undefined) throw problem("The plugin host is closing.");
  }

  private hostFor(pluginId: string, authority: LoadedPlugin["authority"]): PluginBackendHost {
    const bindings = this.bindings;
    if (bindings === undefined) throw new Error("The plugin host has not been bound.");
    const assertCurrent = (): void => {
      if (!authority.active || authority.retired)
        throw problem("This plugin instance is no longer active.");
    };
    const assertAvailable = (): void => {
      if (authority.retired) throw problem("This plugin instance is no longer active.");
    };
    return {
      recoveryState: {
        read: async (): Promise<JsonValue | null> => {
          assertAvailable();
          const value = await this.options.store.readRecoveryState(pluginId);
          assertAvailable();
          return value;
        },
        write: async (value): Promise<void> => {
          // Final cleanup can update its journal while dispatch is revoked. A staged
          // candidate cannot write shared state before becoming the active instance.
          if (authority.retired || (!authority.active && !authority.draining))
            throw problem("This plugin instance is no longer active.");
          await this.options.store.writeRecoveryState(pluginId, value);
        },
      },
      connectionActive: (): boolean => {
        assertAvailable();
        return bindings.connectionActive();
      },
      recordActivity: (input): void => {
        if (authority.active && !authority.retired) bindings.recordActivity(input);
      },
      failure: (error, context): ReturnType<PluginBackendHost["failure"]> => {
        assertAvailable();
        return bindings.failure(error, context);
      },
      execute: async <Command extends HostCommand>(
        command: Command,
      ): Promise<HostCommandResponse<Command["command"]>> => {
        assertCurrent();
        if (command.command.startsWith("plugins.") || command.command === "plugin.execute") {
          throw problem("A plugin cannot manage or invoke other plugins.");
        }
        if (command.command === "profiles.delete" || command.command === "profiles.update") {
          const profile = (await bindings.profiles()).find(
            (entry) => entry.id === command.payload.profileId,
          );
          if (profile?.source?.pluginId !== pluginId)
            throw problem("A plugin can modify only its own profiles.");
        }
        if (
          (command.command === "profiles.create" || command.command === "profiles.update") &&
          command.payload.profile.source !== undefined &&
          command.payload.profile.source.pluginId !== pluginId
        )
          throw problem("A plugin cannot assign profile ownership to another plugin.");
        assertCurrent();
        return bindings.execute({ ...command, version: HOST_PROTOCOL_VERSION });
      },
      profiles: async (): ReturnType<PluginBackendHost["profiles"]> => {
        assertAvailable();
        const profiles = await bindings.profiles();
        assertAvailable();
        return profiles.filter((profile) => profile.source?.pluginId === pluginId);
      },
      deleteProfile: async (id): Promise<void> => {
        assertCurrent();
        const profile = (await bindings.profiles()).find((candidate) => candidate.id === id);
        if (profile?.source?.pluginId !== pluginId)
          throw problem("A plugin can remove only its own profiles.");
        assertCurrent();
        await bindings.deleteProfile(id);
      },
      disconnectOwnedConnection: async (): Promise<void> => {
        assertCurrent();
        await bindings.disconnectPluginConnection(pluginId);
      },
      publish: (name, data): void => {
        if (this.closing !== undefined || !authority.active || authority.retired) return;
        if (!/^[a-zA-Z][a-zA-Z0-9.-]{0,127}$/u.test(name))
          throw new Error("Invalid plugin event name.");
        const event: PluginEvent = { pluginId, name, data: parsePluginJson(data) };
        for (const listener of this.listeners) listener(event);
      },
      probeTopics: (brokers, signal): Promise<readonly string[]> => {
        assertCurrent();
        return (this.options.probeTopics ?? probeTopics)(brokers, signal);
      },
    };
  }

  private async load(installation: ActivePlugin): Promise<LoadedPlugin> {
    this.assertCompatible(installation.manifest);
    const module = await (this.options.loadModule ?? loadModule)(installation.backendPath);
    const authority = { active: false, retired: false, draining: false };
    let backend: PluginBackend | undefined;
    try {
      backend = await module.activate(this.hostFor(installation.manifest.id, authority));
      assertBackend(backend);
    } catch (error) {
      authority.retired = true;
      if (typeof backend?.close === "function") await backend.close().catch(() => undefined);
      throw error;
    }
    return {
      installation,
      backend,
      authority,
      activationId: randomUUID(),
      requests: new Set(),
      connections: new Set(),
    };
  }

  private assertCompatible(manifest: PluginManifest): void {
    const release = this.options.hostRelease ?? STREAMSKOPE_RELEASE;
    const host = manifest.compatibility?.streamskope;
    if (!isPluginCompatibleWithHost(manifest, release))
      throw problem(
        `${manifest.name} requires StreamSkope ${host?.minimum}${host?.maximumExclusive === undefined ? " or later" : ` up to, but excluding, ${host.maximumExclusive}`}; this host is ${release}.`,
        "Use a compatible StreamSkope and plugin release. Saved profiles and recovery data are retained.",
      );
  }

  private activate(loaded: LoadedPlugin): void {
    loaded.authority.active = true;
    this.modules.set(loaded.installation.manifest.id, loaded);
    this.exitResolved.delete(loaded.installation.manifest.id);
    this.exitPrompted.delete(loaded.installation.manifest.id);
  }

  private async retire(loaded: LoadedPlugin): Promise<void> {
    // Only a previously active backend may persist its final cleanup. Discarding
    // an inactive staged candidate must never grant it recovery write authority.
    loaded.authority.draining = loaded.authority.active && !loaded.authority.retired;
    loaded.authority.active = false;
    try {
      try {
        await loaded.backend.close();
      } finally {
        await Promise.allSettled([...loaded.requests]);
      }
    } finally {
      loaded.authority.draining = false;
      loaded.authority.retired = true;
      // Bundles are self-contained; do not retain removed plugin factories in Node's cache.
      delete createRequire(loaded.installation.backendPath).cache[loaded.installation.backendPath];
    }
  }

  private serial<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutations.then(() => {
      this.assertOpen();
      return operation();
    });
    this.mutations = result.catch(() => undefined);
    return result;
  }

  private async withMutationIntent<T>(
    pluginId: string,
    operation: (assertCurrent: () => void) => Promise<T>,
  ): Promise<T> {
    const intent = Symbol();
    this.mutationIntents.set(pluginId, intent);
    const assertCurrent = (): void => {
      this.assertOpen();
      if (this.mutationIntents.get(pluginId) !== intent)
        throw problem(
          "This plugin change was superseded by a newer install or removal.",
          "The latest plugin change takes priority. Review its current state in Preferences > Plugins.",
        );
    };
    try {
      return await operation(assertCurrent);
    } finally {
      if (this.mutationIntents.get(pluginId) === intent) this.mutationIntents.delete(pluginId);
    }
  }

  private async tracked<T>(loaded: LoadedPlugin, operation: () => Promise<T>): Promise<T> {
    const request = Promise.resolve().then(() => {
      this.assertDispatchable(loaded);
      return operation();
    });
    loaded.requests.add(request);
    try {
      return await request;
    } finally {
      loaded.requests.delete(request);
    }
  }

  private assertDispatchable(loaded: LoadedPlugin): void {
    this.assertOpen();
    const id = loaded.installation.manifest.id;
    if (this.changing.has(id))
      throw problem("The plugin is being changed. Retry when the operation finishes.");
    if (loaded.authority.retired || this.modules.get(id) !== loaded)
      throw problem("This plugin instance is no longer active.");
  }

  private async changed(): Promise<PluginSnapshot> {
    this.revision += 1;
    const snapshot = await this.snapshot();
    for (const listener of this.changeListeners) {
      try {
        listener(snapshot);
      } catch {
        /* UI observers cannot roll back a committed change. */
      }
    }
    return snapshot;
  }

  start(): Promise<void> {
    if (this.closing !== undefined) return Promise.reject(problem("The plugin host is closing."));
    this.starting ??= this.startInstalled();
    return this.starting;
  }

  private async startInstalled(): Promise<void> {
    try {
      const installed = await this.options.store.activatePending();
      for (const installation of installed) {
        try {
          this.assertCompatible(installation.manifest);
        } catch (error) {
          // A host downgrade does not invalidate a verified package. Preserve it so
          // upgrading the desktop can activate it again without reinstalling.
          this.errors.set(installation.manifest.id, summary(error));
          continue;
        }
        try {
          this.activate(await this.load(installation));
        } catch (error) {
          this.errors.set(installation.manifest.id, summary(error));
          try {
            const previous = await this.options.store.rollback(installation.manifest.id);
            if (previous !== undefined) {
              this.activate(await this.load(previous));
              this.errors.set(
                installation.manifest.id,
                `The update could not start; the previous version was restored. ${summary(error)}`,
              );
            }
          } catch (rollbackError) {
            this.errors.set(installation.manifest.id, summary(rollbackError));
          }
        }
      }
    } catch (error) {
      // Corrupt plugin storage must never prevent ordinary Kafka workflows from starting.
      this.startupError = summary(error);
    }
  }

  private async requirePlugin(id: string): Promise<LoadedPlugin> {
    parsePluginId(id);
    if (this.closing !== undefined) throw problem("The plugin host is closing.");
    await this.start();
    this.assertOpen();
    if (this.changing.has(id))
      throw problem("The plugin is being changed. Retry when the operation finishes.");
    const loaded = this.modules.get(id);
    if (loaded === undefined)
      throw problem(
        `Plugin ${id} is not active. Saved profile recovery information has been retained.`,
      );
    return loaded;
  }

  async execute(request: PluginRequest): Promise<JsonValue> {
    const loaded = await this.requirePlugin(request.pluginId);
    this.assertDispatchable(loaded);
    if (request.activationId !== loaded.activationId)
      throw problem("This plugin view is no longer active. Reopen it and retry.");
    if (!/^[a-zA-Z][a-zA-Z0-9.-]{0,127}$/u.test(request.method))
      throw problem("Invalid plugin method.");
    return parsePluginJson(
      await this.tracked(loaded, () =>
        loaded.backend.execute({
          method: request.method,
          input: parsePluginJson(request.input),
          requestId: request.requestId,
          correlationId: request.correlationId,
        }),
      ),
    );
  }

  async validateProfile(source: PluginProfileSource, brokers: readonly string[]): Promise<void> {
    const loaded = await this.requirePlugin(source.pluginId);
    this.assertDispatchable(loaded);
    await this.tracked(loaded, () => loaded.backend.validateProfile(source.data, brokers));
  }

  async withProfileConnection<T>(
    source: PluginProfileSource,
    brokers: readonly string[],
    connect: () => Promise<T>,
  ): Promise<T> {
    const loaded = await this.requirePlugin(source.pluginId);
    this.assertDispatchable(loaded);
    const connection = this.tracked(loaded, async () => {
      await loaded.backend.validateProfile(source.data, brokers);
      return connect();
    });
    loaded.connections.add(connection);
    try {
      return await connection;
    } finally {
      loaded.connections.delete(connection);
    }
  }

  subscribe(listener: (event: PluginEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  subscribeChanges(listener: (snapshot: PluginSnapshot) => void): () => void {
    this.changeListeners.add(listener);
    return () => this.changeListeners.delete(listener);
  }

  async list(): Promise<PluginSnapshot> {
    await this.start();
    return this.serial(() => this.snapshot());
  }

  private async snapshot(): Promise<PluginSnapshot> {
    try {
      const installations = await this.options.store.list();
      const plugins = installations.map((installation): PluginInstallation => {
        const loaded = this.modules.get(installation.id);
        const error = this.errors.get(installation.id) ?? installation.error;
        return {
          id: installation.id,
          pending: installation.pending,
          restartRequired: installation.restartRequired,
          ...(installation.installed ? { installed: installation.installed } : {}),
          ...(installation.previous ? { previous: installation.previous } : {}),
          ...(loaded
            ? {
                active: loaded.installation.manifest,
                activationId: loaded.activationId,
                rendererUrl: `/plugins/${installation.id}/${loaded.installation.sha256}/${loaded.activationId}/renderer.js`,
                ...(loaded.installation.stylesPath
                  ? {
                      stylesUrl: `/plugins/${installation.id}/${loaded.installation.sha256}/${loaded.activationId}/renderer.css`,
                    }
                  : {}),
              }
            : {}),
          ...(error ? { error } : {}),
        };
      });
      for (const [id, error] of this.errors) {
        if (!plugins.some((plugin) => plugin.id === id))
          plugins.push({ id, pending: null, restartRequired: false, error });
      }
      return {
        revision: this.revision,
        plugins,
        ...(this.startupError ? { error: this.startupError } : {}),
      };
    } catch (error) {
      return { revision: this.revision, plugins: [], error: summary(error) };
    }
  }

  catalog(refresh = true): Promise<PluginCatalogSnapshot> {
    return this.catalogDiscovery.catalog(refresh);
  }

  async prepareChange(
    pluginId: string,
    operation: PluginChangeOperation,
  ): Promise<PluginChangePrompt | null> {
    parsePluginId(pluginId);
    await this.start();
    return this.serial(async () => {
      const loaded = this.modules.get(pluginId);
      if (loaded === undefined) return null;
      // A healthy backend only needs a renderer retry; it will not stop active work.
      if (operation === "retry" && !this.errors.has(pluginId)) return null;
      const warning = await loaded.backend.beforeChange();
      if (warning === undefined) return null;
      const now = Date.now();
      for (const [token, entry] of this.confirmations) {
        if (entry.expires <= now || entry.pluginId === pluginId) this.confirmations.delete(token);
      }
      if (this.confirmations.size >= 32)
        this.confirmations.delete(this.confirmations.keys().next().value!);
      const token = randomUUID();
      this.confirmations.set(token, {
        pluginId,
        operation,
        activationId: loaded.activationId,
        fingerprint: JSON.stringify(warning),
        expires: now + 5 * 60_000,
      });
      return {
        pluginId,
        token,
        title:
          operation === "install"
            ? "Update plugin?"
            : operation === "retry"
              ? "Retry plugin?"
              : "Remove plugin?",
        message: warning.message,
        detail: warning.detail,
        confirmLabel:
          operation === "install"
            ? "Stop capture and update"
            : operation === "retry"
              ? "Stop capture and retry"
              : "Stop capture and remove",
      };
    });
  }

  private async confirmChange(
    pluginId: string,
    operation: PluginChangeOperation,
    loaded: LoadedPlugin | undefined,
    token?: string,
  ): Promise<void> {
    const confirmation = token === undefined ? undefined : this.confirmations.get(token);
    if (token !== undefined) this.confirmations.delete(token);
    if (loaded === undefined) return;
    const warning = await loaded.backend.beforeChange();
    if (warning === undefined) return;
    if (
      confirmation === undefined ||
      confirmation.pluginId !== pluginId ||
      confirmation.operation !== operation ||
      confirmation.activationId !== loaded.activationId ||
      confirmation.expires <= Date.now() ||
      confirmation.fingerprint !== JSON.stringify(warning)
    ) {
      throw problem(
        "Plugin work changed or requires confirmation. Review the change again before continuing.",
        "Retry the plugin update or removal and confirm stopping its active work.",
      );
    }
  }

  async install(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    return this.withMutationIntent(pluginId, (assertCurrent) =>
      this.installCurrent(pluginId, confirmationToken, assertCurrent),
    );
  }

  async retryActivation(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    return this.withMutationIntent(pluginId, async (assertCurrent) => {
      await this.start();
      return this.serial(async () => {
        assertCurrent();
        const previous = this.modules.get(pluginId);
        // Healthy retries are idempotent; a rejected renderer needs a fresh activation authority.
        if (previous !== undefined && !this.errors.has(pluginId)) return this.snapshot();
        this.changing.add(pluginId);
        try {
          const installation = await this.options.store.getInstalled(pluginId);
          if (installation === undefined)
            throw problem(
              "There is no verified installed plugin to retry.",
              "Install or repair the plugin in Preferences > Plugins.",
            );
          this.assertCompatible(installation.manifest);
          if (previous !== undefined && installation.sha256 !== previous.installation.sha256)
            throw problem("The stored plugin changed. Review its installation before retrying.");
          if (previous !== undefined) await Promise.allSettled([...previous.connections]);
          await this.confirmChange(pluginId, "retry", previous, confirmationToken);
          assertCurrent();
          await this.replaceInstalled(installation, previous, assertCurrent);
          this.changing.delete(pluginId);
          return await this.changed();
        } catch (error) {
          this.errors.set(pluginId, summary(error));
          throw error;
        } finally {
          this.changing.delete(pluginId);
        }
      });
    });
  }

  /** Installation and local recovery use the same candidate activation, cleanup and commit boundary. */
  private async replaceInstalled(
    installation: ActivePlugin,
    previous: LoadedPlugin | undefined,
    assertCurrent: () => void,
  ): Promise<void> {
    const pluginId = installation.manifest.id;
    let candidate: LoadedPlugin | undefined;
    try {
      candidate = await this.load(installation);
      assertCurrent();
      if (previous !== undefined) {
        await previous.backend.prepareUnload("update");
        await Promise.allSettled([...previous.requests]);
      }
      assertCurrent();
      const cleanupError = await this.options.store.commitInstall(pluginId, installation.sha256);
      this.errors.delete(pluginId);
      if (cleanupError !== undefined) this.errors.set(pluginId, cleanupError);
      if (previous !== undefined) {
        try {
          await this.retire(previous);
        } catch (error) {
          this.errors.set(
            pluginId,
            `The update is active, but previous-instance cleanup failed: ${summary(error)}`,
          );
        }
      }
      this.activate(candidate);
      candidate = undefined;
    } finally {
      if (candidate !== undefined) await this.retire(candidate).catch(() => undefined);
    }
  }

  private async installCurrent(
    pluginId: string,
    confirmationToken: string | undefined,
    assertCurrent: () => void,
  ): Promise<PluginSnapshot> {
    await this.start();
    assertCurrent();
    // Downloads do not hold the lifecycle queue or delay shutdown.
    const download = await this.catalogSource.download(pluginId);
    assertCurrent();
    const verified = this.options.store.verifyPackage(download.bytes, download.sha256);
    const manifest = verified.manifest;
    this.assertCompatible(manifest);
    if (manifest.id !== pluginId)
      throw problem("The downloaded plugin identity does not match the selection.");
    return this.serial(async () => {
      assertCurrent();
      this.changing.add(pluginId);
      const previous = this.modules.get(pluginId);
      let installation: ActivePlugin | undefined;
      try {
        if (
          manifest.apiVersion >= 3 &&
          previous !== undefined &&
          comparePluginManifests(previous.installation.manifest, manifest) === 0 &&
          previous.installation.contentSha256 !== verified.contentSha256
        )
          throw problem(
            "This plugin version has different content from the installed package.",
            "Refresh the catalog after a new plugin version is published. The current installation is retained.",
          );
        const stored = (await this.options.store.list()).find((entry) => entry.id === pluginId);
        for (const current of [previous?.installation.manifest, stored?.installed]) {
          if (current !== undefined && comparePluginManifests(manifest, current) < 0)
            throw problem(
              `The available plugin version ${manifest.version} is older than installed version ${current.version}.`,
              "Refresh the plugin catalog and wait for a compatible update.",
            );
        }
        assertCurrent();
        installation = await this.options.store.prepareInstall(download.bytes, download.sha256);
        assertCurrent();
        if (
          previous !== undefined &&
          !this.errors.has(pluginId) &&
          installation.sha256 === previous.installation.sha256 &&
          installation.contentSha256 === previous.installation.contentSha256
        ) {
          this.changing.delete(pluginId);
          return this.snapshot();
        }
        if (previous !== undefined) await Promise.allSettled([...previous.connections]);
        await this.confirmChange(pluginId, "install", previous, confirmationToken);
        assertCurrent();
        await this.replaceInstalled(installation, previous, assertCurrent);
        this.changing.delete(pluginId);
        return await this.changed();
      } catch (error) {
        if (installation !== undefined)
          await this.options.store
            .discardInstall(pluginId, installation.sha256)
            .catch(() => undefined);
        throw error;
      } finally {
        this.changing.delete(pluginId);
      }
    });
  }

  async remove(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    return this.withMutationIntent(pluginId, (assertCurrent) =>
      this.removeCurrent(pluginId, confirmationToken, assertCurrent),
    );
  }

  private async removeCurrent(
    pluginId: string,
    confirmationToken: string | undefined,
    assertCurrent: () => void,
  ): Promise<PluginSnapshot> {
    await this.start();
    return this.serial(async () => {
      assertCurrent();
      this.changing.add(pluginId);
      try {
        const loaded = this.modules.get(pluginId);
        if (loaded !== undefined) await Promise.allSettled([...loaded.connections]);
        await this.confirmChange(pluginId, "remove", loaded, confirmationToken);
        assertCurrent();
        if (loaded !== undefined) {
          await loaded.backend.prepareUnload("remove");
          await Promise.allSettled([...loaded.requests]);
        }
        assertCurrent();
        const cleanupError = await this.options.store.uninstall(pluginId);
        this.errors.delete(pluginId);
        if (cleanupError !== undefined) this.errors.set(pluginId, cleanupError);
        if (loaded !== undefined) {
          this.modules.delete(pluginId);
          try {
            await this.retire(loaded);
          } catch (error) {
            this.errors.set(
              pluginId,
              `The plugin was removed, but instance cleanup failed: ${summary(error)}`,
            );
          }
        }
        this.exitResolved.delete(pluginId);
        this.exitPrompted.delete(pluginId);
        this.changing.delete(pluginId);
        return await this.changed();
      } finally {
        this.changing.delete(pluginId);
      }
    });
  }

  async rendererFailed(
    pluginId: string,
    activationId: string,
    error: string,
  ): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    await this.start();
    return this.serial(async () => {
      const loaded = this.modules.get(pluginId);
      if (loaded === undefined || loaded.activationId !== activationId) return this.snapshot();
      this.changing.add(pluginId);
      try {
        await Promise.allSettled([...loaded.connections]);
        if (await loaded.backend.beforeChange()) {
          this.errors.set(
            pluginId,
            `Plugin UI could not load. Active work was retained; confirm an update or removal to stop it. ${error.slice(0, 1024)}`,
          );
          this.changing.delete(pluginId);
          return await this.changed();
        }
        await loaded.backend.prepareUnload("update");
        await Promise.allSettled([...loaded.requests]);
        const previous = await this.options.store.rollback(pluginId);
        this.modules.delete(pluginId);
        let rollbackError: string | undefined;
        try {
          await this.retire(loaded);
          if (previous !== undefined) this.activate(await this.load(previous));
        } catch (failure) {
          rollbackError = summary(failure);
        }
        this.errors.set(
          pluginId,
          `Plugin UI could not load.${rollbackError ? ` Recovery failed: ${rollbackError}.` : previous ? " The previous version was restored." : " Retry activation in Preferences > Plugins."} ${error.slice(0, 1024)}`,
        );
        this.changing.delete(pluginId);
        return await this.changed();
      } finally {
        this.changing.delete(pluginId);
      }
    });
  }

  async restart(): Promise<void> {
    this.assertOpen();
    if (this.options.restart === undefined) {
      throw problem(
        "Restart this development host to activate plugin changes.",
        "Stop and start npm run dev, then reopen StreamSkope.",
      );
    }
    await this.options.restart();
  }

  async prepareExit(): Promise<PluginExitPrompt | null> {
    await this.start();
    return this.serial(async () => {
      try {
        for (const [pluginId, loaded] of this.modules) {
          if (this.exitResolved.has(pluginId)) continue;
          const prompt = await loaded.backend.beforeExit();
          if (prompt !== undefined) {
            this.exitPrompted.set(pluginId, loaded.activationId);
            return { ...prompt, pluginId };
          }
        }
        return null;
      } catch (error) {
        this.exitResolved.clear();
        throw error;
      }
    });
  }

  async resolveExit(pluginId: string, action: string): Promise<boolean> {
    parsePluginId(pluginId);
    await this.start();
    return this.serial(async () => {
      const loaded = await this.requirePlugin(pluginId);
      if (this.exitPrompted.get(pluginId) !== loaded.activationId)
        throw problem(
          "The plugin changed after this exit prompt. Review its current work before exiting.",
        );
      this.changing.add(pluginId);
      try {
        const allowed = await loaded.backend.resolveExit(action);
        if (allowed) this.exitResolved.add(pluginId);
        else this.exitResolved.clear();
        return allowed;
      } catch (error) {
        this.exitResolved.clear();
        throw error;
      } finally {
        this.changing.delete(pluginId);
      }
    });
  }

  async rendererAsset(pathname: string): Promise<PluginRendererAsset | undefined> {
    const match =
      /^\/plugins\/([a-z][a-z0-9.-]{0,127})\/([a-f0-9]{64})\/([a-f0-9-]{36})\/(renderer\.(?:js|css))$/u.exec(
        pathname,
      );
    if (match === null) return undefined;
    await this.start();
    this.assertOpen();
    const loaded = this.modules.get(match[1]!);
    if (
      loaded === undefined ||
      loaded.installation.sha256 !== match[2] ||
      loaded.activationId !== match[3]
    )
      return undefined;
    const path =
      match[4] === "renderer.js"
        ? loaded.installation.rendererPath
        : loaded.installation.stylesPath;
    if (path === undefined) return undefined;
    const content = await readBoundedFile(path, 32 * 1_048_576, { rejectSymlinks: true });
    return {
      content,
      contentType:
        match[4] === "renderer.js" ? "text/javascript; charset=utf-8" : "text/css; charset=utf-8",
    };
  }

  close(): Promise<void> {
    this.closing ??= (async (): Promise<void> => {
      if (this.starting !== undefined) await this.starting;
      await this.mutations;
      const results = await Promise.allSettled(
        [...this.modules.values()].map((loaded) => this.retire(loaded)),
      );
      this.modules.clear();
      this.listeners.clear();
      this.changeListeners.clear();
      this.confirmations.clear();
      this.mutationIntents.clear();
      this.exitPrompted.clear();
      const failures = results.filter(
        (result): result is PromiseRejectedResult => result.status === "rejected",
      );
      if (failures.length > 0)
        throw new AggregateError(
          failures.map((result) => result.reason as unknown),
          "Plugin cleanup failed.",
        );
    })();
    return this.closing;
  }
}
