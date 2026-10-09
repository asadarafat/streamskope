import { randomUUID } from "node:crypto";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
} from "../../../features/kafka/contracts";
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
  PluginDeliverySnapshot,
  PluginPackageInspection,
  PluginPackageInspectInput,
  PluginChangeOperation,
  PluginChangePrompt,
  PluginEvent,
  PluginExitPrompt,
  PluginManifest,
  PluginProfileSource,
  PluginRequest,
  PluginSnapshot,
  PluginAcquisitionProgress,
  PluginNetworkSnapshot,
  PluginNetworkUpdateInput,
  PluginNetworkTestResult,
} from "../../../plugins/contracts";
import {
  isPluginCompatibleWithHost,
  parsePluginId,
  parsePluginJson,
} from "../../../plugins/validation";
import { STREAMSKOPE_RELEASE } from "../../../plugins/host-release";
import { readBoundedFile } from "../bounded-file";
import type { ProfileProtector } from "../profile-protector";

import { OfficialPluginCatalog, type PluginCatalogSource } from "./catalog";
import { PluginCatalogDiscovery } from "./catalog-discovery";
import { PluginDeliveryController, type InstalledPluginReview } from "./delivery";
import { PluginInstaller, type PluginCandidateAuthority } from "./installer";
import { PluginRuntimeInventory } from "./runtime-inventory";
import { PluginChangeAuthority } from "./change-authority";
import {
  PluginTransitions,
  type PluginTransitionHandle,
  type PluginTransitionMetadata,
} from "./transition";
import { pluginProblem as problem, pluginErrorSummary as summary } from "./problem";
import { assertPluginProfileRefresh } from "./profile-refresh";
import { PluginStore, type ActivePlugin } from "./store";
import { PluginNetworkController } from "./network";
import type { PluginNetworkTransport } from "./network-transport";
import {
  assertPluginBackend,
  loadPluginBackendModule,
  probePluginTopics,
  retirePluginBackend,
  type LoadedPlugin,
} from "./backend-loader";

export interface PluginRuntimeOptions {
  readonly store: PluginStore;
  readonly catalog?: PluginCatalogSource;
  readonly loadModule?: (path: string) => Promise<PluginBackendModule>;
  readonly probeTopics?: PluginBackendHost["probeTopics"];
  readonly restart?: () => void | Promise<void>;
  readonly hostRelease?: string;
  /** Development catalog URLs have no official release provenance and stay in memory only. */
  readonly persistCatalog?: boolean;
  readonly choosePackageFile?: (signal: AbortSignal) => Promise<Uint8Array | null>;
  readonly networkTransport?: PluginNetworkTransport;
  readonly networkProtector?: ProfileProtector;
  /** Persist plugin download policy independently of native proxy capability. */
  readonly durableSettings?: boolean;
  /** Observation threshold only; a waiting hook retains its actual cleanup barrier. */
  readonly transitionWaitingAfterMs?: number;
}

export interface PluginRendererAsset {
  readonly content: Uint8Array;
  readonly contentType: string;
}

/** Only explicitly installed, verified first-party packages are loaded into this trusted host. */
export class PluginRuntime implements PluginRuntimePort {
  private readonly modules = new Map<string, LoadedPlugin>();
  private readonly errors = new Map<string, string>();
  private readonly listeners = new Set<(event: PluginEvent) => void>();
  private readonly exitResolved = new Set<string>();
  private readonly exitPrompted = new Map<string, string>();
  private readonly confirmations = new PluginChangeAuthority();
  private readonly inventory: PluginRuntimeInventory;
  private readonly transitions: PluginTransitions;
  private inventoryReady: Promise<readonly ActivePlugin[]> | undefined;
  private readonly catalogSource: PluginCatalogSource;
  private bindings: PluginHostBindings | undefined;
  private starting: Promise<void> | undefined;
  private closing: Promise<void> | undefined;
  private startupError: string | undefined;
  private readonly catalogDiscovery: PluginCatalogDiscovery;
  private readonly installer: PluginInstaller<LoadedPlugin>;
  private readonly deliveryController: PluginDeliveryController;
  private readonly network: PluginNetworkController;

  constructor(private readonly options: PluginRuntimeOptions) {
    this.inventory = new PluginRuntimeInventory(options.store);
    this.transitions = new PluginTransitions({
      changed: (): void => {
        this.publish();
      },
      ...(options.transitionWaitingAfterMs === undefined
        ? {}
        : { waitingAfterMs: options.transitionWaitingAfterMs }),
    });
    this.catalogSource =
      options.catalog ??
      new OfficialPluginCatalog(
        options.networkTransport?.fetch,
        options.hostRelease ?? STREAMSKOPE_RELEASE,
      );
    this.catalogDiscovery = new PluginCatalogDiscovery({
      source: this.catalogSource,
      cache: options.store.catalogCache,
      hostRelease: options.hostRelease ?? STREAMSKOPE_RELEASE,
      assertOpen: (): void => this.assertOpen(),
      ...(options.persistCatalog === undefined ? {} : { persist: options.persistCatalog }),
    });
    this.network = new PluginNetworkController({
      path: options.store.networkSettingsPath(),
      ...(options.networkTransport === undefined ? {} : { transport: options.networkTransport }),
      ...(options.networkProtector === undefined ? {} : { protector: options.networkProtector }),
      ...(options.durableSettings === undefined
        ? {}
        : { durableSettings: options.durableSettings }),
      changed: (): void => this.catalogDiscovery.invalidateRemote(),
    });
    this.installer = new PluginInstaller({
      store: options.store,
      compatible: this.assertCompatible.bind(this),
      current: (id): LoadedPlugin | undefined => this.modules.get(id),
      healthy: (id): boolean => !this.errors.has(id),
      transitions: this.transitions,
      snapshot: (): PluginSnapshot => this.inventory.snapshot(),
      refresh: (): Promise<boolean> => this.inventory.refresh(),
      confirm: (id, loaded, handle, token, candidate): Promise<void> =>
        this.confirmChange(id, "install", loaded, handle, token, candidate),
      replace: this.replaceInstalled.bind(this),
    });
    this.deliveryController = new PluginDeliveryController({
      store: options.store,
      catalog: this.catalogSource,
      discovery: this.catalogDiscovery,
      hostRelease: options.hostRelease ?? STREAMSKOPE_RELEASE,
      development: options.persistCatalog === false,
      assertOpen: this.assertOpen.bind(this),
      installed: this.installedReview.bind(this),
      prepare: (id, candidate): Promise<PluginChangePrompt | null> =>
        this.prepareChangeAuthority(id, "install", candidate),
      install: (id, bytes, sha256, token, candidate): Promise<PluginSnapshot> =>
        this.transitions.withIntent(id, async (assertCurrent) => {
          await this.start();
          assertCurrent();
          return this.installer.apply(id, bytes, sha256, token, assertCurrent, candidate);
        }),
      ...(options.choosePackageFile === undefined ? {} : { chooseFile: options.choosePackageFile }),
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
        await assertPluginProfileRefresh(command, bindings);

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
        return (this.options.probeTopics ?? probePluginTopics)(brokers, signal);
      },
    };
  }

  private async load(
    installation: ActivePlugin,
    handle: PluginTransitionHandle,
  ): Promise<LoadedPlugin> {
    this.assertCompatible(installation.manifest);
    const module = await (this.options.loadModule ?? loadPluginBackendModule)(
      installation.backendPath,
    );
    const authority = { active: false, retired: false, draining: false };
    let backend: PluginBackend | undefined;
    try {
      backend = await module.activate(this.hostFor(installation.manifest.id, authority));
      assertPluginBackend(backend);
    } catch (error) {
      authority.retired = true;
      if (typeof backend?.close === "function") {
        const rejected = backend;
        await handle.wait("close-candidate", () => rejected.close()).catch(() => undefined);
      }
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

  private async waitConnections(
    loaded: LoadedPlugin | undefined,
    handle: PluginTransitionHandle,
  ): Promise<void> {
    if (loaded !== undefined)
      await handle.wait(
        "wait-connections",
        () => Promise.allSettled([...loaded.connections]),
        this.observation(loaded),
      );
  }

  private async prepareUnload(
    loaded: LoadedPlugin,
    reason: "update" | "remove",
    handle: PluginTransitionHandle,
  ): Promise<void> {
    await handle.wait(
      "prepare-unload",
      () => loaded.backend.prepareUnload(reason),
      this.observation(loaded),
    );
    await handle.wait(
      "drain-requests",
      () => Promise.allSettled([...loaded.requests]),
      this.observation(loaded),
    );
  }

  private observation(loaded: LoadedPlugin | undefined): PluginTransitionMetadata {
    return loaded === undefined
      ? {}
      : {
          activationId: loaded.activationId,
          counts: () => ({ requests: loaded.requests.size, connections: loaded.connections.size }),
        };
  }

  private publish(): PluginSnapshot {
    return this.inventory.publish({
      active: [...this.modules.values()].filter((loaded) => !loaded.authority.retired),
      errors: this.errors,
      transitions: this.transitions.snapshot(),
      ...(this.startupError === undefined ? {} : { error: this.startupError }),
    });
  }

  private async tracked<T>(loaded: LoadedPlugin, operation: () => Promise<T>): Promise<T> {
    const request = Promise.resolve().then(() => {
      this.assertDispatchable(loaded);
      return operation();
    });
    loaded.requests.add(request);
    this.transitions.refresh(loaded.installation.manifest.id);
    try {
      return await request;
    } finally {
      loaded.requests.delete(request);
      this.transitions.refresh(loaded.installation.manifest.id);
    }
  }

  private assertDispatchable(loaded: LoadedPlugin): void {
    this.assertOpen();
    const id = loaded.installation.manifest.id;
    if (this.transitions.isChanging(id))
      throw problem("The plugin is being changed. Retry when the operation finishes.");
    if (loaded.authority.retired || this.modules.get(id) !== loaded)
      throw problem("This plugin instance is no longer active.");
  }

  start(): Promise<void> {
    if (this.closing !== undefined) return Promise.reject(problem("The plugin host is closing."));
    if (this.starting === undefined) {
      this.inventoryReady = this.initializeInventory();
      this.starting = this.startInstalled(this.inventoryReady);
    }
    return this.starting;
  }

  private async initializeInventory(): Promise<readonly ActivePlugin[]> {
    try {
      return await this.options.store.activatePending();
    } catch (error) {
      this.startupError = summary(error);
      return [];
    } finally {
      await this.inventory.refresh();
      this.publish();
    }
  }

  private async startInstalled(ready: Promise<readonly ActivePlugin[]>): Promise<void> {
    for (const installation of await ready) {
      if (this.closing !== undefined) break;
      const pluginId = installation.manifest.id;
      try {
        await this.transitions.run({ pluginId, operation: "startup" }, async (handle) => {
          try {
            this.assertCompatible(installation.manifest);
          } catch (error) {
            // Host downgrades preserve the verified package for a compatible host.
            this.errors.set(pluginId, summary(error));
            return;
          }
          try {
            const loaded = await handle.wait("load-candidate", () =>
              this.load(installation, handle),
            );
            handle.phase("activate-candidate", this.observation(loaded));
            this.activate(loaded);
          } catch (error) {
            this.errors.set(pluginId, summary(error));
            try {
              const previous = await handle.wait(
                "startup-recovery",
                async () => {
                  const restored = await this.options.store.rollback(pluginId);
                  if (await this.inventory.refresh())
                    handle.phase("startup-recovery", { commit: "confirmed" });
                  return restored;
                },
                { commit: "in-progress" },
              );
              if (previous !== undefined) {
                const loaded = await handle.wait("load-candidate", () =>
                  this.load(previous, handle),
                );
                handle.phase("activate-candidate", this.observation(loaded));
                this.activate(loaded);
                this.errors.set(
                  pluginId,
                  `The update could not start; the previous version was restored. ${summary(error)}`,
                );
              }
            } catch (rollbackError) {
              this.errors.set(pluginId, summary(rollbackError));
            }
          }
        });
      } catch (error) {
        if (this.closing === undefined) this.errors.set(pluginId, summary(error));
      }
      this.publish();
    }
  }

  private async requirePlugin(id: string): Promise<LoadedPlugin> {
    parsePluginId(id);
    if (this.closing !== undefined) throw problem("The plugin host is closing.");
    await this.start();
    this.assertOpen();
    if (this.transitions.isChanging(id))
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
    this.transitions.refresh(loaded.installation.manifest.id);
    try {
      return await connection;
    } finally {
      loaded.connections.delete(connection);
      this.transitions.refresh(loaded.installation.manifest.id);
    }
  }

  subscribe(listener: (event: PluginEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  subscribeChanges(listener: (snapshot: PluginSnapshot) => void): () => void {
    return this.inventory.subscribe(listener);
  }

  async list(): Promise<PluginSnapshot> {
    if (this.starting === undefined && this.closing === undefined) void this.start();
    await this.inventoryReady;
    return this.inventory.snapshot();
  }

  catalog(refresh = true, requestId?: string): Promise<PluginCatalogSnapshot> {
    return refresh
      ? this.network.catalog(requestId, this.catalogDiscovery)
      : this.catalogDiscovery.catalog(false);
  }

  delivery(): Promise<PluginDeliverySnapshot> {
    return this.deliveryController.delivery();
  }
  inspectPackage(
    input: PluginPackageInspectInput,
    requestId?: string,
  ): Promise<PluginPackageInspection | null> {
    return this.network.acquire(
      requestId,
      "inspect",
      input.source === "catalog",
      (context) => this.deliveryController.inspectPackage(input, context.signal, context.progress),
      (value): void => {
        if (value !== null) this.deliveryController.discardPackage(value.candidateId);
      },
    );
  }
  networkSettings(): Promise<PluginNetworkSnapshot> {
    return this.network.snapshot();
  }
  updateNetwork(input: PluginNetworkUpdateInput): Promise<PluginNetworkSnapshot> {
    return this.network.update(input);
  }
  testNetwork(requestId: string): Promise<PluginNetworkTestResult> {
    return this.network.test(requestId, this.catalogSource);
  }
  cancelAcquisition(requestId: string): Promise<void> {
    this.network.cancel(requestId);
    return Promise.resolve();
  }
  subscribeAcquisition(listener: (progress: PluginAcquisitionProgress) => void): () => void {
    return this.network.subscribe(listener);
  }
  preparePackageChange(candidateId: string): Promise<PluginChangePrompt | null> {
    return this.deliveryController.preparePackageChange(candidateId);
  }
  installPackage(candidateId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    return this.deliveryController.installPackage(candidateId, confirmationToken);
  }
  discardPackage(candidateId: string): Promise<void> {
    this.deliveryController.discardPackage(candidateId);
    return Promise.resolve();
  }

  private async installedReview(pluginId: string): Promise<InstalledPluginReview> {
    await this.start();
    const installation = (await this.list()).plugins.find((value) => value.id === pluginId);
    const loaded = this.modules.get(pluginId);
    let healthy = loaded !== undefined && !this.errors.has(pluginId);
    try {
      healthy &&=
        (await this.options.store.getInstalled(pluginId))?.sha256 === loaded?.installation.sha256;
    } catch {
      healthy = false;
    }
    const manifest = installation?.active ?? installation?.installed;
    return {
      healthy,
      ...(manifest === undefined ? {} : { manifest }),
      ...(loaded === undefined ? {} : { contentSha256: loaded.installation.contentSha256 }),
    };
  }

  async prepareChange(
    pluginId: string,
    operation: PluginChangeOperation,
  ): Promise<PluginChangePrompt | null> {
    return this.prepareChangeAuthority(pluginId, operation);
  }

  private async prepareChangeAuthority(
    pluginId: string,
    operation: PluginChangeOperation,
    candidate?: PluginCandidateAuthority,
  ): Promise<PluginChangePrompt | null> {
    parsePluginId(pluginId);
    await this.start();
    return this.transitions.run({ pluginId, operation: `review-${operation}` }, async (handle) => {
      candidate?.assertCurrent();
      const loaded = this.modules.get(pluginId);
      if (loaded === undefined) return null;
      // A healthy backend only needs a renderer retry; it will not stop active work.
      if (operation === "retry" && !this.errors.has(pluginId)) return null;
      const warning = await handle.wait(
        "review-change",
        () => loaded.backend.beforeChange(),
        this.observation(loaded),
      );
      candidate?.assertCurrent();
      if (warning === undefined) return null;
      return this.confirmations.prepare(
        pluginId,
        operation,
        loaded.activationId,
        warning,
        candidate,
      );
    });
  }

  private async confirmChange(
    pluginId: string,
    operation: PluginChangeOperation,
    loaded: LoadedPlugin | undefined,
    handle: PluginTransitionHandle,
    token?: string,
    candidate?: PluginCandidateAuthority,
  ): Promise<void> {
    await this.confirmations.confirm(
      pluginId,
      operation,
      loaded?.activationId,
      token,
      candidate,
      () =>
        handle.wait(
          "review-change",
          () => loaded!.backend.beforeChange(),
          this.observation(loaded),
        ),
    );
  }

  async install(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    return this.transitions.withIntent(pluginId, async (assertCurrent) => {
      await this.start();
      assertCurrent();
      const downloaded = await this.network.acquire(undefined, "inspect", true, (context) =>
        this.catalogSource.download(pluginId, {
          signal: context.signal,
          onProgress: context.progress,
        }),
      );
      assertCurrent();
      return this.installer.apply(
        pluginId,
        downloaded.bytes,
        downloaded.sha256,
        confirmationToken,
        assertCurrent,
      );
    });
  }

  async retryActivation(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    return this.transitions.withIntent(pluginId, async (assertIntent) => {
      await this.start();
      await this.transitions.run(
        { pluginId, operation: "retry" },
        async (handle) => {
          const previous = this.modules.get(pluginId);
          // Healthy retries retain the same active backend and all of its work.
          if (previous !== undefined && !this.errors.has(pluginId)) return;
          try {
            const installation = await handle.wait(
              "verify-package",
              () => this.options.store.getInstalled(pluginId),
              this.observation(previous),
            );
            if (installation === undefined)
              throw problem(
                "There is no verified installed plugin to retry.",
                "Install or repair the plugin in Preferences > Plugins.",
              );
            this.assertCompatible(installation.manifest);
            if (previous !== undefined && installation.sha256 !== previous.installation.sha256)
              throw problem("The stored plugin changed. Review its installation before retrying.");
            await this.waitConnections(previous, handle);
            await this.confirmChange(pluginId, "retry", previous, handle, confirmationToken);
            handle.assertCurrent();
            await this.replaceInstalled(installation, previous, handle);
          } catch (error) {
            this.errors.set(pluginId, summary(error));
            throw error;
          }
        },
        assertIntent,
      );
      return this.inventory.snapshot();
    });
  }

  /** Installation and local recovery share candidate activation, cleanup and storage ownership. */
  private async replaceInstalled(
    installation: ActivePlugin,
    previous: LoadedPlugin | undefined,
    handle: PluginTransitionHandle,
  ): Promise<void> {
    const pluginId = installation.manifest.id;
    let candidate: LoadedPlugin | undefined;
    try {
      candidate = await handle.wait(
        "load-candidate",
        () => this.load(installation, handle),
        this.observation(previous),
      );
      handle.assertCurrent();
      if (previous !== undefined) {
        await this.prepareUnload(previous, "update", handle);
      }
      handle.assertCurrent();
      const cleanupError = await handle.wait(
        "commit-storage",
        async () => {
          const warning = await this.options.store.commitInstall(pluginId, installation.sha256);
          if (await this.inventory.refresh())
            handle.phase("commit-storage", { commit: "confirmed" });
          return warning;
        },
        { commit: "in-progress" },
      );
      this.errors.delete(pluginId);
      if (cleanupError !== undefined) this.errors.set(pluginId, cleanupError);
      if (previous !== undefined) {
        try {
          await handle.wait(
            "retire-previous",
            () => retirePluginBackend(previous),
            this.observation(previous),
          );
        } catch (error) {
          this.errors.set(
            pluginId,
            `The update is active, but previous-instance cleanup failed: ${summary(error)}`,
          );
        }
      }
      handle.phase("activate-candidate", this.observation(candidate));
      this.activate(candidate);
      candidate = undefined;
    } finally {
      if (candidate !== undefined) {
        const rejected = candidate;
        await handle
          .wait("close-candidate", () => retirePluginBackend(rejected), this.observation(rejected))
          .catch(() => undefined);
      }
    }
  }

  async remove(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    return this.transitions.withIntent(pluginId, async (assertIntent) => {
      await this.start();
      await this.transitions.run(
        { pluginId, operation: "remove" },
        async (handle) => {
          const loaded = this.modules.get(pluginId);
          await this.waitConnections(loaded, handle);
          await this.confirmChange(pluginId, "remove", loaded, handle, confirmationToken);
          handle.assertCurrent();
          if (loaded !== undefined) {
            await this.prepareUnload(loaded, "remove", handle);
          }
          handle.assertCurrent();
          const cleanupError = await handle.wait(
            "commit-storage",
            async () => {
              const warning = await this.options.store.uninstall(pluginId);
              if (await this.inventory.refresh())
                handle.phase("commit-storage", { commit: "confirmed" });
              return warning;
            },
            { commit: "in-progress" },
          );
          this.errors.delete(pluginId);
          if (cleanupError !== undefined) this.errors.set(pluginId, cleanupError);
          if (loaded !== undefined) {
            this.modules.delete(pluginId);
            try {
              await handle.wait(
                "close-backend",
                () => retirePluginBackend(loaded),
                this.observation(loaded),
              );
            } catch (error) {
              this.errors.set(
                pluginId,
                `The plugin was removed, but instance cleanup failed: ${summary(error)}`,
              );
            }
          }
          this.exitResolved.delete(pluginId);
          this.exitPrompted.delete(pluginId);
        },
        assertIntent,
      );
      return this.inventory.snapshot();
    });
  }

  async rendererFailed(
    pluginId: string,
    activationId: string,
    error: string,
  ): Promise<PluginSnapshot> {
    parsePluginId(pluginId);
    await this.start();
    await this.transitions.run(
      { pluginId, operation: "renderer-recovery", activationId },
      async (handle) => {
        const loaded = this.modules.get(pluginId);
        if (loaded === undefined || loaded.activationId !== activationId) return;
        await handle.wait(
          "wait-connections",
          () => Promise.allSettled([...loaded.connections]),
          this.observation(loaded),
        );
        if (
          await handle.wait(
            "review-change",
            () => loaded.backend.beforeChange(),
            this.observation(loaded),
          )
        ) {
          this.errors.set(
            pluginId,
            `Plugin UI could not load. Active work was retained; confirm an update or removal to stop it. ${error.slice(0, 1024)}`,
          );
          return;
        }
        await this.prepareUnload(loaded, "update", handle);
        handle.assertCurrent();
        const previous = await handle.wait(
          "rollback-storage",
          async () => {
            const restored = await this.options.store.rollback(pluginId);
            if (await this.inventory.refresh())
              handle.phase("rollback-storage", { commit: "confirmed" });
            return restored;
          },
          { commit: "in-progress" },
        );
        this.modules.delete(pluginId);
        let rollbackError: string | undefined;
        try {
          await handle.wait(
            "retire-previous",
            () => retirePluginBackend(loaded),
            this.observation(loaded),
          );
          if (previous !== undefined) {
            const restored = await handle.wait("load-candidate", () => this.load(previous, handle));
            handle.phase("activate-candidate", this.observation(restored));
            this.activate(restored);
          }
        } catch (failure) {
          rollbackError = summary(failure);
        }
        this.errors.set(
          pluginId,
          `Plugin UI could not load.${rollbackError ? ` Recovery failed: ${rollbackError}.` : previous ? " The previous version was restored." : " Retry activation in Preferences > Plugins."} ${error.slice(0, 1024)}`,
        );
      },
    );
    return this.inventory.snapshot();
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
    try {
      return await this.transitions.reviewExit(
        [...this.modules.values()].map((loaded) => ({
          pluginId: loaded.installation.manifest.id,
          operation: "review-exit" as const,
          activationId: loaded.activationId,
        })),
        async (handles) => {
          for (const [pluginId, handle] of handles) {
            const loaded = this.modules.get(pluginId);
            if (loaded === undefined || this.exitResolved.has(pluginId)) continue;
            const prompt = await handle.wait(
              "review-exit",
              () => loaded.backend.beforeExit(),
              this.observation(loaded),
            );
            if (prompt !== undefined) {
              this.exitPrompted.set(pluginId, loaded.activationId);
              return { ...prompt, pluginId };
            }
          }
          return null;
        },
      );
    } catch (error) {
      this.exitResolved.clear();
      throw error;
    }
  }

  async resolveExit(pluginId: string, action: string): Promise<boolean> {
    parsePluginId(pluginId);
    await this.start();
    return this.transitions.run({ pluginId, operation: "resolve-exit" }, async (handle) => {
      const loaded = this.modules.get(pluginId);
      if (loaded === undefined || this.exitPrompted.get(pluginId) !== loaded.activationId)
        throw problem(
          "The plugin changed after this exit prompt. Review its current work before exiting.",
        );
      try {
        const allowed = await handle.wait(
          "resolve-exit",
          () => loaded.backend.resolveExit(action),
          this.observation(loaded),
        );
        if (allowed) this.exitResolved.add(pluginId);
        else this.exitResolved.clear();
        return allowed;
      } catch (error) {
        this.exitResolved.clear();
        throw error;
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
    this.deliveryController.close();
    this.transitions.beginClose();
    this.closing ??= (async (): Promise<void> => {
      const networkClosing = this.network.close();
      void networkClosing.catch(() => undefined);
      if (this.starting !== undefined) await this.starting;
      await this.transitions.settled();
      const results = await Promise.allSettled([
        networkClosing,
        ...[...this.modules.values()].map((loaded) =>
          this.transitions.observeShutdown(
            {
              pluginId: loaded.installation.manifest.id,
              operation: "shutdown",
              activationId: loaded.activationId,
            },
            (handle) =>
              handle.wait(
                "close-backend",
                () => retirePluginBackend(loaded),
                this.observation(loaded),
              ),
          ),
        ),
      ]);
      this.modules.clear();
      this.listeners.clear();
      this.publish();
      this.inventory.close();
      this.confirmations.clear();
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
