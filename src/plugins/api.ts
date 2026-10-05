import type {
  ActivityEntry,
  HostError,
  ProfileSummary,
  StreamSkopeBackend,
} from "../features/kafka/contracts";

import type {
  JsonObject,
  JsonValue,
  PluginCatalogSnapshot,
  PluginChangeOperation,
  PluginChangePrompt,
  PluginChangeWarning,
  PluginEvent,
  PluginExitPrompt,
  PluginProfileSource,
  PluginRequest,
  PluginSnapshot,
} from "./contracts";

export interface PluginActivity {
  readonly correlationId: string;
  readonly detail: string;
  readonly object: string;
  readonly operation: string;
  readonly outcome: ActivityEntry["outcome"];
  readonly severity: ActivityEntry["severity"];
  readonly sensitiveValues?: readonly string[];
}

export interface PluginHostBindings {
  readonly execute: StreamSkopeBackend["execute"];
  connectionActive(): boolean;
  profiles(): Promise<readonly ProfileSummary[]>;
  deleteProfile(id: string): Promise<void>;
  disconnectPluginConnection(pluginId: string): Promise<void>;
  recordActivity(input: PluginActivity): void;
  failure(
    error: unknown,
    context: { readonly correlationId: string; readonly sensitiveValues?: readonly string[] },
  ): HostError;
}

export interface PluginBackendHost extends Omit<PluginHostBindings, "disconnectPluginConnection"> {
  /** Optional API 2 capability. Only non-secret recovery identifiers belong here. */
  readonly recoveryState?: {
    read(): Promise<JsonValue | null>;
    write(value: JsonValue | null): Promise<void>;
  };
  disconnectOwnedConnection(): Promise<void>;
  publish(name: string, data: JsonValue): void;
  probeTopics(brokers: readonly string[], signal?: AbortSignal): Promise<readonly string[]>;
}

export interface PluginBackend {
  execute(request: Omit<PluginRequest, "pluginId" | "activationId">): Promise<JsonValue>;
  validateProfile(data: JsonObject, brokers: readonly string[]): Promise<void>;
  beforeExit(): Promise<Omit<PluginExitPrompt, "pluginId"> | undefined>;
  resolveExit(action: string): Promise<boolean>;
  beforeChange(): Promise<PluginChangeWarning | undefined>;
  prepareUnload(reason: "update" | "remove"): Promise<void>;
  close(): Promise<void>;
}

export interface PluginBackendModule {
  activate(host: PluginBackendHost): PluginBackend | Promise<PluginBackend>;
}

export interface PluginRuntimePort {
  bindHost(host: PluginHostBindings): void;
  start(): Promise<void>;
  execute(request: PluginRequest): Promise<JsonValue>;
  validateProfile(source: PluginProfileSource, brokers: readonly string[]): Promise<void>;
  withProfileConnection<T>(
    source: PluginProfileSource,
    brokers: readonly string[],
    connect: () => Promise<T>,
  ): Promise<T>;
  subscribe(listener: (event: PluginEvent) => void): () => void;
  subscribeChanges(listener: (snapshot: PluginSnapshot) => void): () => void;
  list(): Promise<PluginSnapshot>;
  catalog(refresh?: boolean): Promise<PluginCatalogSnapshot>;
  prepareChange(
    pluginId: string,
    operation: PluginChangeOperation,
  ): Promise<PluginChangePrompt | null>;
  install(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot>;
  retryActivation(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot>;
  remove(pluginId: string, confirmationToken?: string): Promise<PluginSnapshot>;
  rendererFailed(pluginId: string, activationId: string, error: string): Promise<PluginSnapshot>;
  restart(): Promise<void>;
  prepareExit(): Promise<PluginExitPrompt | null>;
  resolveExit(pluginId: string, action: string): Promise<boolean>;
  close(): Promise<void>;
}
