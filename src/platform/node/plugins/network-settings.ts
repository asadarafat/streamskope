import { lstat, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

import type {
  PluginNetworkConfiguration,
  PluginNetworkSnapshot,
  PluginNetworkUpdateInput,
} from "../../../plugins/contracts";
import {
  parsePluginNetworkConfiguration,
  parsePluginNetworkUpdateInput,
} from "../../../plugins/network-validation";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "../atomic-private-text-file";
import { readBoundedFile } from "../bounded-file";
import type { ProfileProtector } from "../profile-protector";

import type {
  PluginNetworkTransport,
  PluginNetworkTransportConfiguration,
} from "./network-transport";
import { pluginProblem } from "./problem";

export const PLUGIN_NETWORK_SETTINGS_MAX_BYTES = 32 * 1024;
const MAX_SETTINGS_BYTES = PLUGIN_NETWORK_SETTINGS_MAX_BYTES;
const DEFAULT_CONFIGURATION: PluginNetworkConfiguration = {
  mode: "system",
  proxyUrl: null,
  offline: false,
};
type Credentials = NonNullable<PluginNetworkTransportConfiguration["credentials"]>;
interface State {
  readonly revision: number;
  readonly configuration: PluginNetworkConfiguration | null;
  readonly credentials?: Credentials;
  readonly protectedCredentials?: string;
  readonly error?: string;
}
interface Options {
  readonly path: string;
  readonly transport?: PluginNetworkTransport;
  readonly protector?: ProfileProtector;
  readonly durableSettings?: boolean;
  readonly changing: () => void;
}
function missing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}
function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid plugin network settings.");
  return value as Record<string, unknown>;
}

export interface PluginNetworkSettingsDocument {
  readonly formatVersion: 1;
  readonly revision: number;
  readonly configuration: PluginNetworkConfiguration;
  readonly protectedCredentials?: string;
}

/** Shared persisted-envelope parser; transport support and decryption remain runtime-owned. */
export function parsePluginNetworkSettingsDocument(value: unknown): PluginNetworkSettingsDocument {
  const raw = object(value);
  if (
    raw.formatVersion !== 1 ||
    typeof raw.revision !== "number" ||
    !Number.isSafeInteger(raw.revision) ||
    raw.revision < 0 ||
    Object.keys(raw).some(
      (key) =>
        !["formatVersion", "revision", "configuration", "protectedCredentials"].includes(key),
    )
  )
    throw new Error("Invalid plugin network settings metadata.");
  const configuration = parsePluginNetworkConfiguration(raw.configuration);
  if (
    raw.protectedCredentials !== undefined &&
    (typeof raw.protectedCredentials !== "string" ||
      raw.protectedCredentials.length > 24 * 1024 ||
      raw.protectedCredentials.length === 0 ||
      Buffer.from(raw.protectedCredentials, "base64").toString("base64") !==
        raw.protectedCredentials)
  )
    throw new Error("Invalid protected proxy credential envelope.");
  return {
    formatVersion: 1,
    revision: raw.revision,
    configuration,
    ...(raw.protectedCredentials === undefined
      ? {}
      : { protectedCredentials: raw.protectedCredentials }),
  };
}

/** Public configuration is durable; proxy credentials are protected or strictly session-only. */
export class PluginNetworkSettings {
  private state: State = { revision: 0, configuration: DEFAULT_CONFIGURATION };
  private initializing: Promise<void> | undefined;
  private queue: Promise<unknown> = Promise.resolve();
  private changing = false;
  private closed = false;
  private readonly lifetime = new AbortController();
  private encryptionAvailable: boolean;
  private readonly durableSettings: boolean;
  constructor(private readonly options: Options) {
    this.encryptionAvailable = options.protector !== undefined;
    this.durableSettings = options.durableSettings ?? options.transport?.nativeAvailable === true;
  }

  private assertOpen(): void {
    if (this.closed) throw pluginProblem("Plugin networking is closing.");
  }
  private transportConfiguration(state: State): PluginNetworkTransportConfiguration {
    const configuration = state.configuration;
    if (configuration === null)
      throw pluginProblem("Plugin network settings must be reset before downloading plugins.");
    return configuration.mode === "system"
      ? { mode: "system" }
      : {
          mode: "custom",
          ...(configuration.proxyUrl === null ? {} : { proxyUrl: configuration.proxyUrl }),
          ...(state.credentials === undefined ? {} : { credentials: state.credentials }),
        };
  }
  private assertSupportedConfiguration(configuration: PluginNetworkConfiguration): void {
    if (
      configuration.mode === "custom" &&
      (this.options.transport?.nativeAvailable !== true ||
        !this.options.transport.supportedProxyProtocols.includes(
          new URL(configuration.proxyUrl!).protocol.slice(0, -1) as "http" | "https",
        ))
    )
      throw pluginProblem(
        "Custom plugin proxies are available in the desktop app with HTTP or HTTPS support.",
      );
  }
  private async safePath(create: boolean): Promise<void> {
    if (create) await mkdir(dirname(this.options.path), { mode: 0o700, recursive: true });
    const parent = await lstat(dirname(this.options.path));
    if (!parent.isDirectory() || parent.isSymbolicLink())
      throw new Error("Plugin network settings require a regular directory.");
    try {
      const metadata = await lstat(this.options.path);
      if (!metadata.isFile() || metadata.isSymbolicLink())
        throw new Error("Plugin network settings require a regular file.");
    } catch (error) {
      if (!missing(error)) throw error;
    }
  }
  private async save(state: State): Promise<void> {
    this.assertOpen();
    if (!this.durableSettings) return;
    await this.safePath(true);
    const contents = JSON.stringify({
      formatVersion: 1,
      revision: state.revision,
      configuration: state.configuration,
      ...(state.protectedCredentials === undefined
        ? {}
        : { protectedCredentials: state.protectedCredentials }),
    });
    if (Buffer.byteLength(contents) > MAX_SETTINGS_BYTES)
      throw new Error("Plugin network settings exceed their storage bound.");
    await writeAtomicPrivateTextFile({
      path: this.options.path,
      contents,
      createTempId: createAtomicPrivateFileTempId,
      signal: this.lifetime.signal,
    });
  }
  private async load(): Promise<void> {
    try {
      if (this.durableSettings) {
        try {
          await this.safePath(false);
          const raw = parsePluginNetworkSettingsDocument(
            JSON.parse(
              (
                await readBoundedFile(this.options.path, MAX_SETTINGS_BYTES, {
                  rejectSymlinks: true,
                })
              ).toString("utf8"),
            ),
          );
          const configuration = raw.configuration;
          this.assertSupportedConfiguration(configuration);
          let credentials: Credentials | undefined;
          let protectedCredentials: string | undefined;
          if (raw.protectedCredentials !== undefined) {
            if (this.options.protector === undefined)
              throw new Error("Protected proxy credentials are unavailable.");
            const decrypted = await this.options.protector.unprotect(
              Buffer.from(raw.protectedCredentials, "base64"),
            );
            const protectedValue = object(JSON.parse(decrypted.plaintext));
            if (
              configuration.proxyUrl === null ||
              protectedValue.proxyUrl !== configuration.proxyUrl ||
              Object.keys(protectedValue).some(
                (key) => !["proxyUrl", "username", "password"].includes(key),
              )
            )
              throw new Error("Protected credentials do not match the remembered proxy endpoint.");
            const value = parsePluginNetworkUpdateInput({
              configuration,
              credentials: {
                action: "replace",
                username: protectedValue.username,
                password: protectedValue.password,
              },
            });
            if (value.credentials.action !== "replace")
              throw new Error("Invalid protected proxy credentials.");
            credentials = {
              username: value.credentials.username,
              password: value.credentials.password,
            };
            protectedCredentials = raw.protectedCredentials;
          }
          this.assertOpen();
          this.state = {
            revision: raw.revision,
            configuration,
            ...(credentials === undefined ? {} : { credentials }),
            ...(protectedCredentials === undefined ? {} : { protectedCredentials }),
          };
        } catch (error) {
          if (!missing(error)) throw error;
        }
      }
      this.assertOpen();
      await this.options.transport?.configure(this.transportConfiguration(this.state));
      this.assertOpen();
    } catch {
      if (!this.closed)
        this.state = {
          revision: this.state.revision,
          configuration: null,
          error:
            "Saved plugin network settings could not be read or applied. Reset them before downloading plugins; installed plugins and file installation remain available.",
        };
    }
  }
  private initialize(): Promise<void> {
    this.initializing ??= this.load();
    return this.initializing;
  }
  private current(): PluginNetworkSnapshot {
    return {
      revision: this.state.revision,
      configuration: this.state.configuration === null ? null : { ...this.state.configuration },
      credentialsConfigured: this.state.credentials !== undefined,
      credentialStorage:
        this.options.transport?.nativeAvailable !== true
          ? "unavailable"
          : this.state.protectedCredentials !== undefined || this.encryptionAvailable
            ? "encrypted"
            : "session",
      nativeAvailable: this.options.transport?.nativeAvailable === true,
      supportedProxyProtocols: this.options.transport?.supportedProxyProtocols ?? [],
      ...(this.state.error === undefined ? {} : { error: this.state.error }),
    };
  }
  async snapshot(): Promise<PluginNetworkSnapshot> {
    this.assertOpen();
    await this.initialize();
    await this.queue;
    this.assertOpen();
    return this.current();
  }
  async remote(): Promise<number> {
    const value = await this.snapshot();
    if (value.configuration === null)
      throw pluginProblem(
        "Plugin network settings need attention before downloads can start.",
        "Open Preferences > Plugins > Plugin download settings and select Reset download settings or save valid settings. Signed file and cached package installation remain available.",
      );
    if (value.configuration.offline)
      throw pluginProblem(
        "Plugin networking is offline.",
        "Turn off Offline mode to refresh or download, or install a signed file or cached package.",
      );
    return value.revision;
  }
  assertCurrent(revision: number): void {
    this.assertOpen();
    if (
      this.changing ||
      this.state.revision !== revision ||
      this.state.configuration === null ||
      this.state.configuration.offline
    )
      throw pluginProblem(
        "Plugin network settings changed during acquisition.",
        "Retry with the current settings, or install a signed file or cached package.",
      );
  }
  async update(input: PluginNetworkUpdateInput): Promise<PluginNetworkSnapshot> {
    this.assertOpen();
    const validated = parsePluginNetworkUpdateInput(input);
    await this.initialize();
    const result = this.queue.then(async (): Promise<PluginNetworkSnapshot> => {
      this.assertOpen();
      const previous = this.state;
      const configuration = validated.configuration;
      this.assertSupportedConfiguration(configuration);
      if (
        validated.credentials.action === "unchanged" &&
        (previous.configuration === null ||
          (previous.credentials !== undefined &&
            previous.configuration.proxyUrl !== configuration.proxyUrl))
      )
        throw pluginProblem(
          "Proxy credentials cannot be reused for a different endpoint.",
          "Clear the credentials or supply credentials for the new proxy.",
        );
      if (
        validated.credentials.action === "replace" &&
        this.options.transport?.nativeAvailable !== true
      )
        throw pluginProblem("Proxy credentials are available only in the desktop app.");
      const credentials =
        validated.credentials.action === "replace"
          ? {
              username: validated.credentials.username,
              password: validated.credentials.password,
            }
          : validated.credentials.action === "unchanged"
            ? previous.credentials
            : undefined;
      this.changing = true;
      this.options.changing();
      let protectedCredentials: string | undefined;
      if (
        credentials !== undefined &&
        this.encryptionAvailable &&
        this.options.protector !== undefined
      ) {
        try {
          const protectedValue = await this.options.protector.protect(
            JSON.stringify({ proxyUrl: configuration.proxyUrl, ...credentials }),
          );
          if (protectedValue.byteLength === 0 || protectedValue.byteLength > 16 * 1024)
            throw new Error("Invalid protected credential size.");
          protectedCredentials = protectedValue.toString("base64");
        } catch {
          this.encryptionAvailable = false;
        }
      }
      const next: State = {
        revision: previous.revision + 1,
        configuration,
        ...(credentials === undefined ? {} : { credentials }),
        ...(protectedCredentials === undefined ? {} : { protectedCredentials }),
      };
      try {
        this.assertOpen();
        await this.options.transport?.configure(this.transportConfiguration(next));
        this.assertOpen();
        await this.save(next);
        this.assertOpen();
        this.state = next;
        return this.current();
      } catch {
        if (this.closed) throw pluginProblem("Plugin networking is closing.");
        try {
          if (previous.configuration === null)
            throw new Error("No previous valid network configuration.");
          await this.options.transport?.configure(this.transportConfiguration(previous));
          this.assertOpen();
          this.state = previous;
        } catch {
          this.state = {
            revision: previous.revision + 1,
            configuration: null,
            error:
              "Plugin network settings could not be applied or restored. Reset the settings before trying a download.",
          };
        }
        throw pluginProblem(
          "Plugin network settings could not be saved or applied.",
          "Check desktop storage access and proxy settings, then retry or reset them. Installed plugins remain available.",
        );
      } finally {
        this.changing = false;
      }
    });
    this.queue = result.catch(() => undefined);
    return result;
  }
  close(): void {
    this.closed = true;
    this.lifetime.abort();
    this.state = {
      revision: this.state.revision,
      configuration: null,
      error: "Plugin networking is closed.",
    };
  }
}
