import type { PluginManifest, PluginSnapshot } from "../../../plugins/contracts";
import { comparePluginManifests } from "../../../plugins/validation";

import { pluginProblem } from "./problem";
import type { ActivePlugin, PluginStore } from "./store";

export interface PluginCandidateAuthority {
  readonly candidateId: string;
  readonly sha256: string;
  readonly assertCurrent: () => void;
}
interface Instance {
  readonly installation: ActivePlugin;
  readonly connections: ReadonlySet<Promise<unknown>>;
}
interface Bindings<Loaded extends Instance> {
  readonly store: PluginStore;
  readonly compatible: (manifest: PluginManifest) => void;
  readonly current: (pluginId: string) => Loaded | undefined;
  readonly healthy: (pluginId: string) => boolean;
  readonly enter: (pluginId: string) => void;
  readonly leave: (pluginId: string) => void;
  readonly serial: <T>(operation: () => Promise<T>) => Promise<T>;
  readonly snapshot: () => Promise<PluginSnapshot>;
  readonly changed: () => Promise<PluginSnapshot>;
  readonly confirm: (
    pluginId: string,
    loaded: Loaded | undefined,
    token?: string,
    candidate?: PluginCandidateAuthority,
  ) => Promise<void>;
  readonly replace: (
    installation: ActivePlugin,
    previous: Loaded | undefined,
    assertCurrent: () => void,
  ) => Promise<void>;
}

/** Every delivery source enters the same verified install, consent, activation and rollback boundary. */
export class PluginInstaller<Loaded extends Instance> {
  constructor(private readonly bindings: Bindings<Loaded>) {}
  apply(
    pluginId: string,
    bytes: Uint8Array,
    sha256: string,
    confirmationToken: string | undefined,
    assertIntent: () => void,
    candidate?: PluginCandidateAuthority,
  ): Promise<PluginSnapshot> {
    const verified = this.bindings.store.verifyPackage(bytes, sha256);
    const manifest = verified.manifest;
    this.bindings.compatible(manifest);
    if (manifest.id !== pluginId)
      throw pluginProblem("The downloaded plugin identity does not match the selection.");
    const assertCurrent = (): void => {
      assertIntent();
      candidate?.assertCurrent();
    };
    return this.bindings.serial(async () => {
      assertCurrent();
      this.bindings.enter(pluginId);
      const previous = this.bindings.current(pluginId);
      let installation: ActivePlugin | undefined;
      try {
        if (
          manifest.apiVersion >= 3 &&
          previous !== undefined &&
          comparePluginManifests(previous.installation.manifest, manifest) === 0 &&
          previous.installation.contentSha256 !== verified.contentSha256
        )
          throw pluginProblem(
            "This plugin version has different content from the installed package.",
            "Refresh the catalog after a new plugin version is published. The current installation is retained.",
          );
        const stored = (await this.bindings.store.list()).find((entry) => entry.id === pluginId);
        for (const current of [previous?.installation.manifest, stored?.installed]) {
          if (current !== undefined && comparePluginManifests(manifest, current) < 0)
            throw pluginProblem(
              `The available plugin version ${manifest.version} is older than installed version ${current.version}.`,
              "Refresh the plugin catalog and wait for a compatible update.",
            );
        }
        assertCurrent();
        installation = await this.bindings.store.prepareInstall(bytes, sha256);
        assertCurrent();
        if (
          previous !== undefined &&
          this.bindings.healthy(pluginId) &&
          installation.sha256 === previous.installation.sha256 &&
          installation.contentSha256 === previous.installation.contentSha256
        ) {
          this.bindings.leave(pluginId);
          return this.bindings.snapshot();
        }
        if (previous !== undefined) await Promise.allSettled([...previous.connections]);
        await this.bindings.confirm(pluginId, previous, confirmationToken, candidate);
        assertCurrent();
        await this.bindings.replace(installation, previous, assertCurrent);
        this.bindings.leave(pluginId);
        return await this.bindings.changed();
      } catch (error) {
        if (installation !== undefined)
          await this.bindings.store
            .discardInstall(pluginId, installation.sha256)
            .catch(() => undefined);
        throw error;
      } finally {
        this.bindings.leave(pluginId);
      }
    });
  }
}
