import type { PluginManifest, PluginSnapshot } from "../../../plugins/contracts";
import { comparePluginManifests } from "../../../plugins/validation";

import { pluginProblem } from "./problem";
import type { ActivePlugin, PluginStore } from "./store";
import type { PluginTransitions, PluginTransitionHandle } from "./transition";

export interface PluginCandidateAuthority {
  readonly candidateId: string;
  readonly sha256: string;
  readonly assertCurrent: () => void;
}
interface Instance {
  readonly installation: ActivePlugin;
  readonly activationId: string;
  readonly requests: ReadonlySet<Promise<unknown>>;
  readonly connections: ReadonlySet<Promise<unknown>>;
}
interface Bindings<Loaded extends Instance> {
  readonly store: PluginStore;
  readonly compatible: (manifest: PluginManifest) => void;
  readonly current: (pluginId: string) => Loaded | undefined;
  readonly healthy: (pluginId: string) => boolean;
  readonly transitions: PluginTransitions;
  readonly snapshot: () => PluginSnapshot;
  readonly refresh: () => Promise<boolean>;
  readonly confirm: (
    pluginId: string,
    loaded: Loaded | undefined,
    handle: PluginTransitionHandle,
    token?: string,
    candidate?: PluginCandidateAuthority,
  ) => Promise<void>;
  readonly replace: (
    installation: ActivePlugin,
    previous: Loaded | undefined,
    handle: PluginTransitionHandle,
  ) => Promise<void>;
}

/** Every delivery source enters the same verified install, consent, activation and rollback boundary. */
export class PluginInstaller<Loaded extends Instance> {
  constructor(private readonly bindings: Bindings<Loaded>) {}
  async apply(
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
    await this.bindings.transitions.run(
      { pluginId, operation: "install" },
      async (handle) => {
        const previous = this.bindings.current(pluginId);
        if (previous !== undefined)
          handle.phase("verify-package", {
            activationId: previous.activationId,
            counts: () => ({
              requests: previous.requests.size,
              connections: previous.connections.size,
            }),
          });
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
          const stored = await handle.wait("verify-package", async () =>
            (await this.bindings.store.list()).find((entry) => entry.id === pluginId),
          );
          for (const current of [previous?.installation.manifest, stored?.installed]) {
            if (current !== undefined && comparePluginManifests(manifest, current) < 0)
              throw pluginProblem(
                `The available plugin version ${manifest.version} is older than installed version ${current.version}.`,
                "Refresh the plugin catalog and wait for a compatible update.",
              );
          }
          handle.assertCurrent();
          installation = await handle.wait("verify-package", () =>
            this.bindings.store.prepareInstall(bytes, sha256),
          );
          handle.assertCurrent();
          if (
            previous !== undefined &&
            this.bindings.healthy(pluginId) &&
            installation.sha256 === previous.installation.sha256 &&
            installation.contentSha256 === previous.installation.contentSha256
          )
            return;
          if (previous !== undefined)
            await handle.wait("wait-connections", () =>
              Promise.allSettled([...previous.connections]),
            );
          await this.bindings.confirm(pluginId, previous, handle, confirmationToken, candidate);
          handle.assertCurrent();
          await this.bindings.replace(installation, previous, handle);
        } catch (error) {
          if (installation !== undefined) {
            const rejected = installation;
            await handle
              .wait("discard-package", async () => {
                await this.bindings.store.discardInstall(pluginId, rejected.sha256);
                await this.bindings.refresh();
              })
              .catch(() => undefined);
          }
          throw error;
        }
      },
      assertCurrent,
    );
    return this.bindings.snapshot();
  }
}
