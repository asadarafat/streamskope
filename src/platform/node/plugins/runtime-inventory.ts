import type {
  PluginInstallation,
  PluginSnapshot,
  PluginTransition,
} from "../../../plugins/contracts";

import type { ActivePlugin, PluginStore } from "./store";

interface ActiveView {
  readonly installation: Pick<ActivePlugin, "manifest" | "sha256" | "stylesPath">;
  readonly activationId: string;
}
interface RuntimeView {
  readonly active: readonly ActiveView[];
  readonly errors: ReadonlyMap<string, string>;
  readonly transitions: readonly PluginTransition[];
  readonly error?: string;
}

function freeze<T>(value: T): T {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

/** Verified disk inventory and immutable, coherently revisioned public projections. */
export class PluginRuntimeInventory {
  private installations: readonly PluginInstallation[] = [];
  private storageError: string | undefined;
  private current: PluginSnapshot = freeze({ revision: 0, plugins: [] });
  private readonly publications: PluginSnapshot[] = [];
  private publishing = false;
  private readonly listeners = new Set<(snapshot: PluginSnapshot) => void>();

  constructor(private readonly store: PluginStore) {}

  /** Read disk only at startup or a storage boundary, never from a progress observer. */
  async refresh(): Promise<boolean> {
    try {
      this.installations = await this.store.list();
      this.storageError = undefined;
      return true;
    } catch (error) {
      this.storageError =
        error instanceof Error
          ? error.message.slice(0, 1_024)
          : "Plugin storage could not be read.";
      return false;
    }
  }

  subscribe(listener: (snapshot: PluginSnapshot) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  snapshot(): PluginSnapshot {
    return this.current;
  }

  publish(view: RuntimeView): PluginSnapshot {
    const active = new Map(view.active.map((entry) => [entry.installation.manifest.id, entry]));
    const transitions = new Map(view.transitions.map((entry) => [entry.pluginId, entry]));
    const rows = new Map(this.installations.map((entry) => [entry.id, entry]));
    for (const id of [...active.keys(), ...view.errors.keys(), ...transitions.keys()]) {
      if (!rows.has(id)) rows.set(id, { id, pending: null, restartRequired: false });
    }
    const plugins = [...rows.values()].map((installation): PluginInstallation => {
      const loaded = active.get(installation.id);
      const error = view.errors.get(installation.id) ?? installation.error;
      const transition = transitions.get(installation.id);
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
              rendererUrl: `/plugins/${loaded.installation.manifest.id}/${loaded.installation.sha256}/${loaded.activationId}/renderer.js`,
              ...(loaded.installation.stylesPath
                ? {
                    stylesUrl: `/plugins/${loaded.installation.manifest.id}/${loaded.installation.sha256}/${loaded.activationId}/renderer.css`,
                  }
                : {}),
            }
          : {}),
        ...(error ? { error } : {}),
        ...(transition ? { transition } : {}),
      };
    });
    const error = view.error ?? this.storageError;
    const next = { plugins, ...(error ? { error } : {}) };
    const { revision, ...previous } = this.current;
    if (JSON.stringify(next) === JSON.stringify(previous)) return this.current;
    this.current = freeze(structuredClone({ revision: revision + 1, ...next }));
    const snapshot = this.current;
    this.publications.push(snapshot);
    if (!this.publishing) {
      this.publishing = true;
      try {
        let publication: PluginSnapshot | undefined;
        while ((publication = this.publications.shift()) !== undefined) {
          for (const listener of this.listeners) {
            try {
              listener(publication);
            } catch {
              /* Observers cannot change host ownership. */
            }
          }
        }
      } finally {
        this.publishing = false;
      }
    }
    return snapshot;
  }

  close(): void {
    this.listeners.clear();
  }
}
