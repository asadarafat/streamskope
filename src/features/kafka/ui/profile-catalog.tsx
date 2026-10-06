import type {
  ProviderProfileCreationAction,
  ProviderProfileManagementControls,
  ProviderProfilesFacet,
  ProviderProfilesSnapshot,
} from "../../../platform/ui/provider-workspaces";
import {
  HOST_ACTIVITY_HISTORY_LIMIT,
  HOST_PROTOCOL_VERSION,
  type ActivityEntry,
  type HostEvent,
  type KafkaClusterDiagnosticsSnapshot,
  type ProfileStoreCapability,
  type ProfileSummary,
} from "../contracts";

import { pluginProfileText, type LoadedPluginRenderer } from "./PluginsProvider";
import { KafkaProfileManagement } from "./KafkaProfileManagement";
import type { StreamSkopeAppProperties } from "./StreamSkopeApp";
import { initialKafkaUiState } from "./state";

export type KafkaConnectionEvent = Extract<HostEvent, { readonly event: "connection.state" }>;

interface KafkaProfileManagementSnapshot {
  readonly profiles: readonly ProfileSummary[];
  readonly activities: readonly ActivityEntry[];
  readonly connectionEvent: KafkaConnectionEvent | undefined;
  readonly clusterDiagnostics: KafkaClusterDiagnosticsSnapshot;
}

/** Only safe control evidence is retained; message batches remain owned by the active workspace. */
export class KafkaProfileCatalog {
  readonly facet: ProviderProfilesFacet;
  private readonly listeners = new Set<() => void>();
  private unsubscribe: (() => void) | undefined;
  private observers = 0;
  private profileSequence = -1;
  private backendSequence = -1;
  private activitySequence = -1;
  private clusterSequence = -1;
  private store: ProfileStoreCapability | null = null;
  private available = true;
  private loading = true;
  private failure: ProviderProfilesSnapshot["failure"] = null;
  private plugins: readonly LoadedPluginRenderer[] = [];
  private pluginsLoading = true;
  private refreshOrdinal = 0;
  private managementSnapshot: KafkaProfileManagementSnapshot = {
    profiles: [],
    activities: [],
    connectionEvent: undefined,
    clusterDiagnostics: initialKafkaUiState.clusterDiagnostics,
  };
  private snapshot: ProviderProfilesSnapshot;

  constructor(readonly properties: StreamSkopeAppProperties) {
    this.snapshot = this.project();
    this.facet = {
      getSnapshot: this.getSnapshot,
      subscribe: this.subscribe,
      refresh: this.refresh,
      connect: this.connect,
      renderManagement: (controls: ProviderProfileManagementControls): React.JSX.Element => (
        <KafkaProfileManagement catalog={this} controls={controls} />
      ),
    };
  }

  readonly getSnapshot = (): ProviderProfilesSnapshot => this.snapshot;
  readonly getManagementSnapshot = (): KafkaProfileManagementSnapshot => this.managementSnapshot;
  readonly initialConnectionEvent = (): KafkaConnectionEvent | undefined =>
    this.managementSnapshot.connectionEvent;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    const release = this.observe();
    return (): void => {
      this.listeners.delete(listener);
      release();
    };
  };

  readonly setPlugins = (plugins: readonly LoadedPluginRenderer[], loading: boolean): void => {
    this.plugins = plugins;
    this.pluginsLoading = loading;
    this.publish();
  };

  readonly refresh = async (): Promise<void> => {
    const release = this.observe();
    const ordinal = ++this.refreshOrdinal;
    this.loading = true;
    this.publish();
    try {
      const response = await this.properties.host.execute({
        command: "profiles.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (ordinal !== this.refreshOrdinal) return;
      this.failure = response.ok
        ? null
        : { summary: response.error.summary, recovery: response.error.recovery };
    } catch {
      if (ordinal === this.refreshOrdinal)
        this.failure = {
          summary: "Kafka profiles could not be loaded.",
          recovery: "Restore the Kafka host and refresh the profiles.",
        };
    } finally {
      if (ordinal === this.refreshOrdinal) {
        this.loading = false;
        this.publish();
      }
      release();
    }
  };

  private readonly connect: ProviderProfilesFacet["connect"] = async (reference) => {
    const release = this.observe();
    try {
      await this.refresh();
      if (!this.snapshot.available || !this.snapshot.storageReady || this.snapshot.failure !== null)
        return {
          ok: false,
          summary: this.snapshot.failure?.summary ?? "Kafka profile storage is unavailable.",
          recovery:
            this.snapshot.failure?.recovery ??
            "Restore protected storage and refresh the profiles before connecting.",
        };
      const profile = this.managementSnapshot.profiles.find((entry) => entry.id === reference.id);
      if (
        profile === undefined ||
        (reference.revision !== undefined && profile.revision !== reference.revision)
      )
        return {
          ok: false,
          summary: "This Kafka profile changed or is no longer available.",
          recovery: "Refresh the profiles and select the current revision before connecting.",
        };
      const response = await this.properties.host.execute({
        command: "profiles.connect",
        id: globalThis.crypto.randomUUID(),
        payload: { profileId: reference.id },
        version: HOST_PROTOCOL_VERSION,
      });
      return response.ok
        ? { ok: true }
        : { ok: false, summary: response.error.summary, recovery: response.error.recovery };
    } catch {
      return {
        ok: false,
        summary: "The Kafka host did not confirm the connection.",
        recovery: "Restore the Kafka host and retry the saved profile connection.",
      };
    } finally {
      release();
    }
  };

  private observe(): () => void {
    this.observers += 1;
    if (this.unsubscribe === undefined) {
      try {
        this.unsubscribe = this.properties.host.subscribe(this.receive);
      } catch {
        this.available = false;
        this.loading = false;
        this.failure = {
          summary: "The Kafka host could not be observed.",
          recovery: "Reload StreamSkope before connecting a Kafka profile.",
        };
        this.publish();
      }
    }
    let released = false;
    return (): void => {
      if (released) return;
      released = true;
      this.observers -= 1;
      if (this.observers === 0) {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
      }
    };
  }

  private readonly receive = (event: HostEvent): void => {
    switch (event.event) {
      case "profiles.changed":
        if (event.sequence <= this.profileSequence) return;
        this.profileSequence = event.sequence;
        this.store = event.payload.store;
        this.managementSnapshot = { ...this.managementSnapshot, profiles: event.payload.profiles };
        this.loading = false;
        break;
      case "connection.state":
        if (event.sequence <= (this.managementSnapshot.connectionEvent?.sequence ?? -1)) return;
        this.managementSnapshot = {
          ...this.managementSnapshot,
          connectionEvent: event,
          clusterDiagnostics:
            event.payload.state !== "connected" ||
            event.payload.connectionName !==
              this.managementSnapshot.connectionEvent?.payload.connectionName
              ? initialKafkaUiState.clusterDiagnostics
              : this.managementSnapshot.clusterDiagnostics,
        };
        break;
      case "backend.availability":
        if (event.sequence <= this.backendSequence) return;
        this.backendSequence = event.sequence;
        this.available = event.payload.state === "ready";
        this.failure = this.available
          ? null
          : {
              summary: "The Kafka host is unavailable.",
              recovery: event.payload.recovery ?? "Reload StreamSkope to restore the Kafka host.",
            };
        break;
      case "activity.recorded":
        if (event.sequence <= this.activitySequence) return;
        this.activitySequence = event.sequence;
        this.managementSnapshot = {
          ...this.managementSnapshot,
          activities: [...this.managementSnapshot.activities, event.payload].slice(
            -HOST_ACTIVITY_HISTORY_LIMIT,
          ),
        };
        break;
      case "clusterDetails.changed":
        if (event.sequence <= this.clusterSequence) return;
        this.clusterSequence = event.sequence;
        this.managementSnapshot = { ...this.managementSnapshot, clusterDiagnostics: event.payload };
        break;
      default:
        return;
    }
    this.publish();
  };

  private project(): ProviderProfilesSnapshot {
    const storageReady = this.store?.state === "ready";
    const connected = this.managementSnapshot.connectionEvent?.payload.state === "connected";
    const creationActions: ProviderProfileCreationAction[] = [
      {
        id: "direct",
        label: "Kafka broker",
        description:
          "Connect to an existing Kafka cluster using brokers, authentication, and trust.",
        kind: "direct",
        available: this.available && storageReady,
      },
      ...this.plugins.flatMap((plugin) =>
        plugin.renderer.connectionActions.map((action) => ({
          id: `plugin:${plugin.manifest.id}:${action.id}`,
          label: action.label,
          description: `Create a Kafka connection through ${plugin.manifest.name}.`,
          kind: "connector" as const,
          available: this.available && storageReady && !connected && !this.pluginsLoading,
        })),
      ),
      {
        id: "manage-plugins",
        label: "Manage plugins",
        description: "Install and manage optional Kafka connection workflows.",
        kind: "manage",
        available: this.available,
      },
    ];
    return {
      profiles: this.managementSnapshot.profiles.map((profile) => ({
        id: profile.id,
        ...(profile.revision === undefined ? {} : { revision: profile.revision }),
        name: profile.name,
        endpoints: profile.brokers,
        authentication: profile.oauth === undefined ? "None" : "OAuth 2.0",
        transport: profile.transport === "plaintext" ? "Plaintext · insecure" : "Verified TLS",
        source:
          profile.source === undefined
            ? "Kafka"
            : (pluginProfileText(
                this.plugins.find((plugin) => plugin.manifest.id === profile.source?.pluginId)
                  ?.renderer,
                "profileLabel",
                profile.source,
              ) ?? "Plugin-managed Kafka"),
        active: profile.active && connected,
        actions: [
          { id: "cluster", label: "Cluster detail", available: profile.active && connected },
        ],
      })),
      loading: this.loading,
      available: this.available,
      storageReady,
      storageLabel:
        this.store === null
          ? "Kafka profile storage is being loaded."
          : this.store.state === "unavailable"
            ? `Kafka profile storage unavailable. ${this.store.recovery ?? "Restore protected storage and restart StreamSkope."}`
            : this.store.durability === "durable"
              ? "Kafka · OS-protected profiles."
              : "Kafka · Session-only profiles. Credentials remain in host memory.",
      failure:
        this.failure ??
        (this.store?.state === "unavailable"
          ? {
              summary: "Profile storage unavailable",
              recovery: this.store.recovery ?? "Restore protected storage and restart StreamSkope.",
            }
          : null),
      creationActions,
    };
  }

  private publish(): void {
    const next = this.project();
    if (JSON.stringify(next) !== JSON.stringify(this.snapshot)) this.snapshot = next;
    for (const listener of this.listeners) listener();
  }
}
