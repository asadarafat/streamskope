import type {
  ProviderConnectionOutcome,
  ProviderProfileReference,
  ProviderProfilesFacet,
  ProviderProfilesSnapshot,
} from "../../../platform/ui/provider-workspaces";

import { NatsProfileManagement } from "./NatsProfileManagement";
import { natsProfileStorageLabel } from "./profile-presentation";
import { createNatsWorkspaceOwner, type NatsWorkspaceOwner } from "./workspace-controller";
import { initialNatsWorkspaceSnapshot, type NatsWorkspaceSnapshot } from "./workspace-state";
import type { NatsWorkspaceSource } from "./workspace-types";

export interface NatsProfileManagementStore {
  readonly getSnapshot: () => NatsWorkspaceSnapshot;
  readonly subscribe: (listener: () => void) => () => void;
  readonly owner: () =>
    Pick<NatsWorkspaceOwner, "createProfile" | "updateProfile" | "deleteProfile"> | undefined;
}

function project(state: NatsWorkspaceSnapshot): ProviderProfilesSnapshot {
  const failure = state.failure ?? state.connection.failure;
  const storageReady = state.profiles?.capability.state === "ready";
  return {
    profiles: (state.profiles?.profiles ?? []).map((profile) => ({
      id: profile.id,
      revision: profile.revision,
      name: profile.name,
      endpoints: profile.servers,
      authentication: profile.authentication.mode === "token" ? "Token" : "None",
      transport: profile.tls.mode === "tls" ? "Verified TLS" : "Plaintext",
      source: "Direct",
      active:
        state.connection.state !== "disconnected" && state.connection.profile?.id === profile.id,
    })),
    loading: state.loading,
    available: state.available,
    storageReady,
    storageLabel: state.available
      ? natsProfileStorageLabel(state.profiles?.capability)
      : "Profile storage cannot be verified while the NATS host is unavailable.",
    failure:
      failure === undefined || failure === null
        ? null
        : {
            summary: failure.summary,
            recovery: failure.recovery ?? "Refresh profiles before retrying.",
          },
    creationActions: [
      {
        id: "new",
        label: "NATS server",
        description:
          "Read live subscriptions from your NATS server using token authentication and verified TLS.",
        kind: "direct",
        available: state.available && storageReady && state.pending.length === 0,
      },
    ],
  };
}

/** Owns profile controls for the shared catalog, independently of a live workspace activation. */
export function createNatsProfilesFacet(
  resolveSource: () => NatsWorkspaceSource,
): ProviderProfilesFacet {
  let state = initialNatsWorkspaceSnapshot();
  let owner: NatsWorkspaceOwner | undefined;
  let stopObserving: (() => void) | undefined;
  let initialized = false;
  const listeners = new Set<() => void>();
  let projected = project(state);
  let projectedKey = JSON.stringify(projected);

  function publish(next: NatsWorkspaceSnapshot): void {
    state = next;
    for (const listener of listeners) listener();
  }
  function getSnapshot(): ProviderProfilesSnapshot {
    const next = project(state);
    const key = JSON.stringify(next);
    if (key !== projectedKey) {
      projectedKey = key;
      projected = next;
    }
    return projected;
  }
  function acquire(): NatsWorkspaceOwner | undefined {
    if (initialized) return owner;
    initialized = true;
    try {
      const source = resolveSource();
      if (source.state === "unavailable") {
        publish(initialNatsWorkspaceSnapshot(source.recovery));
        return undefined;
      }
      owner = createNatsWorkspaceOwner(source.host, () => true, "control-only");
      stopObserving = owner.subscribe(publish);
      publish(owner.snapshot());
      owner.start();
      return owner;
    } catch {
      publish(initialNatsWorkspaceSnapshot("Restore the NATS host and reload StreamSkope."));
      return undefined;
    }
  }
  const store: NatsProfileManagementStore = {
    getSnapshot: (): NatsWorkspaceSnapshot => state,
    owner: acquire,
    subscribe: (listener): (() => void) => {
      listeners.add(listener);
      acquire();
      return (): void => {
        listeners.delete(listener);
        if (listeners.size !== 0) return;
        stopObserving?.();
        owner?.dispose();
        stopObserving = undefined;
        owner = undefined;
        initialized = false;
      };
    },
  };

  async function connect(reference: ProviderProfileReference): Promise<ProviderConnectionOutcome> {
    const original = acquire();
    const profile = state.profiles?.profiles.find((candidate) => candidate.id === reference.id);
    if (original === undefined || !state.available || !profile)
      return {
        ok: false,
        summary: "The selected NATS profile is unavailable.",
        recovery: "Refresh profiles and select an available NATS profile.",
      };
    if (reference.revision === undefined || reference.revision !== profile.revision)
      return {
        ok: false,
        summary: "This NATS profile changed after it was selected.",
        recovery: "Refresh profiles, review the current settings and connect again.",
      };
    if (await original.connectProfile(profile)) return { ok: true };
    const failure = original.snapshot().failure ?? original.snapshot().connection.failure;
    return {
      ok: false,
      summary: failure?.summary ?? "The NATS connection could not be established.",
      recovery: failure?.recovery ?? "Review the profile and refresh host state before retrying.",
    };
  }

  return {
    getSnapshot,
    subscribe: (listener): (() => void) => {
      let observed = getSnapshot();
      return store.subscribe(() => {
        const next = getSnapshot();
        if (next === observed) return;
        observed = next;
        listener();
      });
    },
    refresh: async (): Promise<void> => {
      await acquire()?.refresh();
    },
    connect,
    renderManagement: (controls): React.JSX.Element => (
      <NatsProfileManagement store={store} controls={controls} />
    ),
  };
}
