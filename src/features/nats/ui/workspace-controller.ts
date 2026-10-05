import {
  NATS_PROTOCOL_VERSION,
  type NatsCommand,
  type NatsCommandName,
  type NatsCommandResponse,
  type NatsConnectionSnapshot,
  type NatsEvent,
  type NatsHost,
  type NatsProfileCreateInput,
  type NatsProfileSummary,
  type NatsProfilesSnapshot,
  type NatsProfileUpdateInput,
  type NatsSubscriptionSnapshot,
} from "../contracts";

import { appendNatsRecords, emptyNatsRecordWindow } from "./record-window";
import {
  initialNatsWorkspaceSnapshot,
  mergeNatsCounters,
  type NatsWorkspaceSnapshot,
} from "./workspace-state";

export interface NatsWorkspaceActions {
  readonly createProfile: (input: NatsProfileCreateInput) => Promise<boolean>;
  readonly updateProfile: (
    profile: NatsProfileSummary,
    input: NatsProfileUpdateInput,
  ) => Promise<boolean>;
  readonly deleteProfile: (profile: NatsProfileSummary) => Promise<boolean>;
  readonly connectProfile: (profile: NatsProfileSummary) => Promise<boolean>;
  readonly disconnect: () => Promise<boolean>;
  readonly startSubscription: (subject: string) => Promise<boolean>;
  readonly stopSubscription: () => Promise<boolean>;
  readonly refresh: () => Promise<boolean>;
  readonly selectRecord: (id: string | null) => void;
  readonly clearFailure: () => void;
}
export interface NatsWorkspaceOwner extends NatsWorkspaceActions {
  readonly snapshot: () => NatsWorkspaceSnapshot;
  readonly subscribe: (listener: (snapshot: NatsWorkspaceSnapshot) => void) => () => void;
  readonly start: () => void;
  readonly dispose: () => void;
}

/** Owns one mounted view: host receipts, event ordering and bounded live evidence. */
export function createNatsWorkspaceOwner(
  host: NatsHost,
  isInteractive: () => boolean,
): NatsWorkspaceOwner {
  let state = initialNatsWorkspaceSnapshot();
  let window = emptyNatsRecordWindow();
  let disposed = false;
  let started = false;
  let bootstrapped = false;
  let removeListener: (() => void) | undefined;
  let commandOrdinal = 0;
  let listOrdinal = 0;
  let batchOrdinal = 0;
  const listeners = new Set<(snapshot: NatsWorkspaceSnapshot) => void>();
  const pending = new Map<string, NatsCommandName>();
  function publish(next: NatsWorkspaceSnapshot): void {
    if (disposed) return;
    state = next;
    for (const listener of listeners) {
      try {
        listener(state);
      } catch {
        /* A view observer cannot interrupt host ownership. */
      }
    }
  }
  function applyProfiles(profiles: NatsProfilesSnapshot): void {
    if (profiles.revision < (state.profiles?.revision ?? 0)) return;
    publish({ ...state, profiles });
  }
  function applyConnection(connection: NatsConnectionSnapshot): void {
    if (connection.revision < state.connection.revision) return;
    publish({ ...state, connection });
  }
  function applySubscription(subscription: NatsSubscriptionSnapshot, preserveQueue = false): void {
    if (subscription.revision < state.subscription.revision) return;
    const same = subscription.generation === state.subscription.generation;
    if (!same) window = emptyNatsRecordWindow();
    const counters = same
      ? mergeNatsCounters(state.subscription.counters, subscription.counters)
      : subscription.counters;
    publish({
      ...state,
      subscription: {
        ...subscription,
        counters: preserveQueue
          ? {
              ...counters,
              queuedRecords: state.subscription.counters.queuedRecords,
              queuedBytes: state.subscription.counters.queuedBytes,
            }
          : counters,
      },
      records: window.records,
      viewerOmittedRecords: window.evictedRecords,
      retainedBytes: window.bytes,
      selectedRecord: same ? state.selectedRecord : null,
      selectionNotice: same ? state.selectionNotice : null,
    });
  }
  function receive(event: NatsEvent): void {
    if (disposed) return;
    if (event.event === "backend.availability") {
      const recovered = !state.available && event.payload.state === "ready";
      publish({
        ...state,
        available: event.payload.state === "ready",
        failure:
          event.payload.state === "unavailable"
            ? { summary: "The NATS host is unavailable.", recovery: event.payload.recovery }
            : state.failure,
      });
      if (recovered && bootstrapped && isInteractive()) void refresh();
      return;
    }
    if (event.event === "records.batch") {
      if (
        event.payload.generation !== state.subscription.generation ||
        !["loading", "streaming", "stopping", "stopped", "failed"].includes(
          state.subscription.state,
        )
      )
        return;
      batchOrdinal += 1;
      window = appendNatsRecords(window, event.payload.records);
      const terminal = ["stopped", "failed"].includes(state.subscription.state);
      const counters = mergeNatsCounters(state.subscription.counters, event.payload.counters);
      const retainedSelection =
        state.selectedRecord === null ||
        window.records.some((record) => record.id === state.selectedRecord?.id);
      publish({
        ...state,
        records: window.records,
        viewerOmittedRecords: window.evictedRecords,
        retainedBytes: window.bytes,
        selectedRecord: retainedSelection ? state.selectedRecord : null,
        selectionNotice: retainedSelection
          ? state.selectionNotice
          : "The selected record left the live window. Select a retained record to inspect.",
        subscription: {
          ...state.subscription,
          counters: terminal
            ? {
                ...counters,
                queuedRecords: state.subscription.counters.queuedRecords,
                queuedBytes: state.subscription.counters.queuedBytes,
              }
            : counters,
        },
      });
      return;
    }
    switch (event.event) {
      case "profiles.changed":
        applyProfiles(event.payload);
        break;
      case "connection.state":
        applyConnection(event.payload);
        break;
      case "subscription.changed":
        applySubscription(event.payload);
        break;
    }
  }
  function applyReceipt(response: NatsCommandResponse, admittedBatchOrdinal: number): void {
    if (!response.ok) return;
    const subscription = (snapshot: NatsSubscriptionSnapshot): void =>
      applySubscription(
        snapshot,
        admittedBatchOrdinal !== batchOrdinal &&
          snapshot.revision === state.subscription.revision &&
          snapshot.generation === state.subscription.generation,
      );
    switch (response.command) {
      case "profiles.list":
        applyProfiles(response.result.profiles);
        applyConnection(response.result.connection);
        subscription(response.result.subscription);
        break;
      case "profiles.create":
      case "profiles.update":
      case "profiles.delete":
        applyProfiles(response.result.profiles);
        break;
      case "profiles.connect":
        applyConnection(response.result.connection);
        break;
      case "connection.disconnect":
        applyConnection(response.result.connection);
        subscription(response.result.subscription);
        break;
      case "subscription.start":
      case "subscription.stop":
        subscription(response.result.subscription);
        break;
    }
  }
  async function execute(command: NatsCommand): Promise<boolean> {
    if (disposed || !isInteractive() || (!state.available && command.command !== "profiles.list"))
      return false;
    const ordinal = ++commandOrdinal;
    const admittedBatchOrdinal = batchOrdinal;
    const readOrdinal = command.command === "profiles.list" ? ++listOrdinal : null;
    pending.set(command.id, command.command);
    publish({
      ...state,
      pending: [...pending.values()],
      failure: null,
      loading: readOrdinal === null ? state.loading : true,
    });
    try {
      const response = await host.execute(command);
      if (!disposed) {
        applyReceipt(response, admittedBatchOrdinal);
        if (!response.ok && ordinal === commandOrdinal)
          publish({ ...state, failure: response.error });
      }
      // An admitted write keeps its actual receipt even when its view is retired.
      return response.ok;
    } catch {
      if (!disposed && ordinal === commandOrdinal && isInteractive())
        publish({
          ...state,
          failure: {
            summary: "The NATS request could not be completed.",
            recovery: "Refresh the host state before retrying the operation.",
          },
        });
      return false;
    } finally {
      pending.delete(command.id);
      if (!disposed)
        publish({
          ...state,
          pending: [...pending.values()],
          loading: readOrdinal !== null && readOrdinal === listOrdinal ? false : state.loading,
        });
    }
  }
  function identity(): { readonly version: typeof NATS_PROTOCOL_VERSION; readonly id: string } {
    return { version: NATS_PROTOCOL_VERSION, id: globalThis.crypto.randomUUID() };
  }
  function refresh(): Promise<boolean> {
    return execute({ ...identity(), command: "profiles.list", payload: {} });
  }
  return {
    snapshot: (): NatsWorkspaceSnapshot => state,
    subscribe: (listener): (() => void) => {
      if (disposed) return (): void => undefined;
      listeners.add(listener);
      return (): void => {
        listeners.delete(listener);
      };
    },
    start: (): void => {
      if (started || disposed) return;
      started = true;
      try {
        removeListener = host.subscribe(receive);
        bootstrapped = true;
        void refresh();
      } catch {
        publish({
          ...state,
          available: false,
          loading: false,
          failure: {
            summary: "The NATS host could not be observed.",
            recovery: "Reload StreamSkope before reconnecting.",
          },
        });
      }
    },
    dispose: (): void => {
      if (disposed) return;
      disposed = true;
      try {
        removeListener?.();
      } finally {
        listeners.clear();
      }
    },
    refresh,
    createProfile: (profile): Promise<boolean> =>
      execute({ ...identity(), command: "profiles.create", payload: { profile } }),
    updateProfile: (profile, input): Promise<boolean> =>
      execute({
        ...identity(),
        command: "profiles.update",
        payload: { profileId: profile.id, expectedRevision: profile.revision, profile: input },
      }),
    deleteProfile: (profile): Promise<boolean> =>
      execute({
        ...identity(),
        command: "profiles.delete",
        payload: { profileId: profile.id, expectedRevision: profile.revision },
      }),
    connectProfile: (profile): Promise<boolean> =>
      execute({
        ...identity(),
        command: "profiles.connect",
        payload: { profileId: profile.id, expectedRevision: profile.revision },
      }),
    disconnect: (): Promise<boolean> =>
      execute({ ...identity(), command: "connection.disconnect", payload: {} }),
    startSubscription: (subject): Promise<boolean> =>
      execute({ ...identity(), command: "subscription.start", payload: { subject } }),
    stopSubscription: (): Promise<boolean> =>
      execute({ ...identity(), command: "subscription.stop", payload: {} }),
    selectRecord: (id): void => {
      if (!disposed && isInteractive())
        publish({
          ...state,
          selectedRecord: state.records.find((record) => record.id === id) ?? null,
          selectionNotice: null,
        });
    },
    clearFailure: (): void => {
      if (!disposed && isInteractive()) publish({ ...state, failure: null });
    },
  };
}
