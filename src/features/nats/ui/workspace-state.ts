import type {
  NatsCommandName,
  NatsConnectionSnapshot,
  NatsProfilesSnapshot,
  NatsRecord,
  NatsSubscriptionCounters,
  NatsSubscriptionSnapshot,
} from "../contracts";

export interface NatsWorkspaceFailure {
  readonly summary: string;
  readonly recovery?: string;
  readonly correlationId?: string;
}
export interface NatsWorkspaceSnapshot {
  readonly profiles: NatsProfilesSnapshot | null;
  readonly connection: NatsConnectionSnapshot;
  readonly subscription: NatsSubscriptionSnapshot;
  readonly records: readonly NatsRecord[];
  readonly selectedRecord: NatsRecord | null;
  readonly viewerOmittedRecords: number;
  readonly retainedBytes: number;
  readonly selectionNotice: string | null;
  readonly available: boolean;
  readonly loading: boolean;
  readonly pending: readonly NatsCommandName[];
  readonly failure: NatsWorkspaceFailure | null;
}
export function initialNatsWorkspaceSnapshot(recovery?: string): NatsWorkspaceSnapshot {
  return {
    profiles: null,
    connection: { revision: 0, state: "disconnected", profile: null },
    subscription: {
      revision: 0,
      state: "idle",
      generation: null,
      subject: null,
      counters: {
        receivedRecords: 0,
        applicationOmittedRecords: 0,
        publishedRecords: 0,
        queuedRecords: 0,
        queuedBytes: 0,
        transportOmittedRecords: 0,
      },
    },
    records: [],
    selectedRecord: null,
    viewerOmittedRecords: 0,
    retainedBytes: 0,
    selectionNotice: null,
    available: recovery === undefined,
    loading: recovery === undefined,
    pending: [],
    failure: recovery === undefined ? null : { summary: "The NATS host is unavailable.", recovery },
  };
}
/** List replies are global; per-client omission evidence must never move backwards. */
export function mergeNatsCounters(
  previous: NatsSubscriptionCounters,
  incoming: NatsSubscriptionCounters,
): NatsSubscriptionCounters {
  return {
    ...incoming,
    receivedRecords: Math.max(previous.receivedRecords, incoming.receivedRecords),
    applicationOmittedRecords: Math.max(
      previous.applicationOmittedRecords,
      incoming.applicationOmittedRecords,
    ),
    publishedRecords: Math.max(previous.publishedRecords, incoming.publishedRecords),
    transportOmittedRecords: Math.max(
      previous.transportOmittedRecords,
      incoming.transportOmittedRecords,
    ),
  };
}
