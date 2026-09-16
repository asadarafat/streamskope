import {
  HOST_ACTIVITY_HISTORY_LIMIT,
  KAFKA_MESSAGE_LIMITS,
  KAFKA_RULE_NOTIFICATION_LIMITS,
  KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
  kafkaMessageRetainedBytes,
  type ActivityEntry,
  type KafkaAclSnapshot,
  type BackendAvailability,
  type ConnectionState,
  type ConnectionTemplateSnapshot,
  type TrustAcquisitionRecipeSnapshot,
  type ConsumptionState,
  type HostError,
  type HostEvent,
  type KafkaClusterDiagnosticsSnapshot,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupInventorySnapshot,
  type KafkaExploredMessage,
  type KafkaFetchRequest,
  type KafkaLiveRuleCapability,
  type KafkaLatencySnapshot,
  type KafkaLatencyHistorySnapshot,
  type KafkaOperationalPreferenceSnapshot,
  type KafkaRuleNotification,
  type KafkaRuleSeverity,
  type SchemaCompatibilitySnapshot,
  type SchemaRegistryDetailSnapshot,
  type SchemaRegistryInventorySnapshot,
  type RedpandaTransformDetailSnapshot,
  type RedpandaTransformInventorySnapshot,
  type RedpandaTransformLogsSnapshot,
  type KafkaStreamMonitorSnapshot,
  type KafkaTopicConfigurationHistorySnapshot,
  type KafkaTopicConfigurationSnapshot,
  type ProfileStoreCapability,
  type ProfileSummary,
  type TopicListState,
} from "../contracts";

import {
  initialKafkaRuleUiState,
  reduceKafkaRuleUiState,
  type KafkaRuleUiAction,
  type KafkaRuleUiState,
} from "./rule-state";
import {
  initialKafkaMessageFilters,
  selectFilteredKafkaMessages,
  withKafkaMessageTextFilter,
  type KafkaMessageFilters,
  type KafkaMessageTextFilterField,
} from "./message-operations";

export interface KafkaUiState {
  readonly aclSnapshot: KafkaAclSnapshot;
  readonly activities: readonly ActivityEntry[];
  readonly backend: "checking" | BackendAvailability;
  readonly connectionName: string | null;
  readonly connectionState: ConnectionState;
  readonly clusterDiagnostics: KafkaClusterDiagnosticsSnapshot;
  readonly consumerGroupDetail: KafkaConsumerGroupDetailSnapshot;
  readonly consumerGroupInventory: KafkaConsumerGroupInventorySnapshot;
  readonly consumptionError: HostError | null;
  readonly consumptionRequest: KafkaFetchRequest | null;
  readonly consumptionState: ConsumptionState;
  readonly droppedMessages: number;
  readonly hostDroppedMessages: number;
  readonly lastSequence: number;
  readonly liveRuleCapability: KafkaLiveRuleCapability;
  readonly latency: KafkaLatencySnapshot;
  readonly latencyHistory: KafkaLatencyHistorySnapshot;
  readonly messages: readonly KafkaExploredMessage[];
  readonly messageFilters: KafkaMessageFilters;
  readonly messagesStale: boolean;
  readonly profiles: readonly ProfileSummary[];
  readonly profileStore: ProfileStoreCapability | null;
  readonly preferenceSnapshot: KafkaOperationalPreferenceSnapshot | null;
  readonly refreshedAt: string | null;
  readonly receivedMessages: number;
  readonly rendererDroppedMessages: number;
  readonly rendererWindowEvictions: number;
  readonly retainedMessageBytes: number;
  readonly ruleNotifications: readonly KafkaRuleNotificationUiEntry[];
  readonly ruleState: KafkaRuleUiState;
  readonly schemaCompatibility: SchemaCompatibilitySnapshot | null;
  readonly schemaDetail: SchemaRegistryDetailSnapshot;
  readonly schemaInventory: SchemaRegistryInventorySnapshot;
  readonly streamMonitor: KafkaStreamMonitorUiState;
  readonly templateSnapshot: ConnectionTemplateSnapshot | null;
  readonly recipeSnapshot: TrustAcquisitionRecipeSnapshot | null;
  readonly topicError: HostError | null;
  readonly topicListState: "unavailable" | TopicListState;
  readonly topics: readonly string[];
  readonly topicConfiguration: KafkaTopicConfigurationSnapshot;
  readonly topicConfigurationHistory: KafkaTopicConfigurationHistorySnapshot | null;
  readonly transformDetail: RedpandaTransformDetailSnapshot;
  readonly transformInventory: RedpandaTransformInventorySnapshot;
  readonly transformLogs: RedpandaTransformLogsSnapshot;
}

export interface KafkaRuleNotificationUiEntry extends KafkaRuleNotification {
  readonly sequence: number;
}

export interface KafkaStreamMonitorUiState {
  readonly current: KafkaStreamMonitorSnapshot;
  readonly history: readonly KafkaStreamMonitorSnapshot[];
}

const unavailableTopicConfiguration: KafkaTopicConfigurationSnapshot = {
  connectionName: null,
  entries: [],
  refreshedAt: null,
  state: "unavailable",
  topic: null,
};

const unavailableAclSnapshot: KafkaAclSnapshot = {
  acls: [],
  connectionName: null,
  omittedAcls: 0,
  refreshedAt: null,
  state: "unavailable",
};

const unavailableClusterDiagnostics: KafkaClusterDiagnosticsSnapshot = {
  cluster: null,
  endpoint: null,
  fetchedAt: null,
  profile: null,
  state: "unavailable",
};

const unavailableConsumerGroupInventory: KafkaConsumerGroupInventorySnapshot = {
  connectionName: null,
  groups: [],
  omittedGroups: 0,
  refreshedAt: null,
  state: "unavailable",
};

const unavailableConsumerGroupDetail: KafkaConsumerGroupDetailSnapshot = {
  connectionName: null,
  group: null,
  groupId: null,
  refreshedAt: null,
  state: "unavailable",
};

const unavailableLatency: KafkaLatencySnapshot = {
  evidence: null,
  request: null,
  state: "unavailable",
};

const unavailableStreamMonitorSnapshot: KafkaStreamMonitorSnapshot = {
  connectionName: null,
  delivery: null,
  queue: null,
  request: null,
  sampledAt: null,
  state: "unavailable",
  status: "unavailable",
};

const unavailableStreamMonitor: KafkaStreamMonitorUiState = {
  current: unavailableStreamMonitorSnapshot,
  history: [],
};

const unavailableSchemaInventory: SchemaRegistryInventorySnapshot = {
  connectionName: null,
  endpoint: null,
  omittedSubjects: 0,
  refreshedAt: null,
  state: "unavailable",
  subjects: [],
};

const unavailableSchemaDetail: SchemaRegistryDetailSnapshot = {
  compatibilityLevel: null,
  connectionName: null,
  endpoint: null,
  refreshedAt: null,
  schema: null,
  state: "unavailable",
  subject: null,
  versions: [],
};

const unavailableTransformInventory: RedpandaTransformInventorySnapshot = {
  connectionName: null,
  endpoint: null,
  omittedTransforms: 0,
  refreshedAt: null,
  state: "unavailable",
  transforms: [],
};
const unavailableTransformDetail: RedpandaTransformDetailSnapshot = {
  connectionName: null,
  endpoint: null,
  refreshedAt: null,
  state: "unavailable",
  transform: null,
  transformName: null,
};
const unavailableTransformLogs: RedpandaTransformLogsSnapshot = {
  connectionName: null,
  logs: [],
  omittedLogs: 0,
  refreshedAt: null,
  state: "unavailable",
  transformName: null,
};

export const initialKafkaUiState: KafkaUiState = {
  aclSnapshot: unavailableAclSnapshot,
  activities: [],
  backend: "checking",
  connectionName: null,
  connectionState: "disconnected",
  clusterDiagnostics: unavailableClusterDiagnostics,
  consumerGroupDetail: unavailableConsumerGroupDetail,
  consumerGroupInventory: unavailableConsumerGroupInventory,
  consumptionError: null,
  consumptionRequest: null,
  consumptionState: "unavailable",
  droppedMessages: 0,
  hostDroppedMessages: 0,
  lastSequence: -1,
  liveRuleCapability: {
    applicableRules: 0,
    omittedRules: 0,
    state: "idle",
  },
  latency: unavailableLatency,
  latencyHistory: {
    connectionName: null,
    entries: [],
  },
  messages: [],
  messageFilters: initialKafkaMessageFilters,
  messagesStale: false,
  profiles: [],
  profileStore: null,
  preferenceSnapshot: null,
  refreshedAt: null,
  receivedMessages: 0,
  rendererDroppedMessages: 0,
  rendererWindowEvictions: 0,
  retainedMessageBytes: 0,
  ruleNotifications: [],
  ruleState: initialKafkaRuleUiState,
  schemaCompatibility: null,
  schemaDetail: unavailableSchemaDetail,
  schemaInventory: unavailableSchemaInventory,
  streamMonitor: unavailableStreamMonitor,
  templateSnapshot: null,
  recipeSnapshot: null,
  topicError: null,
  topicListState: "unavailable",
  topics: [],
  topicConfiguration: unavailableTopicConfiguration,
  topicConfigurationHistory: null,
  transformDetail: unavailableTransformDetail,
  transformInventory: unavailableTransformInventory,
  transformLogs: unavailableTransformLogs,
};

function sameFetchRequest(
  left: KafkaFetchRequest | null,
  right: KafkaFetchRequest | null,
): boolean {
  if (left === null || right === null) {
    return left === right;
  }
  if (
    left.topic !== right.topic ||
    left.mode !== right.mode ||
    left.maxMessages !== right.maxMessages
  ) {
    return false;
  }
  return (
    left.mode !== "time-window" ||
    (right.mode === "time-window" &&
      left.startTimeMs === right.startTimeMs &&
      left.endTimeMs === right.endTimeMs)
  );
}

function sameStreamMonitorOwner(
  left: KafkaStreamMonitorSnapshot,
  right: KafkaStreamMonitorSnapshot,
): boolean {
  return (
    left.connectionName === right.connectionName && sameFetchRequest(left.request, right.request)
  );
}

function reduceStreamMonitorSnapshot(
  current: KafkaStreamMonitorUiState,
  snapshot: KafkaStreamMonitorSnapshot,
): KafkaStreamMonitorUiState {
  if (snapshot.state === "unavailable") {
    return unavailableStreamMonitor;
  }
  const historyLimit = Math.min(
    snapshot.delivery?.historySamples ?? KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
    KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
  );
  const history =
    snapshot.state !== "loading" && sameStreamMonitorOwner(current.current, snapshot)
      ? [...current.history, snapshot].slice(-historyLimit)
      : [snapshot];
  return { current: snapshot, history };
}

function staleStreamMonitor(current: KafkaStreamMonitorUiState): KafkaStreamMonitorUiState {
  return current.current.state === "unavailable"
    ? current
    : {
        ...current,
        current: {
          ...current.current,
          state: "stale",
          status: "stale",
        },
      };
}

export type KafkaUiAction =
  | {
      readonly event: HostEvent;
      readonly type: "host.event";
    }
  | {
      readonly action: KafkaRuleUiAction;
      readonly type: "rule.action";
    }
  | {
      readonly activeOnly: boolean;
      readonly type: "messages.rule-filter.changed";
    }
  | {
      readonly field: KafkaMessageTextFilterField;
      readonly type: "messages.filter.text.changed";
      readonly value: string;
    }
  | {
      readonly partition: number | null;
      readonly type: "messages.filter.partition.changed";
    }
  | {
      readonly type: "messages.filters.cleared";
    }
  | {
      readonly sequence: number;
      readonly type: "rules.notification.dismissed";
    };

export function reduceKafkaUiState(state: KafkaUiState, action: KafkaUiAction): KafkaUiState {
  switch (action.type) {
    case "host.event":
      return reduceKafkaHostEvent(state, action.event);
    case "messages.filters.cleared":
      return state.messageFilters === initialKafkaMessageFilters
        ? state
        : { ...state, messageFilters: initialKafkaMessageFilters };
    case "messages.filter.partition.changed":
      if (
        action.partition !== null &&
        (!Number.isSafeInteger(action.partition) || action.partition < 0)
      ) {
        return state;
      }
      return action.partition === state.messageFilters.partition
        ? state
        : {
            ...state,
            messageFilters: { ...state.messageFilters, partition: action.partition },
          };
    case "messages.filter.text.changed": {
      const messageFilters = withKafkaMessageTextFilter(
        state.messageFilters,
        action.field,
        action.value,
      );
      return messageFilters === state.messageFilters ? state : { ...state, messageFilters };
    }
    case "messages.rule-filter.changed":
      return action.activeOnly === state.messageFilters.activeRuleMatchesOnly
        ? state
        : {
            ...state,
            messageFilters: {
              ...state.messageFilters,
              activeRuleMatchesOnly: action.activeOnly,
            },
          };
    case "rule.action":
      return {
        ...state,
        ruleState: reduceKafkaRuleUiState(state.ruleState, action.action),
      };
    case "rules.notification.dismissed": {
      const ruleNotifications = state.ruleNotifications.filter(
        (notification) => notification.sequence !== action.sequence,
      );
      return ruleNotifications.length === state.ruleNotifications.length
        ? state
        : { ...state, ruleNotifications };
    }
  }
}

export function highestKafkaRuleSeverity(message: KafkaExploredMessage): KafkaRuleSeverity | null {
  return message.ruleEvaluation.highestActiveSeverity ?? null;
}

export function selectVisibleKafkaMessages(
  state: Pick<KafkaUiState, "messageFilters" | "messages">,
): readonly KafkaExploredMessage[] {
  return selectFilteredKafkaMessages(state.messages, state.messageFilters);
}

export function selectKafkaMessageById(
  messages: readonly KafkaExploredMessage[],
  id: string | null,
): KafkaExploredMessage | null {
  return id === null ? null : (messages.find((message) => message.id === id) ?? null);
}

function compareKafkaMessageOffsets(left: string, right: string): number {
  if (/^-?\d+$/u.test(left) && /^-?\d+$/u.test(right)) {
    const leftOffset = BigInt(left);
    const rightOffset = BigInt(right);
    return leftOffset < rightOffset ? -1 : leftOffset > rightOffset ? 1 : 0;
  }
  return left.localeCompare(right);
}

function compareKafkaMessages(left: KafkaExploredMessage, right: KafkaExploredMessage): number {
  const timestamp = left.timestamp.localeCompare(right.timestamp);
  if (timestamp !== 0) {
    return timestamp;
  }
  const partition = left.partition - right.partition;
  if (partition !== 0) {
    return partition;
  }
  const offset = compareKafkaMessageOffsets(left.offset, right.offset);
  return offset === 0 ? left.id.localeCompare(right.id) : offset;
}

function orderKafkaMessages(
  messages: readonly KafkaExploredMessage[],
  request: KafkaFetchRequest | null,
): KafkaExploredMessage[] {
  if (request === null) {
    return [...messages];
  }
  const direction = request.mode === "tail" || request.mode === "newest" ? -1 : 1;
  return [...messages].sort((left, right) => direction * compareKafkaMessages(left, right));
}

function mergeKafkaMessages(
  retained: readonly KafkaExploredMessage[],
  incoming: readonly KafkaExploredMessage[],
  request: KafkaFetchRequest | null,
): KafkaExploredMessage[] {
  if (request === null) return [...retained, ...incoming];
  const sorted = orderKafkaMessages(incoming, request);
  const direction = request.mode === "tail" || request.mode === "newest" ? -1 : 1;
  const merged: KafkaExploredMessage[] = [];
  let previousIndex = 0;
  let incomingIndex = 0;
  while (previousIndex < retained.length || incomingIndex < sorted.length) {
    const previous = retained[previousIndex];
    const next = sorted[incomingIndex];
    if (
      previous !== undefined &&
      (next === undefined || direction * compareKafkaMessages(previous, next) <= 0)
    ) {
      merged.push(previous);
      previousIndex += 1;
    } else if (next !== undefined) {
      merged.push(next);
      incomingIndex += 1;
    }
  }
  return merged;
}

export function reduceKafkaHostEvent(state: KafkaUiState, event: HostEvent): KafkaUiState {
  if (event.sequence <= state.lastSequence) {
    return state;
  }
  const sequencedState = {
    ...state,
    lastSequence: event.sequence,
    ruleState: reduceKafkaRuleUiState(state.ruleState, {
      event,
      type: "host.event",
    }),
  };

  switch (event.event) {
    case "activity.recorded":
      return {
        ...sequencedState,
        activities: [...state.activities, event.payload].slice(-HOST_ACTIVITY_HISTORY_LIMIT),
      };
    case "backend.availability":
      return {
        ...sequencedState,
        backend: event.payload.state,
        messagesStale: event.payload.state === "unavailable" && state.messages.length > 0,
        streamMonitor:
          event.payload.state === "unavailable"
            ? staleStreamMonitor(state.streamMonitor)
            : state.streamMonitor,
      };
    case "connection.state":
      return event.payload.state !== "connected" ||
        event.payload.connectionName !== state.connectionName
        ? {
            ...sequencedState,
            connectionName: event.payload.connectionName,
            connectionState: event.payload.state,
            clusterDiagnostics: unavailableClusterDiagnostics,
            aclSnapshot: unavailableAclSnapshot,
            consumerGroupDetail: unavailableConsumerGroupDetail,
            consumerGroupInventory: unavailableConsumerGroupInventory,
            consumptionError: null,
            consumptionState: "unavailable",
            liveRuleCapability: initialKafkaUiState.liveRuleCapability,
            latency: unavailableLatency,
            latencyHistory: initialKafkaUiState.latencyHistory,
            messagesStale: state.messages.length > 0,
            refreshedAt: null,
            ruleNotifications: [],
            schemaCompatibility: null,
            schemaDetail: unavailableSchemaDetail,
            schemaInventory: unavailableSchemaInventory,
            topicError: null,
            topicListState: "unavailable",
            topics: [],
            topicConfiguration: unavailableTopicConfiguration,
            topicConfigurationHistory: null,
            transformDetail: unavailableTransformDetail,
            transformInventory: unavailableTransformInventory,
            transformLogs: unavailableTransformLogs,
            streamMonitor:
              event.payload.state === "connected"
                ? unavailableStreamMonitor
                : staleStreamMonitor(state.streamMonitor),
          }
        : {
            ...sequencedState,
            connectionName: event.payload.connectionName,
            connectionState: event.payload.state,
          };
    case "consumption.state": {
      const resetMessages = event.payload.state === "loading";
      return {
        ...sequencedState,
        consumptionError: event.payload.error ?? null,
        consumptionRequest: event.payload.request,
        consumptionState: event.payload.state,
        droppedMessages: resetMessages
          ? event.payload.droppedMessages
          : event.payload.droppedMessages + state.rendererDroppedMessages,
        hostDroppedMessages: event.payload.droppedMessages,
        liveRuleCapability: event.payload.ruleEvaluation,
        messages: resetMessages ? [] : state.messages,
        messagesStale: false,
        receivedMessages: event.payload.receivedMessages,
        rendererDroppedMessages: resetMessages ? 0 : state.rendererDroppedMessages,
        rendererWindowEvictions: resetMessages ? 0 : state.rendererWindowEvictions,
        retainedMessageBytes: resetMessages ? 0 : state.retainedMessageBytes,
      };
    }
    case "consumerGroups.changed":
      return {
        ...sequencedState,
        consumerGroupInventory: event.payload,
      };
    case "consumerGroup.changed":
      return {
        ...sequencedState,
        consumerGroupDetail: event.payload,
      };
    case "clusterDetails.changed":
      return {
        ...sequencedState,
        clusterDiagnostics: event.payload,
      };
    case "latency.changed":
      return {
        ...sequencedState,
        latency: event.payload,
      };
    case "latency.history.changed":
      return {
        ...sequencedState,
        latencyHistory: event.payload,
      };
    case "messages.batch": {
      const currentTopic = state.consumptionRequest?.topic ?? state.messages[0]?.topic ?? null;
      const retainExisting = currentTopic === event.payload.topic && !state.messagesStale;
      const request =
        state.consumptionRequest?.topic === event.payload.topic ? state.consumptionRequest : null;
      const ordered = mergeKafkaMessages(
        retainExisting ? state.messages : [],
        event.payload.messages,
        request,
      );
      let retainedBytes = event.payload.messages.reduce(
        (bytes, message) => bytes + kafkaMessageRetainedBytes(message),
        retainExisting ? state.retainedMessageBytes : 0,
      );
      const limit = Math.min(
        request?.maxMessages ?? KAFKA_MESSAGE_LIMITS.retainedMessages,
        KAFKA_MESSAGE_LIMITS.retainedMessages,
      );
      let start = 0;
      let end = ordered.length;
      while (end - start > limit || retainedBytes > KAFKA_MESSAGE_LIMITS.retainedBytes) {
        const evicted = request === null ? ordered[start++] : ordered[--end];
        if (evicted === undefined) {
          break;
        }
        retainedBytes -= kafkaMessageRetainedBytes(evicted);
      }
      const messages = start === 0 && end === ordered.length ? ordered : ordered.slice(start, end);
      const rendererEvictions = ordered.length - messages.length;
      const selectionEvictions =
        request === null ? 0 : Math.max(0, ordered.length - request.maxMessages);
      const rendererDroppedMessages =
        (retainExisting ? state.rendererDroppedMessages : 0) +
        rendererEvictions -
        selectionEvictions;
      return {
        ...sequencedState,
        consumptionError: null,
        droppedMessages: event.payload.droppedMessages + rendererDroppedMessages,
        hostDroppedMessages: event.payload.droppedMessages,
        messages,
        messagesStale: false,
        rendererDroppedMessages,
        rendererWindowEvictions:
          (retainExisting ? state.rendererWindowEvictions : 0) + rendererEvictions,
        retainedMessageBytes: retainedBytes,
      };
    }
    case "profiles.changed":
      return {
        ...sequencedState,
        profiles: event.payload.profiles,
        profileStore: event.payload.store,
      };
    case "preferences.changed":
      return {
        ...sequencedState,
        preferenceSnapshot: event.payload,
      };
    case "rules.notification":
      return {
        ...sequencedState,
        ruleNotifications: [
          ...state.ruleNotifications,
          { ...event.payload, sequence: event.sequence },
        ].slice(-KAFKA_RULE_NOTIFICATION_LIMITS.rendererQueue),
      };
    case "templates.changed":
      return {
        ...sequencedState,
        templateSnapshot: event.payload,
      };
    case "recipes.changed":
      return { ...sequencedState, recipeSnapshot: event.payload };
    case "topicConfiguration.changed":
      return {
        ...sequencedState,
        topicConfiguration: event.payload,
      };
    case "topicConfiguration.history":
      return {
        ...sequencedState,
        topicConfigurationHistory: event.payload,
      };
    case "rules.changed":
    case "rules.evaluation":
      return sequencedState;
    case "schemas.changed":
      return {
        ...sequencedState,
        schemaInventory: event.payload,
      };
    case "schema.changed":
      return {
        ...sequencedState,
        schemaDetail: event.payload,
      };
    case "schemaCompatibility.changed":
      return {
        ...sequencedState,
        schemaCompatibility: event.payload,
      };
    case "acls.changed":
      return {
        ...sequencedState,
        aclSnapshot: event.payload,
      };
    case "transforms.changed":
      return { ...sequencedState, transformInventory: event.payload };
    case "transform.changed":
      return { ...sequencedState, transformDetail: event.payload };
    case "transformLogs.changed":
      return { ...sequencedState, transformLogs: event.payload };
    case "streamMetrics.changed":
      return {
        ...sequencedState,
        streamMonitor: reduceStreamMonitorSnapshot(state.streamMonitor, event.payload),
      };
    case "topics.changed":
      return {
        ...sequencedState,
        refreshedAt: event.payload.refreshedAt,
        topicError: event.payload.error ?? null,
        topicListState: event.payload.state,
        topics: event.payload.topics,
      };
  }
}

export function connectionStateLabel(state: ConnectionState): string {
  switch (state) {
    case "connected":
      return "Connected";
    case "connecting":
      return "Connecting";
    case "disconnecting":
      return "Disconnecting";
    case "failed":
      return "Connection failed";
    case "disconnected":
      return "Disconnected";
  }
}
