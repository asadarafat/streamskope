import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  KAFKA_FETCH_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  parseKafkaInvestigationQuery,
  type KafkaSearchFilter,
  type KafkaFetchMode,
  type KafkaFetchRequest,
  type StreamSkopeHost,
} from "../contracts";

import type { KafkaUiState } from "./state";
import { countActiveKafkaMessageFilters } from "./message-operations";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";
import type { NavigationView } from "./workbench-navigation";
import { isKafkaConsumptionActive } from "./workbench-status";
import { useWorkbenchMessageSelection } from "./workbench-runtime-effects";
import {
  initialKafkaTimeWindow,
  kafkaTimeWindowError,
  resolveKafkaTimeWindow,
} from "./query-time-window";
import type { QueryTimeWindowControlsProps } from "./QueryTimeWindowControls";

type TopicWorkbenchState = Pick<
  KafkaUiState,
  | "connectionName"
  | "consumptionState"
  | "consumptionRequest"
  | "preferenceSnapshot"
  | "topicListState"
  | "topics"
  | "messageFilters"
  | "messages"
>;

interface WorkbenchTopicController {
  readonly fetchMaximum: number;
  readonly fetchMode: KafkaFetchMode;
  readonly timeWindow: QueryTimeWindowControlsProps;
  readonly messageRequestError: string | undefined;
  readonly consumptionStopping: boolean;
  readonly selectedMessageId: string | null;
  readonly selectedTopic: string | null;
  readonly selectionNotice: string | undefined;
  readonly topicFilter: string;
  readonly topicRequestError: string | undefined;
  readonly topicWorkspace: TopicWorkspaceView;
  readonly messageSelection: ReturnType<typeof useWorkbenchMessageSelection>;
  readonly visibleMessages: KafkaUiState["messages"];
  readonly rendererPresentationActive: boolean;
  readonly setFetchMaximum: (value: number) => void;
  readonly setFetchMode: (value: KafkaFetchMode) => void;
  readonly setSelectedMessageId: (value: string | null) => void;
  readonly setSelectedTopic: (value: string | null) => void;
  readonly setSelectionNotice: (value: string | undefined) => void;
  readonly setTopicFilter: (value: string) => void;
  readonly setTopicWorkspace: (value: TopicWorkspaceView) => void;
  readonly requestTopics: () => Promise<void>;
  readonly startConsumption: (topic: string, search?: KafkaSearchFilter) => Promise<void>;
  readonly activateTopic: (topic: string) => void;
  readonly stopConsumption: () => Promise<void>;
}

export function useWorkbenchTopics(
  host: StreamSkopeHost,
  connected: boolean,
  navigation: NavigationView,
  setNavigation: (navigation: NavigationView) => void,
  state: TopicWorkbenchState,
): WorkbenchTopicController {
  const [fetchMaximum, setFetchMaximum] = useState<number>(KAFKA_FETCH_LIMITS.defaultMaxMessages);
  const [fetchMode, setFetchMode] = useState<KafkaFetchMode>("tail");
  const [timeWindowDraft, setTimeWindowDraft] = useState(initialKafkaTimeWindow);
  const appliedFetchDefaults = useRef<{
    readonly mode: KafkaFetchMode;
    readonly maxMessages: number;
  }>(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.fetch);
  const [messageRequestError, setMessageRequestError] = useState<string>();
  const [consumptionStopping, setConsumptionStopping] = useState(false);
  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const [selectionNotice, setSelectionNotice] = useState<string>();
  const [topicFilter, setTopicFilter] = useState("");
  const [topicRequestError, setTopicRequestError] = useState<string>();
  const [topicWorkspace, setTopicWorkspace] = useState<TopicWorkspaceView>("messages");
  const messagePresentationActive =
    navigation === "topics" && selectedTopic !== null && topicWorkspace === "messages";
  const rendererPresentationActive =
    navigation === "topics" && (messagePresentationActive || topicWorkspace === "monitor");

  const messageSelection = useWorkbenchMessageSelection(
    messagePresentationActive,
    state.messageFilters,
    state.messages,
  );
  const visibleMessages = messageSelection.messages;

  const consumptionActive = isKafkaConsumptionActive(
    state.consumptionState,
    state.consumptionRequest,
  );
  const confirmedFetchDefaults =
    state.preferenceSnapshot?.preferences.fetch ?? KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.fetch;
  const requestTopics = useCallback(async (): Promise<void> => {
    setTopicRequestError(undefined);
    try {
      await host.execute({
        command: "topics.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    } catch {
      setTopicRequestError(
        "The application host did not accept the request. Open Activity for diagnostics.",
      );
    }
  }, [host]);

  useEffect(() => {
    if (connected) {
      setNavigation("topics");
      setSelectedTopic(null);
      setTopicWorkspace("messages");
      void requestTopics();
    }
  }, [connected, requestTopics, setNavigation, state.connectionName]);

  useEffect(() => {
    setTopicFilter("");
  }, [state.connectionName]);

  useEffect(() => {
    if (
      connected &&
      state.topicListState === "ready" &&
      selectedTopic !== null &&
      !state.topics.includes(selectedTopic)
    ) {
      setSelectedTopic(null);
      setTopicWorkspace("messages");
    }
  }, [connected, selectedTopic, state.topicListState, state.topics]);

  useEffect(() => {
    if (selectedMessageId === null) {
      return;
    }
    if (!state.messages.some((message) => message.id === selectedMessageId)) {
      setSelectedMessageId(null);
      setSelectionNotice("The selected message is no longer retained.");
      return;
    }
    if (!visibleMessages.some((message) => message.id === selectedMessageId)) {
      setSelectionNotice(
        state.messageFilters.activeRuleMatchesOnly &&
          countActiveKafkaMessageFilters(state.messageFilters) === 1
          ? "The selected message is hidden by the rule filter."
          : "The selected message is hidden by the current filters.",
      );
      return;
    }
    setSelectionNotice(undefined);
  }, [selectedMessageId, state.messageFilters, state.messages, visibleMessages]);

  useEffect(() => {
    setSelectedMessageId(null);
    setSelectionNotice(undefined);
  }, [selectedTopic]);

  useEffect(() => {
    if (consumptionActive && state.consumptionRequest !== null) {
      setFetchMode(state.consumptionRequest.mode);
      setFetchMaximum(state.consumptionRequest.maxMessages);
    } else if (
      appliedFetchDefaults.current.mode !== confirmedFetchDefaults.mode ||
      appliedFetchDefaults.current.maxMessages !== confirmedFetchDefaults.maxMessages
    ) {
      setFetchMode(confirmedFetchDefaults.mode);
      setFetchMaximum(confirmedFetchDefaults.maxMessages);
      appliedFetchDefaults.current = confirmedFetchDefaults;
    }
  }, [
    confirmedFetchDefaults.maxMessages,
    confirmedFetchDefaults.mode,
    consumptionActive,
    state.consumptionRequest,
  ]);

  const startConsumption = useCallback(
    async (topic: string, search?: KafkaSearchFilter): Promise<void> => {
      setMessageRequestError(undefined);
      setSelectedMessageId(null);
      setSelectionNotice(undefined);
      try {
        const request: KafkaFetchRequest =
          fetchMode === "time-window"
            ? {
                ...resolveKafkaTimeWindow(timeWindowDraft),
                maxMessages: fetchMaximum,
                mode: fetchMode,
                topic,
              }
            : {
                maxMessages: fetchMaximum,
                mode: fetchMode,
                topic,
              };
        const query = parseKafkaInvestigationQuery({
          schemaVersion: 1,
          request: { ...request, ...(search === undefined ? {} : { search }) },
        });
        const response = await host.execute({
          command: "messages.start",
          id: globalThis.crypto.randomUUID(),
          payload: query.request,
          version: HOST_PROTOCOL_VERSION,
        });
        if (!response.ok) {
          setMessageRequestError(response.error.summary);
        }
      } catch (error) {
        setMessageRequestError(
          error instanceof HostContractValidationError
            ? error.message
            : "The application host did not accept the consume request. Open Activity for diagnostics.",
        );
      }
    },
    [fetchMaximum, fetchMode, host, timeWindowDraft],
  );

  const activateTopic = useCallback(
    (topic: string): void => {
      if (!connected) return;
      setNavigation("topics");
      setSelectedTopic(topic);
      setTopicWorkspace("messages");
      setMessageRequestError(undefined);
      if (connected) {
        void startConsumption(topic);
      }
    },
    [connected, setNavigation, startConsumption],
  );

  const stopConsumption = useCallback(async (): Promise<void> => {
    setMessageRequestError(undefined);
    setConsumptionStopping(true);
    try {
      const response = await host.execute({
        command: "messages.stop",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setMessageRequestError(response.error.summary);
      }
    } catch {
      setMessageRequestError(
        "The application host did not acknowledge stop. Open Activity for diagnostics.",
      );
    } finally {
      setConsumptionStopping(false);
    }
  }, [host]);

  return {
    fetchMaximum,
    fetchMode,
    timeWindow: {
      value: timeWindowDraft,
      onChange: setTimeWindowDraft,
      error: kafkaTimeWindowError(timeWindowDraft),
    },
    messageRequestError,
    consumptionStopping,
    selectedMessageId,
    selectedTopic,
    selectionNotice,
    topicFilter,
    topicRequestError,
    topicWorkspace,
    messageSelection,
    visibleMessages,
    rendererPresentationActive,
    setFetchMaximum,
    setFetchMode,
    setSelectedMessageId,
    setSelectedTopic,
    setSelectionNotice,
    setTopicFilter,
    setTopicWorkspace,
    requestTopics,
    startConsumption,
    activateTopic,
    stopConsumption,
  };
}
