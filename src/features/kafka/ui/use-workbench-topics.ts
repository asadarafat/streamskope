import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  KAFKA_FETCH_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  parseKafkaInvestigationQuery,
  type KafkaSearchFilter,
  type KafkaInvestigationQuery,
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
import type { ObservationNavigation } from "./ObservationFindings";
import { useReadContinuation } from "./use-read-continuation";

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
  | "searchProgress"
  | "backend"
>;

interface WorkbenchTopicController {
  readonly captureQuery: () => KafkaInvestigationQuery;
  readonly restoreQuery: (query: KafkaInvestigationQuery, awaitConnection: boolean) => void;
  readonly fetchMaximum: number;
  readonly fetchMode: KafkaFetchMode;
  readonly timeWindow: QueryTimeWindowControlsProps;
  readonly messageRequestError: string | undefined;
  readonly consumptionStopping: boolean;
  readonly continuationAvailable: boolean;
  readonly continuationBusy: boolean;
  readonly continuationNotice: string | undefined;
  readonly continueConsumption: () => Promise<void>;
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
  readonly openObservedRecord: NonNullable<ObservationNavigation["onOpenRecord"]>;
  readonly stopConsumption: () => Promise<void>;
}

export function useWorkbenchTopics(
  host: StreamSkopeHost,
  connected: boolean,
  navigation: NavigationView,
  setNavigation: (navigation: NavigationView) => void,
  state: TopicWorkbenchState,
): WorkbenchTopicController {
  const pendingQuery = useRef<KafkaInvestigationQuery | null>(null);
  const appliedConnection = useRef<{
    readonly host: StreamSkopeHost;
    readonly name: string | null;
  } | null>(null);
  const preserveQueryDuringPreferenceHydration = useRef(false);
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
  const continuationContextForTopic = useCallback(
    (topic: string | null): string =>
      JSON.stringify({
        topic,
        mode: fetchMode,
        maximum: fetchMaximum,
        window: fetchMode === "time-window" ? timeWindowDraft : null,
        filters: state.messageFilters,
        codecs:
          state.preferenceSnapshot?.preferences.codecs ??
          KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.codecs,
        protection:
          state.preferenceSnapshot?.preferences.protection ??
          KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection,
      }),
    [fetchMode, fetchMaximum, timeWindowDraft, state.messageFilters, state.preferenceSnapshot],
  );
  const onContinued = useCallback((): void => {
    setSelectedMessageId(null);
    setSelectionNotice(undefined);
  }, []);
  const {
    continuationAvailable,
    continuationBusy,
    continuationNotice,
    continueConsumption,
    bindContinuation,
  } = useReadContinuation({
    host,
    context: continuationContextForTopic(selectedTopic),
    progress: state.searchProgress,
    connected: connected && state.backend !== "unavailable",
    active: consumptionActive,
    stopping: consumptionStopping,
    topicMatches: state.consumptionRequest?.topic === selectedTopic,
    onError: setMessageRequestError,
    onContinued,
  });
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
    if (!connected) {
      appliedConnection.current = null;
      return;
    }
    if (
      appliedConnection.current?.host === host &&
      appliedConnection.current.name === state.connectionName
    )
      return;
    appliedConnection.current = { host, name: state.connectionName };
    setNavigation("topics");
    setSelectedTopic(pendingQuery.current?.request.topic ?? null);
    pendingQuery.current = null;
    setTopicWorkspace("messages");
    void requestTopics();
  }, [connected, host, requestTopics, setNavigation, state.connectionName]);

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
      if (!preserveQueryDuringPreferenceHydration.current) {
        setFetchMode(confirmedFetchDefaults.mode);
        setFetchMaximum(confirmedFetchDefaults.maxMessages);
      }
      appliedFetchDefaults.current = confirmedFetchDefaults;
    }
    if (state.preferenceSnapshot !== null) preserveQueryDuringPreferenceHydration.current = false;
  }, [
    confirmedFetchDefaults.maxMessages,
    confirmedFetchDefaults.mode,
    consumptionActive,
    state.consumptionRequest,
    state.preferenceSnapshot,
  ]);

  const requestForTopic = useCallback(
    (topic: string): KafkaFetchRequest =>
      fetchMode === "time-window"
        ? {
            ...resolveKafkaTimeWindow(timeWindowDraft),
            maxMessages: fetchMaximum,
            mode: fetchMode,
            topic,
          }
        : { maxMessages: fetchMaximum, mode: fetchMode, topic },
    [fetchMaximum, fetchMode, timeWindowDraft],
  );
  const captureQuery = useCallback((): KafkaInvestigationQuery => {
    if (selectedTopic === null)
      throw new HostContractValidationError("Query", "choose a topic first");
    if (state.messageFilters.activeRuleMatchesOnly)
      throw new HostContractValidationError(
        "Query",
        "turn off Rule matches only; use a JSON expression to save an independent filter",
      );
    const { key, value, offset, offsetExact, timestamp, partition, expression } =
      state.messageFilters;
    return parseKafkaInvestigationQuery({
      schemaVersion: 1,
      request: requestForTopic(selectedTopic),
      filters: {
        key,
        value,
        offset,
        ...(offsetExact === undefined ? {} : { offsetExact }),
        timestamp,
        partition,
        ...(expression === undefined ? {} : { expression }),
      },
    });
  }, [selectedTopic, requestForTopic, state.messageFilters]);
  const restoreQuery = useCallback(
    (query: KafkaInvestigationQuery, awaitConnection: boolean): void => {
      const validated = parseKafkaInvestigationQuery(query);
      preserveQueryDuringPreferenceHydration.current = state.preferenceSnapshot === null;
      pendingQuery.current = awaitConnection ? validated : null;
      setFetchMode(validated.request.mode);
      setFetchMaximum(validated.request.maxMessages);
      if (validated.request.mode === "time-window")
        setTimeWindowDraft({
          mode: "custom",
          start: new Date(validated.request.startTimeMs).toISOString(),
          end: new Date(validated.request.endTimeMs).toISOString(),
        });
      setSelectedTopic(awaitConnection ? null : validated.request.topic);
      setTopicWorkspace("messages");
      setSelectedMessageId(null);
      setMessageRequestError(undefined);
    },
    [state.preferenceSnapshot],
  );

  const startConsumption = useCallback(
    async (topic: string, search?: KafkaSearchFilter): Promise<void> => {
      setMessageRequestError(undefined);
      setSelectedMessageId(null);
      setSelectionNotice(undefined);
      bindContinuation(continuationContextForTopic(topic));
      try {
        const request = requestForTopic(topic);
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
          bindContinuation(null);
          setMessageRequestError(response.error.summary);
        }
      } catch (error) {
        bindContinuation(null);
        setMessageRequestError(
          error instanceof HostContractValidationError
            ? error.message
            : "The application host did not accept the consume request. Open Activity for diagnostics.",
        );
      }
    },
    [host, requestForTopic, continuationContextForTopic, bindContinuation],
  );

  const activateTopic = useCallback(
    (topic: string): void => {
      if (!connected) return;
      pendingQuery.current = null;
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

  const openObservedRecord = useCallback<NonNullable<ObservationNavigation["onOpenRecord"]>>(
    (locator): void => {
      if (!connected) return;
      const search: KafkaSearchFilter = {
        key: "",
        value: "",
        offset: "",
        offsetExact: locator.offset,
        timestamp: "",
        partition: locator.partition,
      };
      const query = parseKafkaInvestigationQuery({
        schemaVersion: 1,
        request: {
          topic: locator.topic,
          mode: "time-window",
          maxMessages: 1,
          startTimeMs: locator.startTimeMs,
          endTimeMs: locator.endTimeMs,
        },
        filters: search,
      });
      restoreQuery(query, false);
      setNavigation("topics");
      void host
        .execute({
          command: "messages.start",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { ...query.request, search },
        })
        .then((response) => {
          if (!response.ok) setMessageRequestError(response.error.summary);
        })
        .catch(() =>
          setMessageRequestError(
            "The sampled record could not be read. Check the current connection and read coverage.",
          ),
        );
    },
    [connected, host, restoreQuery, setNavigation],
  );

  useEffect(() => {
    const search = state.consumptionRequest?.search;
    if (search?.offsetExact === undefined || selectedMessageId !== null) return;
    const record = visibleMessages.find(
      (message) => message.partition === search.partition && message.offset === search.offsetExact,
    );
    if (record) setSelectedMessageId(record.id);
  }, [state.consumptionRequest, visibleMessages, selectedMessageId]);

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
    captureQuery,
    restoreQuery,
    fetchMaximum,
    fetchMode,
    timeWindow: {
      value: timeWindowDraft,
      onChange: setTimeWindowDraft,
      error: kafkaTimeWindowError(timeWindowDraft),
    },
    messageRequestError,
    consumptionStopping,
    continuationAvailable,
    continuationBusy,
    continuationNotice,
    continueConsumption,
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
    openObservedRecord,
    stopConsumption,
  };
}
