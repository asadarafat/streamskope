import {
  Profiler,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";
import { Box, Drawer, Stack, Typography, useMediaQuery } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_FETCH_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type KafkaFetchMode,
  type KafkaFetchRequest,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../contracts";
import type { StreamSkopeDesktop } from "../../platform/desktop";
import { streamSkopeLayout } from "../../ui/createStreamSkopeTheme";

import { ActivityLogDrawer } from "./ActivityLogDrawer";
import { AclPage } from "./AclPage";
import { ConnectionProfilesPage } from "./ConnectionProfilesPage";
import { ConsumerGroupsPage } from "./ConsumerGroupsPage";
import { SchemaRegistryPage } from "./SchemaRegistryPage";
import { TransformsPage } from "./TransformsPage";
import { countActiveKafkaMessageFilters } from "./message-operations";
import { MessageWorkspace } from "./MessageWorkspace";
import { OverviewPage } from "./OverviewPage";
import type {
  ProfileAction,
  ProfileConnectionOperation,
  ProfilePanelProperties,
} from "./ProfilePanel";
import type { ProfileWorkspaceProperties } from "./ProfileWorkspace";
import { TopicDetailPage } from "./TopicDetailPage";
import { TopicInventoryPage } from "./TopicInventoryPage";
import { RuleWorkspace, type KafkaRuleEditorMode } from "./RuleWorkspace";
import { RuleMatchNotification } from "./RuleMatchNotification";
import type { KafkaRuleUiAction } from "./rule-state";
import {
  createRendererStreamMonitorObserver,
  type RendererStreamMonitorObserver,
} from "./stream-monitor-observer";
import { initialKafkaUiState, reduceKafkaUiState, selectKafkaMessageById } from "./state";
import { createTextDocumentTransfer } from "./text-document-transfer";
import {
  consumerGroupStatusLabel,
  consumptionStateLabel,
  isKafkaConsumptionActive,
  topicStatusLabel,
} from "./workbench-status";
import { WorkbenchApplicationBar } from "./WorkbenchApplicationBar";
import { WorkbenchCommandPalette } from "./WorkbenchCommandPalette";
import { WorkbenchBreadcrumbs } from "./WorkbenchBreadcrumbs";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";
import { WorkbenchSidebar } from "./WorkbenchSidebar";
import {
  useDesktopActions,
  useAutoOpenActivityOnError,
  useRendererStreamMonitorLifecycle,
  useWorkbenchMessageSelection,
} from "./workbench-runtime-effects";
import { WorkbenchStatusBar } from "./WorkbenchStatusBar";
import { useConsumerGroupWorkbench } from "./use-consumer-group-workbench";
import type { NavigationView } from "./workbench-navigation";
import { isNavigationAvailable } from "./workbench-navigation";
import {
  LazyLatencyWorkspace,
  LazyOperationalPreferencesDialog,
  LazyStreamMonitorPanel,
  LazyTopicConfigurationWorkspace,
} from "./workbench-lazy-components";

export interface StreamSkopeWorkbenchProperties {
  readonly desktop?: StreamSkopeDesktop | undefined;
  readonly host: StreamSkopeHost;
  readonly streamMonitorObserver?: RendererStreamMonitorObserver;
}

export function StreamSkopeWorkbench({
  desktop,
  host,
  streamMonitorObserver,
}: StreamSkopeWorkbenchProperties): React.JSX.Element {
  const [state, dispatch] = useReducer(reduceKafkaUiState, initialKafkaUiState);
  const [rendererStreamMonitor] = useState(
    () => streamMonitorObserver ?? createRendererStreamMonitorObserver(),
  );
  const [navigation, setNavigation] = useState<NavigationView>("profiles");
  const [activityOpen, setActivityOpen] = useState(false);
  const [activityHeight, setActivityHeight] = useState<number>(streamSkopeLayout.activityHeight);
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false);
  const compactDesktop = useMediaQuery(
    `(max-width:${String(streamSkopeLayout.fullDesktopMinimumWidth - 0.05)}px)`,
  );
  const [fetchMaximum, setFetchMaximum] = useState<number>(KAFKA_FETCH_LIMITS.defaultMaxMessages);
  const [fetchMode, setFetchMode] = useState<KafkaFetchMode>("tail");
  const [messageRequestError, setMessageRequestError] = useState<string>();
  const [consumptionStopping, setConsumptionStopping] = useState(false);
  const [profileConnectionError, setProfileConnectionError] = useState<string>();
  const [profileConnectionOperation, setProfileConnectionOperation] =
    useState<ProfileConnectionOperation | null>(null);
  const [profileAction, setProfileAction] = useState<{
    readonly action: ProfileAction;
    readonly profileId: string;
  } | null>(null);
  const [profileFilter, setProfileFilter] = useState("");
  const [profileRequestError, setProfileRequestError] = useState<string>();
  const [resourcePaneOpen, setResourcePaneOpen] = useState(false);
  const [preferenceDialogMounted, setPreferenceDialogMounted] = useState(false);
  const [preferenceDialogOpen, setPreferenceDialogOpen] = useState(false);
  const [preferenceRequestError, setPreferenceRequestError] = useState<string>();
  const [ruleEditorMode, setRuleEditorMode] = useState<KafkaRuleEditorMode>(null);
  const [templateRequestError, setTemplateRequestError] = useState<string>();
  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null);
  const [selectedProfileId, setSelectedProfileId] = useState<string | null>(null);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const [selectionNotice, setSelectionNotice] = useState<string>();
  const [topicFilter, setTopicFilter] = useState("");
  const [topicRequestError, setTopicRequestError] = useState<string>();
  const [topicWorkspace, setTopicWorkspace] = useState<TopicWorkspaceView>("messages");
  const activityReturnFocusRef = useRef<HTMLElement | null>(null);
  const [activityQuery, setActivityQuery] = useState("");
  const restoreActivityFocusRef = useRef(false);
  const ruleCreateButtonRef = useRef<HTMLButtonElement>(null);
  const textDocumentTransfer = useMemo(() => createTextDocumentTransfer(desktop), [desktop]);

  const openActivity = useCallback(() => {
    setActivityQuery("");
    const activeElement = globalThis.document.activeElement;
    activityReturnFocusRef.current =
      activeElement instanceof globalThis.HTMLElement ? activeElement : null;
    setActivityOpen(true);
  }, []);
  const openProfileActivity = (correlationId?: string): void => {
    openActivity();
    setActivityQuery(correlationId ?? "");
  };

  const closeActivity = useCallback(() => {
    restoreActivityFocusRef.current = true;
    setActivityOpen(false);
  }, []);

  useDesktopActions(desktop, openActivity, setPreferenceDialogMounted, setPreferenceDialogOpen);

  useEffect(() => {
    const openCommands = (event: KeyboardEvent): void => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLocaleLowerCase() === "k") {
        event.preventDefault();
        setCommandPaletteOpen(true);
      }
    };
    globalThis.addEventListener("keydown", openCommands);
    return (): void => globalThis.removeEventListener("keydown", openCommands);
  }, []);

  useEffect(() => {
    if (!activityOpen && restoreActivityFocusRef.current) {
      restoreActivityFocusRef.current = false;
      activityReturnFocusRef.current?.focus();
    }
  }, [activityOpen]);

  useAutoOpenActivityOnError(state.activities, activityOpen, openActivity);
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

  useRendererStreamMonitorLifecycle({
    dispatch,
    filterDurationMs: messageSelection.durationMs,
    host,
    lastSequence: state.lastSequence,
    observer: rendererStreamMonitor,
    presentationActive: rendererPresentationActive,
    rendererDroppedMessages: state.rendererDroppedMessages,
    retainedMessages: state.messages.length,
    visibleMessages: visibleMessages.length,
  });

  const dispatchRule = useCallback((action: KafkaRuleUiAction): void => {
    dispatch({ action, type: "rule.action" });
  }, []);

  useEffect(() => {
    let active = true;
    async function requestProfiles(): Promise<void> {
      setProfileRequestError(undefined);
      try {
        const response = await host.execute({
          command: "profiles.list",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (active && !response.ok) {
          setProfileRequestError(`${response.error.summary} ${response.error.recovery}`);
        }
      } catch {
        if (active) {
          setProfileRequestError(
            "The application host did not accept the profile request. Open Activity for diagnostics.",
          );
        }
      }
    }
    void requestProfiles();
    return (): void => {
      active = false;
    };
  }, [host]);

  useEffect(() => {
    let active = true;
    async function requestPreferences(): Promise<void> {
      setPreferenceRequestError(undefined);
      try {
        const response = await host.execute({
          command: "preferences.get",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (active && !response.ok) {
          setPreferenceRequestError(`${response.error.summary} ${response.error.recovery}`);
        }
      } catch {
        if (active) {
          setPreferenceRequestError(
            "The application host did not accept the preference request. Open Activity for diagnostics.",
          );
        }
      }
    }
    void requestPreferences();
    return (): void => {
      active = false;
    };
  }, [host]);

  useEffect(() => {
    let active = true;
    const id = globalThis.crypto.randomUUID();
    dispatchRule({
      operation: "list",
      requestId: id,
      type: "operation.started",
    });
    async function requestRules(): Promise<void> {
      try {
        const response = await host.execute({
          command: "rules.list",
          id,
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (!active) {
          return;
        }
        if (response.ok) {
          dispatchRule({ requestId: id, type: "operation.accepted" });
        } else {
          dispatchRule({
            message: `${response.error.summary} ${response.error.recovery}`,
            requestId: id,
            type: "operation.failed",
          });
        }
      } catch {
        if (active) {
          dispatchRule({
            message:
              "The application host did not accept the rule catalog request. Open Activity for diagnostics.",
            requestId: id,
            type: "operation.failed",
          });
        }
      }
    }
    void requestRules();
    return (): void => {
      active = false;
    };
  }, [dispatchRule, host]);

  useEffect(() => {
    let active = true;
    async function requestTemplates(): Promise<void> {
      setTemplateRequestError(undefined);
      try {
        const response = await host.execute({
          command: "templates.list",
          id: globalThis.crypto.randomUUID(),
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        });
        if (active && !response.ok) {
          setTemplateRequestError(`${response.error.summary} ${response.error.recovery}`);
        }
      } catch {
        if (active) {
          setTemplateRequestError(
            "The application host did not accept the template request. Open Activity for diagnostics.",
          );
        }
      }
    }
    void requestTemplates();
    return (): void => {
      active = false;
    };
  }, [host]);

  const connected = state.connectionState === "connected";
  const openResourcePage = useCallback(
    (next: NavigationView): void => {
      if (!isNavigationAvailable(next, connected)) return;
      setNavigation(next);
      if (next === "topics") {
        setSelectedTopic(null);
        setTopicWorkspace("messages");
      }
      setResourcePaneOpen(false);
    },
    [connected],
  );
  const {
    filter: consumerGroupFilter,
    onClearSelection: clearConsumerGroupSelection,
    onFilterChange: setConsumerGroupFilter,
    onNavigationChange: changeNavigation,
    onRefresh: requestConsumerGroups,
    onSelect: selectConsumerGroup,
    requestError: consumerGroupRequestError,
    selectedGroupId: selectedConsumerGroupId,
  } = useConsumerGroupWorkbench({
    connected,
    connectionName: state.connectionName,
    host,
    inventory: state.consumerGroupInventory,
    onNavigationChange: openResourcePage,
  });
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
  }, [connected, requestTopics, state.connectionName]);

  useEffect(() => {
    setTopicFilter("");
  }, [state.connectionName]);

  useEffect(() => {
    setSelectedProfileId((current) => {
      if (current !== null && state.profiles.some((profile) => profile.id === current)) {
        return current;
      }
      return state.profiles.find((profile) => profile.active)?.id ?? state.profiles[0]?.id ?? null;
    });
  }, [state.profiles]);

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
    } else {
      setFetchMode(confirmedFetchDefaults.mode);
      setFetchMaximum(confirmedFetchDefaults.maxMessages);
    }
  }, [
    confirmedFetchDefaults.maxMessages,
    confirmedFetchDefaults.mode,
    consumptionActive,
    state.consumptionRequest,
  ]);

  const startConsumption = useCallback(
    async (topic: string): Promise<void> => {
      setMessageRequestError(undefined);
      setSelectedMessageId(null);
      setSelectionNotice(undefined);
      try {
        const endTimeMs = Date.now();
        const request: KafkaFetchRequest =
          fetchMode === "time-window"
            ? {
                endTimeMs,
                maxMessages: fetchMaximum,
                mode: fetchMode,
                startTimeMs: endTimeMs - KAFKA_FETCH_LIMITS.defaultTimeWindowMs,
                topic,
              }
            : {
                maxMessages: fetchMaximum,
                mode: fetchMode,
                topic,
              };
        const response = await host.execute({
          command: "messages.start",
          id: globalThis.crypto.randomUUID(),
          payload: request,
          version: HOST_PROTOCOL_VERSION,
        });
        if (!response.ok) {
          setMessageRequestError(response.error.summary);
        }
      } catch {
        setMessageRequestError(
          "The application host did not accept the consume request. Open Activity for diagnostics.",
        );
      }
    },
    [fetchMaximum, fetchMode, host],
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
    [connected, startConsumption],
  );

  const toggleProfileConnection = useCallback(
    async (profile: ProfileSummary): Promise<void> => {
      const profileConnected = connected && profile.active;
      const action = profileConnected ? "disconnect" : "connect";
      setProfileConnectionError(undefined);
      setProfileConnectionOperation({ action, profileId: profile.id });
      try {
        const response = await host.execute(
          profileConnected
            ? {
                command: "connection.disconnect",
                id: globalThis.crypto.randomUUID(),
                payload: {},
                version: HOST_PROTOCOL_VERSION,
              }
            : {
                command: "profiles.connect",
                id: globalThis.crypto.randomUUID(),
                payload: { profileId: profile.id },
                version: HOST_PROTOCOL_VERSION,
              },
        );
        if (!response.ok) {
          setProfileConnectionError(
            `${action === "connect" ? "Connect" : "Disconnect"} ${profile.name}: ${response.error.summary} ${response.error.recovery}`,
          );
        }
      } catch {
        setProfileConnectionError(
          `${action === "connect" ? "Connect" : "Disconnect"} ${profile.name}: The application host did not accept the request. Open Activity for diagnostics.`,
        );
      } finally {
        setProfileConnectionOperation(null);
      }
    },
    [connected, host],
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

  const topicStatus = topicStatusLabel(state.topicListState, state.topics.length);
  const resourceStatus =
    navigation === "consumer-groups"
      ? consumerGroupStatusLabel(
          state.consumerGroupInventory.state,
          state.consumerGroupInventory.groups.length,
        )
      : navigation === "schemas"
        ? `${state.schemaInventory.subjects.length.toLocaleString()} schemas`
        : navigation === "acls"
          ? `${state.aclSnapshot.acls.length.toLocaleString()} ACLs`
          : navigation === "transforms"
            ? `${state.transformInventory.transforms.length.toLocaleString()} transforms`
            : topicStatus;
  const selectedMessage = selectKafkaMessageById(visibleMessages, selectedMessageId);
  const selectedProfile =
    state.profiles.find((profile) => profile.id === selectedProfileId) ?? null;
  const contextPaneTemporary = compactDesktop;
  const operationStatus =
    state.consumptionState === "unavailable"
      ? "Idle"
      : consumptionStateLabel(state.consumptionState);
  const ruleNotification = state.ruleNotifications[0];
  const openPreferences = (): void => {
    setPreferenceDialogMounted(true);
    setPreferenceDialogOpen(true);
  };
  const selectNavigation = (next: NavigationView): void => {
    if (!isNavigationAvailable(next, connected)) return;
    if (next === "consumer-groups") {
      clearConsumerGroupSelection();
    }
    changeNavigation(next);
  };
  const changeTopicWorkspace = (next: TopicWorkspaceView): void => {
    setTopicWorkspace(next);
  };
  const profilePanelProperties: ProfilePanelProperties = {
    activityOpen,
    connected,
    connectionOperation: profileConnectionOperation,
    filter: profileFilter,
    host,
    loading: state.profileStore === null && profileRequestError === undefined,
    onFilterChange: setProfileFilter,
    onOpenActivity: openProfileActivity,
    onProfileAction: (profileId, action) => {
      setSelectedProfileId(profileId);
      setProfileAction({ action, profileId });
    },
    onSelectProfile: setSelectedProfileId,
    onToggleConnection: (profile) => {
      void toggleProfileConnection(profile);
    },
    profiles: state.profiles,
    selectedProfileId,
    store: state.profileStore,
    transfer: textDocumentTransfer,
    templateLoading: state.templateSnapshot === null && templateRequestError === undefined,
    templateSnapshot: state.templateSnapshot,
    ...(profileConnectionError === undefined ? {} : { connectionError: profileConnectionError }),
    ...(profileRequestError === undefined ? {} : { requestError: profileRequestError }),
    ...(templateRequestError === undefined ? {} : { templateRequestError }),
  };
  const profileWorkspaceProperties: ProfileWorkspaceProperties = {
    action:
      profileAction !== null && profileAction.profileId === selectedProfile?.id
        ? profileAction.action
        : null,
    activityOpen,
    clusterDiagnostics: state.clusterDiagnostics,
    host,
    onActionClose: () => setProfileAction(null),
    onOpenActivity: openProfileActivity,
    profile: selectedProfile,
    templateLoading: state.templateSnapshot === null && templateRequestError === undefined,
    templateSnapshot: state.templateSnapshot,
    transfer: textDocumentTransfer,
    ...(templateRequestError === undefined ? {} : { templateRequestError }),
  };

  function renderTopicWorkspace(): React.JSX.Element {
    if (selectedTopic !== null && topicWorkspace === "rules") {
      return (
        <RuleWorkspace
          component="section"
          createButtonRef={ruleCreateButtonRef}
          editorMode={ruleEditorMode}
          host={host}
          onEditorModeChange={setRuleEditorMode}
          onRuleAction={dispatchRule}
          state={state.ruleState}
        />
      );
    }
    if (selectedTopic !== null && topicWorkspace === "configuration") {
      return (
        <Suspense
          fallback={
            <Stack
              aria-label="Loading configuration workspace"
              role="status"
              sx={{ alignItems: "center", justifyContent: "center", p: 3 }}
            >
              <Typography color="text.secondary" variant="body2">
                Loading configuration workspace…
              </Typography>
            </Stack>
          }
        >
          <LazyTopicConfigurationWorkspace
            component="section"
            connectionName={state.connectionName}
            history={state.topicConfigurationHistory}
            host={host}
            selectedTopic={selectedTopic}
            snapshot={state.topicConfiguration}
          />
        </Suspense>
      );
    }
    if (selectedTopic !== null && topicWorkspace === "latency") {
      return (
        <Suspense
          fallback={
            <Stack
              aria-label="Loading latency workspace"
              role="status"
              sx={{ alignItems: "center", justifyContent: "center", p: 3 }}
            >
              <Typography color="text.secondary" variant="body2">
                Loading latency workspace…
              </Typography>
            </Stack>
          }
        >
          <LazyLatencyWorkspace
            component="section"
            connectionName={state.connectionName}
            history={state.latencyHistory}
            host={host}
            onOpenActivity={openActivity}
            preferences={state.preferenceSnapshot}
            selectedTopic={selectedTopic}
            snapshot={state.latency}
            transfer={textDocumentTransfer}
          />
        </Suspense>
      );
    }
    if (selectedTopic !== null && topicWorkspace === "monitor") {
      return (
        <Box
          aria-label="Stream Monitor workspace"
          component="section"
          sx={{ height: "100%", minHeight: 0, overflow: "hidden" }}
        >
          <Suspense
            fallback={
              <Stack
                aria-label="Loading Stream Monitor"
                role="status"
                sx={{ alignItems: "center", justifyContent: "center", p: 3 }}
              >
                <Typography color="text.secondary" variant="body2">
                  Loading Stream Monitor…
                </Typography>
              </Stack>
            }
          >
            <LazyStreamMonitorPanel
              activeConnectionName={state.connectionName}
              consumptionError={state.consumptionError}
              history={state.streamMonitor.history}
              onOpenActivity={openActivity}
              rendererObserver={rendererStreamMonitor}
              selectedTopic={selectedTopic}
              snapshot={state.streamMonitor.current}
            />
          </Suspense>
        </Box>
      );
    }
    return (
      <Profiler
        id="kafka-message-workspace"
        onRender={(_id, _phase, actualDuration) => {
          rendererStreamMonitor.recordRenderDuration(actualDuration);
        }}
      >
        <MessageWorkspace
          component="section"
          connectionAvailable={connected}
          consumptionError={state.consumptionError}
          consumptionRequest={state.consumptionRequest}
          consumptionState={state.consumptionState}
          consumptionStopping={consumptionStopping}
          droppedMessages={state.droppedMessages}
          fetchMaximum={fetchMaximum}
          fetchMode={fetchMode}
          filters={state.messageFilters}
          key={selectedTopic ?? "no-topic"}
          liveRuleCapability={state.liveRuleCapability}
          messages={visibleMessages}
          messagesStale={state.messagesStale}
          onClearFilters={() => dispatch({ type: "messages.filters.cleared" })}
          onClearSelection={() => {
            setSelectedMessageId(null);
            setSelectionNotice(undefined);
          }}
          onFetchMaximumChange={setFetchMaximum}
          onFetchModeChange={setFetchMode}
          onPartitionFilterChange={(partition) => {
            dispatch({ partition, type: "messages.filter.partition.changed" });
          }}
          onRuleFilterChange={(activeOnly) => {
            dispatch({ activeOnly, type: "messages.rule-filter.changed" });
          }}
          onSelectMessage={(id) => {
            setSelectedMessageId(id);
            setSelectionNotice(undefined);
          }}
          onStart={() => {
            if (selectedTopic !== null) void startConsumption(selectedTopic);
          }}
          onStop={() => void stopConsumption()}
          onTextFilterChange={(field, value) => {
            dispatch({ field, type: "messages.filter.text.changed", value });
          }}
          retainedMessageCount={state.messages.length}
          windowEvictions={state.rendererWindowEvictions}
          savedProfileCount={state.profiles.length}
          selectedMessage={selectedMessage}
          selectedMessageId={selectedMessageId}
          selectedTopic={selectedTopic}
          selectionNotice={selectionNotice}
          transfer={textDocumentTransfer}
        />
      </Profiler>
    );
  }

  const page =
    navigation === "overview" ? (
      <OverviewPage
        activeConnectionName={state.connectionName}
        backend={state.backend}
        connectionState={state.connectionState}
        consumerGroupCount={state.consumerGroupInventory.groups.length}
        onOpenProfiles={() => selectNavigation("profiles")}
        profileCount={state.profiles.length}
        topicCount={state.topics.length}
      />
    ) : navigation === "profiles" ? (
      <ConnectionProfilesPage
        panel={profilePanelProperties}
        workspace={profileWorkspaceProperties}
      />
    ) : navigation === "consumer-groups" ? (
      <ConsumerGroupsPage
        connected={connected}
        detail={state.consumerGroupDetail}
        filter={consumerGroupFilter}
        inventory={state.consumerGroupInventory}
        onFilterChange={setConsumerGroupFilter}
        onRefresh={requestConsumerGroups}
        onSelect={selectConsumerGroup}
        selectedGroupId={selectedConsumerGroupId}
        {...(consumerGroupRequestError === undefined
          ? {}
          : { requestError: consumerGroupRequestError })}
      />
    ) : navigation === "schemas" ? (
      <SchemaRegistryPage
        compatibility={state.schemaCompatibility}
        connected={connected}
        detail={state.schemaDetail}
        host={host}
        inventory={state.schemaInventory}
      />
    ) : navigation === "acls" ? (
      <AclPage connected={connected} host={host} snapshot={state.aclSnapshot} />
    ) : navigation === "transforms" ? (
      <TransformsPage
        connected={connected}
        detail={state.transformDetail}
        host={host}
        inventory={state.transformInventory}
        logs={state.transformLogs}
      />
    ) : selectedTopic === null ? (
      <TopicInventoryPage
        connected={connected}
        filter={topicFilter}
        onFilterChange={setTopicFilter}
        onOpen={activateTopic}
        onRefresh={() => {
          void requestTopics();
        }}
        refreshedAt={state.refreshedAt}
        topicError={state.topicError}
        topicListState={state.topicListState}
        topics={state.topics}
        {...(topicRequestError === undefined ? {} : { requestError: topicRequestError })}
      />
    ) : (
      <TopicDetailPage
        onWorkspaceChange={changeTopicWorkspace}
        selectedTopic={selectedTopic}
        workspace={topicWorkspace}
      >
        {renderTopicWorkspace()}
      </TopicDetailPage>
    );

  const sidebar = (
    <WorkbenchSidebar connected={connected} navigation={navigation} onChange={selectNavigation} />
  );

  return (
    <Box
      sx={{
        bgcolor: "background.default",
        color: "text.primary",
        display: "grid",
        gridTemplateAreas: contextPaneTemporary
          ? '"application" "workspace" "status"'
          : '"application application" "sidebar workspace" "status status"',
        gridTemplateColumns: contextPaneTemporary
          ? "minmax(0, 1fr)"
          : `${String(streamSkopeLayout.resourceDefaultWidth)}px minmax(0, 1fr)`,
        gridTemplateRows: `${String(streamSkopeLayout.headerHeight)}px minmax(0, 1fr) ${String(
          streamSkopeLayout.statusBarHeight,
        )}px`,
        height: "100dvh",
        minHeight: 480,
        minWidth: 0,
        overflow: "clip",
      }}
    >
      <Box sx={{ gridArea: "application", minWidth: 0 }}>
        <WorkbenchApplicationBar
          navigatorOpen={resourcePaneOpen}
          navigatorTemporary={contextPaneTemporary}
          onOpenCommandPalette={() => setCommandPaletteOpen(true)}
          onOpenPreferences={openPreferences}
          onToggleNavigator={() => setResourcePaneOpen((open) => !open)}
        />
      </Box>

      {contextPaneTemporary ? (
        <Drawer
          anchor="left"
          onClose={() => setResourcePaneOpen(false)}
          open={resourcePaneOpen}
          slotProps={{
            paper: {
              "aria-label": "StreamSkope resources drawer",
              sx: {
                bottom: `${String(streamSkopeLayout.statusBarHeight)}px`,
                height: "auto",
                maxWidth: "88vw",
                top: `${String(streamSkopeLayout.headerHeight)}px`,
                width: streamSkopeLayout.resourceDefaultWidth,
              },
            },
          }}
          variant="temporary"
        >
          {sidebar}
        </Drawer>
      ) : (
        <Box component="aside" sx={{ gridArea: "sidebar", minHeight: 0 }}>
          {sidebar}
        </Box>
      )}

      <Box
        sx={{
          display: "grid",
          gridArea: "workspace",
          gridTemplateRows: `${String(
            streamSkopeLayout.breadcrumbBarHeight,
          )}px minmax(0, 1fr) ${String(
            activityOpen ? activityHeight : streamSkopeLayout.activityCollapsedHeight,
          )}px`,
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        <WorkbenchBreadcrumbs
          navigation={navigation}
          onNavigate={selectNavigation}
          selectedConsumerGroupId={selectedConsumerGroupId}
          selectedTopic={selectedTopic}
        />
        <Box
          id="streamskope-active-page"
          sx={{ display: "grid", minHeight: 0, minWidth: 0, overflow: "hidden" }}
        >
          {page}
        </Box>
        <ActivityLogDrawer
          initialQuery={activityQuery}
          entries={state.activities}
          height={activityHeight}
          onClose={activityOpen ? closeActivity : openActivity}
          onHeightChange={setActivityHeight}
          open={activityOpen}
          transfer={textDocumentTransfer}
        />
      </Box>

      <Box sx={{ gridArea: "status", minWidth: 0 }}>
        <WorkbenchStatusBar
          activeConnectionName={state.connectionName}
          backend={state.backend}
          connectionState={state.connectionState}
          droppedMessages={state.droppedMessages}
          messageRequestError={messageRequestError}
          onReload={() => window.location.reload()}
          operationStatus={operationStatus}
          resourceStatus={resourceStatus}
        />
      </Box>

      <WorkbenchCommandPalette
        connected={connected}
        onClose={() => setCommandPaletteOpen(false)}
        onOpenResource={selectNavigation}
        onOpenTopic={activateTopic}
        onSelectProfile={(profileId) => {
          setSelectedProfileId(profileId);
          setProfileFilter("");
          selectNavigation("profiles");
        }}
        open={commandPaletteOpen}
        profiles={state.profiles}
        topics={state.topics}
      />

      <RuleMatchNotification
        notification={ruleNotification}
        onDismiss={(sequence) => dispatch({ sequence, type: "rules.notification.dismissed" })}
      />

      {preferenceDialogMounted ? (
        <Suspense fallback={null}>
          <LazyOperationalPreferencesDialog
            host={host}
            onClose={() => {
              setPreferenceDialogOpen(false);
            }}
            onOpenActivity={() => {
              setPreferenceDialogOpen(false);
              openActivity();
            }}
            open={preferenceDialogOpen}
            snapshot={state.preferenceSnapshot}
            {...(preferenceRequestError === undefined || state.preferenceSnapshot !== null
              ? {}
              : { loadError: preferenceRequestError })}
          />
        </Suspense>
      ) : null}
    </Box>
  );
}
