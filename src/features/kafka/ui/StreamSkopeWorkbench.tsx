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
import { Box, Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { StreamSkopeDesktop } from "../../../platform/desktop";
import { streamSkopeLayout } from "../../../platform/ui/createStreamSkopeTheme";
import { ProviderWorkbenchShell } from "../../../platform/ui/ProviderWorkbenchShell";
import { useProductNavigator } from "../../../platform/ui/use-product-navigator";
import { StudioButton } from "../../../platform/ui/controls";

import { RelationshipsPage } from "./RelationshipsPage";
import { ObservedHealthPage } from "./ObservedHealthPage";
import { ConnectPage } from "./ConnectPage";
import { EnvironmentPage } from "./EnvironmentPage";
import { ReviewedWriteAction } from "./ReviewedWriteAction";
import { initialKafkaMessageFilters } from "./message-operations";
import { SavedQueriesDialog } from "./SavedQueriesDialog";
import { ActivityLogDrawer } from "./ActivityLogDrawer";
import { AclPage } from "./AclPage";
import { ConnectionProfilesPage } from "./ConnectionProfilesPage";
import { ConsumerGroupsPage } from "./ConsumerGroupsPage";
import { SchemaRegistryPage } from "./SchemaRegistryPage";
import { TransformsPage } from "./TransformsPage";
import { MessageWorkspace } from "./MessageWorkspace";
import { OverviewPage } from "./OverviewPage";
import type { ProfilePanelProperties } from "./ProfilePanel";
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
  topicStatusLabel,
  isKafkaConsumptionActive,
  kafkaConsumptionStopLabel,
} from "./workbench-status";
import { WorkbenchCommandPalette } from "./WorkbenchCommandPalette";
import { investigationCommands } from "./workbench-query-commands";
import { WorkbenchBreadcrumbs } from "./WorkbenchBreadcrumbs";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";
import { useRendererStreamMonitorLifecycle } from "./workbench-runtime-effects";
import { useWorkbenchActivity } from "./use-workbench-activity";
import { useWorkbenchProfiles } from "./use-workbench-profiles";
import { useWorkbenchTopics } from "./use-workbench-topics";
import { WorkbenchStatusBar } from "./WorkbenchStatusBar";
import { useConsumerGroupWorkbench } from "./use-consumer-group-workbench";
import type { NavigationView } from "./workbench-navigation";
import { isNavigationAvailable, WORKBENCH_RESOURCE_GROUPS } from "./workbench-navigation";
import {
  LazyLatencyWorkspace,
  LazyOperationalPreferencesDialog,
  LazyStreamMonitorPanel,
  LazyTopicConfigurationWorkspace,
} from "./workbench-lazy-components";

export interface StreamSkopeWorkbenchProperties {
  readonly desktop?: StreamSkopeDesktop | undefined;
  readonly host: StreamSkopeHost;
  readonly providerControl?: React.ReactNode;
  readonly isInteractive?: (() => boolean) | undefined;
  readonly initialQueryImport?: string | undefined;
  readonly streamMonitorObserver?: RendererStreamMonitorObserver;
}

export function StreamSkopeWorkbench({
  desktop,
  host,
  streamMonitorObserver,
  initialQueryImport,
  providerControl,
  isInteractive,
}: StreamSkopeWorkbenchProperties): React.JSX.Element {
  const [queriesOpen, setQueriesOpen] = useState(initialQueryImport !== undefined);
  const [queryImport, setQueryImport] = useState(initialQueryImport);
  const [state, dispatch] = useReducer(reduceKafkaUiState, initialKafkaUiState);
  const [rendererStreamMonitor] = useState(
    () => streamMonitorObserver ?? createRendererStreamMonitorObserver(),
  );
  const [navigation, setNavigation] = useState<NavigationView>("profiles");
  const navigator = useProductNavigator();
  const [profileRequestError, setProfileRequestError] = useState<string>();
  const [preferenceRequestError, setPreferenceRequestError] = useState<string>();
  const [preferenceSection, setPreferenceSection] = useState<"workbench" | "plugins">("workbench");
  const [ruleEditorMode, setRuleEditorMode] = useState<KafkaRuleEditorMode>(null);
  const ruleCreateButtonRef = useRef<HTMLButtonElement>(null);
  const textDocumentTransfer = useMemo(() => createTextDocumentTransfer(desktop), [desktop]);

  const connected = state.connectionState === "connected";
  const {
    activityOpen,
    activityHeight,
    activityQuery,
    commandPaletteOpen,
    preferenceDialogMounted,
    preferenceDialogOpen,
    closeActivity,
    openActivity,
    openProfileActivity,
    openPreferences,
    setActivityHeight,
    setCommandPaletteOpen,
    setPreferenceDialogOpen,
  } = useWorkbenchActivity(desktop, state.activities, isInteractive);
  const {
    profileConnectionError,
    profileConnectionOperation,
    profileAction,
    profileFilter,
    selectedProfileId,
    setProfileAction,
    setProfileFilter,
    setSelectedProfileId,
    toggleProfileConnection,
  } = useWorkbenchProfiles(host, connected, state.profiles);
  const {
    captureQuery,
    restoreQuery,
    fetchMaximum,
    fetchMode,
    timeWindow,
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
    openObservedRecord,
    stopConsumption,
  } = useWorkbenchTopics(host, connected, navigation, setNavigation, state);

  useRendererStreamMonitorLifecycle({
    dispatch,
    filterDurationMs: messageSelection.durationMs,
    host,
    lastSequence: state.lastSequence,
    observer: rendererStreamMonitor,
    presentationActive: rendererPresentationActive,
    messagesMounted: navigation === "topics" && topicWorkspace === "messages",
    operationId: state.streamMonitor.current.operationId,
    rendererDroppedMessages: state.rendererDroppedMessages,
    rendererWindowEvictions: state.rendererWindowEvictions,
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

  const openResourcePage = useCallback(
    (next: NavigationView): void => {
      if (!isNavigationAvailable(next, connected)) return;
      setNavigation(next);
      if (next === "topics") {
        setSelectedTopic(null);
        setTopicWorkspace("messages");
      }
      navigator.close();
    },
    [connected, navigator.close],
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
  const topicStatus = topicStatusLabel(state.topicListState, state.topics.length);
  useEffect(() => {
    if (navigation !== "observations" || !connected) return;
    if (state.consumerGroupInventory.state === "unavailable") requestConsumerGroups();
  }, [navigation, connected, state.consumerGroupInventory.state, requestConsumerGroups]);
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
  const operationStatus =
    state.consumptionState === "unavailable"
      ? "Idle"
      : consumptionStateLabel(state.consumptionState);
  const ruleNotification = state.ruleNotifications[0];
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
    onOpenPlugins: () => {
      setPreferenceSection("plugins");
      openPreferences();
    },
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
    ...(profileConnectionError === undefined ? {} : { connectionError: profileConnectionError }),
    ...(profileRequestError === undefined ? {} : { requestError: profileRequestError }),
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
    onOpenPlugins: () => {
      setPreferenceSection("plugins");
      openPreferences();
    },
    profile: selectedProfile,
    transfer: textDocumentTransfer,
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
              onOpenObservedHealth={() => selectNavigation("observations")}
              onStop={() => void stopConsumption()}
              stopActionLabel={
                state.consumptionRequest?.topic === selectedTopic
                  ? kafkaConsumptionStopLabel(
                      state.consumptionState,
                      state.consumptionRequest,
                      state.consumptionError,
                    )
                  : null
              }
              consumptionStopping={consumptionStopping}
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
          profiles={state.profiles}
          canWrite={state.preferenceSnapshot?.preferences.protection.readOnly === false}
          host={host}
          unavailableFilterRecords={messageSelection.unavailable}
          readCoverage={state.readCoverage}
          onSearch={(filter) => {
            if (selectedTopic !== null) void startConsumption(selectedTopic, filter);
          }}
          component="section"
          connectionAvailable={connected}
          consumptionError={state.consumptionError}
          consumptionRequest={state.consumptionRequest}
          consumptionState={state.consumptionState}
          consumptionStopping={consumptionStopping}
          droppedMessages={state.droppedMessages}
          fetchMaximum={fetchMaximum}
          fetchMode={fetchMode}
          timeWindow={timeWindow}
          filters={state.messageFilters}
          key={`${state.connectionName ?? "disconnected"}:${state.connectionState}:${selectedTopic ?? "no-topic"}:${JSON.stringify(state.preferenceSnapshot?.preferences.protection)}`}
          liveRuleCapability={state.liveRuleCapability}
          messages={visibleMessages}
          messagesStale={state.messagesStale}
          onClearFilters={() => dispatch({ type: "messages.filters.cleared" })}
          onClearSelection={() => {
            setSelectedMessageId(null);
            setSelectionNotice(undefined);
          }}
          onFetchMaximumChange={setFetchMaximum}
          onFetchModeChange={(mode) => {
            setFetchMode(mode);
            if (mode === "tail" && state.messageFilters.offsetExact !== undefined) {
              dispatch({ type: "messages.filter.text.changed", field: "offset", value: "" });
            }
          }}
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
            if (selectedTopic !== null) {
              const { key, value, offset, offsetExact, timestamp, partition, expression } =
                state.messageFilters;
              void startConsumption(
                selectedTopic,
                offsetExact === undefined || fetchMode === "tail"
                  ? undefined
                  : {
                      key,
                      value,
                      offset,
                      offsetExact,
                      timestamp,
                      partition,
                      ...(expression === undefined ? {} : { expression }),
                    },
              );
            }
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
        host={host}
        canWrite={state.preferenceSnapshot?.preferences.protection.readOnly === false}
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
    ) : navigation === "relationships" ? (
      <RelationshipsPage
        key={`${state.connectionName ?? "disconnected"}:${state.connectionState}`}
        host={host}
      />
    ) : navigation === "observations" ? (
      <ObservedHealthPage
        key={`${state.connectionName ?? "disconnected"}:${state.connectionState}`}
        host={host}
        backendAvailable={state.backend === "ready"}
        connected={connected}
        connectionName={state.connectionName}
        initialTopic={selectedTopic ?? ""}
        initialGroupId={selectedConsumerGroupId ?? ""}
        topics={state.topics}
        groupInventory={state.consumerGroupInventory}
        inventoryStatus={`${state.topics.length} topics (${state.topicListState}); ${state.consumerGroupInventory.groups.length} groups (${state.consumerGroupInventory.state})${state.consumerGroupInventory.omittedGroups > 0 ? `; ${state.consumerGroupInventory.omittedGroups} groups omitted from the bounded inventory` : ""}. Refresh if a resource is missing.`}
        onRefreshResources={() => {
          void requestTopics();
          requestConsumerGroups();
        }}
        onOpenTopic={(topic) => {
          dispatch({ type: "messages.filters.cleared" });
          activateTopic(topic);
        }}
        onOpenGroup={(groupId) => {
          changeNavigation("consumer-groups");
          selectConsumerGroup(groupId);
        }}
        onOpenRecord={(locator) => {
          dispatch({
            type: "query.restored",
            filters: {
              key: "",
              value: "",
              offset: "",
              offsetExact: locator.offset,
              timestamp: "",
              partition: locator.partition,
            },
          });
          openObservedRecord(locator);
        }}
      />
    ) : navigation === "connect" ? (
      <ConnectPage
        key={state.connectionName ?? "disconnected"}
        host={host}
        canWrite={state.preferenceSnapshot?.preferences.protection.readOnly === false}
        onOpenTopic={activateTopic}
      />
    ) : navigation === "environments" ? (
      <EnvironmentPage
        key={state.connectionName ?? "disconnected"}
        host={host}
        profiles={state.profiles}
        transfer={textDocumentTransfer}
        canWrite={state.preferenceSnapshot?.preferences.protection.readOnly === false}
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
      <AclPage
        connected={connected}
        canWrite={state.preferenceSnapshot?.preferences.protection.readOnly === false}
        host={host}
        snapshot={state.aclSnapshot}
      />
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
        createAction={
          <ReviewedWriteAction
            host={host}
            disabled={
              !connected || state.preferenceSnapshot?.preferences.protection.readOnly !== false
            }
            onCreated={() => {
              void requestTopics();
            }}
          />
        }
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
        action={
          <ReviewedWriteAction
            key={selectedTopic}
            host={host}
            topic={selectedTopic}
            disabled={
              !connected || state.preferenceSnapshot?.preferences.protection.readOnly !== false
            }
          />
        }
        onWorkspaceChange={changeTopicWorkspace}
        selectedTopic={selectedTopic}
        workspace={topicWorkspace}
      >
        {renderTopicWorkspace()}
      </TopicDetailPage>
    );

  const resources = WORKBENCH_RESOURCE_GROUPS.map((group) => ({
    ...group,
    items: group.items.map((item) => ({
      ...item,
      available: isNavigationAvailable(item.value, connected),
    })),
  }));

  return (
    <ProviderWorkbenchShell
      navigator={navigator}
      resources={resources}
      navigation={navigation}
      resourceLabel="Kafka resources"
      onNavigate={selectNavigation}
      providerControl={providerControl}
      headerActions={
        <StudioButton aria-label="Saved queries" onClick={() => setQueriesOpen(true)}>
          Queries
        </StudioButton>
      }
      onOpenCommandPalette={() => setCommandPaletteOpen(true)}
      onOpenPreferences={() => {
        setPreferenceSection("workbench");
        openPreferences();
      }}
      breadcrumbs={
        <WorkbenchBreadcrumbs
          navigation={navigation}
          onNavigate={selectNavigation}
          selectedConsumerGroupId={selectedConsumerGroupId}
          selectedTopic={selectedTopic}
        />
      }
      activityHeight={activityOpen ? activityHeight : streamSkopeLayout.activityCollapsedHeight}
      activity={
        <ActivityLogDrawer
          initialQuery={activityQuery}
          entries={state.activities}
          height={activityHeight}
          onClose={activityOpen ? closeActivity : openActivity}
          onHeightChange={setActivityHeight}
          open={activityOpen}
          transfer={textDocumentTransfer}
        />
      }
      status={
        <WorkbenchStatusBar
          activeConnectionName={state.connectionName}
          backend={state.backend}
          connectionState={state.connectionState}
          droppedMessages={state.droppedMessages}
          messageRequestError={messageRequestError}
          onReload={() => window.location.reload()}
          operationStatus={operationStatus}
          resourceStatus={resourceStatus}
          protection={state.preferenceSnapshot}
        />
      }
      overlays={
        <>
          {queriesOpen ? (
            <SavedQueriesDialog
              host={host}
              transfer={textDocumentTransfer}
              initialImport={queryImport}
              profiles={state.profiles}
              currentTopic={selectedTopic}
              readActive={
                ["loading", "fetching", "streaming"].includes(state.consumptionState) ||
                (state.consumptionState === "empty" && state.consumptionRequest?.mode === "tail")
              }
              captureCurrent={captureQuery}
              onClose={() => {
                setQueriesOpen(false);
                setQueryImport(undefined);
              }}
              onRestore={(query, profileId) => {
                const profile = state.profiles.find((entry) => entry.id === profileId);
                const needsConnection =
                  !connected || (profile !== undefined && profile.name !== state.connectionName);
                restoreQuery(query, needsConnection);
                dispatch({
                  type: "query.restored",
                  filters: query.filters ?? query.request.search ?? initialKafkaMessageFilters,
                });
                if (profile !== undefined) setSelectedProfileId(profile.id);
                setNavigation(needsConnection ? "profiles" : "topics");
                setQueriesOpen(false);
                setQueryImport(undefined);
              }}
            />
          ) : null}
          <WorkbenchCommandPalette
            actions={investigationCommands({
              connected,
              selectedTopic,
              selectedProfile,
              profileBusy: profileConnectionOperation !== null,
              readActive: isKafkaConsumptionActive(
                state.consumptionState,
                state.consumptionRequest,
              ),
              stopping: consumptionStopping,
              mode: fetchMode,
              timeError: timeWindow.error,
              filters: state.messageFilters,
              openQueries: () => setQueriesOpen(true),
              toggleProfile: toggleProfileConnection,
              startRead: startConsumption,
              stopRead: stopConsumption,
            })}
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
                disconnected={["disconnected", "failed"].includes(state.connectionState)}
                initialSection={preferenceSection}
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
        </>
      }
    >
      {page}
    </ProviderWorkbenchShell>
  );
}
