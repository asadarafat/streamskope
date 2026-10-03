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

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { StreamSkopeDesktop } from "../../../platform/desktop";
import { streamSkopeLayout } from "../../../platform/ui/createStreamSkopeTheme";

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
} from "./workbench-status";
import { WorkbenchApplicationBar } from "./WorkbenchApplicationBar";
import { WorkbenchCommandPalette } from "./WorkbenchCommandPalette";
import { investigationCommands } from "./workbench-query-commands";
import { WorkbenchBreadcrumbs } from "./WorkbenchBreadcrumbs";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";
import { WorkbenchSidebar } from "./WorkbenchSidebar";
import { useRendererStreamMonitorLifecycle } from "./workbench-runtime-effects";
import { useWorkbenchActivity } from "./use-workbench-activity";
import { useWorkbenchProfiles } from "./use-workbench-profiles";
import { useWorkbenchTopics } from "./use-workbench-topics";
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
  readonly initialQueryImport?: string | undefined;
  readonly streamMonitorObserver?: RendererStreamMonitorObserver;
}

export function StreamSkopeWorkbench({
  desktop,
  host,
  streamMonitorObserver,
  initialQueryImport,
}: StreamSkopeWorkbenchProperties): React.JSX.Element {
  const [queriesOpen, setQueriesOpen] = useState(initialQueryImport !== undefined);
  const [queryImport, setQueryImport] = useState(initialQueryImport);
  const [state, dispatch] = useReducer(reduceKafkaUiState, initialKafkaUiState);
  const [rendererStreamMonitor] = useState(
    () => streamMonitorObserver ?? createRendererStreamMonitorObserver(),
  );
  const [navigation, setNavigation] = useState<NavigationView>("profiles");
  const compactDesktop = useMediaQuery(
    `(max-width:${String(streamSkopeLayout.fullDesktopMinimumWidth - 0.05)}px)`,
  );
  const [profileRequestError, setProfileRequestError] = useState<string>();
  const [resourcePaneOpen, setResourcePaneOpen] = useState(false);
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
  } = useWorkbenchActivity(desktop, state.activities);
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
    stopConsumption,
  } = useWorkbenchTopics(host, connected, navigation, setNavigation, state);

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
          onOpenQueries={() => setQueriesOpen(true)}
          navigatorOpen={resourcePaneOpen}
          navigatorTemporary={contextPaneTemporary}
          onOpenCommandPalette={() => setCommandPaletteOpen(true)}
          onOpenPreferences={() => {
            setPreferenceSection("workbench");
            openPreferences();
          }}
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
          protection={state.preferenceSnapshot}
        />
      </Box>

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
          readActive: isKafkaConsumptionActive(state.consumptionState, state.consumptionRequest),
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
    </Box>
  );
}
