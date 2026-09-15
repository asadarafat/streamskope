import { lazy, Suspense, useMemo, useState } from "react";
import { Box, Drawer, Skeleton, Stack, Typography, useMediaQuery } from "@mui/material";

import {
  KAFKA_FETCH_MODE_LABELS,
  type ConsumptionState,
  type HostError,
  type KafkaExploredMessage,
  type KafkaFetchMode,
  type KafkaFetchRequest,
  type KafkaLiveRuleCapability,
} from "../contracts";
import { streamSkopeLayout } from "../../ui/createStreamSkopeTheme";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../ui/controls";

import {
  KafkaMessageOperationError,
  KAFKA_MESSAGE_OPERATION_LIMITS,
  countActiveKafkaMessageFilters,
  createKafkaMessageExportDocument,
  type KafkaMessageFilters,
  type KafkaMessageTextFilterField,
} from "./message-operations";
import { HorizontalPaneSeparator, usePersistentPaneWidth } from "./HorizontalPaneSeparator";
import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";
import { StatusIndicator, type StatusIndicatorTone } from "./StatusIndicator";
import { WorkspaceState } from "./WorkspaceState";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";
import { consumptionStateLabel, isKafkaConsumptionActive } from "./workbench-status";
import { WorkbenchIcon } from "./WorkbenchIcons";

const LazyMessageDataGrid = lazy(() => import("./MessageDataGrid"));
const LazyMessageInspector = lazy(() => import("./MessageInspector"));
const FETCH_MAXIMUM_PRESETS = [10, 100, 500, 1_000] as const;

export function MessageWorkspace({
  component = "main",
  connectionAvailable,
  consumptionError,
  consumptionRequest,
  consumptionState,
  consumptionStopping,
  droppedMessages,
  fetchMaximum,
  fetchMode,
  filters,
  liveRuleCapability,
  messages,
  messagesStale,
  onClearFilters,
  onClearSelection,
  onFetchMaximumChange,
  onFetchModeChange,
  onPartitionFilterChange,
  onRuleFilterChange,
  onSelectMessage,
  onStart,
  onStop,
  onTextFilterChange,
  retainedMessageCount,
  windowEvictions = 0,
  savedProfileCount,
  selectedMessage,
  selectedMessageId,
  selectedTopic,
  selectionNotice,
  transfer = browserTextDocumentTransfer,
}: {
  readonly component?: "main" | "section";
  readonly connectionAvailable: boolean;
  readonly consumptionError: HostError | null;
  readonly consumptionRequest: KafkaFetchRequest | null;
  readonly consumptionState: ConsumptionState;
  readonly consumptionStopping: boolean;
  readonly droppedMessages: number;
  readonly fetchMaximum: number;
  readonly fetchMode: KafkaFetchMode;
  readonly filters: KafkaMessageFilters;
  readonly liveRuleCapability: KafkaLiveRuleCapability;
  readonly messages: readonly KafkaExploredMessage[];
  readonly messagesStale: boolean;
  readonly onClearFilters: () => void;
  readonly onClearSelection: () => void;
  readonly onFetchMaximumChange: (maximum: number) => void;
  readonly onFetchModeChange: (mode: KafkaFetchMode) => void;
  readonly onPartitionFilterChange: (partition: number | null) => void;
  readonly onRuleFilterChange: (activeOnly: boolean) => void;
  readonly onSelectMessage: (id: string | null) => void;
  readonly onStart: () => void;
  readonly onStop: () => void;
  readonly onTextFilterChange: (field: KafkaMessageTextFilterField, value: string) => void;
  readonly retainedMessageCount: number;
  readonly windowEvictions?: number;
  readonly savedProfileCount: number;
  readonly selectedMessage: KafkaExploredMessage | null;
  readonly selectedMessageId: string | null;
  readonly selectedTopic: string | null;
  readonly selectionNotice: string | undefined;
  readonly transfer?: TextDocumentTransferPort;
}): React.JSX.Element {
  const compactDesktop = useMediaQuery(
    `(max-width:${String(streamSkopeLayout.fullDesktopMinimumWidth - 0.05)}px)`,
  );
  const compactInspectorToolbar = useMediaQuery(
    `(max-width:${String(streamSkopeLayout.messageInspectorFullColumnsMinimumWidth - 0.05)}px)`,
  );
  const [inspectorPaneWidth, setInspectorPaneWidth] = usePersistentPaneWidth(
    "streamskope-inspector-pane-width",
    streamSkopeLayout.inspectorDefaultWidth,
    streamSkopeLayout.inspectorMinimumWidth,
    streamSkopeLayout.inspectorMaximumWidth,
  );
  const [exportError, setExportError] = useState<string>();
  const [exportStatus, setExportStatus] = useState("");
  const [exporting, setExporting] = useState(false);
  const [filterPanelOpen, setFilterPanelOpen] = useState(false);
  const topicMatches =
    selectedTopic !== null &&
    consumptionRequest !== null &&
    selectedTopic === consumptionRequest.topic;
  const active = isKafkaConsumptionActive(consumptionState, consumptionRequest);
  const operationBelongsToTopic = active && topicMatches;
  const readPlanningControlsVisible = selectedMessage === null || !compactInspectorToolbar;
  const fetchMaximumOptions = useMemo(
    () =>
      FETCH_MAXIMUM_PRESETS.some((maximum) => maximum === fetchMaximum)
        ? FETCH_MAXIMUM_PRESETS
        : [...FETCH_MAXIMUM_PRESETS, fetchMaximum].sort((left, right) => left - right),
    [fetchMaximum],
  );
  const visibleMessages = topicMatches ? messages : [];
  const activeFilterCount = countActiveKafkaMessageFilters(filters);
  const reusableProfilesAvailable = savedProfileCount > 0;
  const statusLabel = consumptionStateLabel(consumptionState);
  const statusTone: StatusIndicatorTone =
    consumptionState === "failed"
      ? "error"
      : consumptionState === "streaming"
        ? "success"
        : consumptionState === "fetching" || consumptionState === "loading"
          ? "info"
          : "neutral";
  const requestWindow =
    consumptionRequest?.mode === "time-window"
      ? `${new Date(consumptionRequest.startTimeMs).toISOString()} → ${new Date(
          consumptionRequest.endTimeMs,
        ).toISOString()}`
      : null;
  const requestWindowStart =
    consumptionRequest?.mode === "time-window"
      ? new Date(consumptionRequest.startTimeMs).toISOString()
      : null;
  const readActionLabel = operationBelongsToTopic
    ? consumptionRequest?.mode === "tail"
      ? "Stop tail"
      : "Cancel fetch"
    : fetchMode === "tail"
      ? "Start tail"
      : "Load messages";
  let emptyTitle = connectionAvailable
    ? "Select a topic"
    : reusableProfilesAvailable
      ? "Choose a connection profile"
      : "Messages unavailable";
  let emptyDetail = connectionAvailable
    ? "Choose a topic from Resources to begin message exploration."
    : reusableProfilesAvailable
      ? "Connect a reusable profile from Profiles, or continue with an ad hoc session."
      : "Configure a Kafka connection to browse topics, consume messages, and inspect failures.";
  if (selectedTopic !== null && !topicMatches) {
    emptyTitle = connectionAvailable ? "Ready to read" : "Reconnect to read this topic";
    emptyDetail = connectionAvailable
      ? "Use the message toolbar to start another Kafka read."
      : "Connect a profile to load current records. Previously retained evidence remains unchanged.";
  } else if (topicMatches) {
    if (consumptionState === "loading") {
      emptyTitle = "Starting consumption";
      emptyDetail = `Opening a consumer for ${selectedTopic}.`;
    } else if (consumptionState === "empty") {
      emptyTitle =
        consumptionRequest?.mode === "tail"
          ? "Topic currently has no messages"
          : "No messages found";
      emptyDetail =
        consumptionRequest?.mode === "tail"
          ? "The consumer is active. New records will appear here."
          : "The snapshot contains no readable records.";
    } else if (consumptionState === "fetching") {
      emptyTitle = "Fetching snapshot";
      emptyDetail = "Reading the requested messages from Kafka.";
    } else if (consumptionState === "streaming") {
      emptyTitle = "Waiting for messages";
      emptyDetail = "The consumer is active and no records are retained yet.";
    } else if (consumptionState === "complete") {
      emptyTitle = "Snapshot complete";
      emptyDetail = "The snapshot completed with no messages to display.";
    } else if (consumptionState === "stopped") {
      emptyTitle = "Consumption stopped";
      emptyDetail = "No messages were retained for this topic.";
    }
    if (activeFilterCount > 0 && retainedMessageCount > 0 && visibleMessages.length === 0) {
      emptyTitle = "No messages match the current filters";
      emptyDetail = "Clear or change the message filters to show retained records.";
    }
  }
  const operationalNoticeVisible =
    exportStatus.length > 0 ||
    exportError !== undefined ||
    selectionNotice !== undefined ||
    (topicMatches &&
      (liveRuleCapability.state !== "ready" ||
        messagesStale ||
        droppedMessages > 0 ||
        consumptionState === "failed"));

  function resetExportFeedback(): void {
    setExportError(undefined);
    setExportStatus("");
  }

  async function exportFilteredMessages(): Promise<void> {
    if (selectedTopic === null || visibleMessages.length === 0 || exporting) {
      return;
    }
    setExporting(true);
    resetExportFeedback();
    try {
      const document = createKafkaMessageExportDocument({
        filters,
        messages: visibleMessages,
        retainedMessageCount,
        stale: messagesStale,
        topic: selectedTopic,
      });
      const outcome = await transfer.download(document);
      setExportStatus(
        outcome === "cancelled"
          ? "Filtered message export cancelled."
          : outcome === "saved"
            ? "Filtered message JSON saved."
            : "Filtered message JSON download started.",
      );
    } catch (error) {
      setExportError(
        error instanceof KafkaMessageOperationError
          ? `${error.message} ${error.recovery}`
          : "The filtered JSON export failed. No file was saved. Retry the export.",
      );
    } finally {
      setExporting(false);
    }
  }

  const messageInspector =
    selectedMessage === null ? null : (
      <Suspense
        fallback={<Skeleton aria-label="Loading message inspector" variant="rectangular" />}
      >
        <LazyMessageInspector
          message={selectedMessage}
          onClose={onClearSelection}
          transfer={transfer}
        />
      </Suspense>
    );

  return (
    <Box
      aria-label="Message workspace"
      component={component}
      sx={{
        bgcolor: "background.paper",
        display: "grid",
        height: "100%",
        gridTemplateColumns:
          selectedMessage === null || compactDesktop
            ? "minmax(0, 1fr)"
            : `minmax(0, 1fr) 5px ${String(inspectorPaneWidth)}px`,
        gridTemplateRows: "minmax(0, 1fr)",
        minHeight: 0,
        minWidth: 0,
      }}
    >
      <Box
        sx={{
          bgcolor: "background.paper",
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        {selectedTopic === null ? null : (
          <TopicWorkspaceToolbar label="Message controls">
            {readPlanningControlsVisible ? (
              <>
                <Stack direction="row" sx={{ alignItems: "center", flex: "0 0 auto", gap: 0.5 }}>
                  <Typography color="text.secondary" variant="caption">
                    Read
                  </Typography>
                  <Select
                    disabled={active}
                    inputProps={{ "aria-label": "Read mode" }}
                    onChange={(event) => onFetchModeChange(event.target.value)}
                    sx={{ width: 112 }}
                    value={fetchMode}
                  >
                    {(["tail", "newest", "earliest", "time-window"] as const).map((mode) => (
                      <MenuItem key={mode} value={mode}>
                        {KAFKA_FETCH_MODE_LABELS[mode]}
                      </MenuItem>
                    ))}
                  </Select>
                </Stack>
                <Stack direction="row" sx={{ alignItems: "center", flex: "0 0 auto", gap: 0.5 }}>
                  <Typography color="text.secondary" variant="caption">
                    Limit
                  </Typography>
                  <Select
                    disabled={active}
                    inputProps={{ "aria-label": "Record limit" }}
                    onChange={(event) => onFetchMaximumChange(Number(event.target.value))}
                    sx={{ width: 92 }}
                    value={fetchMaximum}
                  >
                    {fetchMaximumOptions.map((maximum) => (
                      <MenuItem key={maximum} value={maximum}>
                        {maximum.toLocaleString()}
                      </MenuItem>
                    ))}
                  </Select>
                </Stack>
              </>
            ) : null}
            <Button
              aria-label={`${readActionLabel} ${selectedTopic}`}
              disabled={
                operationBelongsToTopic ? consumptionStopping : !connectionAvailable || active
              }
              onClick={operationBelongsToTopic ? onStop : onStart}
              size="small"
              startIcon={<WorkbenchIcon name={operationBelongsToTopic ? "stop" : "play"} />}
              sx={{ flex: "0 0 auto", whiteSpace: "nowrap" }}
              variant={operationBelongsToTopic ? "outlined" : "contained"}
            >
              {consumptionStopping ? "Stopping…" : readActionLabel}
            </Button>
            <Box sx={{ flex: "1 1 96px", minWidth: 0 }}>
              {topicMatches && requestWindow !== null ? (
                <Typography
                  aria-label="Active fetch request"
                  color="text.secondary"
                  component="p"
                  variant="caption"
                >
                  <time dateTime={requestWindowStart ?? undefined}>{requestWindow}</time>
                </Typography>
              ) : null}
            </Box>
            <Typography
              aria-label={`Showing ${visibleMessages.length.toLocaleString()} of ${retainedMessageCount.toLocaleString()} retained messages`}
              color="text.secondary"
              noWrap
              sx={{ flex: "0 0 auto" }}
              title={`Showing ${visibleMessages.length.toLocaleString()} of ${retainedMessageCount.toLocaleString()} retained messages`}
              variant="caption"
            >
              {`${visibleMessages.length.toLocaleString()} / ${retainedMessageCount.toLocaleString()}`}
            </Typography>
            {windowEvictions > 0 ? (
              <Typography
                aria-label="Live window evictions"
                color="text.secondary"
                variant="caption"
                title="Records removed from the bounded display window, not Kafka or export processing loss."
              >
                {windowEvictions.toLocaleString()} evicted from view
              </Typography>
            ) : null}
            <Button
              aria-label={filterPanelOpen ? "Hide message filters" : "Show message filters"}
              aria-controls="kafka-message-filter-region"
              aria-expanded={filterPanelOpen}
              onClick={() => {
                setFilterPanelOpen((current) => !current);
              }}
              size="small"
              sx={{ flex: "0 0 auto", whiteSpace: "nowrap" }}
              variant="text"
            >
              Filters{activeFilterCount === 0 ? "" : ` · ${activeFilterCount.toLocaleString()}`}
            </Button>
            <Button
              aria-label="Export filtered JSON"
              disabled={visibleMessages.length === 0 || exporting}
              onClick={() => {
                void exportFilteredMessages();
              }}
              size="small"
              sx={{ flex: "0 0 auto", whiteSpace: "nowrap" }}
              variant="outlined"
            >
              {exporting ? "Preparing…" : "Export"}
            </Button>
            <StatusIndicator
              ariaLabel="Consumption status"
              label={statusLabel}
              live="polite"
              tone={statusTone}
            />
          </TopicWorkspaceToolbar>
        )}
        {topicMatches && filterPanelOpen ? (
          <Box
            aria-label="Message filters"
            id="kafka-message-filter-region"
            role="region"
            sx={{
              borderBottom: 1,
              borderColor: "divider",
              display: "grid",
              gap: 1,
              gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              px: 2,
              py: 1,
            }}
          >
            <TextField
              fullWidth
              label="Timestamp contains"
              onChange={(event) => {
                resetExportFeedback();
                onTextFilterChange("timestamp", event.target.value);
              }}
              size="small"
              slotProps={{
                htmlInput: { maxLength: KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters },
              }}
              value={filters.timestamp}
            />
            <TextField
              fullWidth
              label="Partition"
              onChange={(event) => {
                const value = event.target.value;
                if (value.length === 0) {
                  resetExportFeedback();
                  onPartitionFilterChange(null);
                  return;
                }
                if (/^\d+$/u.test(value)) {
                  const partition = Number(value);
                  if (Number.isSafeInteger(partition)) {
                    resetExportFeedback();
                    onPartitionFilterChange(partition);
                  }
                }
              }}
              size="small"
              slotProps={{
                htmlInput: {
                  inputMode: "numeric",
                  min: 0,
                  step: 1,
                  type: "number",
                },
              }}
              value={filters.partition ?? ""}
            />
            {(
              [
                ["offset", "Offset contains"],
                ["key", "Key contains"],
                ["value", "Value or retained preview contains"],
              ] as const satisfies readonly (readonly [KafkaMessageTextFilterField, string])[]
            ).map(([field, label]) => (
              <TextField
                fullWidth
                key={field}
                label={label}
                onChange={(event) => {
                  resetExportFeedback();
                  onTextFilterChange(field, event.target.value);
                }}
                size="small"
                slotProps={{
                  htmlInput: { maxLength: KAFKA_MESSAGE_OPERATION_LIMITS.filterCharacters },
                }}
                value={filters[field]}
              />
            ))}
            <Stack
              direction="row"
              spacing={1}
              sx={{
                alignItems: "center",
                flexWrap: "wrap",
                gridColumn: "1 / -1",
                justifyContent: "space-between",
                rowGap: 0.5,
              }}
            >
              <Typography color="text.secondary" noWrap variant="caption">
                {`${activeFilterCount.toLocaleString()} active ${
                  activeFilterCount === 1 ? "filter" : "filters"
                }`}
              </Typography>
              <Button
                disabled={activeFilterCount === 0}
                onClick={() => {
                  resetExportFeedback();
                  onClearFilters();
                }}
                size="small"
                sx={{ whiteSpace: "nowrap" }}
                variant="text"
              >
                Clear message filters
              </Button>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={filters.activeRuleMatchesOnly}
                    onChange={(event) => {
                      resetExportFeedback();
                      onRuleFilterChange(event.target.checked);
                    }}
                    size="small"
                  />
                }
                label="Rule matches only"
                sx={{ m: 0, whiteSpace: "nowrap" }}
              />
            </Stack>
          </Box>
        ) : null}
        <Stack
          spacing={0.5}
          sx={{
            bgcolor: operationalNoticeVisible ? "background.paper" : undefined,
            borderBottom: operationalNoticeVisible ? 1 : 0,
            borderColor: "divider",
            px: 2,
            py: operationalNoticeVisible ? 1 : 0,
          }}
        >
          {exportStatus.length === 0 ? null : (
            <Typography
              aria-label="Message operation status"
              aria-live="polite"
              color="text.secondary"
              role="status"
              variant="body2"
            >
              {exportStatus}
            </Typography>
          )}
          {exportError === undefined ? null : <Alert severity="error">{exportError}</Alert>}
          {selectionNotice === undefined ? null : <Alert severity="info">{selectionNotice}</Alert>}
          {topicMatches && liveRuleCapability.state === "partial" ? (
            <Alert severity="warning">
              {liveRuleCapability.omittedRules.toLocaleString()} applicable live rules were omitted
              from each record; rule results are partial.
            </Alert>
          ) : null}
          {topicMatches && liveRuleCapability.state === "unavailable" ? (
            <Alert severity="warning">
              <Typography component="p" variant="subtitle2">
                Live rule evaluation is unavailable.
              </Typography>
              <Typography component="p" variant="body2">
                {liveRuleCapability.recovery ??
                  "Raw Kafka messages remain available. Open Activity for diagnostics."}
              </Typography>
            </Alert>
          ) : null}
          {messagesStale && topicMatches ? (
            <Alert severity="warning">
              These messages are stale because the active connection ended.
            </Alert>
          ) : null}
          {droppedMessages > 0 && topicMatches ? (
            <Alert severity="warning">
              {droppedMessages.toLocaleString()} messages were dropped or omitted to keep memory
              bounded.
            </Alert>
          ) : null}
          {consumptionState === "failed" && topicMatches ? (
            <Alert severity="error">
              <Typography component="p" variant="subtitle2">
                {consumptionError?.summary ?? "Message consumption failed."}
              </Typography>
              <Typography component="p" variant="body2">
                {consumptionError?.recovery ?? "Open Activity for diagnostics, then retry."}
              </Typography>
            </Alert>
          ) : null}
        </Stack>
        <Box
          sx={{
            bgcolor: "background.default",
            display: "flex",
            flex: "1 1 0",
            flexDirection: "column",
            minHeight:
              visibleMessages.length > 0
                ? streamSkopeLayout.tableHeaderHeight + streamSkopeLayout.tableRowHeight
                : 0,
            overflow: "hidden",
          }}
        >
          {visibleMessages.length > 0 ? (
            <Suspense
              fallback={
                <Stack aria-label="Loading message table" spacing={0.5} sx={{ p: 2 }}>
                  <Skeleton height={38} variant="rectangular" />
                  <Skeleton height={36} variant="rectangular" />
                  <Skeleton height={36} variant="rectangular" />
                </Stack>
              }
            >
              <LazyMessageDataGrid
                compactInspectorColumns={selectedMessage !== null && !compactDesktop}
                messages={visibleMessages}
                onSelectMessage={onSelectMessage}
                selectedMessageId={selectedMessageId}
              />
            </Suspense>
          ) : (
            <Box
              sx={{
                display: "grid",
                flex: "1 1 0",
                minHeight: 0,
                p: 2,
                placeItems: "center",
              }}
            >
              <WorkspaceState
                detail={emptyDetail}
                label="Message workspace state"
                title={emptyTitle}
              />
            </Box>
          )}
        </Box>
      </Box>
      {selectedMessage === null ? null : compactDesktop ? (
        <Drawer
          anchor="right"
          onClose={onClearSelection}
          open
          slotProps={{
            paper: {
              "aria-label": "Selected message inspector",
              sx: {
                maxWidth: "100%",
                width: "min(92vw, 440px)",
              },
            },
          }}
        >
          {messageInspector}
        </Drawer>
      ) : (
        <>
          <HorizontalPaneSeparator
            label="Resize messages and inspector"
            maximum={streamSkopeLayout.inspectorMaximumWidth}
            minimum={streamSkopeLayout.inspectorMinimumWidth}
            onChange={setInspectorPaneWidth}
            paneSide="after"
            value={inspectorPaneWidth}
          />
          {messageInspector}
        </>
      )}
    </Box>
  );
}
