import { useEffect, useMemo, useState } from "react";
import { Box, List, ListItem, ListItemText, Skeleton, Stack, Typography } from "@mui/material";
import {
  DataGrid,
  type GridColDef,
  type GridRowParams,
  type GridRowSelectionModel,
} from "@mui/x-data-grid";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_TOPIC_CONFIGURATION_PRESETS,
  type KafkaTopicConfigurationChange,
  type KafkaTopicConfigurationEntry,
  type KafkaTopicConfigurationHistorySnapshot,
  type KafkaTopicConfigurationPresetId,
  type KafkaTopicConfigurationSnapshot,
  type StreamSkopeHost,
} from "../contracts";
import {
  streamSkopeLayout,
  streamSkopeMuiMonospaceTypography,
} from "../../ui/createStreamSkopeTheme";
import {
  StudioAccordion as Accordion,
  StudioAccordionDetails as AccordionDetails,
  StudioAccordionSummary as AccordionSummary,
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../ui/controls";

import { KafkaConfigurationDocumentation } from "./KafkaConfigurationDocumentation";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";
import { StatusIndicator, type StatusIndicatorTone } from "./StatusIndicator";
import { formatUtcTimestamp } from "./timestamp-presentation";

export interface TopicConfigurationWorkspaceProperties {
  readonly component?: "main" | "section";
  readonly connectionName: string | null;
  readonly history: KafkaTopicConfigurationHistorySnapshot | null;
  readonly host: StreamSkopeHost;
  readonly selectedTopic: string | null;
  readonly snapshot: KafkaTopicConfigurationSnapshot;
}

const columns: readonly GridColDef<KafkaTopicConfigurationEntry>[] = [
  {
    field: "name",
    flex: 1,
    headerName: "Configuration",
    minWidth: 220,
  },
  {
    field: "value",
    flex: 1,
    headerName: "Current value",
    minWidth: 180,
    valueGetter: (_value, row): string => (row.isSensitive ? "Sensitive" : (row.value ?? "—")),
  },
  {
    field: "source",
    headerName: "Source",
    minWidth: 130,
    width: 150,
  },
  {
    field: "type",
    headerName: "Type",
    minWidth: 90,
    width: 110,
  },
  {
    field: "access",
    headerName: "Access",
    minWidth: 100,
    sortable: false,
    valueGetter: (_value, row): string => (row.readOnly ? "Read-only" : "Writable"),
    width: 120,
  },
];

function scopedSnapshot(
  snapshot: KafkaTopicConfigurationSnapshot,
  connectionName: string | null,
  topic: string | null,
): KafkaTopicConfigurationSnapshot {
  return snapshot.connectionName === connectionName && snapshot.topic === topic
    ? snapshot
    : {
        connectionName: null,
        entries: [],
        refreshedAt: null,
        state: "unavailable",
        topic: null,
      };
}

function configurationComparison(
  change: KafkaTopicConfigurationChange,
  entries: readonly KafkaTopicConfigurationEntry[],
): string {
  const entry = entries.find((candidate) => candidate.name === change.name);
  const sensitive = change.isSensitive || entry?.isSensitive === true;
  const currentValue = sensitive ? "Sensitive value" : (entry?.value ?? "Unavailable");
  const proposedValue = sensitive ? "Sensitive value" : change.value;
  const source =
    entry === undefined
      ? "source unavailable"
      : `${entry.source}${entry.isDefault ? ", inherited/default" : ""}`;
  const unchanged = !sensitive && entry?.value === change.value ? " · Unchanged" : "";
  return `Current: ${currentValue} (${source}) → Proposed: ${proposedValue}${unchanged}`;
}

function replaceChange(
  changes: readonly KafkaTopicConfigurationChange[],
  next: KafkaTopicConfigurationChange,
): readonly KafkaTopicConfigurationChange[] {
  const index = changes.findIndex((change) => change.name === next.name);
  return index < 0
    ? [...changes, next]
    : changes.map((change, changeIndex) => (changeIndex === index ? next : change));
}

function operationPayload(
  topic: string,
  changes: readonly KafkaTopicConfigurationChange[],
  presetId: KafkaTopicConfigurationPresetId | null,
): {
  readonly changes: readonly KafkaTopicConfigurationChange[];
  readonly presetId?: KafkaTopicConfigurationPresetId;
  readonly topic: string;
} {
  return {
    changes,
    ...(presetId === null ? {} : { presetId }),
    topic,
  };
}

function HistoryDialog({
  history,
  onClose,
  open,
}: {
  readonly history: KafkaTopicConfigurationHistorySnapshot | null;
  readonly onClose: () => void;
  readonly open: boolean;
}): React.JSX.Element {
  const durability =
    history?.store.durability === "durable" ? "Durable history" : "Session-only history";
  return (
    <Dialog
      aria-labelledby="topic-configuration-history-title"
      fullWidth
      maxWidth="md"
      onClose={onClose}
      open={open}
    >
      <DialogTitle id="topic-configuration-history-title">Configuration history</DialogTitle>
      <DialogContent dividers>
        {history === null ? (
          <Stack aria-label="Loading configuration history" spacing={1}>
            <Skeleton height={30} variant="rectangular" />
            <Skeleton height={54} variant="rectangular" />
          </Stack>
        ) : (
          <Stack spacing={2}>
            <Box>
              <Typography component="p" variant="subtitle2">
                {durability}
              </Typography>
              <Typography color="text.secondary" component="p" variant="body2">
                {history.store.state === "unavailable"
                  ? history.store.recovery
                  : `${history.entries.length.toLocaleString()} matching operation${
                      history.entries.length === 1 ? "" : "s"
                    }`}
              </Typography>
            </Box>
            {history.store.state === "unavailable" ? (
              <Alert severity="warning">
                Topic configuration history is unavailable. Kafka administration remains available.
              </Alert>
            ) : history.entries.length === 0 ? (
              <Typography color="text.secondary" variant="body2">
                No matching configuration operations have been recorded.
              </Typography>
            ) : (
              <List aria-label="Topic configuration history entries" disablePadding>
                {history.entries.map((entry) => (
                  <ListItem divider key={entry.id} sx={{ alignItems: "flex-start", px: 0 }}>
                    <ListItemText
                      primary={`${entry.action === "apply" ? "Apply" : "Validate"} · ${
                        entry.success ? "Succeeded" : "Failed"
                      }`}
                      secondary={
                        <Stack component="span" spacing={0.5} sx={{ mt: 0.5 }}>
                          <Typography
                            color="text.secondary"
                            component="time"
                            dateTime={entry.at}
                            title={entry.at}
                            variant="caption"
                          >
                            {formatUtcTimestamp(entry.at)} · {entry.connectionTarget}
                          </Typography>
                          {entry.changes.map((change) => (
                            <Typography component="span" key={change.name} variant="body2">
                              {change.name}: {change.from ?? "unset"} → {change.to}
                            </Typography>
                          ))}
                          {entry.error === undefined ? null : (
                            <Typography color="error" component="span" variant="body2">
                              {entry.error}
                            </Typography>
                          )}
                          {entry.warning === undefined ? null : (
                            <Typography color="warning.main" component="span" variant="body2">
                              {entry.warning}
                            </Typography>
                          )}
                        </Stack>
                      }
                    />
                  </ListItem>
                ))}
              </List>
            )}
          </Stack>
        )}
      </DialogContent>
      <DialogActions>
        <Button autoFocus onClick={onClose}>
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export function TopicConfigurationWorkspace({
  component = "main",
  connectionName,
  history,
  host,
  selectedTopic,
  snapshot,
}: TopicConfigurationWorkspaceProperties): React.JSX.Element {
  const current = scopedSnapshot(snapshot, connectionName, selectedTopic);
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const [draftValue, setDraftValue] = useState("");
  const [filter, setFilter] = useState("");
  const [historyOpen, setHistoryOpen] = useState(false);
  const [operation, setOperation] = useState<"apply" | "history" | "load" | "validate" | null>(
    null,
  );
  const [pending, setPending] = useState<readonly KafkaTopicConfigurationChange[]>([]);
  const [presetId, setPresetId] = useState<KafkaTopicConfigurationPresetId | null>(null);
  const [requestError, setRequestError] = useState<string>();
  const [selectedName, setSelectedName] = useState<string | null>(null);
  const [skippedNames, setSkippedNames] = useState<readonly string[]>([]);
  const scope = `${connectionName ?? ""}\u0000${selectedTopic ?? ""}`;

  useEffect(() => {
    setConfirmationOpen(false);
    setDraftValue("");
    setFilter("");
    setPending([]);
    setPresetId(null);
    setRequestError(undefined);
    setSelectedName(null);
    setSkippedNames([]);
  }, [scope]);

  useEffect(() => {
    if (connectionName === null || selectedTopic === null) {
      return;
    }
    let active = true;
    void host
      .execute({
        command: "topicConfiguration.load",
        id: globalThis.crypto.randomUUID(),
        payload: { topic: selectedTopic },
        version: HOST_PROTOCOL_VERSION,
      })
      .then((response) => {
        if (active && !response.ok) {
          setRequestError(`${response.error.summary} ${response.error.recovery}`);
        }
      })
      .catch(() => {
        if (active) {
          setRequestError(
            "The application host did not accept the configuration request. Open Activity for diagnostics.",
          );
        }
      });
    return (): void => {
      active = false;
    };
  }, [connectionName, host, selectedTopic]);

  const normalizedFilter = filter.trim().toLowerCase();
  const visibleEntries = useMemo(
    () =>
      normalizedFilter.length === 0
        ? current.entries
        : current.entries.filter(
            (entry) =>
              entry.name.toLowerCase().includes(normalizedFilter) ||
              (entry.value ?? "").toLowerCase().includes(normalizedFilter),
          ),
    [current.entries, normalizedFilter],
  );
  const selectedEntry =
    selectedName === null
      ? null
      : (current.entries.find((entry) => entry.name === selectedName) ?? null);
  const rowSelectionModel = useMemo<GridRowSelectionModel>(
    () => ({
      ids: new Set(selectedName === null ? [] : [selectedName]),
      type: "include",
    }),
    [selectedName],
  );
  const ready = current.state === "ready" || current.state === "stale";
  const busy = operation !== null;

  const executeMutation = async (action: "apply" | "validate"): Promise<void> => {
    if (selectedTopic === null || pending.length === 0) {
      return;
    }
    setOperation(action);
    setRequestError(undefined);
    try {
      const response = await host.execute({
        command: action === "apply" ? "topicConfiguration.apply" : "topicConfiguration.validate",
        id: globalThis.crypto.randomUUID(),
        payload: operationPayload(selectedTopic, pending, presetId),
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setRequestError(`${response.error.summary} ${response.error.recovery}`);
      } else if (action === "apply") {
        setPending([]);
        setPresetId(null);
        setSkippedNames([]);
      }
    } catch {
      setRequestError(
        "The application host did not accept the topic configuration operation. Open Activity for diagnostics.",
      );
    } finally {
      setOperation(null);
      setConfirmationOpen(false);
    }
  };

  const requestHistory = async (): Promise<void> => {
    if (selectedTopic === null) {
      return;
    }
    setHistoryOpen(true);
    setOperation("history");
    setRequestError(undefined);
    try {
      const response = await host.execute({
        command: "topicConfiguration.history",
        id: globalThis.crypto.randomUUID(),
        payload: { topic: selectedTopic },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setRequestError(`${response.error.summary} ${response.error.recovery}`);
      }
    } catch {
      setRequestError(
        "The application host did not accept the history request. Open Activity for diagnostics.",
      );
    } finally {
      setOperation(null);
    }
  };

  const refresh = async (): Promise<void> => {
    if (selectedTopic === null) {
      return;
    }
    setOperation("load");
    setRequestError(undefined);
    try {
      const response = await host.execute({
        command: "topicConfiguration.load",
        id: globalThis.crypto.randomUUID(),
        payload: { topic: selectedTopic },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setRequestError(`${response.error.summary} ${response.error.recovery}`);
      }
    } catch {
      setRequestError(
        "The application host did not accept refresh. Open Activity for diagnostics.",
      );
    } finally {
      setOperation(null);
    }
  };

  const applyPreset = (nextPresetId: KafkaTopicConfigurationPresetId): void => {
    const preset = KAFKA_TOPIC_CONFIGURATION_PRESETS.find(
      (candidate) => candidate.id === nextPresetId,
    );
    if (preset === undefined) {
      return;
    }
    const entriesByName = new Map(current.entries.map((entry) => [entry.name, entry]));
    const presetNames = new Set<string>(preset.changes.map((change) => change.name));
    const retained = pending.filter((change) => !presetNames.has(change.name));
    let next = retained;
    const skipped: string[] = [];
    for (const change of preset.changes) {
      const configuration = entriesByName.get(change.name);
      if (configuration === undefined || configuration.readOnly) {
        skipped.push(change.name);
        continue;
      }
      next = [
        ...next,
        {
          isSensitive: configuration.isSensitive,
          name: configuration.name,
          value: change.value,
        },
      ];
    }
    setPending(next);
    setPresetId(retained.length === 0 ? preset.id : null);
    setSkippedNames(skipped);
  };

  const pendingLabel =
    pending.length === 0
      ? "No pending changes"
      : `${pending.length.toLocaleString()} pending change${pending.length === 1 ? "" : "s"}`;
  const configurationStatusTone: StatusIndicatorTone =
    current.state === "ready"
      ? "success"
      : current.state === "stale"
        ? "warning"
        : current.state === "denied" || current.state === "failed" || current.state === "not-found"
          ? "error"
          : current.state === "loading"
            ? "info"
            : "neutral";

  return (
    <Box
      aria-label="Topic configuration workspace"
      component={component}
      sx={{
        bgcolor: "background.paper",
        display: "grid",
        gridTemplateRows: "auto auto minmax(0, 1fr)",
        minHeight: 0,
        minWidth: 0,
      }}
    >
      <TopicWorkspaceToolbar label="Configuration controls">
        {ready ? (
          <TextField
            onChange={(event) => {
              setFilter(event.target.value);
            }}
            placeholder="Search configuration"
            size="small"
            slotProps={{
              htmlInput: { "aria-label": "Search configuration", type: "search" },
            }}
            sx={{ flex: "1 0 180px", maxWidth: 360 }}
            value={filter}
          />
        ) : null}
        <Box sx={{ flex: 1 }} />
        {selectedTopic === null ? null : (
          <StatusIndicator
            ariaLabel="Configuration status"
            label={
              current.state === "not-found"
                ? "Topic not found"
                : `${current.state.slice(0, 1).toUpperCase()}${current.state.slice(1)}`
            }
            live="polite"
            tone={configurationStatusTone}
          />
        )}
        <Button
          disabled={selectedTopic === null || connectionName === null || busy}
          onClick={() => {
            void refresh();
          }}
          variant="outlined"
        >
          Refresh configuration
        </Button>
        <Button
          disabled={selectedTopic === null || connectionName === null || busy}
          onClick={() => {
            void requestHistory();
          }}
          variant="text"
        >
          Configuration history
        </Button>
      </TopicWorkspaceToolbar>

      <Stack spacing={1} sx={{ px: 2, py: 1 }}>
        {requestError === undefined ? null : <Alert severity="error">{requestError}</Alert>}
        {current.state === "stale" ? (
          <Alert severity="warning">
            <Typography component="p" variant="subtitle2">
              {current.error?.summary ?? "Configuration is stale."}
            </Typography>
            <Typography component="p" variant="body2">
              {current.error?.recovery ?? "Refresh the selected topic before another change."}
            </Typography>
          </Alert>
        ) : null}
        {current.state === "denied" ||
        current.state === "not-found" ||
        current.state === "failed" ? (
          <Alert severity="error">
            <Typography component="p" variant="subtitle2">
              {current.error?.summary ?? "Topic configuration is unavailable."}
            </Typography>
            <Typography component="p" variant="body2">
              {current.error?.recovery ?? "Open Activity for diagnostics and retry."}
            </Typography>
          </Alert>
        ) : null}
        {skippedNames.length === 0 ? null : (
          <Alert severity="warning">
            Skipped read-only configuration: {skippedNames.join(", ")}
          </Alert>
        )}
      </Stack>

      {selectedTopic === null ? (
        <Stack
          spacing={2}
          sx={{ alignItems: "center", justifyContent: "center", minHeight: 280, p: 3 }}
        >
          <Typography component="h2" variant="subtitle2">
            Select a topic
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Choose a topic in navigation to inspect broker configuration.
          </Typography>
        </Stack>
      ) : current.state === "loading" ||
        (current.state === "unavailable" && operation === "load") ? (
        <Stack aria-label="Loading topic configuration" spacing={1} sx={{ p: 2 }}>
          <Skeleton height={40} variant="rectangular" />
          <Skeleton height={36} variant="rectangular" />
          <Skeleton height={36} variant="rectangular" />
        </Stack>
      ) : ready ? (
        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: { xs: "minmax(0, 1fr)", md: "minmax(0, 1fr) 320px" },
            minHeight: 0,
            minWidth: 0,
            overflow: "auto",
          }}
        >
          <Box
            sx={{
              display: "grid",
              gridTemplateRows: "minmax(280px, 1fr)",
              minHeight: 0,
              minWidth: 0,
            }}
          >
            {current.entries.length === 0 ? (
              <Typography color="text.secondary" sx={{ p: 2 }} variant="body2">
                Kafka returned no configuration entries for this topic.
              </Typography>
            ) : visibleEntries.length === 0 ? (
              <Typography color="text.secondary" sx={{ p: 2 }} variant="body2">
                No configuration entries match the current search.
              </Typography>
            ) : (
              <DataGrid
                aria-label="Topic configuration entries"
                columnHeaderHeight={streamSkopeLayout.tableHeaderHeight}
                columns={columns}
                density="compact"
                disableMultipleRowSelection
                getRowId={(row) => row.name}
                hideFooter
                onRowClick={({ row }: GridRowParams<KafkaTopicConfigurationEntry>) => {
                  setSelectedName(row.name);
                  setDraftValue(
                    pending.find((change) => change.name === row.name)?.value ?? row.value ?? "",
                  );
                }}
                rowHeight={streamSkopeLayout.tableRowHeight}
                rowSelectionModel={rowSelectionModel}
                rows={visibleEntries}
                sx={{ border: 0 }}
              />
            )}
          </Box>

          <Stack
            spacing={2}
            sx={{
              borderLeft: { md: 1 },
              borderColor: "divider",
              borderTop: { xs: 1, md: 0 },
              minWidth: 0,
              overflow: "auto",
              p: 2,
            }}
          >
            <Box>
              <Typography component="h2" variant="subtitle2">
                Selected entry
              </Typography>
              {selectedEntry === null ? (
                <Typography color="text.secondary" variant="body2">
                  Select a configuration row to inspect or queue a value.
                </Typography>
              ) : (
                <Stack spacing={1.5} sx={{ mt: 1 }}>
                  <Typography variant="body2">{selectedEntry.name}</Typography>
                  <KafkaConfigurationDocumentation value={selectedEntry.documentation} />
                  <Typography color="text.secondary" variant="caption">
                    {selectedEntry.source} · {selectedEntry.type} ·{" "}
                    {selectedEntry.readOnly ? "Read-only" : "Writable"}
                  </Typography>
                  <TextField
                    disabled={selectedEntry.readOnly}
                    fullWidth
                    label="Proposed value"
                    onChange={(event) => {
                      setDraftValue(event.target.value);
                    }}
                    value={draftValue}
                  />
                  {selectedEntry.readOnly ? (
                    <Alert severity="info">
                      Kafka marks this configuration read-only. It cannot be queued or applied.
                    </Alert>
                  ) : null}
                  <Button
                    disabled={selectedEntry.readOnly || busy}
                    onClick={() => {
                      setPending((changes) =>
                        replaceChange(changes, {
                          isSensitive: selectedEntry.isSensitive,
                          name: selectedEntry.name,
                          value: draftValue,
                        }),
                      );
                      setPresetId(null);
                      setSkippedNames([]);
                    }}
                    variant="contained"
                  >
                    Queue change
                  </Button>
                </Stack>
              )}
            </Box>

            <Accordion disableGutters elevation={0} variant="outlined">
              <AccordionSummary
                expandIcon={
                  <Box aria-hidden component="span">
                    ⌄
                  </Box>
                }
              >
                <Typography component="span" variant="subtitle2">
                  Advanced presets
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                <FormControl fullWidth size="small">
                  <InputLabel id="guarded-preset-label">Guarded preset</InputLabel>
                  <Select
                    label="Guarded preset"
                    labelId="guarded-preset-label"
                    onChange={(event) => {
                      applyPreset(event.target.value as KafkaTopicConfigurationPresetId);
                    }}
                    value=""
                  >
                    {KAFKA_TOPIC_CONFIGURATION_PRESETS.map((preset) => (
                      <MenuItem key={preset.id} value={preset.id}>
                        {preset.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </AccordionDetails>
            </Accordion>

            <Box>
              <Stack direction="row" sx={{ alignItems: "center", justifyContent: "space-between" }}>
                <Typography
                  color={pending.length === 0 ? "text.secondary" : "text.primary"}
                  component={pending.length === 0 ? "p" : "h2"}
                  variant={pending.length === 0 ? "body2" : "subtitle1"}
                >
                  {pendingLabel}
                </Typography>
                {pending.length === 0 ? null : (
                  <Button
                    disabled={busy}
                    onClick={() => {
                      setPending([]);
                      setPresetId(null);
                      setSkippedNames([]);
                    }}
                    size="small"
                  >
                    Clear
                  </Button>
                )}
              </Stack>
              {pending.length === 0 ? null : (
                <List aria-label="Pending configuration changes" dense disablePadding>
                  {pending.map((change) => (
                    <ListItem
                      disableGutters
                      key={change.name}
                      secondaryAction={
                        <Button
                          aria-label={`Remove ${change.name}`}
                          onClick={() => {
                            setPending((changes) =>
                              changes.filter((candidate) => candidate.name !== change.name),
                            );
                            setPresetId(null);
                          }}
                          size="small"
                        >
                          Remove
                        </Button>
                      }
                    >
                      <ListItemText
                        primary={change.name}
                        secondary={configurationComparison(change, current.entries)}
                        slotProps={{
                          primary: { variant: "body2" },
                          secondary: { variant: "body2" },
                        }}
                      />
                    </ListItem>
                  ))}
                </List>
              )}
            </Box>

            {pending.length === 0 ? null : (
              <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
                <Button
                  disabled={busy}
                  fullWidth
                  onClick={() => {
                    void executeMutation("validate");
                  }}
                  variant="outlined"
                >
                  Dry-run changes
                </Button>
                <Button
                  disabled={busy}
                  fullWidth
                  onClick={() => {
                    setConfirmationOpen(true);
                  }}
                  variant="contained"
                >
                  Apply changes
                </Button>
              </Stack>
            )}
          </Stack>
        </Box>
      ) : (
        <Stack sx={{ alignItems: "center", justifyContent: "center", minHeight: 280, p: 3 }}>
          <Typography component="h2" variant="subtitle2">
            Configuration unavailable
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Refresh the selected topic or open Activity for diagnostics.
          </Typography>
        </Stack>
      )}

      <Dialog
        aria-labelledby="apply-topic-configuration-title"
        onClose={() => setConfirmationOpen(false)}
        open={confirmationOpen}
      >
        <DialogTitle id="apply-topic-configuration-title">Apply topic configuration?</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={1.5}>
            <Typography variant="body2">
              Apply {pending.length.toLocaleString()} named configuration
              {pending.length === 1 ? "" : "s"} to topic{" "}
              <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
                {selectedTopic}
              </Box>{" "}
              on connection {connectionName}?
            </Typography>
            <Typography color="text.secondary" variant="body2">
              Kafka will set only the names shown below. The mutation may not be reversible.
            </Typography>
            <List dense disablePadding>
              {pending.map((change) => (
                <ListItem disableGutters key={change.name}>
                  <ListItemText
                    primary={change.name}
                    secondary={configurationComparison(change, current.entries)}
                  />
                </ListItem>
              ))}
            </List>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button autoFocus onClick={() => setConfirmationOpen(false)}>
            Cancel
          </Button>
          <Button
            color="primary"
            onClick={() => {
              void executeMutation("apply");
            }}
            variant="contained"
          >
            Apply named changes
          </Button>
        </DialogActions>
      </Dialog>

      <HistoryDialog
        history={
          history?.connectionName === connectionName && history.topic === selectedTopic
            ? history
            : null
        }
        onClose={() => setHistoryOpen(false)}
        open={historyOpen}
      />
    </Box>
  );
}
