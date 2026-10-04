import { useEffect, useMemo, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

import {
  StudioButton as Button,
  StudioAlert as Alert,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as FormControlLabel,
  StudioAccordion as Accordion,
  StudioAccordionSummary as AccordionSummary,
  StudioAccordionDetails as AccordionDetails,
} from "../../../platform/ui/controls";
import { type KafkaConsumerGroupInventorySnapshot, type StreamSkopeHost } from "../contracts";
import { analyzeObservations } from "../contracts/observation-analysis";
import { observationIdentity, type ObservationInput } from "../contracts/observations";

import { ObservationAnalysisPanel } from "./ObservationAnalysisPanel";
import { ObservationControls } from "./ObservationControls";
import { ObservationFindings, type ObservationNavigation } from "./ObservationFindings";
import { ObservationPartitionTable } from "./ObservationPartitionTable";
import { ObservationSummary } from "./ObservationSummary";
import { ObservationTrends } from "./ObservationTrends";
import { useObservedHealth } from "./use-observed-health";

function threshold(value: string, label: string, maximum = Number.MAX_SAFE_INTEGER): number | null {
  if (!value.trim()) return null;
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0 || number > maximum)
    throw new Error(`${label} must be between zero and ${maximum.toLocaleString()}.`);
  return number;
}
export interface ObservedHealthPageProperties extends ObservationNavigation {
  readonly host: StreamSkopeHost;
  readonly connected?: boolean;
  readonly connectionName?: string | null;
  readonly initialTopic?: string;
  readonly initialGroupId?: string;
  readonly topics?: readonly string[];
  readonly groupInventory?: KafkaConsumerGroupInventorySnapshot;
  readonly inventoryStatus?: string;
  readonly onRefreshResources?: () => void;
}

export function ObservedHealthPage({
  host,
  connected = true,
  connectionName = null,
  initialTopic = "",
  initialGroupId = "",
  topics = [],
  groupInventory,
  inventoryStatus = "Enter a resource name if its inventory is unavailable.",
  onRefreshResources,
  ...navigation
}: ObservedHealthPageProperties): React.JSX.Element {
  const [topic, setTopic] = useState(initialTopic);
  const [groupId, setGroupId] = useState(initialGroupId);
  const [lagThreshold, setLagThreshold] = useState("");
  const [latencyThreshold, setLatencyThreshold] = useState("");
  const [sampleRecords, setSampleRecords] = useState(false);
  const [confirmation, setConfirmation] = useState("");
  const [selectionError, setSelectionError] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const health = useObservedHealth(host);
  const { series, latest } = health;
  const selectionInitialized = useRef(false);
  useEffect(() => {
    if (!series || selectionInitialized.current) return;
    selectionInitialized.current = true;
    if (!topic.trim()) {
      setTopic(series.topic);
      setGroupId(series.groupId ?? "");
    }
  }, [series, topic]);
  const actionable = health.current && health.fresh;
  const analysis = useMemo(
    () =>
      series
        ? analyzeObservations(series, actionable ? health.now : Number.POSITIVE_INFINITY)
        : null,
    [series, actionable, health.now],
  );
  const request = (): ObservationInput | null => {
    try {
      if (
        topic.trim().length > 249 ||
        !/^[A-Za-z0-9._-]+$/.test(topic.trim()) ||
        topic.trim() === "." ||
        topic.trim() === ".."
      )
        throw new Error(
          "Choose a valid topic name: up to 249 letters, numbers, dots, hyphens or underscores.",
        );
      if (groupId.trim().length > 512)
        throw new Error("Consumer group names must be 512 characters or fewer.");
      const input = {
        topic: topic.trim(),
        groupId: groupId.trim() || null,
        sampleRecords,
        thresholds: {
          lag: threshold(lagThreshold, "Lag threshold"),
          requestMs: threshold(latencyThreshold, "Request time threshold", 60_000),
        },
      };
      setSelectionError(null);
      return input;
    } catch (error) {
      setSelectionError(error instanceof Error ? error.message : "Check the selected thresholds.");
      setSettingsOpen(true);
      return null;
    }
  };
  const capture = (): void => {
    const input = request();
    if (input) void health.capture(input);
  };
  const start = (): void => {
    const input = request();
    if (input) health.start(input);
  };
  return (
    <Stack
      component="main"
      aria-label="Observed health page"
      spacing={2.5}
      sx={{ p: { xs: 2, md: 3 }, overflow: "auto", height: "100%", minWidth: 0 }}
    >
      <Stack spacing={0.5}>
        <Typography component="h1" variant="h5">
          Observed health
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Investigate one topic and optional consumer group using Kafka metadata and offset
          progress.
        </Typography>
        <Typography variant="caption" color="text.secondary">
          Connection: {connectionName ?? (connected ? "Current connection" : "Disconnected")}.
          Broker CPU/disk and processing success require other monitoring.
        </Typography>
      </Stack>
      <ObservationControls
        topic={topic}
        groupId={groupId}
        topics={topics}
        groups={groupInventory?.groups.map((group) => group.id) ?? []}
        busy={health.busy}
        running={health.running}
        connected={connected}
        historyReady={health.historyReady}
        cooldownSeconds={health.cooldownSeconds}
        operation={health.operation}
        selectionError={selectionError}
        inventoryStatus={inventoryStatus}
        onTopicChange={setTopic}
        onGroupChange={setGroupId}
        onRefresh={() => onRefreshResources?.()}
        onCapture={capture}
        onStart={start}
        onStop={health.stop}
      />
      {groupInventory?.error && (
        <Alert severity="warning">
          Consumer-group inventory: {groupInventory.error.summary} {groupInventory.error.recovery}{" "}
          You can still enter a permitted group name.
        </Alert>
      )}
      <FormControlLabel
        control={
          <Checkbox
            checked={sampleRecords}
            disabled={health.busy || health.running}
            onChange={(event) => setSampleRecords(event.target.checked)}
          />
        }
        label="Sample records for size and key distribution"
      />
      {health.error && (
        <Alert severity="error">
          <Typography component="h2" variant="subtitle2">
            {health.error.summary}
          </Typography>
          {health.error.recovery}
          <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
            {health.error.operation === "capture" && (
              <Button
                disabled={
                  !connected ||
                  !health.historyReady ||
                  health.busy ||
                  health.cooldownSeconds > 0 ||
                  !topic.trim()
                }
                onClick={capture}
              >
                Retry observation
              </Button>
            )}
            {(health.error.operation === "history" ||
              health.error.hostError?.code === "OBSERVATION_HISTORY_UNAVAILABLE") && (
              <Button
                disabled={health.busy}
                onClick={() => {
                  void health.refreshHistory();
                }}
              >
                Reload history
              </Button>
            )}
          </Stack>
        </Alert>
      )}
      {health.snapshot.series.length > 0 && (
        <TextField
          select
          label="Recorded observation series"
          disabled={health.busy || health.running}
          value={health.selected}
          onChange={(event) => {
            health.setSelected(event.target.value);
            const selection = health.snapshot.series.find(
              (value) => observationIdentity(value) === event.target.value,
            );
            if (selection) {
              setTopic(selection.topic);
              setGroupId(selection.groupId ?? "");
            }
          }}
        >
          {health.snapshot.series.map((value) => (
            <MenuItem key={observationIdentity(value)} value={observationIdentity(value)}>
              {value.topic} · {value.groupId ?? "No group"} · cluster {value.clusterId}
            </MenuItem>
          ))}
        </TextField>
      )}
      {series && latest && analysis ? (
        <>
          <ObservationSummary
            series={series}
            current={health.current}
            fresh={health.fresh}
            connectionName={connectionName}
            analysis={analysis}
          />
          <ObservationFindings
            series={series}
            analysis={analysis}
            actionable={actionable}
            {...navigation}
          />
          <ObservationTrends series={series} />
          <ObservationPartitionTable sample={latest} />
          <Accordion>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="observation-analysis-details"
              id="observation-analysis-title"
            >
              <Typography>Analysis details</Typography>
            </AccordionSummary>
            <AccordionDetails id="observation-analysis-details">
              <Stack spacing={2}>
                <ObservationAnalysisPanel series={series} fresh={actionable} {...navigation} />
                <Typography variant="body2">
                  Collection: {latest.requestMs.toFixed(1)} ms across {latest.providerCalls}{" "}
                  provider calls. Includes scheduling, sequential metadata/group reads and optional
                  sampling; it is not broker processing latency.
                </Typography>
                <Typography variant="caption" sx={{ overflowWrap: "anywhere" }}>
                  Source: kafka-api · {new Date(latest.observedAt).toISOString()} · cluster{" "}
                  {series.clusterId} · topic ID {series.topicId}.
                </Typography>
                <ObservationTrends series={series} diagnostic />
              </Stack>
            </AccordionDetails>
          </Accordion>
        </>
      ) : (
        !health.busy &&
        !health.error && (
          <Alert severity="info">
            No observation yet. Choose an existing topic and capture once. Add a consumer group to
            measure its offset backlog; start observing to compare progress over time.
          </Alert>
        )
      )}
      <Accordion expanded={settingsOpen} onChange={(_event, expanded) => setSettingsOpen(expanded)}>
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          aria-controls="observation-history-settings"
          id="observation-history-title"
        >
          <Typography>History and collection settings</Typography>
        </AccordionSummary>
        <AccordionDetails id="observation-history-settings">
          <Stack spacing={2}>
            <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
              <TextField
                label="Lag alert threshold (optional)"
                value={lagThreshold}
                disabled={health.busy || health.running}
                inputMode="decimal"
                onChange={(event) => setLagThreshold(event.target.value)}
                helperText="Offset positions; only a complete group observation can breach."
              />
              <TextField
                label="Request time alert threshold, ms (optional)"
                value={latencyThreshold}
                disabled={health.busy || health.running}
                inputMode="decimal"
                onChange={(event) => setLatencyThreshold(event.target.value)}
                helperText="Client collection cost, including optional reads."
              />
            </Stack>
            <Typography variant="body2">
              Optional sampling reads a bounded recent window, up to 200 protected records, 2 MiB
              and five seconds. Raw keys and payloads are discarded after aggregation. Incomplete
              samples do not support hot-key or record-size conclusions.
            </Typography>
            <Typography variant="body2">
              Threshold breaches appear here and once per transition in Activity. Collection and
              local alerts stop when you leave this page, disconnect or close the app. Missing data
              never satisfies a threshold.
            </Typography>
            <Typography variant="body2">
              History:{" "}
              {health.snapshot.durability === "durable"
                ? "Private desktop storage; retained across restarts"
                : "Browser host session only"}
              . At most eight identities, 240 samples each, 24 hours and 4 MiB. Older evidence is
              evicted; payloads, raw keys and credentials are not stored.
            </Typography>
            <Button
              disabled={health.busy || health.running}
              onClick={() => {
                void health.refreshHistory();
              }}
            >
              Reload retained history
            </Button>
            <Stack spacing={1}>
              <TextField
                label="Clear all history confirmation"
                value={confirmation}
                disabled={health.busy || health.running}
                helperText="Type CLEAR HISTORY to delete every retained series, including other connections."
                onChange={(event) => setConfirmation(event.target.value)}
              />
              <Button
                color="error"
                disabled={health.busy || health.running || confirmation !== "CLEAR HISTORY"}
                onClick={() => {
                  void health.clear().then((cleared) => {
                    if (cleared) setConfirmation("");
                  });
                }}
              >
                Clear all observation history
              </Button>
            </Stack>
          </Stack>
        </AccordionDetails>
      </Accordion>
    </Stack>
  );
}
