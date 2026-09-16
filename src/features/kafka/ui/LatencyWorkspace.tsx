import { useEffect, useRef, useState } from "react";
import {
  Box,
  Divider,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type KafkaLatencyAcknowledgements,
  type KafkaLatencyHistoryMetric,
  type KafkaLatencyHistorySnapshot,
  type KafkaLatencyProbeEvidence,
  type KafkaLatencySnapshot,
  type KafkaOperationalPreferenceSnapshot,
  type StreamSkopeHost,
} from "../contracts";
import { streamSkopeMuiMonospaceTypography } from "../../../platform/ui/createStreamSkopeTheme";
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
} from "../../../platform/ui/controls";

import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";
import { DiagnosticMetric } from "./DiagnosticMetric";
import { MetricPlot } from "./MetricPlot";
import { StatusIndicator, type StatusIndicatorTone } from "./StatusIndicator";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";
import { formatUtcTimestamp, formatUtcTimestampStacked } from "./timestamp-presentation";

export type LatencyWorkspaceTransferPort = TextDocumentTransferPort;

export interface LatencyWorkspaceProperties {
  readonly component?: "main" | "section";
  readonly connectionName: string | null;
  readonly history: KafkaLatencyHistorySnapshot;
  readonly host: StreamSkopeHost;
  readonly onOpenActivity: () => void;
  readonly preferences: KafkaOperationalPreferenceSnapshot | null;
  readonly selectedTopic: string | null;
  readonly snapshot: KafkaLatencySnapshot;
  readonly transfer?: LatencyWorkspaceTransferPort;
}

function acknowledgementLabel(value: KafkaLatencyAcknowledgements): string {
  switch (value) {
    case -1:
      return "All in-sync replicas";
    case 0:
      return "No broker response";
    case 1:
      return "Leader";
  }
}

function acknowledgementExplanation(value: KafkaLatencyAcknowledgements): string {
  return value === 0
    ? "Producer timing measures send completion without a broker response."
    : `Producer timing measures acknowledgement from ${value === -1 ? "all in-sync replicas" : "the partition leader"}.`;
}

function command(commandName: "latency.export" | "latency.stop"): HostCommand {
  return {
    command: commandName,
    id: globalThis.crypto.randomUUID(),
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  };
}

function formatDuration(value: number | null): string {
  return value === null
    ? "Unavailable"
    : `${value.toLocaleString(undefined, { maximumFractionDigits: 3 })} ms`;
}

function formatHistoryMetric(metric: KafkaLatencyHistoryMetric | null): string {
  return metric === null
    ? "Unavailable"
    : `${formatDuration(metric.averageMs)} / ${formatDuration(metric.p95Ms)}`;
}

function preferenceStatus(preferences: KafkaOperationalPreferenceSnapshot | null): string {
  if (preferences === null) {
    return "Factory latency defaults are in use until the host confirms preferences.";
  }
  if (preferences.store.state === "unavailable") {
    return `Preference storage is unavailable. Factory latency defaults are in use. ${preferences.store.recovery ?? ""}`.trim();
  }
  return `Confirmed ${preferences.store.durability} preferences initialize the next probe.`;
}

function statusText(snapshot: KafkaLatencySnapshot): string {
  switch (snapshot.state) {
    case "unavailable":
      return "Latency probing is unavailable.";
    case "idle":
      return "No latency probe has run for this connection.";
    case "running":
      return `Running ${String(snapshot.request.messageCount)}-record probe on ${snapshot.request.topic}.`;
    case "ready":
      return `Current latency evidence: ${String(snapshot.evidence.observedMessages)} of ${String(
        snapshot.evidence.requestedMessages,
      )} records observed.`;
    case "partial":
      return `Partial latency evidence: ${String(snapshot.evidence.observedMessages)} of ${String(
        snapshot.evidence.requestedMessages,
      )} records observed.`;
    case "stale":
      return "Displayed latency evidence is stale and cannot be exported.";
    case "cancelled":
      return `Latency probe cancelled. ${snapshot.error.recovery}`;
    case "failed":
      return `Latency probe failed. ${snapshot.error.summary} ${snapshot.error.recovery}`;
  }
}

function metricRows(evidence: KafkaLatencyProbeEvidence): readonly {
  readonly averageMs: number | null;
  readonly label: string;
  readonly p95Ms: number | null;
  readonly samples: number;
}[] {
  const produce = evidence.producer.summary;
  const fetch = evidence.fetch.summary;
  const endToEnd = evidence.endToEnd;
  return [
    {
      averageMs: produce?.averageMs ?? null,
      label:
        evidence.producer.semantics === "send-completion"
          ? "Produce send completion"
          : "Produce acknowledgement",
      p95Ms: produce?.p95Ms ?? null,
      samples: produce?.samples ?? 0,
    },
    {
      averageMs: fetch?.averageMs ?? null,
      label: "Kafka fetch request",
      p95Ms: fetch?.p95Ms ?? null,
      samples: fetch?.samples ?? 0,
    },
    {
      averageMs: endToEnd?.averageMs ?? null,
      label: "Publish to observe",
      p95Ms: endToEnd?.p95Ms ?? null,
      samples: endToEnd?.samples ?? 0,
    },
    {
      averageMs: evidence.network.tcpConnectMs,
      label: "TCP connect",
      p95Ms: evidence.network.tcpConnectMs,
      samples: evidence.network.tcpConnectMs === null ? 0 : 1,
    },
    {
      averageMs: evidence.network.tlsHandshakeMs,
      label: "TLS handshake",
      p95Ms: evidence.network.tlsHandshakeMs,
      samples: evidence.network.tlsHandshakeMs === null ? 0 : 1,
    },
  ];
}

export function LatencyWorkspace({
  component = "main",
  connectionName,
  history,
  host,
  onOpenActivity,
  preferences,
  selectedTopic,
  snapshot,
  transfer = browserTextDocumentTransfer,
}: LatencyWorkspaceProperties): React.JSX.Element {
  const defaults =
    preferences?.preferences.latency ?? KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.latency;
  const initialRequest = snapshot.state === "running" ? snapshot.request : defaults;
  const [acknowledgements, setAcknowledgements] = useState<KafkaLatencyAcknowledgements>(
    initialRequest.acknowledgements,
  );
  const [busy, setBusy] = useState<"export" | "start" | "stop">();
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const [localError, setLocalError] = useState<string>();
  const [messageCount, setMessageCount] = useState<number>(initialRequest.messageCount);
  const [runbookBusy, setRunbookBusy] = useState(false);
  const [status, setStatus] = useState("");
  const [timeoutMs, setTimeoutMs] = useState<number>(initialRequest.timeoutMs);
  const runButton = useRef<HTMLButtonElement>(null);
  const running = snapshot.state === "running";
  const contextAvailable = connectionName !== null && selectedTopic !== null;
  const exportAvailable = snapshot.state === "ready" || snapshot.state === "partial";
  const runbookUrl =
    preferences?.store.state === "ready" ? preferences.preferences.latency.runbookUrl : null;

  useEffect(() => {
    if (snapshot.state === "running") {
      setAcknowledgements(snapshot.request.acknowledgements);
      setMessageCount(snapshot.request.messageCount);
      setTimeoutMs(snapshot.request.timeoutMs);
    } else {
      setAcknowledgements(defaults.acknowledgements);
      setMessageCount(defaults.messageCount);
      setTimeoutMs(defaults.timeoutMs);
    }
  }, [
    defaults.acknowledgements,
    defaults.messageCount,
    defaults.timeoutMs,
    snapshot.state,
    snapshot.state === "running" ? snapshot.request.acknowledgements : null,
    snapshot.state === "running" ? snapshot.request.messageCount : null,
    snapshot.state === "running" ? snapshot.request.timeoutMs : null,
  ]);

  function closeConfirmation(): void {
    setConfirmationOpen(false);
    globalThis.setTimeout(() => {
      runButton.current?.focus();
    }, 0);
  }

  async function start(): Promise<void> {
    if (selectedTopic === null) {
      return;
    }
    closeConfirmation();
    setBusy("start");
    setLocalError(undefined);
    setStatus("");
    try {
      const response = await host.execute({
        command: "latency.start",
        id: globalThis.crypto.randomUUID(),
        payload: {
          acknowledgements,
          messageCount,
          timeoutMs,
          topic: selectedTopic,
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok && response.error.code !== "CANCELLED") {
        setLocalError(`${response.error.summary} ${response.error.recovery}`);
      }
    } catch {
      setLocalError(
        "The application host did not accept the latency request. No successful probe was confirmed. Open Activity for diagnostics.",
      );
    } finally {
      setBusy((current) => (current === "start" ? undefined : current));
    }
  }

  async function stop(): Promise<void> {
    setBusy("stop");
    setLocalError(undefined);
    setStatus("");
    try {
      const response = await host.execute(command("latency.stop"));
      if (!response.ok) {
        setLocalError(`${response.error.summary} ${response.error.recovery}`);
      }
    } catch {
      setLocalError(
        "The application host did not confirm probe cancellation. Open Activity for diagnostics.",
      );
    } finally {
      setBusy((current) => (current === "stop" ? undefined : current));
    }
  }

  async function exportEvidence(): Promise<void> {
    setBusy("export");
    setLocalError(undefined);
    setStatus("");
    try {
      const response = await host.execute(command("latency.export"));
      if (!response.ok) {
        setLocalError(`${response.error.summary} ${response.error.recovery}`);
      } else if (!("document" in response.result)) {
        setLocalError(
          "The application host returned no latency JSON. Run a current probe and retry.",
        );
      } else {
        const outcome = await transfer.download(response.result.document);
        setStatus(
          outcome === "cancelled"
            ? "Latency JSON export cancelled."
            : outcome === "saved"
              ? "Latency JSON saved."
              : "Latency JSON download started.",
        );
      }
    } catch {
      setLocalError("The latency JSON export failed. No file was saved. Retry the export.");
    } finally {
      setBusy((current) => (current === "export" ? undefined : current));
    }
  }

  async function openRunbook(): Promise<void> {
    if (runbookUrl === null) {
      return;
    }
    setRunbookBusy(true);
    setLocalError(undefined);
    setStatus("");
    try {
      await host.openExternalUrl(runbookUrl);
      setStatus("Runbook request accepted by the platform.");
    } catch {
      setLocalError(
        "The platform did not accept the runbook request. Check browser popup or operating-system policy, then retry. Open Activity for diagnostics.",
      );
    } finally {
      setRunbookBusy(false);
    }
  }

  const evidence =
    snapshot.state === "ready" || snapshot.state === "partial" || snapshot.state === "stale"
      ? snapshot.evidence
      : null;
  const latencyStateTone: StatusIndicatorTone =
    snapshot.state === "failed"
      ? "error"
      : snapshot.state === "partial" || snapshot.state === "stale"
        ? "warning"
        : snapshot.state === "ready"
          ? "success"
          : snapshot.state === "running"
            ? "info"
            : "neutral";
  const setupContent = (
    <Stack aria-label="Latency probe setup" component="section" spacing={1.5}>
      <Typography color="text.secondary" variant="caption">
        {preferenceStatus(preferences)}
      </Typography>
      {runbookUrl === null ? (
        <Typography color="text.secondary" variant="caption">
          Configure a latency runbook in Workbench Preferences to expose an external recovery
          action.
        </Typography>
      ) : (
        <Stack
          aria-label="Latency runbook"
          direction={{ sm: "row", xs: "column" }}
          spacing={1}
          sx={{ alignItems: { sm: "center" } }}
        >
          <Typography noWrap sx={{ minWidth: 0 }} variant="body2">
            {runbookUrl}
          </Typography>
          <Button
            disabled={runbookBusy}
            onClick={() => {
              void openRunbook();
            }}
            variant="outlined"
          >
            Open runbook
          </Button>
        </Stack>
      )}
      <Box>
        <Typography component="h3" variant="subtitle2">
          Next probe
        </Typography>
        <Typography color="text.secondary" variant="caption">
          These controls apply to the next run; completed evidence remains unchanged.
        </Typography>
      </Box>
      <Stack
        direction={{ sm: "row", xs: "column" }}
        spacing={1.5}
        sx={{ alignItems: { sm: "flex-start" } }}
      >
        <FormControl disabled={running || busy !== undefined} size="small" sx={{ minWidth: 150 }}>
          <InputLabel id="latency-message-count-label">Probe records</InputLabel>
          <Select
            id="latency-message-count"
            label="Probe records"
            labelId="latency-message-count-label"
            onChange={(event) => {
              setMessageCount(Number(event.target.value));
            }}
            value={messageCount}
          >
            {[1, 5, 10, 20, 50, 100, 200].map((count) => (
              <MenuItem key={count} value={count}>
                {count}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControl disabled={running || busy !== undefined} size="small" sx={{ minWidth: 220 }}>
          <InputLabel id="latency-acknowledgements-label">Kafka acknowledgements</InputLabel>
          <Select
            id="latency-acknowledgements"
            label="Kafka acknowledgements"
            labelId="latency-acknowledgements-label"
            onChange={(event) => {
              setAcknowledgements(Number(event.target.value) as KafkaLatencyAcknowledgements);
            }}
            value={acknowledgements}
          >
            {([-1, 1, 0] as const).map((value) => (
              <MenuItem key={value} value={value}>
                {acknowledgementLabel(value)}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControl disabled={running || busy !== undefined} size="small" sx={{ minWidth: 150 }}>
          <InputLabel id="latency-timeout-label">Probe timeout</InputLabel>
          <Select
            id="latency-timeout"
            label="Probe timeout"
            labelId="latency-timeout-label"
            onChange={(event) => {
              setTimeoutMs(Number(event.target.value));
            }}
            value={timeoutMs}
          >
            {[5_000, 10_000, 30_000, 60_000].map((value) => (
              <MenuItem key={value} value={value}>
                {value / 1_000} seconds
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Stack>
      <Typography color="text.secondary" variant="caption">
        {acknowledgementExplanation(running ? snapshot.request.acknowledgements : acknowledgements)}
      </Typography>
      <Alert severity="warning">
        A probe publishes {running ? snapshot.request.messageCount : messageCount} synthetic record
        {(running ? snapshot.request.messageCount : messageCount) === 1 ? "" : "s"} to{" "}
        <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
          {running ? snapshot.request.topic : (selectedTopic ?? "the selected topic")}
        </Box>
        . Kafka records are immutable; StreamSkope cannot remove them.
      </Alert>
      {running ? (
        <Button
          disabled={busy === "stop"}
          onClick={() => {
            void stop();
          }}
          sx={{ alignSelf: "flex-start" }}
          variant="contained"
        >
          Stop latency probe
        </Button>
      ) : (
        <Button
          disabled={!contextAvailable || busy !== undefined}
          onClick={() => {
            setConfirmationOpen(true);
          }}
          ref={runButton}
          sx={{ alignSelf: "flex-start" }}
          variant="contained"
        >
          Run latency probe
        </Button>
      )}
    </Stack>
  );

  return (
    <Box
      aria-label="Latency workspace"
      component={component}
      sx={{
        bgcolor: "background.paper",
        height: "100%",
        minHeight: 0,
        overflow: "auto",
      }}
    >
      <TopicWorkspaceToolbar label="Latency controls">
        <Box sx={{ flex: 1, minWidth: 110 }}>
          <Typography component="h2" noWrap variant="subtitle2">
            Latency probe
          </Typography>
        </Box>
        <Button
          disabled={!exportAvailable || busy !== undefined}
          onClick={() => {
            void exportEvidence();
          }}
          variant="outlined"
        >
          Export latency JSON
        </Button>
        <Button onClick={onOpenActivity} variant="text">
          Open activity
        </Button>
        <StatusIndicator
          ariaLabel="Latency state"
          label={`${snapshot.state.slice(0, 1).toUpperCase()}${snapshot.state.slice(1)}`}
          live="polite"
          tone={latencyStateTone}
        />
      </TopicWorkspaceToolbar>

      <Stack spacing={2} sx={{ p: 2, "& > *": { flexShrink: 0 } }}>
        <Typography color="text.secondary" variant="body2">
          Measure one bounded write, fetch, network, and publish-to-observe path for the selected
          Kafka topic.
        </Typography>

        {!contextAvailable ? (
          <Alert severity="info">
            Connect a Kafka cluster and select a topic to run a latency probe.
          </Alert>
        ) : null}
        {localError === undefined ? null : <Alert severity="error">{localError}</Alert>}
        <Typography
          aria-label="Latency operation status"
          aria-live="polite"
          role="status"
          variant="body2"
        >
          {status.length > 0 ? status : statusText(snapshot)}
        </Typography>

        {evidence === null ? null : (
          <>
            <Divider />
            <Box>
              <Typography component="h3" variant="subtitle2">
                Latest completed probe
              </Typography>
            </Box>
            <Box
              aria-label="Latency probe context"
              component="dl"
              sx={{
                display: "grid",
                gap: 1.5,
                gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                m: 0,
              }}
            >
              {[
                ["Cluster", evidence.connection.name],
                ["Topic", evidence.topic],
                ["Started", evidence.startedAt],
                ["Completed", evidence.completedAt],
                [
                  "Observed",
                  `${String(evidence.observedMessages)} / ${String(evidence.requestedMessages)}`,
                ],
                ["Acknowledgements", acknowledgementLabel(evidence.acknowledgements)],
              ].map(([label, value]) => {
                const timestamp = label === "Started" || label === "Completed";
                return (
                  <Box key={label}>
                    <Typography color="text.secondary" component="dt" variant="caption">
                      {label}
                    </Typography>
                    <Typography
                      component="dd"
                      noWrap={label === "Topic"}
                      sx={{
                        m: 0,
                        whiteSpace: timestamp ? "pre-line" : undefined,
                      }}
                      title={timestamp ? value : undefined}
                      variant="body2"
                    >
                      {timestamp && value !== undefined
                        ? formatUtcTimestampStacked(value)
                        : (value ?? "Not available")}
                    </Typography>
                  </Box>
                );
              })}
            </Box>

            <Box
              aria-label="Current latency measurements"
              component="section"
              sx={{
                bgcolor: "background.paper",
                borderBottom: 1,
                borderColor: "divider",
                borderTop: 1,
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(145px, 1fr))",
                m: 0,
                "& > :last-child": { borderRight: 0 },
              }}
            >
              <DiagnosticMetric
                label="Publish average"
                value={formatDuration(evidence.endToEnd?.averageMs ?? null)}
              />
              <DiagnosticMetric
                label="Publish P95"
                value={formatDuration(evidence.endToEnd?.p95Ms ?? null)}
              />
              <DiagnosticMetric
                label="Produce average"
                value={formatDuration(evidence.producer.summary?.averageMs ?? null)}
              />
              <DiagnosticMetric
                label="Fetch average"
                value={formatDuration(evidence.fetch.summary?.averageMs ?? null)}
              />
              <DiagnosticMetric
                label="Observed"
                value={`${String(evidence.observedMessages)} / ${String(evidence.requestedMessages)}`}
              />
            </Box>

            {snapshot.state === "partial" ? (
              <Typography component="h3" variant="subtitle2">
                Partial latency evidence
              </Typography>
            ) : null}
            {snapshot.state === "stale" ? (
              <Alert severity="warning">
                This evidence belongs to a previous connection context and cannot be exported.
              </Alert>
            ) : null}
            {evidence.issues.map((issue) => (
              <Alert key={issue.stage} severity="warning">
                <Typography component="p" variant="subtitle2">
                  {issue.summary}
                </Typography>
                <Typography component="p" variant="body2">
                  {issue.recovery}
                </Typography>
              </Alert>
            ))}

            {history.entries.length < 2 ? null : (
              <MetricPlot
                sampleLabels={history.entries.map((entry) => entry.completedAt)}
                series={[
                  {
                    label: "Average",
                    values: history.entries.map((entry) => entry.endToEnd?.averageMs ?? null),
                  },
                  {
                    label: "P95",
                    values: history.entries.map((entry) => entry.endToEnd?.p95Ms ?? null),
                  },
                ]}
                title="Publish-to-observe latency trend"
                unit="ms"
              />
            )}

            <TableContainer
              aria-label="Latency metrics scroll area"
              role="region"
              sx={{ overflowX: "auto" }}
              tabIndex={0}
            >
              <Table
                aria-label="Latency metrics"
                size="small"
                stickyHeader
                sx={{ minWidth: 620, "& td, & th": { whiteSpace: "nowrap" } }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell>Metric</TableCell>
                    <TableCell align="right">Samples</TableCell>
                    <TableCell align="right">Average</TableCell>
                    <TableCell align="right">P95</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {metricRows(evidence).map((metric) => (
                    <TableRow key={metric.label}>
                      <TableCell component="th" scope="row">
                        {metric.label}
                      </TableCell>
                      <TableCell align="right">{metric.samples}</TableCell>
                      <TableCell align="right">{formatDuration(metric.averageMs)}</TableCell>
                      <TableCell align="right">{formatDuration(metric.p95Ms)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>

            <TableContainer
              aria-label="Fetch latency by broker scroll area"
              role="region"
              sx={{ overflowX: "auto" }}
              tabIndex={0}
            >
              <Table
                aria-label="Fetch latency by broker"
                size="small"
                stickyHeader
                sx={{ minWidth: 620, "& td, & th": { whiteSpace: "nowrap" } }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell>Broker</TableCell>
                    <TableCell align="right">Node</TableCell>
                    <TableCell align="right">Samples</TableCell>
                    <TableCell align="right">Average</TableCell>
                    <TableCell align="right">P95</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {evidence.fetch.perBroker.map((broker) => (
                    <TableRow key={broker.nodeId}>
                      <TableCell sx={streamSkopeMuiMonospaceTypography}>{broker.broker}</TableCell>
                      <TableCell align="right">{broker.nodeId}</TableCell>
                      <TableCell align="right">{broker.summary.samples}</TableCell>
                      <TableCell align="right">
                        {formatDuration(broker.summary.averageMs)}
                      </TableCell>
                      <TableCell align="right">{formatDuration(broker.summary.p95Ms)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </>
        )}

        {history.entries.length === 0 ? null : (
          <>
            <Divider />
            <Box>
              <Typography component="h3" variant="subtitle2">
                Recent probes
              </Typography>
              <Typography color="text.secondary" variant="caption">
                Up to 20 completed summaries for {history.connectionName}. Full evidence remains
                available only for the current probe.
              </Typography>
            </Box>
            <TableContainer
              aria-label="Latency probe history scroll area"
              role="region"
              sx={{ maxHeight: 320, overflowX: "auto" }}
              tabIndex={0}
            >
              <Table
                aria-label="Latency probe history"
                size="small"
                stickyHeader
                sx={{ minWidth: 1_180, "& td, & th": { whiteSpace: "nowrap" } }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell>Topic</TableCell>
                    <TableCell>State</TableCell>
                    <TableCell align="right">Observed</TableCell>
                    <TableCell>Acknowledgements</TableCell>
                    <TableCell align="right">End-to-end avg / P95</TableCell>
                    <TableCell align="right">Fetch avg / P95</TableCell>
                    <TableCell align="right">Producer avg / P95</TableCell>
                    <TableCell align="right">Issues</TableCell>
                    <TableCell>Completed</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {[...history.entries].reverse().map((entry) => (
                    <TableRow key={entry.runId}>
                      <TableCell
                        component="th"
                        scope="row"
                        sx={[streamSkopeMuiMonospaceTypography, { whiteSpace: "nowrap" }]}
                      >
                        {entry.topic}
                      </TableCell>
                      <TableCell>{entry.state === "ready" ? "Ready" : "Partial"}</TableCell>
                      <TableCell align="right">
                        {entry.observedMessages} / {entry.requestedMessages}
                      </TableCell>
                      <TableCell>{acknowledgementLabel(entry.acknowledgements)}</TableCell>
                      <TableCell align="right">{formatHistoryMetric(entry.endToEnd)}</TableCell>
                      <TableCell align="right">{formatHistoryMetric(entry.fetch)}</TableCell>
                      <TableCell align="right">{formatHistoryMetric(entry.producer)}</TableCell>
                      <TableCell align="right">{entry.issueCount}</TableCell>
                      <TableCell sx={{ whiteSpace: "nowrap" }} title={entry.completedAt}>
                        {formatUtcTimestamp(entry.completedAt)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </>
        )}

        {evidence === null ? (
          setupContent
        ) : (
          <Accordion
            disableGutters
            elevation={0}
            slotProps={{ transition: { timeout: 0, unmountOnExit: true } }}
            variant="outlined"
          >
            <AccordionSummary
              aria-label="Run again"
              expandIcon={
                <Box aria-hidden component="span">
                  ⌄
                </Box>
              }
            >
              <Box>
                <Typography component="span" variant="subtitle2">
                  Run again
                </Typography>
                <Typography color="text.secondary" component="p" variant="caption">
                  Review setup and the immutable-record consequence before another probe.
                </Typography>
              </Box>
            </AccordionSummary>
            <AccordionDetails>{setupContent}</AccordionDetails>
          </Accordion>
        )}
      </Stack>
      <Dialog
        aria-labelledby="latency-confirmation-title"
        onClose={closeConfirmation}
        open={confirmationOpen}
      >
        <DialogTitle id="latency-confirmation-title">Run latency probe?</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={1.5}>
            <Typography>
              Publish {messageCount} synthetic records to{" "}
              <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
                {selectedTopic}
              </Box>{" "}
              on cluster {connectionName}.
            </Typography>
            <Typography>
              Kafka records are immutable. StreamSkope cannot remove those records after the broker
              accepts them.
            </Typography>
            <Typography color="text.secondary" variant="body2">
              {acknowledgementExplanation(acknowledgements)} The probe runs once and stops after
              {` ${String(timeoutMs / 1_000)} seconds at most.`}
            </Typography>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeConfirmation} variant="text">
            Cancel
          </Button>
          <Button
            autoFocus
            onClick={() => {
              void start();
            }}
            variant="contained"
          >
            Run latency probe
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
