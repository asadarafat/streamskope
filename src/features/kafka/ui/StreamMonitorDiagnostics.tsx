import { useState } from "react";
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";

import { KAFKA_FETCH_MODE_LABELS, type KafkaStreamMonitorSnapshot } from "../contracts";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";

import { MetricPlot } from "./MetricPlot";
import {
  measurementAge,
  monitorBytes,
  monitorValue,
  withinMonitorWindow,
} from "./stream-monitor-presentation";
import type { RendererStreamMonitorSnapshot } from "./stream-monitor-observer";
import { formatUtcTimestamp } from "./timestamp-presentation";

export function MonitorDetails({
  label,
  rows,
}: {
  readonly label: string;
  readonly rows: readonly { readonly label: string; readonly value: string }[];
}): React.JSX.Element {
  return (
    <Box aria-label={label} component="dl" sx={{ m: 0 }}>
      {rows.map((row) => (
        <StudioDetailRow key={row.label} label={row.label} value={row.value} />
      ))}
    </Box>
  );
}

export function StreamMonitorDiagnostics({
  snapshot,
  hostHistory,
  renderer,
  now,
  window,
}: {
  readonly snapshot: KafkaStreamMonitorSnapshot;
  readonly hostHistory: readonly KafkaStreamMonitorSnapshot[];
  readonly renderer: RendererStreamMonitorSnapshot;
  readonly now: number;
  readonly window: readonly [string, string];
}): React.JSX.Element {
  const [expanded, setExpanded] = useState(false);
  const delivery = snapshot.delivery;
  const queue = snapshot.queue;
  const rendererHistory = renderer.history.filter(
    (sample) =>
      sample.operationId === snapshot.operationId && withinMonitorWindow(sample.sampledAt, window),
  );
  const timed = (value: number | null | undefined, sampledAt: string | null | undefined): string =>
    `${monitorValue(value, "ms")} · ${measurementAge(sampledAt ?? null, now)}`;
  const rows = rendererHistory.filter(
    (sample) =>
      sample.eventSampledAt !== null ||
      sample.renderSampledAt !== null ||
      sample.filterSampledAt !== null,
  );
  const frames = rendererHistory.filter((sample) => sample.fpsSampledAt !== null);
  return (
    <Box
      component="details"
      onToggle={(event) => setExpanded(event.currentTarget.open)}
      sx={{ borderTop: 1, borderColor: "divider", p: 2 }}
    >
      <Box
        component="summary"
        sx={{
          cursor: "pointer",
          typography: "subtitle2",
          py: 0.5,
          "&:focus-visible": { outline: "2px solid", outlineColor: "primary.main" },
        }}
      >
        Diagnostics
      </Box>
      {expanded ? (
        <>
          <Typography color="text.secondary" sx={{ mt: 1 }} variant="body2">
            Technical measurements and retained samples for this request. Application frame rate
            measures the visible document, not topic throughput.
          </Typography>
          <Box
            sx={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
              gap: 2,
              my: 2,
            }}
          >
            <Box component="section" aria-label="Request context">
              <Typography component="h3" variant="subtitle2">
                Request context
              </Typography>
              <MonitorDetails
                label="Stream context"
                rows={[
                  { label: "Evidence cluster", value: snapshot.connectionName ?? "Unavailable" },
                  { label: "Topic", value: snapshot.request?.topic ?? "Unavailable" },
                  { label: "Request ID", value: snapshot.operationId ?? "Unavailable" },
                  {
                    label: "Fetch mode",
                    value: snapshot.request
                      ? KAFKA_FETCH_MODE_LABELS[snapshot.request.mode]
                      : "Unavailable",
                  },
                  { label: "Maximum results", value: monitorValue(snapshot.request?.maxMessages) },
                  { label: "Lifecycle", value: snapshot.state },
                  {
                    label: "Last host sample",
                    value:
                      snapshot.sampledAt === null
                        ? "Unavailable"
                        : formatUtcTimestamp(snapshot.sampledAt),
                  },
                ]}
              />
            </Box>
            <Box component="section" aria-label="Host delivery diagnostics">
              <Typography component="h3" variant="subtitle2">
                Host delivery
              </Typography>
              <MonitorDetails
                label="Host delivery metrics"
                rows={[
                  {
                    label: "Received from consumer",
                    value: monitorValue(delivery?.receivedMessages),
                  },
                  {
                    label: "Published to display",
                    value: monitorValue(delivery?.publishedMessages),
                  },
                  { label: "Published batches", value: monitorValue(delivery?.batchCount) },
                  { label: "Last batch size", value: monitorValue(delivery?.lastBatchMessages) },
                  { label: "Rate window", value: monitorValue(delivery?.rateWindowMs, "ms") },
                  {
                    label: "Last queue wait",
                    value: timed(delivery?.queueWaitMs, delivery?.queueWaitSampledAt),
                  },
                  {
                    label: "Last host publication",
                    value: timed(delivery?.publicationDurationMs, delivery?.publicationSampledAt),
                  },
                  {
                    label: "Tuning source",
                    value:
                      delivery === null
                        ? "Unavailable"
                        : delivery.tuningSource === "confirmed"
                          ? "Confirmed preferences"
                          : "Factory fallback",
                  },
                  {
                    label: "Effective batch",
                    value: monitorValue(delivery?.batchSize, "messages"),
                  },
                  { label: "Shaping interval", value: monitorValue(delivery?.intervalMs, "ms") },
                  {
                    label: "History limit",
                    value: monitorValue(delivery?.historySamples, "samples"),
                  },
                  {
                    label: "Peak buffer",
                    value:
                      queue === null
                        ? "Unavailable"
                        : `${monitorValue(queue.peakMessages, "messages")} · ${monitorBytes(queue.peakBytes)}`,
                  },
                ]}
              />
            </Box>
            <Box component="section" aria-label="Renderer diagnostics">
              <Typography component="h3" variant="subtitle2">
                Renderer
              </Typography>
              <Typography color="text.secondary" variant="body2">
                {renderer.messagesMounted
                  ? "Messages workspace mounted."
                  : "Messages workspace unmounted. Filter and render timings below are last measured evidence."}
              </Typography>
              <MonitorDetails
                label="Renderer metrics"
                rows={[
                  { label: "Pending host events", value: monitorValue(renderer.eventBacklog) },
                  {
                    label: "Last event to React commit",
                    value: timed(renderer.eventToCommitMs, renderer.eventSampledAt),
                  },
                  {
                    label: "Last message filtering",
                    value: timed(renderer.filterDurationMs, renderer.filterSampledAt),
                  },
                  {
                    label: "Last message workspace render",
                    value: timed(renderer.renderDurationMs, renderer.renderSampledAt),
                  },
                  {
                    label: "Retained display rows",
                    value: monitorValue(renderer.retainedMessages),
                  },
                  {
                    label: "Visible rows at last message render",
                    value: renderer.messagesMounted
                      ? monitorValue(renderer.visibleMessages)
                      : "Unavailable while Messages is unmounted",
                  },
                  {
                    label: "Application frame rate",
                    value: `${monitorValue(renderer.fps, "FPS")} · ${measurementAge(renderer.fpsSampledAt, now)}`,
                  },
                  { label: "Frame sample window", value: monitorValue(renderer.fpsWindowMs, "ms") },
                  { label: "Frame sampling", value: renderer.samplingState },
                ]}
              />
            </Box>
          </Box>
          <Box
            sx={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
              gap: 2,
            }}
          >
            <MetricPlot
              height={164}
              timeDomain={window}
              zeroBaseline
              sampleLabels={rows.map((sample) => sample.sampledAt!)}
              series={[
                { label: "Event to commit", values: rows.map((sample) => sample.eventToCommitMs) },
                { label: "Message render", values: rows.map((sample) => sample.renderDurationMs) },
                {
                  label: "Message filtering",
                  values: rows.map((sample) => sample.filterDurationMs),
                },
              ]}
              title="Renderer work trend"
              unit="ms"
            />
            <MetricPlot
              height={164}
              timeDomain={window}
              zeroBaseline
              sampleLabels={frames.map((sample) => sample.fpsSampledAt!)}
              series={[{ label: "Application frames", values: frames.map((sample) => sample.fps) }]}
              title="Application frame-rate trend"
              unit="FPS"
            />
          </Box>
          <Typography component="h3" variant="subtitle2" sx={{ mt: 2 }}>
            Samples in selected time window
          </Typography>
          <TableContainer aria-label="Host samples scroll area" role="region" tabIndex={0}>
            <Table size="small" aria-label="Recent host samples">
              <TableHead>
                <TableRow>
                  <TableCell>Sample (UTC)</TableCell>
                  <TableCell>State</TableCell>
                  <TableCell align="right">Buffered</TableCell>
                  <TableCell align="right">Published</TableCell>
                  <TableCell align="right">Omitted</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {hostHistory.map((sample, index) => (
                  <TableRow key={`${sample.sampledAt}-${index}`}>
                    <TableCell>
                      {sample.sampledAt === null
                        ? "Unavailable"
                        : formatUtcTimestamp(sample.sampledAt)}
                    </TableCell>
                    <TableCell>{sample.state}</TableCell>
                    <TableCell align="right">
                      {monitorValue(sample.queue?.currentMessages)}
                    </TableCell>
                    <TableCell align="right">
                      {monitorValue(sample.delivery?.publishedMessages)}
                    </TableCell>
                    <TableCell align="right">
                      {monitorValue(sample.queue?.droppedMessages)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <TableContainer
            aria-label="Renderer samples scroll area"
            role="region"
            tabIndex={0}
            sx={{ mt: 2 }}
          >
            <Table size="small" aria-label="Recent renderer samples">
              <TableHead>
                <TableRow>
                  <TableCell>Sample (UTC)</TableCell>
                  <TableCell align="right">Event commit</TableCell>
                  <TableCell align="right">Message render</TableCell>
                  <TableCell align="right">App FPS</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {rendererHistory.map((sample, index) => (
                  <TableRow key={`${sample.sampledAt}-${index}`}>
                    <TableCell>
                      {sample.sampledAt === null
                        ? "Unavailable"
                        : formatUtcTimestamp(sample.sampledAt)}
                    </TableCell>
                    <TableCell align="right">
                      {monitorValue(sample.eventToCommitMs, "ms")}
                    </TableCell>
                    <TableCell align="right">
                      {monitorValue(sample.renderDurationMs, "ms")}
                    </TableCell>
                    <TableCell align="right">{monitorValue(sample.fps)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </>
      ) : null}
    </Box>
  );
}
