import { type ReactNode, useCallback, useSyncExternalStore } from "react";
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

import {
  KAFKA_FETCH_MODE_LABELS,
  type HostError,
  type KafkaStreamMonitorSnapshot,
} from "../contracts";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";
import { StudioAlert, StudioButton } from "../../../platform/ui/controls";
import { studioSpace } from "../../../platform/ui/muiSpacing";

import {
  RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT,
  RENDERER_STREAM_MONITOR_PRESSURE_LIMITS,
  type RendererStreamMonitorObserver,
  type RendererStreamMonitorSample,
  type RendererStreamMonitorSnapshot,
} from "./stream-monitor-observer";
import { DiagnosticMetric } from "./DiagnosticMetric";
import { MetricPlot } from "./MetricPlot";
import { StatusIndicator, type StatusIndicatorTone } from "./StatusIndicator";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";
import { formatUtcTimestamp, formatUtcTimestampStacked } from "./timestamp-presentation";

const RECENT_SAMPLE_PRESENTATION_LIMIT = 12;

export interface StreamMonitorPanelProperties {
  readonly activeConnectionName: string | null;
  readonly consumptionError: HostError | null;
  readonly history: readonly KafkaStreamMonitorSnapshot[];
  readonly onOpenActivity: () => void;
  readonly rendererObserver: RendererStreamMonitorObserver;
  readonly selectedTopic: string | null;
  readonly snapshot: KafkaStreamMonitorSnapshot;
}

interface MetricRow {
  readonly label: string;
  readonly value: string;
}

type MonitorPresentationStatus =
  | "backpressure"
  | "degraded"
  | "idle"
  | "nominal"
  | "sampling"
  | "stale"
  | "terminal"
  | "unavailable";

interface PresentedStatus {
  readonly color: StatusIndicatorTone;
  readonly explanation: string;
  readonly kind: MonitorPresentationStatus;
  readonly label: string;
}

function formatNumber(value: number): string {
  return value.toLocaleString();
}

function formatBytes(value: number): string {
  if (value >= 1_048_576) {
    return `${(value / 1_048_576).toLocaleString(undefined, {
      maximumFractionDigits: 2,
    })} MiB`;
  }
  if (value >= 1_024) {
    return `${(value / 1_024).toLocaleString(undefined, {
      maximumFractionDigits: 2,
    })} KiB`;
  }
  return `${formatNumber(value)} B`;
}

function formatDuration(value: number | null): string {
  return value === null
    ? "Unavailable"
    : `${value.toLocaleString(undefined, { maximumFractionDigits: 3 })} ms`;
}

function formatRate(value: number | null): string {
  return value === null
    ? "Unavailable"
    : `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })} msg/s`;
}

function formatSampleTime(value: string | null): string {
  return value === null ? "Unavailable" : formatUtcTimestampStacked(value);
}

function countLabel(value: number, singular: string, plural: string): string {
  return `${formatNumber(value)} ${value === 1 ? singular : plural}`;
}

function rendererPressureExplanation(renderer: RendererStreamMonitorSnapshot): string | null {
  if (renderer.rendererDroppedMessages > 0) {
    return "Renderer evictions confirm record loss.";
  }
  if (renderer.eventBacklog >= RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT) {
    return `Renderer event backlog reached its ${formatNumber(
      RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT,
    )}-event observation bound.`;
  }
  if (
    renderer.eventToCommitMs !== null &&
    renderer.eventToCommitMs > RENDERER_STREAM_MONITOR_PRESSURE_LIMITS.eventToCommitMs
  ) {
    return `Host-event-to-commit time exceeded ${formatNumber(
      RENDERER_STREAM_MONITOR_PRESSURE_LIMITS.eventToCommitMs,
    )} ms.`;
  }
  if (
    renderer.filterDurationMs !== null &&
    renderer.filterDurationMs > RENDERER_STREAM_MONITOR_PRESSURE_LIMITS.filterDurationMs
  ) {
    return `Message filtering work exceeded ${formatNumber(
      RENDERER_STREAM_MONITOR_PRESSURE_LIMITS.filterDurationMs,
    )} ms.`;
  }
  if (
    renderer.renderDurationMs !== null &&
    renderer.renderDurationMs > RENDERER_STREAM_MONITOR_PRESSURE_LIMITS.renderDurationMs
  ) {
    return `Message workspace render work exceeded ${formatNumber(
      RENDERER_STREAM_MONITOR_PRESSURE_LIMITS.renderDurationMs,
    )} ms.`;
  }
  return null;
}

function lifecycleLabel(state: KafkaStreamMonitorSnapshot["state"]): string {
  switch (state) {
    case "unavailable":
      return "Unavailable";
    case "loading":
      return "Loading";
    case "fetching":
      return "Fetching";
    case "streaming":
      return "Streaming";
    case "complete":
      return "Complete";
    case "stopped":
      return "Stopped";
    case "empty":
      return "Empty";
    case "failed":
      return "Failed";
    case "stale":
      return "Stale";
  }
}

function presentedStatus(
  snapshot: KafkaStreamMonitorSnapshot,
  renderer: RendererStreamMonitorSnapshot,
): PresentedStatus {
  if (snapshot.state === "unavailable" || snapshot.status === "unavailable") {
    return {
      color: "neutral",
      explanation: "No current stream evidence is available.",
      kind: "unavailable",
      label: "Unavailable",
    };
  }
  if (snapshot.state === "stale" || snapshot.status === "stale") {
    return {
      color: "warning",
      explanation: "The retained evidence no longer belongs to an active connection.",
      kind: "stale",
      label: "Stale",
    };
  }
  if (snapshot.state === "failed" || snapshot.status === "degraded") {
    return {
      color: "error",
      explanation: "The message operation failed; the last measurements are not healthy.",
      kind: "degraded",
      label: "Degraded",
    };
  }
  const rendererPressure = rendererPressureExplanation(renderer);
  if (snapshot.state === "complete" || snapshot.state === "stopped" || snapshot.state === "empty") {
    const lifecycle = lifecycleLabel(snapshot.state);
    const hostPressure =
      snapshot.status === "backpressure"
        ? "The host queue confirms record loss or capacity pressure."
        : null;
    const pressure = hostPressure ?? rendererPressure;
    if (pressure !== null) {
      return {
        color: "warning",
        explanation: `The ${lifecycle.toLowerCase()} operation is no longer sampling. Last ${
          hostPressure === null ? "renderer" : "host"
        } evidence: ${pressure}`,
        kind: "terminal",
        label: `${lifecycle} · Backpressure`,
      };
    }
    const evidenceLabel = snapshot.status === "nominal" ? "Nominal" : "Idle";
    return {
      color: snapshot.status === "nominal" ? "success" : "neutral",
      explanation: `The ${lifecycle.toLowerCase()} operation is no longer sampling.`,
      kind: "terminal",
      label: `${lifecycle} · ${evidenceLabel}`,
    };
  }
  if (snapshot.status === "backpressure") {
    return {
      color: "warning",
      explanation: "The host queue confirms record loss or capacity pressure.",
      kind: "backpressure",
      label: "Backpressure",
    };
  }
  if (rendererPressure !== null) {
    return {
      color: "warning",
      explanation: rendererPressure,
      kind: "backpressure",
      label: "Backpressure",
    };
  }
  if (snapshot.state === "loading" || snapshot.state === "fetching") {
    return {
      color: "neutral",
      explanation: "The request is active, but no complete delivery sample exists yet.",
      kind: "sampling",
      label: "Sampling",
    };
  }
  if (snapshot.status === "idle") {
    return {
      color: "neutral",
      explanation: "The stream is active with no queued records or measured delivery.",
      kind: "idle",
      label: "Idle",
    };
  }
  if (renderer.samplingState === "sampling") {
    return {
      color: "neutral",
      explanation: "Host delivery is current; visible-frame sampling is still in progress.",
      kind: "sampling",
      label: "Sampling",
    };
  }
  if (renderer.samplingState === "hidden") {
    return {
      color: "neutral",
      explanation: "Host delivery is current; renderer frame sampling is paused while hidden.",
      kind: "unavailable",
      label: "Renderer hidden",
    };
  }
  if (renderer.samplingState === "unavailable") {
    return {
      color: "neutral",
      explanation: "Host delivery is current; renderer frame evidence is unavailable.",
      kind: "unavailable",
      label: "Renderer unavailable",
    };
  }
  if (snapshot.status === "nominal") {
    return {
      color: "success",
      explanation: "Current evidence confirms delivery without observed record loss.",
      kind: "nominal",
      label: "Nominal",
    };
  }
  return {
    color: "neutral",
    explanation: "Current monitor evidence does not support a health conclusion.",
    kind: "unavailable",
    label: "Unavailable",
  };
}

function MetricList({
  label,
  rows,
}: {
  readonly label: string;
  readonly rows: readonly MetricRow[];
}): React.JSX.Element {
  return (
    <Box aria-label={label} component="dl" sx={{ m: 0 }}>
      {rows.map((row) => (
        <StudioDetailRow key={row.label} label={row.label} value={row.value} />
      ))}
    </Box>
  );
}

function MonitorSection({
  children,
  title,
}: {
  readonly children: ReactNode;
  readonly title: string;
}): React.JSX.Element {
  const headingId = `stream-monitor-${title.toLowerCase().replaceAll(" ", "-")}-heading`;
  return (
    <Box
      aria-labelledby={headingId}
      component="section"
      sx={{ borderTop: 1, borderColor: "divider", minWidth: 0, py: studioSpace.space8 }}
    >
      <Typography
        component="h3"
        id={headingId}
        sx={{ px: studioSpace.space12, pb: studioSpace.space8 }}
        variant="subtitle2"
      >
        {title}
      </Typography>
      <Box
        className="studio-monitor-section-body"
        sx={{
          bgcolor: "background.paper",
          minWidth: 0,
          overflow: "hidden",
          "& tr > th:first-of-type, & tr > td:first-of-type": { pl: studioSpace.space12 },
        }}
      >
        {children}
      </Box>
    </Box>
  );
}

function queueRows(snapshot: KafkaStreamMonitorSnapshot): readonly MetricRow[] {
  const queue = snapshot.queue;
  return [
    {
      label: "Current queue",
      value:
        queue === null
          ? "Unavailable"
          : `${formatNumber(queue.currentMessages)} / ${formatNumber(queue.capacityMessages)} messages`,
    },
    {
      label: "Peak queue",
      value: queue === null ? "Unavailable" : countLabel(queue.peakMessages, "message", "messages"),
    },
    {
      label: "Current queued bytes",
      value:
        queue === null
          ? "Unavailable"
          : `${formatBytes(queue.currentBytes)} / ${formatBytes(queue.capacityBytes)}`,
    },
    {
      label: "Peak queued bytes",
      value: queue === null ? "Unavailable" : formatBytes(queue.peakBytes),
    },
    {
      label: "Drops",
      value:
        queue === null
          ? "Unavailable"
          : `${formatNumber(queue.droppedMessages)} total · ${formatNumber(
              queue.droppedSincePrevious,
            )} since prior sample`,
    },
    {
      label: "Drop rate",
      value: queue === null ? "Unavailable" : formatRate(queue.droppedPerSecond),
    },
  ];
}

function deliveryRows(snapshot: KafkaStreamMonitorSnapshot): readonly MetricRow[] {
  const delivery = snapshot.delivery;
  return [
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
      value:
        delivery === null ? "Unavailable" : countLabel(delivery.batchSize, "message", "messages"),
    },
    {
      label: "Shaping interval",
      value: delivery === null ? "Unavailable" : `${formatNumber(delivery.intervalMs)} ms`,
    },
    {
      label: "History limit",
      value:
        delivery === null
          ? "Unavailable"
          : countLabel(delivery.historySamples, "sample", "samples"),
    },
    {
      label: "Received",
      value: delivery === null ? "Unavailable" : formatNumber(delivery.receivedMessages),
    },
    {
      label: "Delivered",
      value: delivery === null ? "Unavailable" : formatNumber(delivery.deliveredMessages),
    },
    {
      label: "Published batches",
      value: delivery === null ? "Unavailable" : formatNumber(delivery.batchCount),
    },
    {
      label: "Latest batch",
      value:
        delivery === null
          ? "Unavailable"
          : countLabel(delivery.lastBatchMessages, "message", "messages"),
    },
    {
      label: "Delivery rate",
      value: delivery === null ? "Unavailable" : formatRate(delivery.messagesPerSecond),
    },
    {
      label: "Queue wait",
      value: delivery === null ? "Unavailable" : formatDuration(delivery.queueWaitMs),
    },
    {
      label: "Host publication",
      value: delivery === null ? "Unavailable" : formatDuration(delivery.publicationDurationMs),
    },
  ];
}

function rendererRows(renderer: RendererStreamMonitorSnapshot): readonly MetricRow[] {
  return [
    {
      label: "Event backlog",
      value: countLabel(renderer.eventBacklog, "event", "events"),
    },
    {
      label: "Event to React commit",
      value: formatDuration(renderer.eventToCommitMs),
    },
    {
      label: "Message filtering",
      value: formatDuration(renderer.filterDurationMs),
    },
    {
      label: "Message workspace render",
      value: formatDuration(renderer.renderDurationMs),
    },
    {
      label: "Retained rows",
      value: countLabel(renderer.retainedMessages, "message", "messages"),
    },
    {
      label: "Visible rows",
      value: countLabel(renderer.visibleMessages, "message", "messages"),
    },
    {
      label: "Renderer evictions",
      value: countLabel(renderer.rendererDroppedMessages, "message", "messages"),
    },
    {
      label: "Visible frame rate",
      value: renderer.fps === null ? "Unavailable" : `${formatNumber(renderer.fps)} FPS`,
    },
    {
      label: "Frame sampling",
      value: lifecycleLabelForRenderer(renderer.samplingState),
    },
  ];
}

function lifecycleLabelForRenderer(state: RendererStreamMonitorSnapshot["samplingState"]): string {
  switch (state) {
    case "unavailable":
      return "Unavailable";
    case "sampling":
      return "Sampling";
    case "ready":
      return "Ready";
    case "hidden":
      return "Document hidden";
  }
}

function hostHistoryRows(
  history: readonly KafkaStreamMonitorSnapshot[],
): readonly KafkaStreamMonitorSnapshot[] {
  return history.slice(-RECENT_SAMPLE_PRESENTATION_LIMIT);
}

function rendererHistoryRows(
  history: readonly RendererStreamMonitorSample[],
): readonly RendererStreamMonitorSample[] {
  return history.slice(-RECENT_SAMPLE_PRESENTATION_LIMIT);
}

export function StreamMonitorPanel({
  activeConnectionName,
  consumptionError,
  history,
  onOpenActivity,
  rendererObserver,
  selectedTopic,
  snapshot,
}: StreamMonitorPanelProperties): React.JSX.Element {
  const subscribe = useCallback(
    (listener: () => void): (() => void) => rendererObserver.subscribe(listener),
    [rendererObserver],
  );
  const getSnapshot = useCallback(
    (): RendererStreamMonitorSnapshot => rendererObserver.getSnapshot(),
    [rendererObserver],
  );
  const renderer = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const status = presentedStatus(snapshot, renderer);
  const hostSampleUnavailable =
    snapshot.state === "unavailable" || snapshot.status === "unavailable";
  const topic = snapshot.request?.topic ?? selectedTopic;
  const recentHost = hostHistoryRows(history);
  const recentRenderer = rendererHistoryRows(renderer.history);
  const summary = (
    <>
      <TopicWorkspaceToolbar label="Monitor controls">
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography component="h2" noWrap variant="subtitle2">
            Stream Monitor
          </Typography>
        </Box>
        <StatusIndicator
          ariaLabel="Stream monitor status"
          label={status.label}
          live="polite"
          tone={status.color}
        />
      </TopicWorkspaceToolbar>
      {hostSampleUnavailable ? null : (
        <Typography color="text.secondary" component="p" sx={{ px: 2, py: 1 }} variant="body2">
          {status.explanation}
        </Typography>
      )}
    </>
  );

  if (hostSampleUnavailable) {
    return (
      <Box
        aria-label="Stream monitor"
        component="section"
        sx={{
          bgcolor: "background.paper",
          height: "100%",
          minHeight: 0,
          overflow: "auto",
        }}
      >
        {summary}
        <Box component="section" sx={{ maxWidth: 720, p: 2 }}>
          <Typography component="h3" variant="subtitle2">
            No live sample for {topic ?? "the selected topic"}.
          </Typography>
          <Typography color="text.secondary" sx={{ mt: 0.5 }} variant="body2">
            Activate the selected topic to start message consumption and collect current host and
            renderer evidence.
          </Typography>
        </Box>
      </Box>
    );
  }

  return (
    <Box
      aria-label="Stream monitor"
      component="section"
      sx={{
        bgcolor: "background.paper",
        height: "100%",
        minHeight: 0,
        overflow: "auto",
      }}
    >
      {summary}

      <Box
        aria-label="Current stream measurements"
        component="section"
        sx={{
          bgcolor: "background.paper",
          borderBottom: 1,
          borderColor: "divider",
          borderTop: 1,
          display: "grid",
          gridTemplateColumns: {
            md: "repeat(5, minmax(0, 1fr))",
            xs: "repeat(2, minmax(0, 1fr))",
          },
          m: 0,
          "& > :last-child": { borderRight: 0 },
        }}
      >
        <DiagnosticMetric
          label="Queue depth"
          value={
            snapshot.queue === null
              ? "Unavailable"
              : `${formatNumber(snapshot.queue.currentMessages)} / ${formatNumber(snapshot.queue.capacityMessages)}`
          }
        />
        <DiagnosticMetric
          label="Delivery rate"
          value={
            snapshot.delivery === null
              ? "Unavailable"
              : formatRate(snapshot.delivery.messagesPerSecond)
          }
        />
        <DiagnosticMetric
          label="Host publication"
          value={
            snapshot.delivery === null
              ? "Unavailable"
              : formatDuration(snapshot.delivery.publicationDurationMs)
          }
        />
        <DiagnosticMetric label="React commit" value={formatDuration(renderer.eventToCommitMs)} />
        <DiagnosticMetric
          label="Visible frames"
          value={renderer.fps === null ? "Unavailable" : `${formatNumber(renderer.fps)} FPS`}
        />
      </Box>

      {status.kind === "stale" ? (
        <StudioAlert severity="warning" sx={{ mx: 2, my: 1 }}>
          Retained evidence is stale. Start a new message request for current measurements.
        </StudioAlert>
      ) : null}

      {status.kind === "degraded" ? (
        <StudioAlert
          action={
            <StudioButton color="inherit" onClick={onOpenActivity} size="small">
              Open activity
            </StudioButton>
          }
          severity="error"
          sx={{ mx: 2, my: 1 }}
        >
          <Typography component="p" variant="subtitle2">
            {consumptionError?.summary ?? "Message consumption failed."}
          </Typography>
          <Typography component="p" variant="body2">
            {consumptionError?.recovery ??
              "Open Activity for the exact failure and recovery guidance."}
          </Typography>
        </StudioAlert>
      ) : null}

      <Box
        aria-label="Stream trends"
        component="section"
        sx={{
          display: "grid",
          gap: 1.5,
          gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
          p: 2,
        }}
      >
        <Typography component="h3" sx={{ gridColumn: "1 / -1" }} variant="subtitle2">
          Trends
        </Typography>
        <MetricPlot
          interpolation="step"
          sampleLabels={recentHost.map(
            (sample, index) => sample.sampledAt ?? `Sample ${String(index + 1)}`,
          )}
          series={[
            {
              label: "Queue",
              values: recentHost.map((sample) => sample.queue?.currentMessages ?? null),
            },
          ]}
          title="Queue depth trend"
          unit="messages"
        />
        <MetricPlot
          sampleLabels={recentHost.map(
            (sample, index) => sample.sampledAt ?? `Sample ${String(index + 1)}`,
          )}
          series={[
            {
              label: "Delivery",
              values: recentHost.map((sample) => sample.delivery?.messagesPerSecond ?? null),
            },
          ]}
          title="Delivery rate trend"
          unit="msg/s"
        />
        <MetricPlot
          sampleLabels={recentRenderer.map(
            (sample, index) => sample.sampledAt ?? `Sample ${String(index + 1)}`,
          )}
          series={[
            {
              label: "Event to commit",
              values: recentRenderer.map((sample) => sample.eventToCommitMs),
            },
            { label: "Render", values: recentRenderer.map((sample) => sample.renderDurationMs) },
          ]}
          title="Renderer work trend"
          unit="ms"
        />
        <MetricPlot
          sampleLabels={recentRenderer.map(
            (sample, index) => sample.sampledAt ?? `Sample ${String(index + 1)}`,
          )}
          series={[{ label: "Frames", values: recentRenderer.map((sample) => sample.fps) }]}
          title="Visible frame-rate trend"
          unit="FPS"
        />
      </Box>

      <MonitorSection title="Context">
        <MetricList
          label="Stream context"
          rows={[
            {
              label: snapshot.state === "stale" ? "Evidence cluster" : "Active cluster",
              value: snapshot.connectionName ?? activeConnectionName ?? "Unavailable",
            },
            {
              label: "Topic",
              value: topic ?? "Unavailable",
            },
            {
              label: "Fetch mode",
              value:
                snapshot.request === null
                  ? "Unavailable"
                  : KAFKA_FETCH_MODE_LABELS[snapshot.request.mode],
            },
            {
              label: "Maximum results",
              value:
                snapshot.request === null
                  ? "Unavailable"
                  : formatNumber(snapshot.request.maxMessages),
            },
            {
              label: "Lifecycle",
              value: lifecycleLabel(snapshot.state),
            },
            {
              label: "Last sampled",
              value: formatSampleTime(snapshot.sampledAt),
            },
          ]}
        />
      </MonitorSection>

      <MonitorSection title="Host queue">
        <MetricList label="Host queue metrics" rows={queueRows(snapshot)} />
      </MonitorSection>
      <MonitorSection title="Delivery">
        <MetricList label="Host delivery metrics" rows={deliveryRows(snapshot)} />
      </MonitorSection>
      <MonitorSection title="Renderer">
        <MetricList label="Renderer metrics" rows={rendererRows(renderer)} />
      </MonitorSection>
      <MonitorSection title="Recent host samples">
        {recentHost.length === 0 ? (
          <Typography color="text.secondary" sx={{ px: 2, pb: 1 }} variant="body2">
            No host samples retained.
          </Typography>
        ) : (
          <TableContainer aria-label="Recent host samples scroll area" role="region" tabIndex={0}>
            <Table aria-label="Recent host samples">
              <TableHead>
                <TableRow>
                  <TableCell>Sample</TableCell>
                  <TableCell>State</TableCell>
                  <TableCell align="right">Queue</TableCell>
                  <TableCell align="right">Delivered</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {recentHost.map((sample, index) => (
                  <TableRow key={`${sample.sampledAt ?? "unavailable"}-${String(index)}`}>
                    <TableCell title={sample.sampledAt ?? undefined}>
                      {sample.sampledAt === null
                        ? "Unavailable"
                        : formatUtcTimestamp(sample.sampledAt)}
                    </TableCell>
                    <TableCell>{lifecycleLabel(sample.state)}</TableCell>
                    <TableCell align="right">
                      {sample.queue === null
                        ? "Unavailable"
                        : formatNumber(sample.queue.currentMessages)}
                    </TableCell>
                    <TableCell align="right">
                      {sample.delivery === null
                        ? "Unavailable"
                        : formatNumber(sample.delivery.deliveredMessages)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </MonitorSection>
      <MonitorSection title="Recent renderer samples">
        {recentRenderer.length === 0 ? (
          <Typography color="text.secondary" sx={{ px: 2, pb: 2 }} variant="body2">
            No renderer samples retained.
          </Typography>
        ) : (
          <TableContainer
            aria-label="Recent renderer samples scroll area"
            role="region"
            tabIndex={0}
          >
            <Table aria-label="Recent renderer samples">
              <TableHead>
                <TableRow>
                  <TableCell>Sample</TableCell>
                  <TableCell align="right">Backlog</TableCell>
                  <TableCell align="right">Commit</TableCell>
                  <TableCell align="right">FPS</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {recentRenderer.map((sample, index) => (
                  <TableRow key={`${sample.sampledAt ?? "unavailable"}-${String(index)}`}>
                    <TableCell title={sample.sampledAt ?? undefined}>
                      {sample.sampledAt === null
                        ? "Unavailable"
                        : formatUtcTimestamp(sample.sampledAt)}
                    </TableCell>
                    <TableCell align="right">{formatNumber(sample.eventBacklog)}</TableCell>
                    <TableCell align="right">{formatDuration(sample.eventToCommitMs)}</TableCell>
                    <TableCell align="right">
                      {sample.fps === null ? "Unavailable" : formatNumber(sample.fps)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </MonitorSection>
    </Box>
  );
}
