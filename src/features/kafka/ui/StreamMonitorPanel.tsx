import { useCallback, useEffect, useState, useSyncExternalStore } from "react";
import { Box, MenuItem, Select, Stack, Typography } from "@mui/material";

import {
  KAFKA_FETCH_MODE_LABELS,
  type HostError,
  type KafkaStreamMonitorSnapshot,
} from "../contracts";
import { StudioAlert, StudioButton } from "../../../platform/ui/controls";

import {
  type RendererStreamMonitorObserver,
  initialRendererStreamMonitorSample,
} from "./stream-monitor-observer";
import { MetricPlot } from "./MetricPlot";
import { StatusIndicator } from "./StatusIndicator";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";
import { MonitorDetails, StreamMonitorDiagnostics } from "./StreamMonitorDiagnostics";
import {
  MONITOR_TIME_WINDOWS,
  measurementAge,
  monitorBytes,
  monitorIsActive,
  monitorLossRows,
  monitorNumber,
  monitorRateSamples,
  monitorStatus,
  monitorValue,
  monitorWindow,
  scopedHostHistory,
  withinMonitorWindow,
} from "./stream-monitor-presentation";

export interface StreamMonitorPanelProperties {
  readonly activeConnectionName: string | null;
  readonly consumptionActive: boolean;
  readonly consumptionStopping: boolean;
  readonly consumptionError: HostError | null;
  readonly history: readonly KafkaStreamMonitorSnapshot[];
  readonly onOpenActivity: () => void;
  readonly onOpenObservedHealth: () => void;
  readonly onStop: () => void;
  readonly rendererObserver: RendererStreamMonitorObserver;
  readonly selectedTopic: string | null;
  readonly snapshot: KafkaStreamMonitorSnapshot;
}

function OperatorMetric({
  label,
  value,
  detail,
}: {
  readonly label: string;
  readonly value: string;
  readonly detail: string;
}): React.JSX.Element {
  return (
    <Box component="dl" sx={{ m: 0, minWidth: 0, p: 1.5, border: 1, borderColor: "divider" }}>
      <Typography component="dt" color="text.secondary" variant="body2">
        {label}
      </Typography>
      <Typography component="dd" variant="h6" sx={{ m: 0, mt: 0.5, overflowWrap: "anywhere" }}>
        {value}
      </Typography>
      <Typography color="text.secondary" variant="caption" component="dd" sx={{ m: 0, mt: 0.5 }}>
        {detail}
      </Typography>
    </Box>
  );
}

export function StreamMonitorPanel({
  activeConnectionName,
  consumptionActive,
  consumptionStopping,
  consumptionError,
  history,
  onOpenActivity,
  onOpenObservedHealth,
  onStop,
  rendererObserver,
  selectedTopic,
  snapshot,
}: StreamMonitorPanelProperties): React.JSX.Element {
  const subscribe = useCallback(
    (listener: () => void) => rendererObserver.subscribe(listener),
    [rendererObserver],
  );
  const getSnapshot = useCallback(() => rendererObserver.getSnapshot(), [rendererObserver]);
  const observedRenderer = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const renderer =
    observedRenderer.operationId === snapshot.operationId
      ? observedRenderer
      : {
          ...initialRendererStreamMonitorSample,
          operationId: null,
          messagesMounted: false,
          history: [],
        };
  const [now, setNow] = useState(Date.now);
  const [windowSeconds, setWindowSeconds] = useState(60);
  useEffect(() => {
    const timer = globalThis.setInterval(() => setNow(Date.now()), 1_000);
    return () => globalThis.clearInterval(timer);
  }, []);
  const status = monitorStatus(snapshot, now);
  const window = monitorWindow(snapshot, now, windowSeconds);
  const scopedHistory = scopedHostHistory(snapshot, history);
  const hostSamples = scopedHistory.filter((sample) =>
    withinMonitorWindow(sample.sampledAt, window),
  );
  const rates = monitorRateSamples(scopedHistory, window);
  const queue = snapshot.queue;
  const delivery = snapshot.delivery;
  const historical = !monitorIsActive(snapshot) || status.stale;
  const topic = snapshot.request?.topic ?? selectedTopic;
  const stopLabel = snapshot.request?.mode === "tail" ? "Stop tail" : "Cancel fetch";
  const occupancy =
    queue === null
      ? null
      : Math.max(
          queue.currentMessages / queue.capacityMessages,
          queue.currentBytes / queue.capacityBytes,
        ) * 100;
  return (
    <Box
      aria-label="Stream monitor"
      component="section"
      sx={{ bgcolor: "background.paper", height: "100%", minHeight: 0, overflow: "auto" }}
    >
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
          tone={status.tone}
        />
        {consumptionActive ? (
          <StudioButton
            aria-label={`${stopLabel} ${topic ?? ""}`}
            disabled={consumptionStopping}
            onClick={onStop}
            size="small"
            variant="outlined"
          >
            {consumptionStopping ? "Stopping…" : stopLabel}
          </StudioButton>
        ) : null}
      </TopicWorkspaceToolbar>
      {snapshot.state === "unavailable" ? (
        <Box sx={{ p: 2 }}>
          <Typography component="h3" variant="subtitle2">
            No live sample for {topic ?? "the selected topic"}.
          </Typography>
          <Typography color="text.secondary" sx={{ mt: 0.5 }} variant="body2">
            Start a message request in Messages to collect delivery measurements.
          </Typography>
        </Box>
      ) : (
        <>
          <Stack spacing={0.5} sx={{ px: 2, py: 1.5 }}>
            <Typography variant="body2">{status.explanation}</Typography>
            <Typography color="text.secondary" variant="caption">
              {snapshot.connectionName ?? activeConnectionName ?? "Unknown cluster"} · {topic} ·{" "}
              {snapshot.request === null
                ? "Request unavailable"
                : `${KAFKA_FETCH_MODE_LABELS[snapshot.request.mode]} · ${monitorNumber(snapshot.request.maxMessages)} record limit`}{" "}
              · {measurementAge(snapshot.sampledAt, now)}
              {historical ? " · Historical request evidence" : " · Current request"}
            </Typography>
          </Stack>
          {status.stale ? (
            <StudioAlert severity="warning" sx={{ mx: 2, mb: 1.5 }}>
              Retained evidence is stale. Check the connection or start a new message request.
            </StudioAlert>
          ) : null}
          {snapshot.state === "failed" ? (
            <StudioAlert
              action={
                <StudioButton color="inherit" onClick={onOpenActivity} size="small">
                  Open activity
                </StudioButton>
              }
              severity="error"
              sx={{ mx: 2, mb: 1.5 }}
            >
              <Typography variant="body2">
                {consumptionError?.summary ?? "Message consumption failed."}{" "}
                {consumptionError?.recovery ?? "Open Activity for failure and recovery guidance."}
              </Typography>
            </StudioAlert>
          ) : null}
          <Box
            aria-label={historical ? "Last stream measurements" : "Current stream measurements"}
            component="section"
            sx={{
              display: "grid",
              gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
              gap: 1.5,
              px: 2,
            }}
          >
            <OperatorMetric
              label={historical ? "Last delivery rate" : "Published rate"}
              value={monitorValue(delivery?.messagesPerSecond, "msg/s")}
              detail={`${measurementAge(delivery?.rateSampledAt ?? null, now)} · ${monitorValue(delivery?.rateWindowMs, "ms")} window`}
            />
            <OperatorMetric
              label={historical ? "Last buffered records" : "Buffered records"}
              value={
                queue === null
                  ? "Unavailable"
                  : `${monitorNumber(queue.currentMessages)} / ${monitorNumber(queue.capacityMessages)}`
              }
              detail={
                queue === null
                  ? "No buffer sample"
                  : `${monitorBytes(queue.currentBytes)} / ${monitorBytes(queue.capacityBytes)} · ${monitorValue(occupancy, "%")} capacity`
              }
            />
            <OperatorMetric
              label="Host display omissions"
              value={monitorValue(queue?.droppedMessages)}
              detail="Cumulative host omissions; reasons below"
            />
            <OperatorMetric
              label="Oldest buffered record"
              value={
                queue?.currentMessages === 0
                  ? "None buffered"
                  : monitorValue(queue?.oldestMessageAgeMs, "ms")
              }
              detail={
                queue?.currentMessages === 0
                  ? "Buffer is empty"
                  : measurementAge(snapshot.sampledAt, now)
              }
            />
          </Box>
          <Box aria-label="Stream trends" component="section" sx={{ p: 2 }}>
            <Stack
              direction="row"
              sx={{ alignItems: "center", justifyContent: "space-between", gap: 1, mb: 1.5 }}
            >
              <Typography component="h3" variant="subtitle2">
                Delivery and buffering
              </Typography>
              <Select
                size="small"
                inputProps={{ "aria-label": "Chart time window" }}
                value={windowSeconds}
                onChange={(event) => setWindowSeconds(Number(event.target.value))}
              >
                {MONITOR_TIME_WINDOWS.map((seconds) => (
                  <MenuItem key={seconds} value={seconds}>
                    {seconds < 60 ? `${seconds}s` : `${seconds / 60} min`}
                  </MenuItem>
                ))}
              </Select>
            </Stack>
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))",
                gap: 1.5,
              }}
            >
              <MetricPlot
                height={190}
                timeDomain={window}
                zeroBaseline
                sampleLabels={rates.map((sample) => sample.sampledAt)}
                series={[{ label: "Published", values: rates.map((sample) => sample.value) }]}
                title="Delivery rate trend"
                unit="msg/s"
              />
              <MetricPlot
                height={190}
                timeDomain={window}
                zeroBaseline
                interpolation="step"
                sampleLabels={hostSamples.map((sample) => sample.sampledAt!)}
                series={[
                  {
                    label: "Buffered",
                    values: hostSamples.map((sample) => sample.queue?.currentMessages ?? null),
                  },
                ]}
                title="Buffer depth trend"
                unit="messages"
              />
            </Box>
            <Typography color="text.secondary" variant="caption" sx={{ display: "block", mt: 1 }}>
              Both charts use the same{" "}
              {windowSeconds < 60 ? `${windowSeconds}-second` : `${windowSeconds / 60}-minute`} UTC
              window. Gaps mean no measurement. Delivery is host publication to the display
              transport.
            </Typography>
          </Box>
          <Box aria-label="Historical display loss" component="section" sx={{ px: 2, pb: 2 }}>
            <Typography component="h3" variant="subtitle2">
              Display history and omissions
            </Typography>
            <Typography color="text.secondary" variant="body2" sx={{ my: 0.5 }}>
              Totals belong to this request and do not indicate current pressure. Display retention
              can remove older rows. These counters do not establish loss in Kafka.
            </Typography>
            <MonitorDetails
              label="Display omission reasons"
              rows={monitorLossRows(snapshot, renderer)}
            />
          </Box>
          <StreamMonitorDiagnostics
            snapshot={snapshot}
            hostHistory={hostSamples}
            renderer={renderer}
            now={now}
            window={window}
          />
        </>
      )}
      <Stack
        direction="row"
        sx={{
          alignItems: "center",
          px: 2,
          py: 1,
          gap: 1,
          flexWrap: "wrap",
          borderTop: 1,
          borderColor: "divider",
        }}
      >
        <Typography color="text.secondary" variant="body2">
          For cluster and broker observations:
        </Typography>
        <StudioButton onClick={onOpenObservedHealth} size="small">
          Observed health
        </StudioButton>
      </Stack>
    </Box>
  );
}
