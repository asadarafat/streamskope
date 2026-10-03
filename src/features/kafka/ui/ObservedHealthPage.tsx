import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  StudioButton as Button,
  StudioAlert as Alert,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  observationLag,
  type ObservationSeries,
  type ObservationSnapshot,
} from "../contracts/observations";

import { MetricPlot } from "./MetricPlot";

function nullableThreshold(value: string): number | null {
  if (!value.trim()) return null;
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) throw new Error("Enter a non-negative threshold.");
  return number;
}
export function ObservedHealthPage({
  host,
}: {
  readonly host: StreamSkopeHost;
}): React.JSX.Element {
  const [topic, setTopic] = useState(""),
    [groupId, setGroupId] = useState("");
  const [lagThreshold, setLagThreshold] = useState(""),
    [latencyThreshold, setLatencyThreshold] = useState("");
  const [snapshot, setSnapshot] = useState<ObservationSnapshot>({
    schemaVersion: 1,
    series: [],
    durability: "session",
  });
  const [selected, setSelected] = useState(""),
    [running, setRunning] = useState(false),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const [confirmation, setConfirmation] = useState(""),
    [now, setNow] = useState(Date.now());
  const mounted = useRef(true),
    generation = useRef(0),
    inFlight = useRef(false);
  const series = snapshot.series.find((s) => observationIdentity(s) === selected);
  const latest = series?.samples.at(-1);
  const fresh =
    latest !== undefined && now >= latest.observedAt && now - latest.observedAt <= limits.staleMs;
  const update = (next: ObservationSnapshot): void => {
    setSnapshot(next);
    setSelected((previous) =>
      next.series.some((s) => observationIdentity(s) === previous)
        ? previous
        : next.series.at(-1)
          ? observationIdentity(next.series.at(-1)!)
          : "",
    );
  };
  useEffect(() => {
    mounted.current = true;
    void host
      .execute({
        command: "observations.history",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .then((r) => {
        if (!mounted.current) return;
        if (r.ok) update(r.result.snapshot);
        else setError(r.error.summary + " " + r.error.recovery);
      })
      .catch(() => {
        if (mounted.current) setError("Observation history could not be loaded.");
      });
    const clock = setInterval(() => setNow(Date.now()), 1000);
    return (): void => {
      mounted.current = false;
      generation.current++;
      clearInterval(clock);
      void host
        .execute({
          command: "observations.cancel",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {},
        })
        .catch(() => undefined);
    };
  }, [host]);
  const capture = async (): Promise<void> => {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError("");
    const current = generation.current;
    try {
      const r = await host.execute({
        command: "observations.capture",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {
          topic: topic.trim(),
          groupId: groupId.trim() || null,
          thresholds: {
            lag: nullableThreshold(lagThreshold),
            requestMs: nullableThreshold(latencyThreshold),
          },
        },
      });
      if (!mounted.current || generation.current !== current) return;
      if (!r.ok) {
        setError(r.error.summary + " " + r.error.recovery);
        return;
      }
      const next = r.result.capture;
      setSnapshot((previous) => ({
        schemaVersion: 1,
        durability: next.durability,
        series: [
          ...previous.series.filter(
            (s) => observationIdentity(s) !== observationIdentity(next.series),
          ),
          next.series,
        ].slice(-limits.series),
      }));
      setSelected(observationIdentity(next.series));
      setNow(Date.now());
    } catch {
      if (mounted.current && generation.current === current)
        setError(
          "Observation unavailable. Check the selection, thresholds and current connection.",
        );
    } finally {
      inFlight.current = false;
      if (mounted.current) setBusy(false);
    }
  };
  const captureRef = useRef(capture);
  captureRef.current = capture;
  useEffect(() => {
    if (!running) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async (): Promise<void> => {
      await captureRef.current();
      if (!cancelled)
        timer = setTimeout(() => {
          void tick();
        }, limits.intervalMs);
    };
    void tick();
    return (): void => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [running]);
  const stop = (): void => {
    setRunning(false);
    generation.current++;
    void host
      .execute({
        command: "observations.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .catch(() => undefined);
  };
  const clear = async (): Promise<void> => {
    stop();
    setBusy(true);
    try {
      const r = await host.execute({
        command: "observations.clear",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { confirmation: "CLEAR HISTORY" },
      });
      if (mounted.current) {
        if (r.ok) {
          update(r.result.snapshot);
          setConfirmation("");
          setError("");
        } else setError(r.error.summary);
      }
    } catch {
      if (mounted.current) setError("History could not be cleared.");
    } finally {
      if (mounted.current) setBusy(false);
    }
  };
  return (
    <Stack
      component="main"
      aria-label="Observed health page"
      spacing={2}
      sx={{ p: 3, overflow: "auto", height: "100%" }}
    >
      <Typography component="h1" variant="h5">
        Observed health
      </Typography>
      <Typography>
        Observe a selected topic and optional consumer group. Kafka API metadata and offset
        positions describe this client’s view; broker CPU/disk and consumer processing health are
        unavailable.
      </Typography>
      <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
        <TextField
          label="Observed topic"
          value={topic}
          disabled={busy || running}
          onChange={(e) => setTopic(e.target.value)}
        />
        <TextField
          label="Observed consumer group (optional)"
          value={groupId}
          disabled={busy || running}
          onChange={(e) => setGroupId(e.target.value)}
        />
        <TextField
          label="Lag alert threshold (optional)"
          value={lagThreshold}
          disabled={busy || running}
          onChange={(e) => setLagThreshold(e.target.value)}
        />
        <TextField
          label="Request time alert threshold, ms (optional)"
          value={latencyThreshold}
          disabled={busy || running}
          onChange={(e) => setLatencyThreshold(e.target.value)}
        />
      </Stack>
      <Stack direction="row" spacing={1}>
        <Button
          disabled={busy || running || !topic.trim()}
          onClick={() => {
            void capture();
          }}
        >
          Capture observation
        </Button>
        <Button disabled={busy || running || !topic.trim()} onClick={() => setRunning(true)}>
          Start observing
        </Button>
        <Button disabled={!running && !busy} onClick={stop}>
          Stop observing
        </Button>
      </Stack>
      <Typography color="text.secondary" variant="body2">
        {running ? "Collecting" : "Stopped"} · At least 10 seconds between captures; 15-second
        deadline; 1–128 partitions. Sampling and local alerts stop when you leave this page or close
        the app. Threshold breaches appear here and once per transition in Activity. Missing values
        never satisfy a threshold.
      </Typography>
      {error && <Alert severity="error">{error}</Alert>}
      <Typography variant="body2">
        History:{" "}
        {snapshot.durability === "durable"
          ? "Private desktop storage; retained across restarts"
          : "Browser session only"}
        . At most 8 identities, 240 samples each, 24 hours and 4 MiB. Older samples are evicted;
        payloads, raw keys and credentials are not stored.
      </Typography>
      {snapshot.series.length > 0 && (
        <TextField
          select
          label="Recorded observation series"
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
        >
          {snapshot.series.map((s) => (
            <MenuItem key={observationIdentity(s)} value={observationIdentity(s)}>
              {s.topic} · {s.groupId ?? "No group"} · cluster {s.clusterId}
            </MenuItem>
          ))}
        </TextField>
      )}
      {series && latest && (
        <>
          <Alert severity={!fresh || latest.state === "partial" ? "warning" : "info"}>
            {fresh ? "Recent observation" : "Stale observation"} ·{" "}
            {new Date(latest.observedAt).toISOString()} · {latest.state} · Group coverage:{" "}
            {latest.groupCoverage}. Cluster {series.clusterId}; topic ID {series.topicId}. This
            retained series may describe a different connection; only a new capture qualifies the
            current one.
          </Alert>
          {latest.alerts.length > 0 && (
            <Alert severity="warning">
              {fresh ? "Local alert" : "Historical threshold breach"}:{" "}
              {latest.alerts
                .map(
                  (a) =>
                    `${a.metric} ${a.observed.toLocaleString()} exceeded ${a.threshold.toLocaleString()}`,
                )
                .join("; ")}
              . Sample-specific evidence; no notification service runs while the desktop is closed.
            </Alert>
          )}
          <Typography>
            {latest.brokerCount} advertised brokers · Controller{" "}
            {latest.controllerKnown ? "advertised" : "unknown"} ·{" "}
            {latest.partitions.filter((p) => p.leader === null).length} partitions without a known
            leader · {latest.partitions.filter((p) => p.inSyncReplicas < p.replicas).length}{" "}
            under-replicated partitions. Advertised broker presence does not prove each broker is
            reachable.
          </Typography>
          <Typography>
            Group state: {latest.groupState ?? "unavailable"} · Visible members:{" "}
            {latest.members ?? "unavailable"} · Selected-topic lag:{" "}
            {observationLag(latest)?.toLocaleString() ?? "unknown"} offset positions · Collection:{" "}
            {latest.requestMs.toFixed(1)} ms across {latest.providerCalls} provider calls (each may
            issue several Kafka requests).
          </Typography>
          <ObservationHistoryPlots series={series} />
          <Table size="small" aria-label="Observed partition positions">
            <TableHead>
              <TableRow>
                {["Partition", "Leader", "ISR / replicas", "End position", "Committed", "Lag"].map(
                  (s) => (
                    <TableCell key={s}>{s}</TableCell>
                  ),
                )}
              </TableRow>
            </TableHead>
            <TableBody>
              {latest.partitions.map((p) => (
                <TableRow key={p.partition}>
                  <TableCell>{p.partition}</TableCell>
                  <TableCell>{p.leader ?? "unknown"}</TableCell>
                  <TableCell>
                    {p.inSyncReplicas} / {p.replicas}
                  </TableCell>
                  <TableCell>{p.endOffset ?? "unknown"}</TableCell>
                  <TableCell>{p.committedOffset ?? "unknown"}</TableCell>
                  <TableCell>{p.lag ?? "unknown"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </>
      )}
      <Stack direction="row" spacing={1}>
        <TextField
          label="Clear all history confirmation"
          value={confirmation}
          disabled={busy || running}
          helperText="Type CLEAR HISTORY to delete all retained observation series."
          onChange={(e) => setConfirmation(e.target.value)}
        />
        <Button
          disabled={busy || running || confirmation !== "CLEAR HISTORY"}
          onClick={() => {
            void clear();
          }}
        >
          Clear all observation history
        </Button>
      </Stack>
    </Stack>
  );
}
function ObservationHistoryPlots({
  series,
}: {
  readonly series: ObservationSeries;
}): React.JSX.Element {
  const samples = series.samples.flatMap((s, i) => {
    const previous = series.samples[i - 1];
    return previous &&
      (s.segmentId !== previous.segmentId || s.observedAt - previous.observedAt > limits.staleMs)
      ? [null, s]
      : [s];
  });
  const labels = samples.map((s, i) =>
    new Date(s?.observedAt ?? (samples[i - 1]?.observedAt ?? 0) + 1).toISOString(),
  );
  return (
    <Stack spacing={1}>
      <MetricPlot
        title="Selected-topic consumer lag"
        unit="offset positions"
        interpolation="step"
        sampleLabels={labels}
        series={[
          { label: "Measured lag", values: samples.map((s) => (s ? observationLag(s) : null)) },
        ]}
      />
      <MetricPlot
        title="Kafka API observation request time"
        unit="ms"
        sampleLabels={labels}
        series={[
          { label: "Client elapsed time", values: samples.map((s) => s?.requestMs ?? null) },
        ]}
      />
    </Stack>
  );
}
