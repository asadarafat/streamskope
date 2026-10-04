import { useMemo } from "react";
import {
  Box,
  Stack,
  Typography,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
} from "@mui/material";

import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";
import type { ObservationSeries } from "../contracts/observations";
import { analyzeObservations } from "../contracts/observation-analysis";

import type { ObservationNavigation } from "./ObservationFindings";

export function ObservationAnalysisPanel({
  series,
  fresh,
  onOpenRecord,
}: {
  readonly series: ObservationSeries;
  readonly fresh: boolean;
} & ObservationNavigation): React.JSX.Element {
  const analysis = useMemo(
    () => analyzeObservations(series, fresh ? Date.now() : Number.POSITIVE_INFINITY),
    [series, fresh],
  );
  const latest = series.samples.at(-1),
    records = latest?.records;
  const format = (n: number | null): string =>
    n === null ? "unknown" : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  const evidence = (ids: readonly string[]): string =>
    ids
      .map((id) => series.samples.find((s) => s.id === id)?.observedAt)
      .filter((t): t is number => t !== undefined)
      .map((t) => new Date(t).toISOString())
      .join(", ");
  const forecast = analysis.forecast;
  return (
    <Stack component="section" aria-label="Observation analysis" spacing={2}>
      <Typography component="h2" variant="h6">
        Explain these observations
      </Typography>
      <Typography variant="body2">
        {analysis.contiguousSamples} samples in the recent continuous segment. Gaps, restarts,
        decreasing offsets and changed partition identities break continuity. Only recent evidence
        produces hints or forecasts.
      </Typography>
      <Typography component="h3" variant="subtitle1">
        Lag scenario, 60 seconds ahead
      </Typography>
      {forecast.state === "ready" ? (
        <Alert severity="info">
          Projected lag: {format(forecast.estimate)} positions; heuristic range{" "}
          {format(forecast.lower)}–{format(forecast.upper)}. Held-out mean absolute error:{" "}
          {format(forecast.backtestMae)} positions across the last three observations.{" "}
          {forecast.reason}
        </Alert>
      ) : (
        <Alert severity="info">Insufficient evidence. {forecast.reason}</Alert>
      )}
      {forecast.evidence.length > 0 && (
        <Typography variant="caption">
          Evidence: {forecast.evidence.length} samples from{" "}
          {evidence([forecast.evidence[0]!, forecast.evidence.at(-1)!])}.
        </Typography>
      )}
      <Typography component="h3" variant="subtitle1">
        Baseline changes and anomalies
      </Typography>
      <Typography variant="body2">
        Compare with up to 12 prior continuous samples; at least eight are required. A deviation
        must exceed the largest of six median absolute deviations, 50% of the median, or the metric
        floor (1 position/s, 10 ms, 32 bytes). These heuristics can miss changes or produce false
        positives. Three successive deviations identify a baseline change. Record-size baselines
        require at least 20 records and complete window coverage per sample.
      </Typography>
      <Box sx={{ overflowX: "auto" }}>
        <Table size="small" aria-label="Observation anomalies">
          <TableHead>
            <TableRow>
              {[
                "Metric / meaning",
                "Observed",
                "Prior median",
                "Deviation threshold",
                "Result",
              ].map((s) => (
                <TableCell key={s}>{s}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {analysis.anomalies.map((a) => (
              <TableRow key={a.metric}>
                <TableCell>
                  {a.metric} ({a.unit})
                </TableCell>
                <TableCell>{format(a.observed)}</TableCell>
                <TableCell>{format(a.median)}</TableCell>
                <TableCell>{format(a.threshold)}</TableCell>
                <TableCell>
                  {a.state} · {a.samples} baseline samples
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Box>
      {!analysis.fresh && (
        <Typography>
          No current anomaly classification; retained evidence is not current.
        </Typography>
      )}
      <Typography component="h3" variant="subtitle1">
        Partition and key distribution
      </Typography>
      <Typography>
        {analysis.skew
          ? `${analysis.skew.suspected ? "Skew suspected" : "Measured distribution"}: largest partition accounts for ${format(100 * analysis.skew.maximumShare)}% of ${analysis.skew.total} appended offset positions across ${analysis.skew.partitions} partitions. Source: ${analysis.skew.source}.`
          : "Partition growth: insufficient complete interval evidence (at least 20 positions across two partitions)."}
      </Typography>
      {records ? (
        <>
          <Typography>
            Protected record sample: {records.state} / {records.reason}; {records.count} records,{" "}
            {records.bytes.toLocaleString()} bytes, mean {format(records.meanBytes)} bytes, p95{" "}
            {format(records.p95Bytes)} bytes. Window {new Date(records.startTimeMs).toISOString()}{" "}
            to {new Date(records.endTimeMs).toISOString()} (end excluded).
          </Typography>
          <Typography>
            Key coverage: {records.knownKeys} available non-null keys, {records.nullKeys} null,{" "}
            {records.unavailableKeys} unavailable.{" "}
            {analysis.hotKey
              ? `${analysis.hotKey.suspected ? "Hot key suspected" : "Sampled key distribution"}: the most frequent key represents ${format(analysis.hotKey.share * 100)}% of available non-null keys.`
              : "A complete recent window, at least 20 available non-null keys and no unavailable keys are required for a hot-key hint."}{" "}
            Capped, overlapping windows can be biased; these are not cluster-wide traffic
            statistics.
          </Typography>
          {records.analysisEligible === false && (
            <Alert severity="warning">
              This bounded window does not cover all required partition ranges. Its counts describe
              the sample only; it cannot support a hot-key or record-size anomaly conclusion.
            </Alert>
          )}
          <Box sx={{ overflowX: "auto" }}>
            <Table size="small" aria-label="Sampled frequent key locators">
              <TableHead>
                <TableRow>
                  <TableCell>Rank in this sample</TableCell>
                  <TableCell>Count</TableCell>
                  <TableCell>Example partition / offset</TableCell>
                  {onOpenRecord && <TableCell>Investigate</TableCell>}
                </TableRow>
              </TableHead>
              <TableBody>
                {records.topKeys.map((k, i) => (
                  <TableRow key={i}>
                    <TableCell>{i + 1}</TableCell>
                    <TableCell>{k.count}</TableCell>
                    <TableCell>
                      {k.partition} / {k.offset}
                    </TableCell>
                    {onOpenRecord && (
                      <TableCell>
                        <Button
                          disabled={!fresh}
                          onClick={() =>
                            onOpenRecord({
                              topic: series.topic,
                              partition: k.partition,
                              offset: k.offset,
                              startTimeMs: records.startTimeMs,
                              endTimeMs: records.endTimeMs,
                            })
                          }
                          aria-label={`Find sampled record partition ${k.partition} offset ${k.offset}`}
                        >
                          Find sampled record
                        </Button>
                      </TableCell>
                    )}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>
          <Typography variant="caption">
            Find opens a protected bounded read at the example partition and exact offset. Deleted,
            compacted or out-of-window records may be unavailable. Key bytes are not returned or
            retained by this analysis; ranks identify this sample only.
          </Typography>
        </>
      ) : (
        <Typography>
          Record sampling is off. Enable it before collection to inspect bounded message-size and
          key distributions.
        </Typography>
      )}
    </Stack>
  );
}
