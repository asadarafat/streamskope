import { Box, Typography } from "@mui/material";

import type { ObservationAnalysis } from "../contracts/observation-analysis";
import type { ObservationSeries, ObservationSettings } from "../contracts/observations";
import { observationContinuityBreak } from "../contracts/observation-continuity";

export function ObservationEvidence({
  series,
  analysis,
  actionable,
  now,
  settings,
}: {
  readonly series: ObservationSeries;
  readonly analysis: ObservationAnalysis;
  readonly actionable: boolean;
  readonly now: number;
  readonly settings: ObservationSettings | null;
}): React.JSX.Element {
  const latest = series.samples.at(-1)!;
  const previous = series.samples.at(-2);
  const boundary = previous ? observationContinuityBreak(previous, latest) : null;
  const selectedSettings =
    settings?.clusterId === series.clusterId &&
    settings.topicId === series.topicId &&
    settings.input.topic === series.topic &&
    settings.input.groupId === series.groupId &&
    settings.savedAt === latest.observedAt
      ? settings.input
      : null;
  const known = (field: "endOffset" | "committedOffset" | "lag"): number =>
    latest.partitions.filter((p) => p[field] !== null).length;
  const thresholds = selectedSettings
    ? `Lag: ${selectedSettings.thresholds.lag === null ? "not set" : `${selectedSettings.thresholds.lag.toLocaleString()} positions`}; collection time: ${selectedSettings.thresholds.requestMs === null ? "not set" : `${selectedSettings.thresholds.requestMs.toLocaleString()} ms`}`
    : "Captured settings unavailable for this sample; each recorded breach retains its threshold.";
  const rows = [
    ["Source", `Kafka API · cluster ${series.clusterId} · topic ID ${series.topicId}`],
    [
      "Collected (UTC)",
      `${new Date(latest.startedAt).toISOString()} – ${new Date(latest.observedAt).toISOString()}`,
    ],
    [
      "Freshness",
      now < latest.observedAt
        ? "Clock mismatch; no current diagnosis"
        : `${Math.floor((now - latest.observedAt) / 1000)} seconds old · ${actionable ? "verified current connection" : "historical, stale or unverified; capture again before resource actions"}`,
    ],
    [
      "Available evidence",
      `${series.samples.length} retained raw samples; ${analysis.contiguousSamples} recent comparable samples · ${latest.state}`,
    ],
    [
      "Partition coverage",
      `${latest.partitions.length} partitions; end positions ${known("endOffset")}/${latest.partitions.length}, committed positions ${known("committedOffset")}/${latest.partitions.length}, lag ${known("lag")}/${latest.partitions.length} known`,
    ],
    [
      "Group coverage",
      series.groupId === null
        ? "No group selected"
        : `${series.groupId} · ${latest.groupCoverage} · ${latest.members ?? "unknown"} visible members`,
    ],
    [
      "Comparability",
      boundary
        ? `Latest interval breaks at ${boundary}; earlier progress cannot establish a current rate`
        : analysis.interval
          ? "Latest interval is comparable; offset progress does not establish processing success"
          : "No comparable current interval",
    ],
    ["Local thresholds", thresholds],
    [
      "Record coverage",
      latest.records
        ? `${latest.records.count} protected records · ${latest.records.state} (${latest.records.reason}); ${latest.records.knownKeys} known keys, ${latest.records.nullKeys} null keys, ${latest.records.unavailableKeys} unavailable · ${new Date(latest.records.startTimeMs).toISOString()} – ${new Date(latest.records.endTimeMs).toISOString()}`
        : "No recorded record sample; key distribution and record-size evidence unavailable",
    ],
  ];
  return (
    <Box component="section" aria-label="Observation evidence" sx={{ minWidth: 0 }}>
      <Typography component="h3" variant="subtitle2">
        Source and coverage
      </Typography>
      <Box
        component="dl"
        sx={{
          display: "grid",
          gridTemplateColumns: { xs: "1fr", sm: "minmax(140px, 180px) minmax(0, 1fr)" },
          gap: 0.75,
          m: 0,
          mt: 1,
        }}
      >
        {rows.map(([label, value]) => (
          <Box key={label} sx={{ display: "contents" }}>
            <Typography component="dt" variant="body2" color="text.secondary">
              {label}
            </Typography>
            <Typography component="dd" variant="body2" sx={{ m: 0, overflowWrap: "anywhere" }}>
              {value}
            </Typography>
          </Box>
        ))}
      </Box>
    </Box>
  );
}
