import { Box, Chip, Stack, Typography } from "@mui/material";

import { StudioAlert as Alert } from "../../../platform/ui/controls";
import type { ObservationAnalysis } from "../contracts/observation-analysis";
import { observationLag, type ObservationSeries } from "../contracts/observations";

import { observationNumber } from "./observation-presentation";

export function ObservationSummary({
  series,
  current,
  fresh,
  connectionName,
  analysis,
}: {
  readonly series: ObservationSeries;
  readonly current: boolean;
  readonly fresh: boolean;
  readonly connectionName: string | null;
  readonly analysis: ObservationAnalysis;
}): React.JSX.Element {
  const latest = series.samples.at(-1)!;
  const missingLeaders = latest.partitions.filter((p) => p.leader === null).length;
  const underReplicated = latest.partitions.filter((p) => p.inSyncReplicas < p.replicas).length;
  const metrics = [
    {
      label: "Selected-topic lag",
      value:
        series.groupId === null ? "No group selected" : observationNumber(observationLag(latest)),
      detail: "Offset positions, not record count",
    },
    {
      label: "Append progress",
      value: observationNumber(analysis.interval?.appendedPerSecond ?? null),
      detail: "Offset positions/s",
    },
    {
      label: "Commit progress",
      value: observationNumber(analysis.interval?.committedPerSecond ?? null),
      detail: "Offset positions/s",
    },
    {
      label: "Consumer group",
      value: series.groupId === null ? "Not selected" : (latest.groupState ?? "Unknown"),
      detail:
        series.groupId === null
          ? "Add a group for lag evidence"
          : `${latest.members ?? "Unknown"} visible members`,
    },
    {
      label: "Missing leaders",
      value: String(missingLeaders),
      detail: `Across ${latest.partitions.length} selected-topic partitions`,
    },
    {
      label: "Under-replicated",
      value: String(underReplicated),
      detail: "ISR smaller than assigned replicas",
    },
  ];
  return (
    <Stack component="section" aria-label="Observation summary" spacing={1.5}>
      <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap", alignItems: "center" }}>
        <Typography component="h2" variant="h6" sx={{ overflowWrap: "anywhere" }}>
          {series.topic}
        </Typography>
        <Chip
          size="small"
          label={
            !current
              ? "Retained evidence"
              : !fresh
                ? "Stale evidence"
                : latest.state === "partial"
                  ? "Partial evidence"
                  : "Recent evidence"
          }
          color={current && fresh && latest.state === "ready" ? "default" : "warning"}
        />
        <Typography variant="body2" color="text.secondary">
          {new Date(latest.observedAt).toLocaleString()} · {series.groupId ?? "No consumer group"}
        </Typography>
      </Stack>
      {!current && (
        <Alert severity="warning">
          This retained series has not been verified against{" "}
          {connectionName ?? "the current connection"}. Capture a new observation before following
          resource links.
        </Alert>
      )}
      {current && !fresh && (
        <Alert severity="warning">
          Stale observation: no current diagnosis. Capture again to refresh the evidence.
        </Alert>
      )}
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 1,
        }}
      >
        {metrics.map((metric) => (
          <Box
            key={metric.label}
            sx={{ p: 1.5, border: 1, borderColor: "divider", borderRadius: 1, minWidth: 0 }}
          >
            <Typography variant="body2" color="text.secondary">
              {metric.label}
            </Typography>
            <Typography variant="h6" sx={{ overflowWrap: "anywhere" }}>
              {metric.value}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {metric.detail}
            </Typography>
          </Box>
        ))}
      </Box>
      <Typography variant="caption" color="text.secondary">
        Coverage:{" "}
        {latest.groupCoverage === "not-selected"
          ? "topic metadata only"
          : `group ${latest.groupCoverage}`}{" "}
        · {latest.brokerCount} advertised brokers. These observations do not establish cluster-wide
        health or consumer processing success.
      </Typography>
    </Stack>
  );
}
