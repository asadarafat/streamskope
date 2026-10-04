import { Stack, Typography } from "@mui/material";

import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";
import type { ObservationAnalysis } from "../contracts/observation-analysis";
import type { ObservationSeries } from "../contracts/observations";

import { findingPriority } from "./observation-presentation";

export interface ObservationNavigation {
  readonly onOpenTopic?: (topic: string) => void;
  readonly onOpenGroup?: (groupId: string) => void;
  readonly onOpenRecord?: (locator: {
    readonly topic: string;
    readonly partition: number;
    readonly offset: string;
    readonly startTimeMs: number;
    readonly endTimeMs: number;
  }) => void;
}
export function ObservationFindings({
  series,
  analysis,
  actionable,
  onOpenTopic,
  onOpenGroup,
}: {
  readonly series: ObservationSeries;
  readonly analysis: ObservationAnalysis;
  readonly actionable: boolean;
} & ObservationNavigation): React.JSX.Element {
  const latest = series.samples.at(-1)!;
  const hints = [...analysis.hints].sort((a, b) => findingPriority(a) - findingPriority(b));
  const timestamps = (ids: readonly string[]): string =>
    ids
      .slice(-3)
      .map((id) => series.samples.find((s) => s.id === id)?.observedAt)
      .filter((at): at is number => at !== undefined)
      .map((at) => new Date(at).toLocaleTimeString())
      .join(", ");
  return (
    <Stack component="section" aria-label="Observation findings" spacing={1}>
      <Typography component="h2" variant="h6">
        Investigate findings
      </Typography>
      {!actionable && (
        <Typography variant="body2">
          Refresh this connection’s evidence to evaluate current findings.
        </Typography>
      )}
      {(latest.issues ?? []).map((issue, i) => (
        <Alert severity="warning" key={`${issue.measurement}:${i}`}>
          <Typography component="h3" variant="subtitle2">
            {actionable ? "Incomplete" : "Recorded incomplete"}{" "}
            {issue.measurement.replaceAll("-", " ")}
          </Typography>
          {issue.summary} {issue.recovery}
        </Alert>
      ))}
      {latest.alerts.length > 0 && (
        <Alert severity="warning">
          {actionable ? "Local threshold breach" : "Historical threshold breach"}:{" "}
          {latest.alerts
            .map(
              (alert) =>
                `${alert.metric === "lag" ? "Lag" : "Collection time"} ${alert.observed.toLocaleString()} exceeded ${alert.threshold.toLocaleString()}${alert.metric === "requestMs" ? " ms" : " positions"}`,
            )
            .join("; ")}
          . No notification service runs while this page is closed.
        </Alert>
      )}
      {actionable && hints.length === 0 && (
        <Typography variant="body2">
          No supported finding from the available evidence. This does not establish a healthy
          cluster.
        </Typography>
      )}
      {hints.map((hint) => (
        <Alert severity={findingPriority(hint) <= 1 ? "warning" : "info"} key={hint.title}>
          <Typography component="h3" variant="subtitle2">
            {hint.title}
          </Typography>
          {hint.detail}
          <Typography variant="caption" sx={{ display: "block" }}>
            Evidence collected at {timestamps(hint.evidence)}
            {hint.evidence.length > 3 ? `; ${hint.evidence.length} observations total` : ""}. A
            hypothesis, not an established cause.
          </Typography>
        </Alert>
      ))}
      <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
        {onOpenTopic && (
          <Button disabled={!actionable} onClick={() => onOpenTopic(series.topic)}>
            Inspect topic {series.topic}
          </Button>
        )}
        {onOpenGroup && series.groupId !== null && (
          <Button disabled={!actionable} onClick={() => onOpenGroup(series.groupId!)}>
            Inspect consumer group {series.groupId}
          </Button>
        )}
      </Stack>
    </Stack>
  );
}
