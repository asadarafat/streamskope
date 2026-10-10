import { Stack, Typography } from "@mui/material";

import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";
import type { ObservationAnalysis } from "../contracts/observation-analysis";
import type { ObservationSeries, ObservationSettings } from "../contracts/observations";

import { findingPriority } from "./observation-presentation";
import { ObservationEvidence } from "./ObservationEvidence";
import { findingEvidence, prepareObservationInvestigation } from "./observation-investigations";
import type { PreparedKafkaInvestigation } from "./investigation-view-settings";

export interface ObservationNavigation {
  readonly onSaveInvestigation?: (investigation: PreparedKafkaInvestigation) => void;
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
  now,
  settings,
  onSaveInvestigation,
  onOpenTopic,
  onOpenGroup,
}: {
  readonly series: ObservationSeries;
  readonly analysis: ObservationAnalysis;
  readonly actionable: boolean;
  readonly now: number;
  readonly settings: ObservationSettings | null;
} & ObservationNavigation): React.JSX.Element {
  const latest = series.samples.at(-1)!;
  const hints = [...analysis.hints].sort((a, b) => findingPriority(a) - findingPriority(b));
  const support = hints.map((hint) => findingEvidence(series, hint.evidence, now));
  const canAct =
    actionable &&
    findingEvidence(series, [latest.id], now).fresh &&
    support.every((evidence) => evidence.complete && evidence.fresh);
  return (
    <Stack component="section" aria-label="Observation findings" spacing={1}>
      <Typography component="h2" variant="h6">
        Investigate findings
      </Typography>
      <ObservationEvidence
        series={series}
        analysis={analysis}
        actionable={actionable}
        now={now}
        settings={settings}
      />
      {!canAct && (
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
          . The authorized host watch can add local Activity entries while this page is closed;
          external notifications are not configured.
        </Alert>
      )}
      {actionable && hints.length === 0 && (
        <Typography variant="body2">
          No supported finding from the available evidence. This does not establish a healthy
          cluster.
        </Typography>
      )}
      {hints.map((hint, index) => (
        <Alert severity={findingPriority(hint) <= 1 ? "warning" : "info"} key={hint.title}>
          <Typography component="h3" variant="subtitle2">
            {hint.title}
          </Typography>
          {support[index]!.complete
            ? hint.detail
            : "Supporting observations are unavailable; this finding cannot be verified."}
          <Typography variant="caption" sx={{ display: "block" }}>
            Rule: {hint.criteria}
          </Typography>
          <Typography variant="caption" sx={{ display: "block", overflowWrap: "anywhere" }}>
            {support[index]!.complete
              ? `${hint.evidence.length} supporting observations · ${new Date(support[index]!.firstAt!).toISOString()} – ${new Date(support[index]!.lastAt!).toISOString()} · ${support[index]!.fresh && actionable ? "current evidence" : "historical or unverified evidence"}`
              : "Evidence incomplete"}
            . Support IDs: {hint.evidence.join(", ") || "none"}. A hypothesis, not an established
            cause.
          </Typography>
        </Alert>
      ))}
      <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
        {onOpenTopic && (
          <Button disabled={!canAct} onClick={() => onOpenTopic(series.topic)}>
            Inspect topic {series.topic}
          </Button>
        )}
        {onOpenGroup && series.groupId !== null && (
          <Button disabled={!canAct} onClick={() => onOpenGroup(series.groupId!)}>
            Inspect consumer group {series.groupId}
          </Button>
        )}
        {onSaveInvestigation && (
          <Button
            disabled={!canAct}
            onClick={() => onSaveInvestigation(prepareObservationInvestigation(series, "topic"))}
          >
            Save topic investigation
          </Button>
        )}
        {onSaveInvestigation && series.groupId !== null && (
          <Button
            disabled={!canAct}
            onClick={() =>
              onSaveInvestigation(prepareObservationInvestigation(series, "consumer-group"))
            }
          >
            Save group investigation
          </Button>
        )}
      </Stack>
    </Stack>
  );
}
