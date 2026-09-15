import { Box, Stack, Typography } from "@mui/material";
import { useId, useState } from "react";

import { formatUtcClockSeconds } from "./timestamp-presentation";

export interface MetricPlotSeries {
  readonly label: string;
  readonly values: readonly (number | null)[];
}

export interface MetricPlotProperties {
  readonly interpolation?: "linear" | "step";
  readonly sampleLabels: readonly string[];
  readonly series: readonly MetricPlotSeries[];
  readonly title: string;
  readonly unit: string;
}

interface Point {
  readonly x: number;
  readonly y: number;
}

const PLOT_COLORS = [
  "var(--streamskope-plot-primary)",
  "var(--streamskope-plot-secondary)",
  "var(--streamskope-plot-tertiary)",
] as const;

function measuredValues(series: readonly MetricPlotSeries[]): readonly number[] {
  return series.flatMap((candidate) =>
    candidate.values.filter((value): value is number => value !== null && Number.isFinite(value)),
  );
}

function seriesPoints(
  values: readonly (number | null)[],
  positions: readonly number[],
  minimum: number,
  maximum: number,
): readonly (Point | null)[] {
  const horizontalRange = 580;
  const verticalRange = 92;
  const valueRange = maximum - minimum;
  return positions.map((position, index) => {
    const value = values[index] ?? null;
    if (value === null || !Number.isFinite(value)) {
      return null;
    }
    return {
      x: 10 + position * horizontalRange,
      y:
        valueRange === 0
          ? minimum === 0
            ? 102
            : 56
          : 102 - ((value - minimum) / valueRange) * verticalRange,
    };
  });
}

function pointSegments(points: readonly (Point | null)[]): readonly (readonly Point[])[] {
  const segments: Point[][] = [];
  let segment: Point[] = [];
  for (const point of points) {
    if (point === null) {
      if (segment.length > 0) {
        segments.push(segment);
        segment = [];
      }
      continue;
    }
    segment.push(point);
  }
  if (segment.length > 0) {
    segments.push(segment);
  }

  return segments;
}

function metricPath(
  points: readonly (Point | null)[],
  interpolation: NonNullable<MetricPlotProperties["interpolation"]>,
): string {
  return pointSegments(points)
    .map((candidate) => {
      const first = candidate[0];
      if (first === undefined) {
        return "";
      }
      if (candidate.length === 1) {
        return `M ${String(first.x)} ${String(first.y)} l 0.01 0`;
      }
      const commands = [`M ${String(first.x)} ${String(first.y)}`];
      for (let index = 1; index < candidate.length; index += 1) {
        const next = candidate[index];
        if (next === undefined) {
          continue;
        }
        commands.push(
          interpolation === "step"
            ? `H ${String(next.x)} V ${String(next.y)}`
            : `L ${String(next.x)} ${String(next.y)}`,
        );
      }
      return commands.join(" ");
    })
    .join(" ");
}

function formatMetric(value: number, unit: string): string {
  const formatted = value.toLocaleString(undefined, { maximumFractionDigits: 3 });
  return unit.length === 0 ? formatted : `${formatted} ${unit}`;
}

function latestEvidence(
  candidate: MetricPlotSeries,
  labels: readonly string[],
  unit: string,
): string {
  const latest = candidate.values[labels.length - 1];
  if (latest !== null && latest !== undefined && Number.isFinite(latest))
    return formatMetric(latest, unit);
  for (let index = labels.length - 2; index >= 0; index -= 1) {
    const value = candidate.values[index];
    if (value !== null && value !== undefined && Number.isFinite(value)) {
      return `Unavailable; last measured ${formatMetric(value, unit)} at ${labels[index] ?? "unknown time"}`;
    }
  }
  return "Unavailable";
}

export function MetricPlot({
  interpolation = "linear",
  sampleLabels,
  series,
  title,
  unit,
}: MetricPlotProperties): React.JSX.Element {
  const plotId = useId();
  const [activeKey, setActiveKey] = useState<string | null>(null);
  const [inspecting, setInspecting] = useState(false);
  const times = sampleLabels.map((label) => Date.parse(label));
  const firstTime = times[0] ?? 0;
  const lastTime = times.at(-1) ?? 0;
  const chronological = times.every(
    (time, index) => Number.isFinite(time) && (index === 0 || time >= (times[index - 1] ?? time)),
  );
  const timeAxis = chronological && lastTime > firstTime;
  const positions = times.map((time, index) =>
    timeAxis ? (time - firstTime) / (lastTime - firstTime) : index / Math.max(times.length - 1, 1),
  );
  const occurrences = new Map<string, number>();
  const sampleKeys = sampleLabels.map((label) => {
    const occurrence = occurrences.get(label) ?? 0;
    occurrences.set(label, occurrence + 1);
    return `${label}:${String(occurrence)}`;
  });
  const exactSamples = series.flatMap((candidate) =>
    sampleLabels.flatMap((label, index) => {
      const value = candidate.values[index];
      return value === null || value === undefined || !Number.isFinite(value)
        ? []
        : [
            {
              key: `${candidate.label}:${sampleKeys[index] ?? label}`,
              label: `${candidate.label} at ${label}: ${formatMetric(value, unit)}`,
            },
          ];
    }),
  );
  const activeIndex = Math.max(
    0,
    exactSamples.findIndex((point) => point.key === activeKey),
  );
  const activeSample = exactSamples[activeIndex];
  const values = measuredValues(series);
  const availableSeries = series.filter((candidate) => measuredValues([candidate]).length > 0);
  if (values.length === 0 || sampleLabels.length === 0 || availableSeries.length === 0) {
    return (
      <Box component="section" sx={{ border: 1, borderColor: "divider", minWidth: 0, p: 1.5 }}>
        <Typography component="h3" variant="subtitle2">
          {title}
        </Typography>
        <Typography color="text.secondary" sx={{ mt: 0.5 }} variant="body2">
          No measured samples
        </Typography>
      </Box>
    );
  }

  const minimum = Math.min(...values);
  const maximum = Math.max(...values);
  const description = series
    .map((candidate) => {
      const samples = measuredValues([candidate]);
      const latest = latestEvidence(candidate, sampleLabels, unit);
      return `${candidate.label}: ${String(samples.length)} ${samples.length === 1 ? "sample" : "samples"}, latest ${
        latest
      }`;
    })
    .join(". ");

  return (
    <Box
      aria-label={`${title}. ${description}`}
      component="div"
      role="region"
      sx={{
        bgcolor: "var(--streamskope-surface-recessed)",
        border: 1,
        borderColor: "divider",
        minWidth: 0,
        p: 1.5,
      }}
    >
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: "baseline", justifyContent: "space-between" }}
      >
        <Typography component="h3" variant="subtitle2">
          {title}
        </Typography>
        <Typography color="text.secondary" variant="caption">
          {sampleLabels.length.toLocaleString()} retained
        </Typography>
      </Stack>
      <Box
        sx={{
          display: "grid",
          gap: 0.75,
          gridTemplateColumns: "auto minmax(0, 1fr)",
          mt: 1,
        }}
      >
        <Stack
          aria-label={`Range ${formatMetric(minimum, unit)} to ${formatMetric(maximum, unit)}`}
          sx={{ height: 118, justifyContent: "space-between", py: 0.25 }}
        >
          <Typography
            aria-label={`Range maximum ${formatMetric(maximum, unit)}`}
            color="text.secondary"
            variant="caption"
          >
            {formatMetric(maximum, unit)}
          </Typography>
          <Typography color="text.secondary" variant="caption">
            {formatMetric((minimum + maximum) / 2, unit)}
          </Typography>
          <Typography
            aria-label={`Range minimum ${formatMetric(minimum, unit)}`}
            color="text.secondary"
            variant="caption"
          >
            {formatMetric(minimum, unit)}
          </Typography>
        </Stack>
        <Box
          aria-label={`${title} plot`}
          component="svg"
          preserveAspectRatio="none"
          role="group"
          tabIndex={0}
          aria-describedby={`${plotId}-sample`}
          onFocus={() => setInspecting(true)}
          onBlur={() => setInspecting(false)}
          onKeyDown={(event) => {
            const increment =
              event.key === "ArrowRight" || event.key === "ArrowDown"
                ? 1
                : event.key === "ArrowLeft" || event.key === "ArrowUp"
                  ? -1
                  : 0;
            if (increment === 0 && event.key !== "Home" && event.key !== "End") return;
            event.preventDefault();
            const next =
              event.key === "Home"
                ? 0
                : event.key === "End"
                  ? exactSamples.length - 1
                  : Math.max(0, Math.min(exactSamples.length - 1, activeIndex + increment));
            setActiveKey(exactSamples[next]?.key ?? null);
            setInspecting(true);
          }}
          sx={{
            display: "block",
            height: 118,
            width: "100%",
            "&:focus-visible": {
              outline: "2px solid var(--mui-palette-primary-main)",
              outlineOffset: 2,
              strokeWidth: 4,
            },
          }}
          viewBox="0 0 600 112"
        >
          {[10, 33, 56, 79, 102].map((y) => (
            <line
              aria-hidden
              key={y}
              stroke="var(--streamskope-plot-grid)"
              strokeWidth="1"
              vectorEffect="non-scaling-stroke"
              x1="10"
              x2="590"
              y1={y}
              y2={y}
            />
          ))}
          {series.map((candidate, index) => {
            if (measuredValues([candidate]).length === 0) return null;
            const points = seriesPoints(candidate.values, positions, minimum, maximum);
            const path = metricPath(points, interpolation);
            const color = PLOT_COLORS[index % PLOT_COLORS.length];
            return (
              <g key={candidate.label}>
                <path
                  aria-hidden
                  d={path}
                  fill="none"
                  stroke={color}
                  strokeDasharray={index === 0 ? undefined : index === 1 ? "6 4" : "2 4"}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="2"
                  vectorEffect="non-scaling-stroke"
                />
                <g aria-label={`${candidate.label} exact samples`} role="list">
                  {points.map((point, pointIndex) => {
                    const value = candidate.values[pointIndex] ?? null;
                    const sampleLabel =
                      sampleLabels[pointIndex] ?? `Sample ${String(pointIndex + 1)}`;
                    if (point === null || value === null || !Number.isFinite(value)) {
                      return null;
                    }
                    const exactLabel = `${candidate.label} at ${sampleLabel}: ${formatMetric(value, unit)}`;
                    return (
                      <circle
                        aria-label={exactLabel}
                        cx={point.x}
                        cy={point.y}
                        data-plot-point
                        fill="var(--streamskope-surface-recessed)"
                        key={sampleKeys[pointIndex]}
                        r={
                          inspecting &&
                          activeSample?.key ===
                            `${candidate.label}:${sampleKeys[pointIndex] ?? sampleLabel}`
                            ? 5
                            : 3
                        }
                        role="listitem"
                        stroke={color}
                        strokeWidth="2"
                        vectorEffect="non-scaling-stroke"
                      >
                        <title>{exactLabel}</title>
                      </circle>
                    );
                  })}
                </g>
              </g>
            );
          })}
        </Box>
      </Box>
      <Typography
        id={`${plotId}-sample`}
        role="status"
        variant="caption"
        color="text.secondary"
        sx={{ display: "block", minHeight: "1.5em" }}
      >
        {inspecting ? activeSample?.label : "Focus chart; use arrow keys to inspect samples."}
      </Typography>
      <Stack direction="row" spacing={1.5} sx={{ flexWrap: "wrap", rowGap: 0.5 }}>
        {series.map((candidate, index) => {
          const latest = latestEvidence(candidate, sampleLabels, unit);
          return (
            <Stack
              direction="row"
              key={candidate.label}
              spacing={0.6}
              sx={{ alignItems: "center" }}
            >
              <Box
                aria-hidden
                sx={{
                  bgcolor: PLOT_COLORS[index % PLOT_COLORS.length],
                  height: 2,
                  width: 14,
                }}
              />
              <Typography color="text.secondary" variant="caption">
                {candidate.label}
              </Typography>
              <Typography variant="caption">{latest}</Typography>
            </Stack>
          );
        })}
      </Stack>
      <Stack direction="row" sx={{ justifyContent: "space-between", mt: 0.75 }}>
        <Typography color="text.secondary" noWrap variant="caption">
          {timeAxis ? formatUtcClockSeconds(sampleLabels[0] ?? "") : "1"}
        </Typography>
        <Typography color="text.secondary" variant="caption">
          {timeAxis ? "Time (UTC)" : "Sample sequence"}
        </Typography>
        <Typography color="text.secondary" noWrap variant="caption">
          {timeAxis ? formatUtcClockSeconds(sampleLabels.at(-1) ?? "") : sampleLabels.length}
        </Typography>
      </Stack>
    </Box>
  );
}
