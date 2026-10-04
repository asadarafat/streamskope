import { Stack, Typography } from "@mui/material";

import {
  OBSERVATION_LIMITS as limits,
  observationLag,
  type ObservationSeries,
} from "../contracts/observations";
import { observationInterval } from "../contracts/observation-analysis";

import { MetricPlot } from "./MetricPlot";

export function ObservationTrends({
  series,
  diagnostic = false,
}: {
  readonly series: ObservationSeries;
  readonly diagnostic?: boolean;
}): React.JSX.Element {
  const samples = series.samples.flatMap((sample, index) => {
    const previous = series.samples[index - 1];
    return previous &&
      (sample.segmentId !== previous.segmentId ||
        sample.observedAt - previous.observedAt > limits.staleMs)
      ? [null, sample]
      : [sample];
  });
  const labels = samples.map((sample, index) =>
    new Date(sample?.observedAt ?? (samples[index - 1]?.observedAt ?? 0) + 1).toISOString(),
  );
  const interval = samples.map((sample, index) =>
    sample && samples[index - 1] ? observationInterval(samples[index - 1]!, sample) : null,
  );
  if (diagnostic)
    return (
      <MetricPlot
        title="Kafka API observation request time"
        unit="ms"
        sampleLabels={labels}
        series={[
          {
            label: "Client elapsed time",
            values: samples.map((sample) => sample?.requestMs ?? null),
          },
        ]}
      />
    );
  return (
    <Stack component="section" aria-label="Observation trends" spacing={1}>
      <Typography component="h2" variant="h6">
        Observed progress
      </Typography>
      <Typography variant="caption" color="text.secondary">
        Offset positions describe append and commit progress, not successful record processing. Gaps
        break the lines; these charts include retained historical measurements.
      </Typography>
      <Stack
        direction={{ xs: "column", lg: "row" }}
        spacing={1}
        sx={{ "& > *": { minWidth: 0, flex: 1 } }}
      >
        <MetricPlot
          title="Selected-topic consumer lag"
          unit="offset positions"
          interpolation="step"
          sampleLabels={labels}
          series={[
            {
              label: "Measured lag",
              values: samples.map((sample) => (sample ? observationLag(sample) : null)),
            },
          ]}
        />
        <MetricPlot
          title="Append and commit progress"
          unit="offset positions/s"
          sampleLabels={labels}
          series={[
            { label: "Append", values: interval.map((value) => value?.appendedPerSecond ?? null) },
            { label: "Commit", values: interval.map((value) => value?.committedPerSecond ?? null) },
          ]}
        />
      </Stack>
    </Stack>
  );
}
