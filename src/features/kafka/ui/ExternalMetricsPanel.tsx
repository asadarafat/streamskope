import { useState } from "react";
import { Stack, Typography, Table, TableHead, TableRow, TableCell, TableBody } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

import {
  StudioAccordion as Accordion,
  StudioAccordionSummary as AccordionSummary,
  StudioAccordionDetails as AccordionDetails,
  StudioButton as Button,
  StudioAlert as Alert,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import type { StreamSkopeHost } from "../contracts";
import { EXTERNAL_METRICS, EXTERNAL_METRIC_LIMITS } from "../contracts/external-metrics";
import { parseExternalMetricsInput } from "../contracts/external-metrics-validation";

import { useExternalMetrics } from "./use-external-metrics";

const states = {
  ready: "Captured",
  missing: "Missing series",
  ambiguous: "Multiple series",
  annotated: "Response annotated; coverage unknown",
  invalid: "Unsupported response",
  stale: "Stale source",
  future: "Source clock ahead",
} as const;
export function ExternalMetricsPanel({
  host,
  available,
  endpoint,
}: {
  readonly host: StreamSkopeHost;
  readonly available: boolean;
  readonly endpoint: string | undefined;
}): React.JSX.Element {
  const [job, setJob] = useState(""),
    [instance, setInstance] = useState("");
  const [inputError, setInputError] = useState<string | null>(null);
  const metrics = useExternalMetrics(host, available, endpoint);
  const now = Date.now();
  const read = (): void => {
    try {
      const input = parseExternalMetricsInput({ job, instance, endpoint });
      setInputError(null);
      void metrics.read(input);
    } catch {
      setInputError(
        "Enter the exact, non-empty job and instance labels from your exporter, without surrounding whitespace.",
      );
    }
  };
  return (
    <Accordion slotProps={{ heading: { component: "h2" } }}>
      <AccordionSummary
        expandIcon={<ExpandMoreIcon />}
        id="external-metrics-title"
        aria-controls="external-metrics-details"
      >
        <Typography>Prometheus metrics (optional)</Typography>
      </AccordionSummary>
      <AccordionDetails id="external-metrics-details">
        <Stack spacing={2}>
          <Typography variant="body2">
            Read two Kafka JMX partition-count gauges from one selected exporter. These values are
            separate from Kafka API evidence; its identity is not bound to your connected cluster.
            No automatic scraping or retained history.
          </Typography>
          {!endpoint ? (
            <Alert severity="info">
              Add a Prometheus metrics URL and its authentication/trust in the Kafka connection
              profile, then reconnect. Your exporter must expose the declared gauges below.
            </Alert>
          ) : (
            <Typography variant="caption" sx={{ overflowWrap: "anywhere" }}>
              Endpoint: {endpoint}
            </Typography>
          )}
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
            <TextField
              label="Exporter job"
              value={job}
              disabled={metrics.pending}
              onChange={(event) => setJob(event.target.value)}
            />
            <TextField
              label="Exporter instance"
              value={instance}
              disabled={metrics.pending}
              onChange={(event) => setInstance(event.target.value)}
            />
            <Button disabled={!available || !endpoint || metrics.pending} onClick={read}>
              Read metrics
            </Button>
            <Button
              disabled={!available}
              onClick={() => {
                void metrics.cancel();
              }}
            >
              Cancel metrics read
            </Button>
          </Stack>
          {metrics.pending && (
            <Typography role="status">
              Waiting for the original metrics request and HTTP cleanup.
            </Typography>
          )}
          {(inputError || metrics.error) && (
            <Alert severity="warning">{inputError ?? metrics.error}</Alert>
          )}
          <Table size="small" aria-label="External Kafka metrics">
            <TableHead>
              <TableRow>
                <TableCell>Gauge / scope</TableCell>
                <TableCell>Partitions</TableCell>
                <TableCell>Coverage / source time</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {EXTERNAL_METRICS.map((definition, index) => {
                const row = metrics.snapshot?.metrics[index];
                const current =
                  row?.state === "ready" &&
                  row.scrapedAt !== null &&
                  now >= row.scrapedAt &&
                  now - row.scrapedAt <= EXTERNAL_METRIC_LIMITS.staleMs;
                return (
                  <TableRow key={definition.name}>
                    <TableCell>
                      <Typography variant="body2">{definition.label}</Typography>
                      <Typography variant="caption">{definition.scope}</Typography>
                      <Typography
                        component="code"
                        variant="caption"
                        sx={{ display: "block", overflowWrap: "anywhere" }}
                      >
                        {definition.name}
                      </Typography>
                    </TableCell>
                    <TableCell>{current ? row.value : "Unknown"}</TableCell>
                    <TableCell>
                      {row
                        ? row.state === "ready" && !current
                          ? "Expired capture"
                          : states[row.state]
                        : "Not read"}
                      {row?.scrapedAt !== null && row?.scrapedAt !== undefined && (
                        <Typography variant="caption" sx={{ display: "block" }}>
                          {new Date(row.scrapedAt).toISOString()} ·{" "}
                          {Math.max(0, Math.floor((now - row.scrapedAt) / 1000))} s ago
                        </Typography>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          {metrics.snapshot && (
            <Typography variant="caption">
              Selected job {metrics.snapshot.job} · instance {metrics.snapshot.instance}. Evaluated{" "}
              {new Date(metrics.snapshot.evaluatedAt).toISOString()}; completed{" "}
              {new Date(metrics.snapshot.collectedAt).toISOString()}. Positive counts describe this
              exporter; zero counts do not prove overall health or processing success.
            </Typography>
          )}
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
}
