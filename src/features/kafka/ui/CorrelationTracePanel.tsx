import { useEffect, useRef, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  parseCorrelationTraceInput,
  type CorrelationTraceInput,
  type CorrelationTraceResult,
} from "../contracts/correlation-trace";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";

import { QueryTimeWindowControls } from "./QueryTimeWindowControls";
import { QueryReadCoverage } from "./QueryReadCoverage";
import {
  initialKafkaTimeWindow,
  kafkaTimeWindowError,
  resolveKafkaTimeWindow,
} from "./query-time-window";

export function CorrelationTracePanel({
  host,
  topic,
  enabled,
}: {
  readonly host: StreamSkopeHost;
  readonly topic: string;
  readonly enabled: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [topics, setTopics] = useState(topic);
  const [value, setValue] = useState("");
  const [source, setSource] = useState<CorrelationTraceInput["selector"]["source"]>("header");
  const [path, setPath] = useState("correlation-id");
  const [format, setFormat] = useState<CorrelationTraceInput["selector"]["format"]>("json");
  const [window, setWindow] = useState(initialKafkaTimeWindow);
  const [result, setResult] = useState<CorrelationTraceResult>();
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const active = useRef<string | undefined>(undefined);
  const generation = useRef(0);
  const cancel = async (): Promise<void> => {
    if (!active.current) return;
    setCancelling(true);
    try {
      const response = await host.execute({
        command: "records.trace.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { traceId: active.current },
      });
      if (!response.ok) setError(response.error.summary);
    } catch {
      setError("Cancellation could not reach the host. The trace still has a time limit.");
    }
  };
  useEffect(() => {
    setResult(undefined);
    setError(undefined);
    setBusy(false);
    setCancelling(false);
    setTopics(topic);
    if (!enabled) setOpen(false);
    return (): void => {
      generation.current++;
      const traceId = active.current;
      active.current = undefined;
      if (traceId)
        void host
          .execute({
            command: "records.trace.cancel",
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: { traceId },
          })
          .catch(() => undefined);
    };
  }, [host, enabled, topic]);
  const trace = async (): Promise<void> => {
    const request = ++generation.current;
    const traceId = crypto.randomUUID();
    setError(undefined);
    setResult(undefined);
    setBusy(true);
    setCancelling(false);
    active.current = traceId;
    try {
      const payload = parseCorrelationTraceInput({
        traceId,
        topics: topics
          .split(/[\n,]/u)
          .map((item) => item.trim())
          .filter(Boolean),
        value,
        selector: { source, path: source === "key" ? "" : path, format },
        ...resolveKafkaTimeWindow(window),
      });
      const response = await host.execute({
        command: "records.trace",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload,
      });
      if (request !== generation.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setResult(response.result.trace);
    } catch (error) {
      if (request === generation.current)
        setError(error instanceof Error ? error.message : "Trace failed.");
    } finally {
      if (request === generation.current) {
        active.current = undefined;
        setBusy(false);
      }
    }
  };
  const changed = (): void => {
    setResult(undefined);
    setError(undefined);
  };
  return (
    <>
      <Button disabled={!enabled} onClick={() => setOpen(true)}>
        Trace correlation
      </Button>
      <Dialog
        open={open}
        onClose={() => {
          if (!busy) setOpen(false);
        }}
        fullWidth
        maxWidth="lg"
      >
        <DialogTitle>Trace a correlation ID</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography>
              Search an exact value in up to eight topics. This independent read leaves the message
              reader running and never commits consumer offsets.
            </Typography>
            <TextField
              label="Trace topics"
              multiline
              minRows={2}
              value={topics}
              disabled={busy}
              helperText="One topic per line, or comma separated. Only these topics are searched."
              onChange={(event) => {
                setTopics(event.target.value);
                changed();
              }}
            />
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: { xs: "1fr", sm: "1fr 1fr" },
                gap: 2,
                alignItems: "start",
              }}
            >
              <TextField
                select
                label="Correlation source"
                value={source}
                disabled={busy}
                onChange={(event) => {
                  const next = event.target.value as typeof source;
                  setSource(next);
                  setPath(
                    next === "header"
                      ? "correlation-id"
                      : next === "payload"
                        ? "/correlationId"
                        : "",
                  );
                  changed();
                }}
              >
                <MenuItem value="header">Header (UTF-8)</MenuItem>
                <MenuItem value="key">Key (UTF-8)</MenuItem>
                <MenuItem value="payload">Payload field</MenuItem>
              </TextField>
              {source === "key" ? null : (
                <TextField
                  label={source === "header" ? "Header name" : "Payload JSON Pointer"}
                  value={path}
                  disabled={busy}
                  helperText={
                    source === "payload"
                      ? "Example: /metadata/correlationId; empty selects the root scalar."
                      : "Header names and values are case sensitive."
                  }
                  onChange={(event) => {
                    setPath(event.target.value);
                    changed();
                  }}
                />
              )}
              {source === "payload" ? (
                <TextField
                  select
                  label="Trace payload encoding"
                  value={format}
                  disabled={busy}
                  onChange={(event) => {
                    setFormat(event.target.value as typeof format);
                    changed();
                  }}
                >
                  <MenuItem value="json">UTF-8 JSON</MenuItem>
                  <MenuItem value="avro">Confluent Avro</MenuItem>
                  <MenuItem value="protobuf">Confluent Protobuf</MenuItem>
                </TextField>
              ) : null}
              <TextField
                label="Exact correlation value"
                value={value}
                disabled={busy}
                onChange={(event) => {
                  setValue(event.target.value);
                  changed();
                }}
              />
            </Box>
            <QueryTimeWindowControls
              actionLabel="Start trace"
              value={window}
              disabled={busy}
              error={kafkaTimeWindowError(window)}
              onChange={(next) => {
                setWindow(next);
                changed();
              }}
            />
            <Typography variant="caption">
              At most 1,000 candidate records per topic, 200 matches, 32 MiB evaluated and 30
              seconds overall. Limits and unreadable records leave partial evidence.
            </Typography>
            <Stack direction="row" spacing={1}>
              <Button
                variant="contained"
                disabled={!enabled || busy || !value || !!kafkaTimeWindowError(window)}
                onClick={() => {
                  void trace();
                }}
              >
                Start trace
              </Button>
              {busy ? (
                <Button
                  disabled={cancelling}
                  onClick={() => {
                    void cancel();
                  }}
                >
                  Cancel trace
                </Button>
              ) : null}
            </Stack>
            {busy ? (
              <Typography role="status">
                {cancelling
                  ? "Stopping and collecting partial coverage…"
                  : "Tracing selected topics…"}
              </Typography>
            ) : null}
            {error ? <Alert severity="error">{error}</Alert> : null}
            {result ? (
              <>
                <Alert
                  severity={
                    result.topics.every((item) => item.state === "searched") ? "info" : "warning"
                  }
                >
                  {result.matches.length} matching{" "}
                  {result.matches.length === 1 ? "record" : "records"} on {result.connectionName}.{" "}
                  {result.topics.every((item) => item.state === "searched")
                    ? "All requested retained offset ranges reached."
                    : "Partial evidence: review each topic below."}{" "}
                  Matching IDs do not prove causality; duplicates at different offsets remain
                  separate.
                </Alert>
                <Typography variant="caption">
                  {new Date(result.input.startTimeMs).toISOString()} ≤ record timestamp &lt;{" "}
                  {new Date(result.input.endTimeMs).toISOString()}. Result order is topic scan
                  order, not a causal timeline.
                </Typography>
                <Box
                  sx={{
                    overflowX: "auto",
                    "& th, & td": {
                      p: 1,
                      textAlign: "left",
                      borderBottom: 1,
                      borderColor: "divider",
                    },
                  }}
                >
                  <table aria-label="Correlation matches">
                    <thead>
                      <tr>
                        <th>Topic</th>
                        <th>Partition</th>
                        <th>Offset</th>
                        <th>Timestamp</th>
                        <th>Preview (up to 512 characters)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.matches.map((item) => (
                        <tr key={JSON.stringify([item.topic, item.partition, item.offset])}>
                          <td>{item.topic}</td>
                          <td>{item.partition}</td>
                          <td>{item.offset}</td>
                          <td>{item.timestamp}</td>
                          <td style={{ maxWidth: 400, overflowWrap: "anywhere" }}>
                            {item.preview}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </Box>
                {result.topics.map((item) => (
                  <Box
                    component="section"
                    aria-label={`Trace coverage for ${item.topic}`}
                    key={item.topic}
                  >
                    <Typography component="h3" variant="subtitle2">
                      {item.topic}: {item.state} ({item.reason})
                    </Typography>
                    <Typography variant="body2">
                      {item.evaluated} evaluated; {item.unavailable} unavailable; {item.matches}{" "}
                      matches.
                    </Typography>
                    <QueryReadCoverage coverage={item.coverage} search={false} />
                  </Box>
                ))}
              </>
            ) : null}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setOpen(false)}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
