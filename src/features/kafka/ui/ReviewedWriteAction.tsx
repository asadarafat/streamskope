import { useEffect, useState } from "react";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import {
  HOST_PROTOCOL_VERSION,
  parseKafkaWriteInput,
  type KafkaWriteInput,
  type KafkaWriteReview,
  type KafkaWriteOutcome,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioLabeledControl as FormControlLabel,
  StudioSwitch as Switch,
} from "../../../platform/ui/controls";

interface Properties {
  readonly host: StreamSkopeHost;
  readonly disabled: boolean;
  readonly topic?: string;
  readonly onCreated?: () => void;
}

function encode(value: string): string {
  return btoa(
    Array.from(new TextEncoder().encode(value), (byte) => String.fromCharCode(byte)).join(""),
  );
}

export function ReviewedWriteAction({
  host,
  disabled,
  topic,
  onCreated,
}: Properties): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState(topic ?? "");
  const [partition, setPartition] = useState("0");
  const [partitions, setPartitions] = useState("1");
  const [replicas, setReplicas] = useState("1");
  const [settings, setSettings] = useState("[]");
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [nullKey, setNullKey] = useState(true);
  const [tombstone, setTombstone] = useState(false);
  const [base64, setBase64] = useState(false);
  const [headers, setHeaders] = useState("[]");
  const [review, setReview] = useState<KafkaWriteReview>();
  const [outcome, setOutcome] = useState<KafkaWriteOutcome>();
  const [failure, setFailure] = useState<string>();
  const [pending, setPending] = useState(false);
  const [attempted, setAttempted] = useState(false);
  useEffect(() => {
    setOpen(false);
    setReview(undefined);
    setOutcome(undefined);
    setAttempted(false);
    setName(topic ?? "");
  }, [topic]);
  const producing = topic !== undefined;
  const title = producing ? "Produce message" : "Create topic";
  async function prepare(): Promise<void> {
    setPending(true);
    setFailure(undefined);
    try {
      const bytes = (text: string): string => (base64 ? text : encode(text));
      const input: KafkaWriteInput = parseKafkaWriteInput(
        producing
          ? {
              kind: "record",
              topic,
              partition: Number(partition),
              record: {
                state: "complete",
                encoding: "base64",
                key: nullKey ? null : bytes(key),
                value: tombstone ? null : bytes(value),
                headers: (JSON.parse(headers) as unknown[]).map((entry) => {
                  if (
                    entry === null ||
                    typeof entry !== "object" ||
                    !("key" in entry) ||
                    typeof entry.key !== "string" ||
                    !("value" in entry) ||
                    (entry.value !== null && typeof entry.value !== "string")
                  )
                    throw new Error(
                      "Headers must be an array of {key, value} entries; values can be null.",
                    );
                  return {
                    key: encode(entry.key),
                    value: entry.value === null ? null : bytes(entry.value),
                  };
                }),
              },
            }
          : {
              kind: "topic",
              topic: name,
              partitions: Number(partitions),
              replicationFactor: Number(replicas),
              configs: JSON.parse(settings) as unknown,
            },
      );
      const result = await host.execute({
        command: "writes.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (!result.ok) setFailure(`${result.error.summary} ${result.error.recovery}`);
      else {
        setReview(result.result.review);
        setOutcome(undefined);
        setAttempted(false);
      }
    } catch (error) {
      setFailure(error instanceof Error ? error.message : "Review the form values.");
    } finally {
      setPending(false);
    }
  }
  async function apply(): Promise<void> {
    if (review === undefined || pending || disabled) return;
    setPending(true);
    setAttempted(true);
    setFailure(undefined);
    try {
      const response = await host.execute({
        command: "writes.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId },
      });
      if (!response.ok) setFailure(`${response.error.summary} ${response.error.recovery}`);
      else {
        setOutcome(response.result.outcome);
        if (response.result.outcome.state === "acknowledged" && !producing) onCreated?.();
      }
    } catch {
      setFailure(
        "The host response was lost. The write may have happened. Check this attempt's result or inspect the destination before starting another.",
      );
    } finally {
      setPending(false);
    }
  }
  return (
    <>
      <Button
        disabled={disabled}
        onClick={() => {
          setOpen(true);
          setReview(undefined);
          setOutcome(undefined);
          setFailure(undefined);
          setAttempted(false);
        }}
        variant="outlined"
      >
        {title}
      </Button>
      <Dialog
        open={open}
        onClose={() => {
          if (!pending) setOpen(false);
        }}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>{title}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            {disabled ? (
              <Alert severity="warning">
                Connect with read-only mode disabled before applying a write.
              </Alert>
            ) : null}
            {failure === undefined ? null : <Alert severity="error">{failure}</Alert>}
            {review === undefined ? (
              <>
                {producing ? (
                  <>
                    <Typography>
                      Destination: {topic}. One record per confirmed attempt, up to 64 KiB including
                      headers.
                    </Typography>
                    <TextField
                      label="Partition"
                      value={partition}
                      onChange={(event) => setPartition(event.target.value)}
                    />
                    <FormControlLabel
                      label="Use Base64 for key, value and header values"
                      control={
                        <Switch checked={base64} onChange={(_, checked) => setBase64(checked)} />
                      }
                    />
                    <FormControlLabel
                      label="Null key"
                      control={
                        <Switch checked={nullKey} onChange={(_, checked) => setNullKey(checked)} />
                      }
                    />
                    <TextField
                      label="Message key"
                      value={key}
                      disabled={nullKey}
                      onChange={(event) => setKey(event.target.value)}
                    />
                    <FormControlLabel
                      label="Tombstone (null value)"
                      control={
                        <Switch
                          checked={tombstone}
                          onChange={(_, checked) => setTombstone(checked)}
                        />
                      }
                    />
                    <TextField
                      multiline
                      minRows={4}
                      label="Message value"
                      value={value}
                      disabled={tombstone}
                      onChange={(event) => setValue(event.target.value)}
                    />
                    <TextField
                      multiline
                      minRows={2}
                      label="Ordered headers (JSON array)"
                      helperText={
                        'Example: [{"key":"source","value":"support"}]. Repeated names and null values are preserved.'
                      }
                      value={headers}
                      onChange={(event) => setHeaders(event.target.value)}
                    />
                  </>
                ) : (
                  <>
                    <TextField
                      label="Topic name"
                      value={name}
                      onChange={(event) => setName(event.target.value)}
                    />
                    <TextField
                      label="Partitions"
                      value={partitions}
                      onChange={(event) => setPartitions(event.target.value)}
                    />
                    <TextField
                      label="Replication factor"
                      value={replicas}
                      onChange={(event) => setReplicas(event.target.value)}
                    />
                    <TextField
                      multiline
                      minRows={3}
                      label="Topic settings (JSON array)"
                      helperText={
                        'Example: [{"name":"retention.ms","value":"86400000"}]. Unspecified settings use broker defaults.'
                      }
                      value={settings}
                      onChange={(event) => setSettings(event.target.value)}
                    />
                  </>
                )}
              </>
            ) : (
              <>
                <Typography component="h3" variant="subtitle1">
                  Review destination: {review.connectionName} / {review.input.topic}
                </Typography>
                <Alert severity="warning">
                  This changes Kafka. Confirm the destination and content below. A lost
                  acknowledgement is an unknown outcome; do not blindly repeat the operation. Review
                  expires at {review.expiresAt}.
                </Alert>
                {review.input.kind === "record" ? (
                  <Typography>
                    Partition {review.input.partition};{" "}
                    {review.input.record.key === null ? "null key" : "key bytes provided"};{" "}
                    {review.input.record.value === null ? "tombstone" : "value bytes provided"};{" "}
                    {review.input.record.headers.length} ordered headers. Key, value and header
                    bytes below use Base64.
                  </Typography>
                ) : (
                  <Typography>
                    {review.input.partitions} partitions, replication factor{" "}
                    {review.input.replicationFactor}. Name and broker count checked; Kafka validates
                    settings and permissions when you confirm.
                  </Typography>
                )}
                <Typography
                  component="pre"
                  variant="body2"
                  sx={{
                    whiteSpace: "pre-wrap",
                    overflowWrap: "anywhere",
                    maxHeight: 240,
                    overflow: "auto",
                  }}
                >
                  {JSON.stringify(review.input, null, 2)}
                </Typography>
              </>
            )}
            {outcome === undefined ? null : (
              <Alert severity={outcome.state === "acknowledged" ? "success" : "warning"}>
                <strong>
                  {outcome.state === "acknowledged"
                    ? "Acknowledged"
                    : outcome.state === "unknown"
                      ? "Outcome unknown"
                      : "Rejected"}
                </strong>
                : {outcome.detail}
                {outcome.receipt === null ? null : (
                  <Typography>
                    Topic {outcome.receipt.topic}, partition {outcome.receipt.partition}, offset{" "}
                    {outcome.receipt.offset}.
                  </Typography>
                )}
              </Alert>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={pending} onClick={() => setOpen(false)}>
            Close
          </Button>
          {review === undefined ? (
            <Button
              disabled={pending || disabled}
              onClick={() => {
                void prepare();
              }}
            >
              Review {producing ? "message" : "topic"}
            </Button>
          ) : outcome === undefined ? (
            <>
              {attempted ? null : (
                <Button disabled={pending} onClick={() => setReview(undefined)}>
                  Edit
                </Button>
              )}
              <Button
                variant="contained"
                disabled={pending || disabled}
                onClick={() => {
                  void apply();
                }}
              >
                {pending
                  ? "Waiting for outcome…"
                  : attempted
                    ? "Check attempt result"
                    : producing
                      ? "Confirm produce"
                      : "Confirm create"}
              </Button>
            </>
          ) : null}
        </DialogActions>
      </Dialog>
    </>
  );
}
