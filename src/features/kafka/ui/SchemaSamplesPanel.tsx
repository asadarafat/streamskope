import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseSchemaSampleInput,
  parseRecordBatchInput,
  type SchemaSamples,
  type RecordBatchReview,
  type RecordBatchOutcome,
} from "../contracts/schema-samples";
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
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

export function SchemaSamplesPanel({
  schema,
  host,
  enabled,
}: {
  readonly schema: SchemaVersionDetail;
  readonly host: StreamSkopeHost;
  readonly enabled: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [seed, setSeed] = useState("1");
  const [count, setCount] = useState("5");
  const [messageType, setMessageType] = useState("");
  const [topic, setTopic] = useState("");
  const [partition, setPartition] = useState("0");
  const [rate, setRate] = useState("1");
  const [samples, setSamples] = useState<SchemaSamples>();
  const [selected, setSelected] = useState(0);
  const [review, setReview] = useState<RecordBatchReview>();
  const [outcome, setOutcome] = useState<RecordBatchOutcome>();
  const [confirmation, setConfirmation] = useState("");
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState<"generate" | "review" | "publish">();
  const [cancelling, setCancelling] = useState(false);
  const generation = useRef(0);
  const activePlan = useRef<string | undefined>(undefined);
  const cancel = async (): Promise<void> => {
    const planId = activePlan.current;
    if (!planId) return;
    setCancelling(true);
    try {
      const response = await host.execute({
        command: "records.batch.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId },
      });
      if (!response.ok) setError(response.error.summary);
    } catch {
      setError("Cancellation could not reach the host. Inspect Kafka before another attempt.");
    }
  };
  useEffect(() => {
    generation.current++;
    setSamples(undefined);
    setReview(undefined);
    setOutcome(undefined);
    setBusy(undefined);
    setError(undefined);
    return (): void => {
      generation.current++;
      const planId = activePlan.current;
      if (planId) {
        activePlan.current = undefined;
        void host
          .execute({
            command: "records.batch.cancel",
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: { planId },
          })
          .catch(() => undefined);
      }
    };
  }, [schema, enabled, host]);
  const reset = (): void => {
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setError(undefined);
  };
  const generate = async (): Promise<void> => {
    const request = ++generation.current;
    setBusy("generate");
    reset();
    setSamples(undefined);
    try {
      const payload = parseSchemaSampleInput({
        subject: schema.subject,
        version: schema.version,
        seed: Number(seed),
        count: Number(count),
        messageType,
      });
      const response = await host.execute({
        command: "schemas.samples",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload,
      });
      if (request !== generation.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setSamples(response.result.samples);
      setSelected(0);
    } catch (error) {
      if (request === generation.current)
        setError(error instanceof Error ? error.message : "Generation failed.");
    } finally {
      if (request === generation.current) setBusy(undefined);
    }
  };
  const reviewBatch = async (): Promise<void> => {
    if (!samples) return;
    const request = ++generation.current;
    setBusy("review");
    reset();
    try {
      const input = parseRecordBatchInput({
        topic,
        partition: Number(partition),
        ratePerSecond: Number(rate),
        records: samples.samples.map((sample) => sample.record),
      });
      const response = await host.execute({
        command: "records.batch.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (request !== generation.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setReview(response.result.review);
    } catch (error) {
      if (request === generation.current)
        setError(error instanceof Error ? error.message : "Review failed.");
    } finally {
      if (request === generation.current) setBusy(undefined);
    }
  };
  const publish = async (): Promise<void> => {
    if (!review || confirmation !== review.input.topic || activePlan.current) return;
    const request = ++generation.current;
    activePlan.current = review.planId;
    setBusy("publish");
    setError(undefined);
    setCancelling(false);
    try {
      const response = await host.execute({
        command: "records.batch.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId },
      });
      if (request !== generation.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setOutcome(response.result.outcome);
    } catch {
      if (request === generation.current)
        setError(
          "The batch result is unavailable. Inspect the destination; do not automatically resend this batch.",
        );
    } finally {
      activePlan.current = undefined;
      if (request === generation.current) {
        setBusy(undefined);
        setReview(undefined);
      }
    }
  };
  const locked = busy !== undefined;
  return (
    <>
      <Button variant="outlined" disabled={!enabled} onClick={() => setOpen(true)}>
        Generate samples
      </Button>
      <Dialog
        open={open}
        onClose={() => {
          if (!locked) setOpen(false);
        }}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          Schema samples — {schema.subject}@{schema.version}
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography>
              Generate a reproducible preview from this exact schema. Preview writes nothing.
              Publishing requires a separate destination review and confirmation.
            </Typography>
            <Stack direction="row" spacing={2}>
              <TextField
                label="Seed"
                type="number"
                value={seed}
                disabled={locked}
                onChange={(event) => {
                  setSeed(event.target.value);
                  setSamples(undefined);
                  reset();
                }}
              />
              <TextField
                label="Sample count"
                type="number"
                value={count}
                helperText="1–50"
                disabled={locked}
                onChange={(event) => {
                  setCount(event.target.value);
                  setSamples(undefined);
                  reset();
                }}
              />
            </Stack>
            {schema.schemaType === "PROTOBUF" ? (
              <TextField
                label="Protobuf message type"
                value={messageType}
                helperText="Fully qualified type; empty selects the first writer message."
                disabled={locked}
                onChange={(event) => {
                  setMessageType(event.target.value);
                  setSamples(undefined);
                  reset();
                }}
              />
            ) : null}
            <Button
              disabled={!enabled || locked}
              onClick={() => {
                void generate();
              }}
            >
              {busy === "generate" ? "Generating…" : "Generate preview"}
            </Button>
            {samples ? (
              <>
                <Alert severity="info">
                  {samples.encoding}. Seed {samples.seed}; {samples.samples.length} valid samples.
                  Original record keys are null and headers empty.
                </Alert>
                <TextField
                  select
                  label="Preview sample"
                  value={selected}
                  onChange={(event) => setSelected(Number(event.target.value))}
                >
                  {samples.samples.map((_sample, index) => (
                    <MenuItem key={index} value={index}>
                      Sample {index + 1}
                    </MenuItem>
                  ))}
                </TextField>
                <StudioCodeBlock
                  aria-label="Generated sample"
                  sx={{ m: 0, p: 2, maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap" }}
                >
                  {samples.samples[selected]?.json}
                </StudioCodeBlock>
                <TextField
                  label="Destination topic"
                  value={topic}
                  disabled={locked}
                  onChange={(event) => {
                    setTopic(event.target.value);
                    reset();
                  }}
                />
                <Stack direction="row" spacing={2}>
                  <TextField
                    label="Destination partition"
                    type="number"
                    value={partition}
                    disabled={locked}
                    onChange={(event) => {
                      setPartition(event.target.value);
                      reset();
                    }}
                  />
                  <TextField
                    label="Maximum records per second"
                    type="number"
                    value={rate}
                    helperText="1–10; publication is sequential"
                    disabled={locked}
                    onChange={(event) => {
                      setRate(event.target.value);
                      reset();
                    }}
                  />
                </Stack>
                <Button
                  disabled={!enabled || locked}
                  onClick={() => {
                    void reviewBatch();
                  }}
                >
                  Review batch destination
                </Button>
              </>
            ) : null}
            {review ? (
              <>
                <Alert severity="warning">
                  Publish {review.input.records.length} records to {review.connectionName} →{" "}
                  {review.input.topic}, partition {review.input.partition}, at most{" "}
                  {review.input.ratePerSecond}/s. This adds records to Kafka. Review expires at{" "}
                  {review.expiresAt}.
                </Alert>
                <TextField
                  label="Type destination topic to confirm"
                  value={confirmation}
                  disabled={locked}
                  onChange={(event) => setConfirmation(event.target.value)}
                />
                <Button
                  variant="contained"
                  disabled={!enabled || locked || confirmation !== review.input.topic}
                  onClick={() => {
                    void publish();
                  }}
                >
                  Publish reviewed batch
                </Button>
              </>
            ) : null}
            {busy === "publish" ? (
              <>
                <Typography role="status">
                  {cancelling
                    ? "Stopping after the in-flight record settles…"
                    : "Publishing sequentially. No automatic retries."}
                </Typography>
                <Button
                  disabled={cancelling}
                  onClick={() => {
                    void cancel();
                  }}
                >
                  Cancel remaining records
                </Button>
              </>
            ) : null}
            {outcome ? (
              <Alert severity={outcome.stopReason === "complete" ? "success" : "warning"}>
                Batch {outcome.stopReason}:{" "}
                {outcome.outcomes.filter((item) => item.state === "acknowledged").length}{" "}
                acknowledged, {outcome.outcomes.filter((item) => item.state === "rejected").length}{" "}
                rejected, {outcome.outcomes.filter((item) => item.state === "unknown").length}{" "}
                uncertain, {outcome.unsent} unsent / {outcome.total} total. Inspect Kafka before a
                new attempt.
              </Alert>
            ) : null}
            {error ? <Alert severity="error">{error}</Alert> : null}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={locked} onClick={() => setOpen(false)}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
