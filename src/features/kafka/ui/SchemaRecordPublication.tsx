import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { KafkaCompleteRecord } from "../contracts/record-bytes";
import {
  parseRecordBatchInput,
  type RecordBatchReview,
  type RecordBatchOutcome,
} from "../contracts/schema-samples";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

/** Shared finite publication; schema previews never bypass destination review or receipt accounting. */
export function SchemaRecordPublication({
  host,
  records,
  enabled,
  onBusyChange,
}: {
  readonly host: StreamSkopeHost;
  readonly records: readonly KafkaCompleteRecord[];
  readonly enabled: boolean;
  readonly onBusyChange: (busy: boolean) => void;
}): React.JSX.Element {
  const [topic, setTopic] = useState("");
  const [partition, setPartition] = useState("0");
  const [rate, setRate] = useState("1");
  const [review, setReview] = useState<RecordBatchReview>();
  const [outcome, setOutcome] = useState<RecordBatchOutcome>();
  const [confirmation, setConfirmation] = useState("");
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const revision = useRef(0);
  const activePlan = useRef<string | undefined>(undefined);
  const reset = (): void => {
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setError(undefined);
  };
  useEffect(() => {
    revision.current++;
    reset();
    setBusy(false);
    setPublishing(false);
    onBusyChange(false);
    return (): void => {
      revision.current++;
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
  }, [host, records, enabled, onBusyChange]);
  const setWorking = (value: boolean): void => {
    setBusy(value);
    onBusyChange(value);
  };
  const reviewDestination = async (): Promise<void> => {
    const request = ++revision.current;
    setWorking(true);
    reset();
    try {
      const input = parseRecordBatchInput({
        topic,
        partition: Number(partition),
        ratePerSecond: Number(rate),
        records,
      });
      const response = await host.execute({
        command: "records.batch.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (request !== revision.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setReview(response.result.review);
    } catch (failure) {
      if (request === revision.current)
        setError(failure instanceof Error ? failure.message : "Destination review failed.");
    } finally {
      if (request === revision.current) setWorking(false);
    }
  };
  const publish = async (): Promise<void> => {
    if (!review || confirmation !== review.input.topic || activePlan.current) return;
    const request = ++revision.current;
    activePlan.current = review.planId;
    setWorking(true);
    setPublishing(true);
    setError(undefined);
    setCancelling(false);
    try {
      const response = await host.execute({
        command: "records.batch.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId },
      });
      if (request !== revision.current) return;
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      setOutcome(response.result.outcome);
    } catch {
      if (request === revision.current)
        setError(
          "The batch result is unavailable. Inspect Kafka before repeating any uncertain write.",
        );
    } finally {
      if (activePlan.current === review.planId) activePlan.current = undefined;
      if (request === revision.current) {
        setWorking(false);
        setPublishing(false);
        setReview(undefined);
      }
    }
  };
  const cancel = async (): Promise<void> => {
    const planId = activePlan.current;
    if (!planId) return;
    const request = revision.current;
    setCancelling(true);
    try {
      const response = await host.execute({
        command: "records.batch.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId },
      });
      if (request === revision.current && !response.ok) setError(response.error.summary);
    } catch {
      if (request === revision.current)
        setError(
          "Cancellation could not reach the host. Inspect Kafka and Activity for the admitted result.",
        );
    }
  };
  return (
    <Stack spacing={2}>
      <TextField
        label="Destination topic"
        value={topic}
        disabled={busy || !enabled}
        onChange={(event) => {
          setTopic(event.target.value);
          reset();
        }}
      />
      <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
        <TextField
          label="Destination partition"
          type="number"
          value={partition}
          disabled={busy || !enabled}
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
          disabled={busy || !enabled}
          onChange={(event) => {
            setRate(event.target.value);
            reset();
          }}
        />
      </Stack>
      <Button
        disabled={!enabled || busy}
        onClick={() => {
          void reviewDestination();
        }}
      >
        Review batch destination
      </Button>
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
            disabled={busy}
            onChange={(event) => setConfirmation(event.target.value)}
          />
          <Button
            variant="contained"
            disabled={!enabled || busy || confirmation !== review.input.topic}
            onClick={() => {
              void publish();
            }}
          >
            Publish reviewed batch
          </Button>
        </>
      ) : null}
      {publishing ? (
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
          {outcome.outcomes.filter((item) => item.state === "acknowledged").length} acknowledged,{" "}
          {outcome.outcomes.filter((item) => item.state === "rejected").length} rejected,{" "}
          {outcome.outcomes.filter((item) => item.state === "unknown").length} uncertain,{" "}
          {outcome.unsent} unsent / {outcome.total} total. Inspect Kafka before a new attempt.
        </Alert>
      ) : null}
      {error ? <Alert severity="error">{error}</Alert> : null}
    </Stack>
  );
}
