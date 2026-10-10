import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  parseTopicAdministrationInput,
  type TopicAdministrationReview,
  type TopicAdministrationOutcome,
} from "../contracts/topic-administration";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioSelect as Select,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";

export function TopicAdministrationAction({
  host,
  topic,
  canWrite,
  onChanged,
  onDeleted,
}: {
  readonly host: StreamSkopeHost;
  readonly topic: string;
  readonly canWrite: boolean;
  readonly onChanged: () => void | Promise<void>;
  readonly onDeleted: () => void;
}): React.JSX.Element {
  const [open, setOpen] = useState(false),
    [kind, setKind] = useState<"expand" | "delete">("expand");
  const [partitions, setPartitions] = useState(""),
    [confirmation, setConfirmation] = useState("");
  const [review, setReview] = useState<TopicAdministrationReview>(),
    [outcome, setOutcome] = useState<TopicAdministrationOutcome>();
  const [busy, setBusy] = useState(false),
    [attempted, setAttempted] = useState(false),
    [error, setError] = useState<string>();
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setOpen(false);
    setReview(undefined);
    setOutcome(undefined);
    setBusy(false);
    setAttempted(false);
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [host, topic, canWrite]);
  const clearReview = (): void => {
    generation.current++;
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setAttempted(false);
    setError(undefined);
  };
  const close = (): void => {
    if (busy) return;
    generation.current++;
    setOpen(false);
    if (outcome?.input.kind === "delete" && outcome.state === "acknowledged") {
      void onChanged();
      onDeleted();
    }
  };
  const prepare = async (): Promise<void> => {
    const current = generation.current;
    setBusy(true);
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setAttempted(false);
    setError(undefined);
    try {
      const input = parseTopicAdministrationInput(
        kind === "delete" ? { kind, topic } : { kind, topic, partitions: Number(partitions) },
      );
      const response = await host.execute({
        command: "topics.change.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (current !== generation.current) return;
      if (response.ok) setReview(response.result.review);
      else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError(
          "Review an authorized non-internal topic. Expansion requires a larger integer count, up to 4096 partitions.",
        );
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review || busy || attempted || !canWrite || confirmation !== review.confirmation) return;
    const current = generation.current;
    setBusy(true);
    setAttempted(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "topics.change.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (current !== generation.current) return;
      if (response.ok) {
        setOutcome(response.result.outcome);
        // Refreshing a deleted topic immediately unmounts its detail page and receipt.
        if (
          response.result.outcome.state === "acknowledged" &&
          response.result.outcome.input.kind === "expand"
        )
          void onChanged();
      } else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError(
          "The result did not reach this view. Inspect Activity and the topic before another attempt; this review cannot be sent again.",
        );
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  return (
    <>
      <Button disabled={!canWrite} variant="outlined" onClick={() => setOpen(true)}>
        Manage topic…
      </Button>
      <Dialog open={open} onClose={close} maxWidth="sm" fullWidth>
        <DialogTitle>Review topic change</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            <Typography variant="body2">{topic}</Typography>
            <FormControl fullWidth size="small">
              <InputLabel id="topic-change-action">Action</InputLabel>
              <Select
                labelId="topic-change-action"
                label="Action"
                value={kind}
                disabled={busy}
                onChange={(event) => {
                  clearReview();
                  setKind(event.target.value);
                }}
              >
                <MenuItem value="expand">Increase partitions</MenuItem>
                <MenuItem value="delete">Delete topic</MenuItem>
              </Select>
            </FormControl>
            {kind === "expand" ? (
              <>
                <Alert severity="warning">
                  Partition expansion cannot be undone. Key routing can change and ordering across
                  old and new partitions is not preserved. Kafka expands by name; avoid concurrent
                  topic deletion or recreation.
                </Alert>
                <TextField
                  label="New total partitions"
                  value={partitions}
                  disabled={busy}
                  onChange={(event) => {
                    clearReview();
                    setPartitions(event.target.value);
                  }}
                  helperText="Enter the new total, not the number to add. Maximum 4096."
                  inputMode="numeric"
                />
              </>
            ) : (
              <Alert severity="warning">
                Deletion removes this topic and its retained messages. There is no undo. Review
                downstream producers, consumers and connectors before continuing. Only UUID-based
                deletion is supported.
              </Alert>
            )}
            {review && (
              <>
                <Typography variant="body2">
                  Review destination: {review.connectionName} / {review.baseline.identity.topic}
                </Typography>
                <Typography variant="body2">
                  Current partitions: {review.baseline.partitions}.{" "}
                  {review.input.kind === "expand"
                    ? `Requested total: ${review.input.partitions}.`
                    : "Requested: delete this topic UUID."}
                </Typography>
                <Typography variant="caption" sx={{ overflowWrap: "anywhere" }}>
                  Topic UUID: {review.baseline.identity.topicId}. Review expires {review.expiresAt}.
                </Typography>
                {(review.input.kind === "delete"
                  ? review.baseline.deletePermission
                  : review.baseline.expandPermission) === "unknown" && (
                  <Alert severity="info">
                    Permission evidence is unavailable. The broker will authorize the confirmed
                    change.
                  </Alert>
                )}
                <TextField
                  label="Confirm exact topic change"
                  value={confirmation}
                  disabled={busy || attempted}
                  onChange={(event) => setConfirmation(event.target.value)}
                  helperText={`Type ${review.confirmation}`}
                />
              </>
            )}
            {outcome && (
              <Alert
                severity={
                  outcome.state === "acknowledged" &&
                  outcome.verification === "verified" &&
                  outcome.cleanup === "confirmed"
                    ? "success"
                    : "warning"
                }
              >
                {outcome.state} · readback {outcome.verification} · cleanup {outcome.cleanup}.{" "}
                {outcome.detail}
              </Alert>
            )}
            {error && <Alert severity="error">{error}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={close}>
            Close
          </Button>
          <Button
            disabled={busy || !canWrite || attempted}
            onClick={() => {
              void prepare();
            }}
          >
            Review change
          </Button>
          <Button
            variant="contained"
            disabled={
              busy || !canWrite || !review || attempted || confirmation !== review.confirmation
            }
            onClick={() => {
              void apply();
            }}
          >
            Apply reviewed change
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
