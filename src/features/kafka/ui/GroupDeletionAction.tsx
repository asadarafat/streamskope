import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type {
  GroupAdministrationReview,
  GroupAdministrationOutcome,
} from "../contracts/group-administration";
import {
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioAlert as Alert,
} from "../../../platform/ui/controls";

export function GroupDeletionAction({
  host,
  groupId,
  enabled,
  canWrite,
  onDeleted,
}: {
  readonly host: StreamSkopeHost;
  readonly groupId: string;
  readonly enabled: boolean;
  readonly canWrite: boolean;
  readonly onDeleted: () => void;
}): React.JSX.Element {
  const [open, setOpen] = useState(false),
    [busy, setBusy] = useState(false),
    [attempted, setAttempted] = useState(false);
  const [review, setReview] = useState<GroupAdministrationReview>(),
    [outcome, setOutcome] = useState<GroupAdministrationOutcome>();
  const [confirmation, setConfirmation] = useState(""),
    [error, setError] = useState<string>();
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setOpen(false);
    setBusy(false);
    setAttempted(false);
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [host, groupId, enabled, canWrite]);
  const close = (): void => {
    if (busy) return;
    generation.current++;
    setOpen(false);
    if (outcome?.state === "acknowledged") onDeleted();
  };
  const prepare = async (): Promise<void> => {
    const current = generation.current;
    setBusy(true);
    setReview(undefined);
    setOutcome(undefined);
    setAttempted(false);
    setError(undefined);
    setConfirmation("");
    try {
      const response = await host.execute({
        command: "consumerGroups.delete.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { groupId },
      });
      if (generation.current !== current) return;
      if (response.ok) setReview(response.result.review);
      else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (generation.current === current)
        setError(
          "Group review is unavailable. Stop consumers and inspect group permissions, then review again.",
        );
    } finally {
      if (generation.current === current) setBusy(false);
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
        command: "consumerGroups.delete.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (generation.current !== current) return;
      if (response.ok) setOutcome(response.result.outcome);
      else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (generation.current === current)
        setError(
          "The result did not reach this view. Inspect Activity and the group before another attempt; this review cannot be resent.",
        );
    } finally {
      if (generation.current === current) setBusy(false);
    }
  };
  return (
    <>
      <Button
        variant="outlined"
        disabled={!enabled || !canWrite}
        onClick={() => {
          generation.current++;
          setReview(undefined);
          setOutcome(undefined);
          setConfirmation("");
          setAttempted(false);
          setError(undefined);
          setOpen(true);
        }}
      >
        Delete group…
      </Button>
      <Dialog open={open} onClose={close} maxWidth="sm" fullWidth>
        <DialogTitle>Review consumer group deletion</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            <Typography>{groupId}</Typography>
            <Alert severity="warning">
              Stop every consumer first. Deletion removes this group's committed offsets, not topic
              messages. A consumer can recreate the group. Kafka groups have no UUID; a concurrent
              identical recreation cannot be distinguished. Keep consumers stopped until the result
              is reconciled.
            </Alert>
            <Button disabled={busy || attempted} onClick={() => void prepare()}>
              Review group deletion
            </Button>
            {review && (
              <>
                <Typography>
                  Connection: {review.connectionName} · expires {review.expiresAt}
                </Typography>
                <Typography>
                  Group state: {review.baseline.state} · members: {review.baseline.members} · DELETE
                  permission: {review.baseline.deletePermission}
                </Typography>
                <Typography variant="body2">
                  The host will recheck the cluster, complete committed-offset fingerprint,
                  inactivity and permissions before admission.
                </Typography>
                <TextField
                  label="Confirm exact group deletion"
                  helperText={review.confirmation}
                  disabled={busy || attempted}
                  value={confirmation}
                  onChange={(e) => setConfirmation(e.target.value)}
                  fullWidth
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
            color="error"
            variant="contained"
            disabled={
              !review ||
              busy ||
              attempted ||
              !canWrite ||
              confirmation !== review.confirmation ||
              Date.now() >= Date.parse(review.expiresAt)
            }
            onClick={() => void apply()}
          >
            Delete reviewed group
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
