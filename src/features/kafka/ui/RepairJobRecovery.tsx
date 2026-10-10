import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type ProfileSummary, type StreamSkopeHost } from "../contracts";
import type { RepairJobSummary } from "../contracts/repair-jobs";
import type { RepairContinuationReview } from "../contracts/repair-recovery";
import { replayConfirmation } from "../contracts/record-replay";
import {
  StudioButton as Button,
  StudioAlert as Alert,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

export function RepairJobRecovery({
  host,
  job,
  jobs,
  profiles,
  changed,
}: {
  readonly host: StreamSkopeHost;
  readonly job: RepairJobSummary;
  readonly jobs: readonly RepairJobSummary[];
  readonly changed: () => Promise<void>;
  readonly profiles: readonly ProfileSummary[];
}): React.JSX.Element {
  const [expanded, setExpanded] = useState(false),
    [busy, setBusy] = useState(false),
    [profileId, setProfileId] = useState(""),
    [review, setReview] = useState<RepairContinuationReview>(),
    [confirmation, setConfirmation] = useState(""),
    [archiveConfirmation, setArchiveConfirmation] = useState(""),
    [index, setIndex] = useState("1"),
    [offset, setOffset] = useState(""),
    [error, setError] = useState<string>(),
    [notice, setNotice] = useState<string>(),
    [attempted, setAttempted] = useState(false),
    [previewIndex, setPreviewIndex] = useState("0");
  const generation = useRef(0),
    plan = useRef<string | undefined>(undefined);
  const cancel = async (id: string): Promise<void> => {
    const result = await host.execute({
      command: "records.replay.cancel",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { planId: id },
    });
    if (!result.ok) throw new Error(`${result.error.summary} ${result.error.recovery}`);
  };
  useEffect(
    (): (() => void) => (): void => {
      generation.current++;
      const id = plan.current;
      if (id) void cancel(id).catch(() => undefined);
    },
    [host],
  );
  const run = async (task: (current: number) => Promise<void>): Promise<void> => {
    if (busy) return;
    const current = generation.current;
    setBusy(true);
    setError(undefined);
    setNotice(undefined);
    try {
      await task(current);
    } catch (failure) {
      if (current === generation.current)
        setError(
          failure instanceof Error
            ? failure.message
            : "The operation did not return a confirmed result. Inspect Activity and refresh history.",
        );
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  const destination = (): { id: string; revision: number } | null => {
    if (!profileId) return null;
    const profile = profiles.find((p) => p.id === profileId);
    if (!profile) throw new Error("Select a current saved profile.");
    return { id: profile.id, revision: profile.revision ?? 1 };
  };
  const chain = (): readonly { id: string; revision: number }[] => {
    const result: { id: string; revision: number }[] = [];
    let current: RepairJobSummary | undefined = job;
    while (current) {
      result.push({ id: current.id, revision: current.revision });
      current = current.continuationId
        ? jobs.find((j) => j.id === current!.continuationId)
        : undefined;
    }
    return result;
  };
  return (
    <Stack spacing={1}>
      <Button
        size="small"
        disabled={busy}
        onClick={() =>
          void run(async () => {
            if (expanded) {
              if (plan.current) {
                await cancel(plan.current);
                plan.current = undefined;
              }
              setReview(undefined);
              setExpanded(false);
              return;
            }
            setExpanded(true);
          })
        }
      >
        {expanded ? "Close recovery controls" : "Recovery controls"}
      </Button>
      {error && <Alert severity="error">{error}</Alert>}
      {notice && <Alert severity="info">{notice}</Alert>}
      {expanded && (
        <>
          <Typography variant="body2">
            Connect before recovery. The destination must still identify the original cluster and
            topic. Current credentials and protection apply. Unknown writes stay unknown, even when
            matching bytes are observed.
          </Typography>
          <TextField
            select
            label="Recovery destination"
            value={profileId}
            disabled={busy || !!review}
            onChange={(e) => setProfileId(e.target.value)}
          >
            <MenuItem value="">Active connection</MenuItem>
            {profiles.map((p) => (
              <MenuItem key={p.id} value={p.id}>
                {p.name}
              </MenuItem>
            ))}
          </TextField>
          <Button
            disabled={busy || !job.canContinue || !!review}
            onClick={() =>
              void run(async (current) => {
                const result = await host.execute({
                  command: "records.repair.review",
                  id: crypto.randomUUID(),
                  version: HOST_PROTOCOL_VERSION,
                  payload: { jobId: job.id, targetProfile: destination() },
                });
                if (!result.ok) throw new Error(`${result.error.summary} ${result.error.recovery}`);
                if (current !== generation.current) {
                  await cancel(result.result.continuation.review.planId);
                  return;
                }
                plan.current = result.result.continuation.review.planId;
                setReview(result.result.continuation);
                setAttempted(false);
                setConfirmation("");
              })
            }
          >
            Review definitely unsent records
          </Button>
          {review && (
            <>
              <Typography>
                {review.review.batch.records.length} records in a new linked attempt. Skipped:{" "}
                {review.skipped.acknowledged} acknowledged, {review.skipped.rejected} rejected,{" "}
                {review.skipped.uncertain} uncertain. The original attempt is retained.
              </Typography>
              <TextField
                select
                label="Continuation preview record"
                value={previewIndex}
                onChange={(e) => setPreviewIndex(e.target.value)}
                disabled={busy}
              >
                {review.review.batch.records.map((_, i) => (
                  <MenuItem key={i} value={String(i)}>
                    Record {i + 1}
                  </MenuItem>
                ))}
              </TextField>
              <StudioCodeBlock aria-label="Frozen continuation bytes">
                {JSON.stringify(
                  review.review.batch.records[Number(previewIndex)] ??
                    review.review.batch.records[0],
                  null,
                  2,
                )}
              </StudioCodeBlock>
              <TextField
                label={`Type ${replayConfirmation(review.review)}`}
                value={confirmation}
                disabled={busy || attempted}
                onChange={(e) => setConfirmation(e.target.value)}
              />
              <Button
                disabled={busy || attempted || confirmation !== replayConfirmation(review.review)}
                onClick={() =>
                  void run(async () => {
                    setAttempted(true);
                    const result = await host.execute({
                      command: "records.replay.apply",
                      id: crypto.randomUUID(),
                      version: HOST_PROTOCOL_VERSION,
                      payload: { planId: review.review.planId, confirmation },
                    });
                    if (!result.ok)
                      throw new Error(`${result.error.summary} ${result.error.recovery}`);
                    plan.current = undefined;
                    setNotice(
                      `Continuation stopped: ${result.result.outcome.stopReason}. Refresh receipts before another attempt.`,
                    );
                    setReview(undefined);
                    await changed();
                  })
                }
              >
                Apply reviewed continuation
              </Button>
              <Button
                disabled={!plan.current}
                onClick={() =>
                  void cancel(plan.current!)
                    .then(() => {
                      plan.current = undefined;
                      setReview(undefined);
                    })
                    .catch(() =>
                      setError(
                        "Cancellation cleanup is unconfirmed. Retry cancellation and inspect Activity.",
                      ),
                    )
                }
              >
                Cancel continuation
              </Button>
            </>
          )}
          <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
            <TextField
              label="Record in this attempt (1-based)"
              value={index}
              disabled={busy || !!review}
              onChange={(e) => setIndex(e.target.value)}
            />
            <TextField
              label="Destination offset to inspect"
              value={offset}
              disabled={busy || !!review}
              onChange={(e) => setOffset(e.target.value)}
            />
            <Button
              disabled={busy || !!review || !offset}
              onClick={() =>
                void run(async () => {
                  const recordIndex = Number(index) - 1;
                  if (
                    !Number.isSafeInteger(recordIndex) ||
                    recordIndex < 0 ||
                    recordIndex >= job.total
                  )
                    throw new Error("Choose a record within this attempt.");
                  const result = await host.execute({
                    command: "records.repair.reconcile",
                    id: crypto.randomUUID(),
                    version: HOST_PROTOCOL_VERSION,
                    payload: { jobId: job.id, recordIndex, offset, targetProfile: destination() },
                  });
                  if (!result.ok)
                    throw new Error(`${result.error.summary} ${result.error.recovery}`);
                  setNotice(
                    `Observation: ${result.result.finding.state}; cleanup ${result.result.finding.cleanup}. This is not a dispatch acknowledgement.`,
                  );
                  await changed();
                })
              }
            >
              Inspect destination offset
            </Button>
          </Stack>
          {job.canArchive && (
            <>
              <Typography variant="body2">
                Archive removes this entire attempt chain from protected history, including any
                definitely unsent records. Back up application data first. Active or uncertain
                chains cannot be archived.
              </Typography>
              <TextField
                label={`Confirm archive: ${job.id}`}
                value={archiveConfirmation}
                disabled={busy || !!review}
                onChange={(e) => setArchiveConfirmation(e.target.value)}
              />
              <Button
                disabled={busy || !!review || archiveConfirmation !== job.id}
                onClick={() =>
                  void run(async () => {
                    const result = await host.execute({
                      command: "records.repair.archive",
                      id: crypto.randomUUID(),
                      version: HOST_PROTOCOL_VERSION,
                      payload: { jobId: job.id, confirmation: archiveConfirmation, chain: chain() },
                    });
                    if (!result.ok)
                      throw new Error(`${result.error.summary} ${result.error.recovery}`);
                    await changed();
                  })
                }
              >
                Archive confirmed chain
              </Button>
            </>
          )}
        </>
      )}
    </Stack>
  );
}
