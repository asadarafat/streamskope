import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableBody, TableCell, TableHead, TableRow } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaConsumerGroupDetails,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseOffsetResetInput,
  type OffsetResetReview,
  type OffsetResetOutcome,
} from "../contracts/offset-reset";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioCheckbox as Checkbox,
} from "../../../platform/ui/controls";

export function ResetOffsetsAction({
  host,
  group,
  enabled,
  canWrite,
}: {
  readonly host: StreamSkopeHost;
  readonly group: KafkaConsumerGroupDetails | null;
  readonly enabled: boolean;
  readonly canWrite: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [targets, setTargets] = useState<Record<string, string>>({});
  const [review, setReview] = useState<OffsetResetReview>();
  const [outcome, setOutcome] = useState<OffsetResetOutcome>();
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [attempted, setAttempted] = useState(false);
  const [error, setError] = useState<string>();
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setOpen(false);
    setTargets({});
    setReview(undefined);
    setOutcome(undefined);
    setBusy(false);
    setAttempted(false);
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [group?.id, enabled, host]);
  const rows = group?.offsets.slice(0, 32) ?? [];
  const prepare = async (): Promise<void> => {
    if (!group) return;
    const current = generation.current;
    setBusy(true);
    setError(undefined);
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setAttempted(false);
    try {
      const input = parseOffsetResetInput({
        groupId: group.id,
        targets: rows.flatMap((r) => {
          const offset = targets[`${r.topic}:${r.partition}`];
          return offset === undefined ? [] : [{ topic: r.topic, partition: r.partition, offset }];
        }),
      });
      const response = await host.execute({
        command: "consumerGroups.reset.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (current !== generation.current) return;
      if (response.ok) setReview(response.result.review);
      else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError("Select partitions and enter non-negative integer offsets, then preview again.");
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review || busy || attempted) return;
    const current = generation.current;
    setBusy(true);
    setAttempted(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "consumerGroups.reset.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (current !== generation.current) return;
      if (response.ok) setOutcome(response.result.outcome);
      else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError(
          "The result could not reach this view. Inspect Activity and committed offsets before any new attempt.",
        );
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  const ready =
    review?.baseline.inactive &&
    review.baseline.groupRead !== "denied" &&
    review.baseline.partitions.every(
      (p) => BigInt(p.offset) >= BigInt(p.low) && BigInt(p.offset) <= BigInt(p.high),
    );
  const clearReview = (): void => {
    setReview(undefined);
    setOutcome(undefined);
    setAttempted(false);
    setError(undefined);
  };
  return (
    <>
      <Button
        disabled={!enabled || !group || !rows.length}
        onClick={() => setOpen(true)}
        variant="outlined"
      >
        Reset offsets…
      </Button>
      <Dialog
        open={open}
        onClose={() => {
          if (!busy) {
            generation.current++;
            setOpen(false);
          }
        }}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>Review consumer offset reset</DialogTitle>
        <DialogContent>
          <Stack spacing={2}>
            <Alert severity="warning">
              Stop every consumer in this group first. Preview does not change offsets. Kafka cannot
              atomically compare this review with a reset; keep the group stopped until every result
              is reconciled.
            </Alert>
            <Typography>
              Group: {group?.id}. Select at most 32 partitions and their next offsets. Unselected
              partitions stay outside this operation.
            </Typography>
            {(group?.offsets.length ?? 0) > 32 && (
              <Alert severity="info">
                Only the first 32 committed partitions are shown. Use another bounded operation for
                additional partitions.
              </Alert>
            )}
            <Table size="small" aria-label="Select reset partitions">
              <TableHead>
                <TableRow>
                  <TableCell>Select</TableCell>
                  <TableCell>Topic / partition</TableCell>
                  <TableCell>Committed</TableCell>
                  <TableCell>End</TableCell>
                  <TableCell>Proposed next offset</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {rows.map((r) => {
                  const key = `${r.topic}:${r.partition}`;
                  return (
                    <TableRow key={key}>
                      <TableCell>
                        <Checkbox
                          disabled={busy}
                          checked={targets[key] !== undefined}
                          aria-label={`Reset ${key}`}
                          onChange={(_, checked) => {
                            clearReview();
                            setTargets((old) => {
                              const next = { ...old };
                              if (checked) next[key] = r.committedOffset ?? "0";
                              else delete next[key];
                              return next;
                            });
                          }}
                        />
                      </TableCell>
                      <TableCell>
                        {r.topic} / {r.partition}
                      </TableCell>
                      <TableCell>{r.committedOffset ?? "None"}</TableCell>
                      <TableCell>{r.endOffset ?? "Unknown"}</TableCell>
                      <TableCell>
                        <TextField
                          disabled={busy || targets[key] === undefined}
                          label={`Next offset ${key}`}
                          value={targets[key] ?? ""}
                          onChange={(e) => {
                            clearReview();
                            setTargets({ ...targets, [key]: e.target.value });
                          }}
                        />
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
            {review && (
              <>
                <Typography>
                  Reviewed on {review.connectionName} · expires {review.expiresAt} · group{" "}
                  {review.baseline.state} · group READ permission {review.baseline.groupRead}
                </Typography>
                {!ready && (
                  <Alert severity="error">
                    Apply is unavailable: the group must be inactive, permissions must not be
                    denied, and every target must be inside retention bounds.
                  </Alert>
                )}
                <Table size="small" aria-label="Offset reset preview">
                  <TableHead>
                    <TableRow>
                      {[
                        "Topic / partition",
                        "Before",
                        "After",
                        "Earliest",
                        "End",
                        "Replay upper bound",
                      ].map((label) => (
                        <TableCell key={label}>{label}</TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {review.baseline.partitions.map((p) => (
                      <TableRow key={`${p.topic}:${p.partition}`}>
                        <TableCell>
                          {p.topic} / {p.partition}
                        </TableCell>
                        <TableCell>{p.before ?? "None"}</TableCell>
                        <TableCell>{p.offset}</TableCell>
                        <TableCell>{p.low}</TableCell>
                        <TableCell>{p.high}</TableCell>
                        <TableCell>{p.replayUpperBound}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                <Typography variant="body2">
                  Offset distance is an upper bound, not a message count. Compaction, retention,
                  aborted transactions and gaps can reduce records reprocessed. Moving forward can
                  skip records. Topic READ permission is authoritative at dispatch; a preview cannot
                  guarantee it.
                </Typography>
                <Typography variant="body2">
                  Examples: {review.exampleStatus}. Up to three records from the proposed positions;
                  key/value previews below are Base64 prefixes, at most 192 bytes each.
                </Typography>
                {review.examples.map((e) => (
                  <Typography
                    key={`${e.topic}:${e.partition}:${e.offset}`}
                    component="pre"
                    sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
                  >{`${e.topic}/${e.partition}@${e.offset}\nkey: ${e.key ?? "null"}\nvalue: ${e.value ?? "null (tombstone)"}`}</Typography>
                ))}
                <TextField
                  disabled={busy || attempted}
                  label="Type the exact group ID to confirm"
                  value={confirmation}
                  onChange={(e) => setConfirmation(e.target.value)}
                />
              </>
            )}
            {outcome && (
              <>
                <Alert
                  severity={outcome.partitions.every((p) => p.verified) ? "success" : "warning"}
                >
                  {outcome.detail}
                </Alert>
                <Table size="small" aria-label="Offset reset results">
                  <TableHead>
                    <TableRow>
                      {["Partition", "Requested", "Outcome", "Observed", "Verified"].map((h) => (
                        <TableCell key={h}>{h}</TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {outcome.partitions.map((p) => (
                      <TableRow key={`${p.topic}:${p.partition}`}>
                        <TableCell>
                          {p.topic}/{p.partition}
                        </TableCell>
                        <TableCell>{p.offset}</TableCell>
                        <TableCell>{p.state}</TableCell>
                        <TableCell>{p.observed ?? "Unknown"}</TableCell>
                        <TableCell>{p.verified ? "Yes" : "No"}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </>
            )}
            {!canWrite && (
              <Alert severity="info">Read-only mode permits preview and blocks apply.</Alert>
            )}
            {error && <Alert severity="error">{error}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button
            disabled={busy}
            onClick={() => {
              generation.current++;
              setOpen(false);
            }}
          >
            Close
          </Button>
          <Button
            disabled={busy || !enabled || Object.keys(targets).length === 0}
            onClick={() => {
              void prepare();
            }}
          >
            Preview reset
          </Button>
          <Button
            disabled={
              !canWrite || !ready || busy || attempted || confirmation !== review?.input.groupId
            }
            onClick={() => {
              void apply();
            }}
            variant="contained"
          >
            Apply reviewed reset
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
