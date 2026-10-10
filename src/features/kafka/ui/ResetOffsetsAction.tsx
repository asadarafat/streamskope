import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableBody, TableCell, TableHead, TableRow } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaConsumerGroupDetails,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseOffsetResetRequest,
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
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioSelect as Select,
  StudioMenuItem as MenuItem,
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
  const [position, setPosition] = useState<"explicit" | "earliest" | "latest" | "timestamp">(
    "explicit",
  );
  const [timestamp, setTimestamp] = useState("");
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
    setPosition("explicit");
    setTimestamp("");
    setConfirmation("");
    setReview(undefined);
    setOutcome(undefined);
    setBusy(false);
    setAttempted(false);
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [group?.id, enabled, host, canWrite]);
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
      if (
        position === "timestamp" &&
        (!/Z$/iu.test(timestamp) ||
          !Number.isSafeInteger(Date.parse(timestamp)) ||
          Date.parse(timestamp) < 0)
      )
        throw new Error("Enter a nonnegative UTC timestamp ending in Z.");
      const selected = rows.filter((r) => targets[`${r.topic}:${r.partition}`] !== undefined);
      const input = parseOffsetResetRequest(
        position === "explicit"
          ? {
              groupId: group.id,
              targets: selected.map((r) => ({
                topic: r.topic,
                partition: r.partition,
                offset: targets[`${r.topic}:${r.partition}`],
              })),
            }
          : {
              groupId: group.id,
              partitions: selected.map((r) => ({ topic: r.topic, partition: r.partition })),
              position:
                position === "timestamp"
                  ? { kind: position, timestampMs: String(Date.parse(timestamp)) }
                  : { kind: position },
            },
      );
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
        setError(
          "Select partitions and valid offsets or an ISO 8601 UTC time ending in Z, then preview again.",
        );
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review || busy || attempted || !canWrite || confirmation !== review.input.groupId) return;
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
    generation.current++;
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setAttempted(false);
    setError(undefined);
  };
  return (
    <>
      <Button
        disabled={!enabled || !group || !rows.length}
        onClick={() => {
          clearReview();
          setOpen(true);
        }}
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
            <FormControl fullWidth size="small">
              <InputLabel id="reset-position">Reset position</InputLabel>
              <Select
                labelId="reset-position"
                label="Reset position"
                disabled={busy || attempted}
                value={position}
                onChange={(e) => {
                  clearReview();
                  setPosition(e.target.value);
                }}
              >
                <MenuItem value="explicit">Explicit offsets</MenuItem>
                <MenuItem value="earliest">Earliest retained</MenuItem>
                <MenuItem value="latest">Current end</MenuItem>
                <MenuItem value="timestamp">At or after UTC time</MenuItem>
              </Select>
            </FormControl>
            {position === "timestamp" && (
              <TextField
                disabled={busy || attempted}
                label="UTC time (ISO 8601)"
                helperText="Include Z, for example 2026-10-10T14:00:00Z. No matching retained record requires another preview."
                value={timestamp}
                onChange={(e) => {
                  clearReview();
                  setTimestamp(e.target.value);
                }}
              />
            )}
            {position === "latest" && (
              <Alert severity="warning">
                Moving to the current end skips all retained records before the reviewed end
                offsets. New records may arrive after this preview.
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
                          disabled={busy || attempted}
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
                          disabled={
                            busy ||
                            attempted ||
                            position !== "explicit" ||
                            targets[key] === undefined
                          }
                          label={`Next offset ${key}`}
                          value={
                            position === "explicit"
                              ? (targets[key] ?? "")
                              : (review?.input.targets.find(
                                  (target) =>
                                    target.topic === r.topic && target.partition === r.partition,
                                )?.offset ?? "")
                          }
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
                  Examples: {review.exampleStatus}. Up to three decoded, protected records from the
                  proposed positions; key/value text is limited to 512 characters each and uses the
                  same decoding and masking pipeline as the message grid.
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
                  severity={
                    outcome.partitions.every((p) => p.verified && p.cleanup === "confirmed")
                      ? "success"
                      : "warning"
                  }
                >
                  {outcome.detail}
                </Alert>
                <Table size="small" aria-label="Offset reset results">
                  <TableHead>
                    <TableRow>
                      {["Partition", "Requested", "Outcome", "Observed", "Verified", "Cleanup"].map(
                        (h) => (
                          <TableCell key={h}>{h}</TableCell>
                        ),
                      )}
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
                        <TableCell>{p.cleanup}</TableCell>
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
            disabled={busy || attempted || !enabled || Object.keys(targets).length === 0}
            onClick={() => {
              void prepare();
            }}
          >
            Preview reset
          </Button>
          <Button
            disabled={
              !canWrite ||
              !ready ||
              busy ||
              attempted ||
              confirmation !== review?.input.groupId ||
              (review !== undefined && Date.now() >= Date.parse(review.expiresAt))
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
