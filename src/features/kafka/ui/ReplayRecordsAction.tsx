import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaExploredMessage,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseRecordReplayInput,
  replayConfirmation,
  type RecordReplayReview,
  type RecordReplayOutcome,
} from "../contracts/record-replay";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioCheckbox as Checkbox,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

function encode(value: string): string {
  return btoa(Array.from(new TextEncoder().encode(value), (b) => String.fromCharCode(b)).join(""));
}
export function ReplayRecordsAction({
  host,
  profiles,
  messages,
  selected,
  enabled,
  canWrite,
}: {
  readonly host: StreamSkopeHost;
  readonly profiles: readonly ProfileSummary[];
  readonly messages: readonly KafkaExploredMessage[];
  readonly selected: KafkaExploredMessage | null;
  readonly enabled: boolean;
  readonly canWrite: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false),
    [records, setRecords] = useState<readonly KafkaExploredMessage[]>([]),
    [ids, setIds] = useState<readonly string[]>([]);
  const [profileId, setProfileId] = useState(""),
    [topic, setTopic] = useState(""),
    [partition, setPartition] = useState("0"),
    [rate, setRate] = useState("1");
  const [keyMode, setKeyMode] = useState("keep"),
    [key, setKey] = useState(""),
    [find, setFind] = useState(""),
    [replacement, setReplacement] = useState(""),
    [removeHeaders, setRemoveHeaders] = useState(""),
    [appendHeaders, setAppendHeaders] = useState("[]");
  const [review, setReview] = useState<RecordReplayReview>(),
    [outcome, setOutcome] = useState<RecordReplayOutcome>(),
    [error, setError] = useState<string>();
  const [confirmation, setConfirmation] = useState(""),
    [previewIndex, setPreviewIndex] = useState("0"),
    [busy, setBusy] = useState<"review" | "apply">(),
    [attempted, setAttempted] = useState(false),
    [cancelling, setCancelling] = useState(false);
  const generation = useRef(0),
    plan = useRef<string | undefined>(undefined);
  const cancelPlan = async (id: string): Promise<void> => {
    const response = await host.execute({
      command: "records.replay.cancel",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { planId: id },
    });
    if (!response.ok)
      throw new Error("Cancellation did not complete. Inspect Activity before retrying.");
  };
  useEffect(() => {
    generation.current++;
    setOpen(false);
    setReview(undefined);
    setOutcome(undefined);
    setBusy(undefined);
    setError(undefined);
    return (): void => {
      generation.current++;
      const id = plan.current;
      plan.current = undefined;
      if (id) void cancelPlan(id).catch(() => undefined);
    };
  }, [host, enabled]);
  const clearReview = (): void => {
    const id = plan.current;
    plan.current = undefined;
    if (id)
      void cancelPlan(id).catch(() =>
        setError("Destination cleanup could not be confirmed. Inspect Activity."),
      );
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setAttempted(false);
  };
  const prepare = async (): Promise<void> => {
    const current = generation.current;
    clearReview();
    setBusy("review");
    setError(undefined);
    setPreviewIndex("0");
    try {
      const profile = profiles.find((p) => p.id === profileId);
      if (profileId && !profile) throw new Error("Select a current saved profile.");
      const headers: unknown = JSON.parse(appendHeaders);
      if (!Array.isArray(headers) || headers.length > 16)
        throw new Error("Use an array of at most 16 headers.");
      const appended = headers.map((value: unknown) => {
        if (
          !value ||
          typeof value !== "object" ||
          !("name" in value) ||
          typeof value.name !== "string" ||
          !("value" in value) ||
          (value.value !== null && typeof value.value !== "string")
        )
          throw new Error("Each header needs a name and a string or null value.");
        return {
          key: encode(value.name),
          value: value.value === null ? null : encode(value.value),
        };
      });
      const input = parseRecordReplayInput({
        targetProfile: profile ? { id: profile.id, revision: profile.revision ?? 1 } : null,
        topic,
        partition: Number(partition),
        ratePerSecond: Number(rate),
        records: records
          .filter((r) => ids.includes(r.id))
          .map((r) => ({
            topic: r.topic,
            partition: r.partition,
            offset: r.offset,
            timestampMs:
              Number.isFinite(Date.parse(r.timestamp)) && Date.parse(r.timestamp) >= 0
                ? String(Date.parse(r.timestamp))
                : null,
            original: r.original,
          })),
        transform: {
          key: keyMode === "keep" ? null : { value: keyMode === "null" ? null : encode(key) },
          valueText: find ? { search: find, replacement } : null,
          removeHeaders: removeHeaders
            .split(",")
            .map((h) => h.trim())
            .filter(Boolean)
            .map(encode),
          appendHeaders: appended,
        },
      });
      const response = await host.execute({
        command: "records.replay.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (current !== generation.current) {
        if (response.ok) void cancelPlan(response.result.review.planId).catch(() => undefined);
        return;
      }
      if (response.ok) {
        plan.current = response.result.review.planId;
        setReview(response.result.review);
      } else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError(
          "Check complete source bytes, destination, integer partition/rate and transformation fields. Replay is limited to 50 records, 16 KiB per record and 512 KiB total.",
        );
    } finally {
      if (current === generation.current) setBusy(undefined);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review || busy || attempted) return;
    const current = generation.current;
    setBusy("apply");
    setAttempted(true);
    setError(undefined);
    setCancelling(false);
    try {
      const response = await host.execute({
        command: "records.replay.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (current !== generation.current) return;
      if (response.ok) {
        setOutcome(response.result.outcome);
        plan.current = undefined;
      } else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError(
          "The result did not reach this view. Inspect Activity and the destination before another attempt; replay can create duplicates.",
        );
    } finally {
      if (current === generation.current) setBusy(undefined);
    }
  };
  const close = (): void => {
    if (busy) return;
    generation.current++;
    clearReview();
    setOpen(false);
  };
  const edit =
    (set: (v: string) => void): ((event: React.ChangeEvent<HTMLInputElement>) => void) =>
    (event) => {
      clearReview();
      set(event.target.value);
    };
  return (
    <>
      <Button
        disabled={!enabled || !messages.length}
        size="small"
        variant="outlined"
        onClick={() => {
          clearReview();
          setError(undefined);
          const snapshot = structuredClone(
            selected
              ? [selected, ...messages.filter((m) => m.id !== selected.id)].slice(0, 50)
              : messages.slice(0, 50),
          );
          setRecords(snapshot);
          setIds(snapshot[0]?.original?.state === "complete" ? [snapshot[0].id] : []);
          setTopic(snapshot[0]?.topic ?? "");
          setOpen(true);
        }}
      >
        Replay…
      </Button>
      <Dialog open={open} onClose={close} maxWidth="lg" fullWidth>
        <DialogTitle>Copy or replay selected records</DialogTitle>
        <DialogContent>
          <Stack spacing={2}>
            <Alert severity="warning">
              Replay writes new records and can trigger consumers again. Copying to the same topic
              can create a processing loop. Each new plan can duplicate earlier writes. Source
              offsets are never committed or deleted.
            </Alert>
            <Typography>
              Frozen selection from this view, at most 50 records. Original key, value and ordered
              headers are preserved unless explicitly transformed. Tombstones stay null.
            </Typography>
            <Table size="small" aria-label="Replay source records">
              <TableHead>
                <TableRow>
                  <TableCell>Select</TableCell>
                  <TableCell>Source</TableCell>
                  <TableCell>Value</TableCell>
                  <TableCell>Original bytes</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {records.map((r) => (
                  <TableRow key={r.id}>
                    <TableCell>
                      <Checkbox
                        aria-label={`Replay ${r.topic}/${r.partition}@${r.offset}`}
                        checked={ids.includes(r.id)}
                        disabled={!!busy || r.original?.state !== "complete"}
                        onChange={(_, checked) => {
                          clearReview();
                          setIds((old) =>
                            checked ? [...old, r.id] : old.filter((id) => id !== r.id),
                          );
                        }}
                      />
                    </TableCell>
                    <TableCell>
                      {r.topic}/{r.partition}@{r.offset}
                    </TableCell>
                    <TableCell>
                      {r.original?.state === "complete" && r.original.value === null
                        ? "Tombstone"
                        : r.preview.slice(0, 80)}
                    </TableCell>
                    <TableCell>
                      {r.original?.state === "complete"
                        ? "Complete"
                        : "Unavailable — cannot replay"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
              <TextField
                select
                label="Destination profile"
                value={profileId}
                disabled={!!busy}
                onChange={edit(setProfileId)}
                sx={{ minWidth: 210 }}
              >
                <MenuItem value="">Active connection</MenuItem>
                {profiles.map((p) => (
                  <MenuItem key={p.id} value={p.id}>
                    {p.name}
                  </MenuItem>
                ))}
              </TextField>
              <TextField
                label="Destination topic"
                value={topic}
                disabled={!!busy}
                onChange={edit(setTopic)}
              />
              <TextField
                label="Destination partition"
                value={partition}
                disabled={!!busy}
                onChange={edit(setPartition)}
              />
              <TextField
                label="Records per second (1–10)"
                value={rate}
                disabled={!!busy}
                onChange={edit(setRate)}
              />
            </Stack>
            <Typography variant="body2">
              Saved destinations open separately; your current reader stays connected. Prepare any
              managed capture beforehand. Replay uses existing topics and does not install or resume
              plugins.
            </Typography>
            <Typography variant="subtitle2">Optional transformations</Typography>
            <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
              <TextField
                select
                label="Key transformation"
                value={keyMode}
                disabled={!!busy}
                onChange={edit(setKeyMode)}
                sx={{ minWidth: 180 }}
              >
                <MenuItem value="keep">Keep original</MenuItem>
                <MenuItem value="null">Set null</MenuItem>
                <MenuItem value="text">Replace with UTF-8</MenuItem>
              </TextField>
              <TextField
                label="Replacement key"
                value={key}
                disabled={!!busy || keyMode !== "text"}
                onChange={edit(setKey)}
              />
            </Stack>
            <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
              <TextField
                label="Find literal UTF-8 value text"
                value={find}
                disabled={!!busy}
                onChange={edit(setFind)}
              />
              <TextField
                label="Replace value text with"
                value={replacement}
                disabled={!!busy || !find}
                onChange={edit(setReplacement)}
              />
            </Stack>
            <Typography variant="body2">
              Literal replacement affects all matches in UTF-8 values; binary values reject this
              transformation. It does not validate schemas or translate Schema Registry IDs between
              clusters. Verify the exact output below.
            </Typography>
            <TextField
              label="Remove header names (comma-separated)"
              value={removeHeaders}
              disabled={!!busy}
              onChange={edit(setRemoveHeaders)}
            />
            <TextField
              label="Append headers (JSON name/value array)"
              value={appendHeaders}
              disabled={!!busy}
              onChange={edit(setAppendHeaders)}
              multiline
              minRows={2}
            />
            {review && (
              <>
                <Typography>
                  From {review.sourceName} → {review.targetName} / {review.input.topic} / partition{" "}
                  {review.input.partition}. {review.batch.records.length} records at{" "}
                  {review.batch.ratePerSecond}/second. Expires {review.expiresAt}.
                </Typography>
                <Typography variant="body2">
                  Destination cluster {review.destination.clusterId}; topic identity{" "}
                  {review.destination.topicId}. Original timestamps are requested where known;
                  broker LogAppendTime policy can replace them. A new offset is always assigned.
                </Typography>
                <TextField
                  select
                  label="Inspect replay record"
                  value={previewIndex}
                  onChange={(e) => setPreviewIndex(e.target.value)}
                >
                  {review.input.records.map((r, i) => (
                    <MenuItem key={i} value={String(i)}>
                      {r.topic}/{r.partition}@{r.offset}
                    </MenuItem>
                  ))}
                </TextField>
                <Typography variant="subtitle2">Before (original Base64 bytes)</Typography>
                <StudioCodeBlock sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                  {JSON.stringify(review.input.records[Number(previewIndex)]?.original, null, 2)}
                </StudioCodeBlock>
                <Typography variant="subtitle2">After (exact reviewed Base64 bytes)</Typography>
                <StudioCodeBlock sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                  {JSON.stringify(review.batch.records[Number(previewIndex)], null, 2)}
                </StudioCodeBlock>
                <TextField
                  label={`Type ${replayConfirmation(review)} to confirm`}
                  value={confirmation}
                  disabled={!!busy || attempted}
                  onChange={(e) => setConfirmation(e.target.value)}
                />
              </>
            )}
            {outcome && (
              <>
                <Alert severity={outcome.stopReason === "complete" ? "success" : "warning"}>
                  {outcome.outcomes.filter((o) => o.state === "acknowledged").length} acknowledged;{" "}
                  {outcome.outcomes.filter((o) => o.state === "unknown").length} unknown;{" "}
                  {outcome.outcomes.filter((o) => o.state === "rejected").length} rejected;{" "}
                  {outcome.unsent} unsent. Stopped: {outcome.stopReason}. Cleanup: {outcome.cleanup}
                  .
                </Alert>
                <Table size="small" aria-label="Replay outcomes">
                  <TableHead>
                    <TableRow>
                      <TableCell>Source</TableCell>
                      <TableCell>Outcome</TableCell>
                      <TableCell>Destination receipt</TableCell>
                      <TableCell>Byte read-back</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {review?.input.records.map((r, i) => {
                      const result = outcome.outcomes[i];
                      return (
                        <TableRow key={i}>
                          <TableCell>
                            {r.topic}/{r.partition}@{r.offset}
                          </TableCell>
                          <TableCell>{result?.state ?? "unsent"}</TableCell>
                          <TableCell>
                            {result?.receipt
                              ? `${result.receipt.topic}/${result.receipt.partition}@${result.receipt.offset}`
                              : "None"}
                          </TableCell>
                          <TableCell>{result?.verification ?? "not-applicable"}</TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </>
            )}
            {!canWrite && <Alert severity="info">Read-only mode blocks replay publication.</Alert>}
            {error && <Alert severity="error">{error}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={!!busy} onClick={close}>
            Close
          </Button>
          {busy === "apply" ? (
            <Button
              disabled={cancelling}
              onClick={() => {
                const id = plan.current;
                if (!id) return;
                setCancelling(true);
                void cancelPlan(id).catch(() => {
                  setError(
                    "Cancellation could not be confirmed. Inspect Activity and the destination.",
                  );
                  setCancelling(false);
                });
              }}
            >
              {cancelling ? "Cancelling…" : "Cancel replay"}
            </Button>
          ) : (
            <>
              <Button
                disabled={!!busy || !ids.length || !enabled}
                onClick={() => {
                  void prepare();
                }}
              >
                Preview replay
              </Button>
              <Button
                disabled={
                  !!busy ||
                  !canWrite ||
                  !review ||
                  attempted ||
                  confirmation !== replayConfirmation(review)
                }
                onClick={() => {
                  void apply();
                }}
                variant="contained"
              >
                Apply reviewed replay
              </Button>
            </>
          )}
        </DialogActions>
      </Dialog>
    </>
  );
}
