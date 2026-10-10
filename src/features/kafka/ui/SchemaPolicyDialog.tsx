import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import {
  SCHEMA_COMPATIBILITY_LEVELS,
  type SchemaCompatibilityLevel,
} from "../contracts/schema-changes";
import type {
  SchemaPolicyBaseline,
  SchemaPolicyReview,
  SchemaPolicyOutcome,
} from "../contracts/schema-policy";
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

export function SchemaPolicyDialog({
  host,
  writer,
  enabled,
  onClose,
}: {
  readonly host: StreamSkopeHost;
  readonly writer: SchemaVersionDetail;
  readonly enabled: boolean;
  readonly onClose: () => void;
}): React.JSX.Element {
  const [baseline, setBaseline] = useState<SchemaPolicyBaseline>();
  const [choice, setChoice] = useState<SchemaCompatibilityLevel | "inherit">("inherit");
  const [review, setReview] = useState<SchemaPolicyReview>();
  const [outcome, setOutcome] = useState<SchemaPolicyOutcome>();
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false),
    [error, setError] = useState<string>();
  const revision = useRef(0);
  const read = async (initial = false): Promise<void> => {
    const submitted = ++revision.current;
    setBusy(true);
    setError(undefined);
    setReview(undefined);
    setConfirmation("");
    try {
      const response = await host.execute({
        command: "schemas.policy.load",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { subject: writer.subject },
      });
      if (submitted !== revision.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (response.command !== "schemas.policy.load")
        throw new Error("The host returned an unexpected policy read.");
      setBaseline(response.result.baseline);
      if (initial) setChoice(response.result.baseline.policy.subjectLevel ?? "inherit");
    } catch (failure) {
      if (submitted === revision.current)
        setError(failure instanceof Error ? failure.message : "The policy could not be read.");
    } finally {
      if (submitted === revision.current) setBusy(false);
    }
  };
  useEffect(() => {
    revision.current++;
    setReview(undefined);
    setConfirmation("");
    setBusy(false);
    setBaseline(undefined);
    if (enabled) void read(true);
    return (): void => {
      revision.current++;
    };
  }, [enabled, host, writer.subject]);
  const prepare = async (): Promise<void> => {
    const submitted = ++revision.current;
    setBusy(true);
    setError(undefined);
    setReview(undefined);
    setConfirmation("");
    try {
      const response = await host.execute({
        command: "schemas.policy.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {
          subject: writer.subject,
          expectedWriter: { id: writer.id, version: writer.version },
          change: choice === "inherit" ? { mode: "inherit" } : { mode: "set", level: choice },
        },
      });
      if (submitted !== revision.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (response.command !== "schemas.policy.review")
        throw new Error("The host returned an unexpected policy review.");
      setReview(response.result.review);
      setBaseline(response.result.review.before);
    } catch (failure) {
      if (submitted === revision.current)
        setError(failure instanceof Error ? failure.message : "The policy could not be reviewed.");
    } finally {
      if (submitted === revision.current) setBusy(false);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review) return;
    const submitted = ++revision.current;
    setBusy(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "schemas.policy.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (submitted !== revision.current) return;
      setReview(undefined);
      setConfirmation("");
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (response.command !== "schemas.policy.apply")
        throw new Error("The host returned an unexpected policy receipt.");
      setOutcome(response.result.outcome);
      const observed = response.result.outcome.observed;
      if (observed)
        setBaseline((current) => (current ? { ...current, policy: observed } : current));
    } catch (failure) {
      if (submitted === revision.current) {
        setReview(undefined);
        setError(
          failure instanceof Error
            ? failure.message
            : "No policy acknowledgement was received. Read current policy before another attempt.",
        );
      }
    } finally {
      if (submitted === revision.current) setBusy(false);
    }
  };
  const current = review?.before.policy ?? baseline?.policy,
    after = review?.after;
  const selectedCurrent =
    baseline?.writer.id === writer.id && baseline.writer.version === writer.version;
  const locked = busy || !enabled || outcome !== undefined;
  return (
    <Dialog open fullWidth maxWidth="sm" onClose={() => !busy && onClose()}>
      <DialogTitle>Compatibility policy — {writer.subject}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2} sx={{ pt: 0.5 }}>
          <Typography color="text.secondary">
            Choose a subject override or inherit the global default. This affects compatibility
            checks for future registrations; it does not validate existing records or schemas.
          </Typography>
          {busy ? <Typography role="status">Waiting for Registry…</Typography> : null}
          {current ? (
            <Table size="small" aria-label="Compatibility policy comparison">
              <TableHead>
                <TableRow>
                  <TableCell>Scope</TableCell>
                  <TableCell>{review ? "Reviewed before" : "Last read"}</TableCell>
                  {after ? <TableCell>After</TableCell> : null}
                </TableRow>
              </TableHead>
              <TableBody>
                <TableRow>
                  <TableCell>Global default</TableCell>
                  <TableCell>{current.globalLevel}</TableCell>
                  {after ? <TableCell>{after.globalLevel}</TableCell> : null}
                </TableRow>
                <TableRow>
                  <TableCell>Subject override</TableCell>
                  <TableCell>{current.subjectLevel ?? "Inherited"}</TableCell>
                  {after ? <TableCell>{after.subjectLevel ?? "Inherited"}</TableCell> : null}
                </TableRow>
                <TableRow>
                  <TableCell>Effective level</TableCell>
                  <TableCell>{current.effectiveLevel}</TableCell>
                  {after ? <TableCell>{after.effectiveLevel}</TableCell> : null}
                </TableRow>
              </TableBody>
            </Table>
          ) : null}
          <FormControl fullWidth disabled={locked || !baseline}>
            <InputLabel id="subject-policy-choice">Subject policy</InputLabel>
            <Select
              labelId="subject-policy-choice"
              label="Subject policy"
              value={choice}
              onChange={(event) => {
                revision.current++;
                setReview(undefined);
                setConfirmation("");
                setError(undefined);
                setChoice(event.target.value);
              }}
            >
              <MenuItem value="inherit">Inherit global default</MenuItem>
              {SCHEMA_COMPATIBILITY_LEVELS.map((level) => (
                <MenuItem key={level} value={level}>
                  {level}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          {choice === "NONE" || after?.effectiveLevel === "NONE" ? (
            <Alert severity="warning">
              NONE disables compatibility enforcement for future registrations.
            </Alert>
          ) : null}
          {choice === "inherit" ? (
            <Typography color="text.secondary">
              Inheritance removes only the subject's basic compatibility override. Future changes to
              the global default will affect this subject. Global configuration remains read-only
              here.
            </Typography>
          ) : null}
          {baseline && !selectedCurrent ? (
            <Alert severity="warning">
              The selected writer is no longer latest. Close, refresh and select the latest version
              before reviewing policy.
            </Alert>
          ) : null}
          {!enabled ? (
            <Alert severity="warning">
              The connection or selection changed. Close and reopen policy on the current subject.
            </Alert>
          ) : null}
          {error ? <Alert severity="error">{error}</Alert> : null}
          {review ? (
            <>
              <Typography>
                Review on {review.connectionName}; expires {review.expiresAt}.
              </Typography>
              <Alert severity="info">
                Writer and basic configuration are rechecked before dispatch. External clients can
                still race the policy write.
              </Alert>
              <TextField
                label={`Type ${writer.subject} to confirm policy change`}
                value={confirmation}
                disabled={locked}
                fullWidth
                onChange={(event) => setConfirmation(event.target.value)}
              />
            </>
          ) : null}
          {outcome ? (
            <Alert severity={outcome.verification === "verified" ? "success" : "warning"}>
              <strong>
                {outcome.state} · {outcome.verification}
              </strong>
              <br />
              {outcome.detail}
              {outcome.observed ? (
                <>
                  <br />
                  Read back: {outcome.observed.effectiveLevel} (
                  {outcome.observed.subjectLevel === null ? "global default" : "subject override"}).
                </>
              ) : null}
            </Alert>
          ) : null}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={busy} onClick={onClose}>
          Close
        </Button>
        {outcome ? (
          <Button disabled={busy || !enabled} onClick={() => void read()}>
            Read current policy
          </Button>
        ) : (
          <>
            <Button
              disabled={locked || !selectedCurrent}
              onClick={() => void prepare()}
              variant="outlined"
            >
              Review policy change
            </Button>
            <Button
              disabled={
                locked ||
                !review ||
                confirmation !== writer.subject ||
                Date.now() >= Date.parse(review.expiresAt)
              }
              onClick={() => void apply()}
              variant="contained"
            >
              Apply reviewed policy
            </Button>
          </>
        )}
      </DialogActions>
    </Dialog>
  );
}
