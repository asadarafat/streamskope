import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  type SchemaRegistryType,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseSchemaChangeInput,
  type SchemaChangeReview,
  type SchemaChangeOutcome,
} from "../contracts/schema-changes";
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

import { DocumentDiff } from "./DocumentDiff";

export function SchemaEvolutionDialog({
  host,
  initial,
  enabled,
  onClose,
  onRegistered,
}: {
  readonly host: StreamSkopeHost;
  readonly initial: SchemaVersionDetail | null;
  readonly enabled: boolean;
  readonly onClose: () => void;
  readonly onRegistered: (subject: string) => void;
}): React.JSX.Element {
  const [subject, setSubject] = useState(initial?.subject ?? "");
  const [schemaType, setSchemaType] = useState<SchemaRegistryType>(initial?.schemaType ?? "AVRO");
  const [schema, setSchema] = useState(initial?.schema ?? "");
  const [references, setReferences] = useState(JSON.stringify(initial?.references ?? [], null, 2));
  const [review, setReview] = useState<SchemaChangeReview>();
  const [outcome, setOutcome] = useState<SchemaChangeOutcome>();
  const [confirmation, setConfirmation] = useState("");
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const revision = useRef(0);
  useEffect(
    () => (): void => {
      revision.current++;
    },
    [],
  );
  useEffect(() => {
    revision.current++;
    setReview(undefined);
    setConfirmation("");
    setBusy(false);
  }, [enabled, host]);
  const edit = (run: () => void): void => {
    revision.current++;
    setReview(undefined);
    setConfirmation("");
    setError(undefined);
    run();
  };
  const prepare = async (): Promise<void> => {
    const submitted = ++revision.current;
    setBusy(true);
    setError(undefined);
    setReview(undefined);
    setConfirmation("");
    try {
      const input = parseSchemaChangeInput({
        draft: {
          subject: subject.trim(),
          schemaType,
          schema,
          references: JSON.parse(references) as unknown,
          normalize: true,
          version: "latest",
        },
        expectedWriter: initial === null ? null : { id: initial.id, version: initial.version },
      });
      const response = await host.execute({
        command: "schemas.change.review",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (submitted !== revision.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (response.command !== "schemas.change.review")
        throw new Error("The host returned an unexpected review.");
      setReview(response.result.review);
    } catch (failure) {
      if (submitted === revision.current)
        setError(failure instanceof Error ? failure.message : "The draft could not be reviewed.");
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
      const response = await host.execute(
        parseHostCommand({
          command: "schemas.change.apply",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { planId: review.planId, confirmation },
        }),
      );
      if (submitted !== revision.current) return;
      setReview(undefined);
      setConfirmation("");
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (response.command !== "schemas.change.apply")
        throw new Error("The host returned an unexpected registration result.");
      setOutcome(response.result.outcome);
    } catch (failure) {
      if (submitted === revision.current) {
        setReview(undefined);
        setError(
          failure instanceof Error
            ? failure.message
            : "No registration acknowledgement was received. Inspect subject versions before another attempt.",
        );
      }
    } finally {
      if (submitted === revision.current) setBusy(false);
    }
  };
  const locked = busy || !enabled || outcome !== undefined;
  return (
    <Dialog open fullWidth maxWidth="md" onClose={() => !busy && onClose()}>
      <DialogTitle>
        {initial === null
          ? "Create schema subject"
          : `Evolve schema — ${initial.subject}@${String(initial.version)}`}
      </DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2} sx={{ pt: 0.5 }}>
          <Typography color="text.secondary">
            Edit the definition, review its difference and current Registry policy, then confirm
            registration. Registration does not publish records.
          </Typography>
          <TextField
            label="Subject"
            value={subject}
            disabled={locked || initial !== null}
            fullWidth
            onChange={(event) => edit(() => setSubject(event.target.value))}
          />
          <FormControl fullWidth disabled={locked}>
            <InputLabel id="evolution-type">Schema type</InputLabel>
            <Select
              labelId="evolution-type"
              label="Schema type"
              value={schemaType}
              onChange={(event) => edit(() => setSchemaType(event.target.value))}
            >
              {["AVRO", "JSON", "PROTOBUF"].map((type) => (
                <MenuItem key={type} value={type}>
                  {type}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            fullWidth
            multiline
            minRows={10}
            label="Proposed schema"
            value={schema}
            disabled={locked}
            onChange={(event) => edit(() => setSchema(event.target.value))}
          />
          <TextField
            fullWidth
            multiline
            minRows={2}
            label="Pinned references"
            helperText="JSON array of name, subject and exact version; 128 KiB UTF-8 total draft limit."
            value={references}
            disabled={locked}
            onChange={(event) => edit(() => setReferences(event.target.value))}
          />
          {error ? <Alert severity="error">{error}</Alert> : null}
          {!enabled ? (
            <Alert severity="warning">
              The connection or schema selection changed. Close and reopen this draft using the
              current latest writer.
            </Alert>
          ) : null}
          {review ? (
            <>
              <Typography component="h3" variant="subtitle2">
                Review on {review.connectionName}
              </Typography>
              <Typography>
                Policy: {review.policy.effectiveLevel} (
                {review.policy.subjectLevel === null ? "global default" : "subject override"}).
                Review expires {review.expiresAt}.
              </Typography>
              <Alert
                severity={
                  review.compatible && review.policy.effectiveLevel !== "NONE"
                    ? "success"
                    : "warning"
                }
              >
                {review.before === null
                  ? "New subject: no existing writer is available for comparison. Schema validity is checked by the Registry during registration."
                  : review.policy.effectiveLevel === "NONE"
                    ? "Compatibility enforcement is disabled (NONE). The Registry still validates schema syntax during registration."
                    : review.compatible
                      ? "Registry compatibility passed for this draft under the current policy."
                      : "Registry compatibility failed. Revise the draft before registration."}
              </Alert>
              {review.before ? (
                <>
                  <Typography>
                    Before: {review.before.subject}@{review.before.version}, ID {review.before.id}
                  </Typography>
                  <DocumentDiff
                    before={review.before.schema}
                    after={review.input.draft.schema}
                    mode={
                      review.before.schemaType === "PROTOBUF" ||
                      review.input.draft.schemaType === "PROTOBUF"
                        ? "text"
                        : "json"
                    }
                  />
                  <Typography component="h4" variant="subtitle2">
                    Schema type and references
                  </Typography>
                  <DocumentDiff
                    before={JSON.stringify({
                      schemaType: review.before.schemaType,
                      references: review.before.references,
                    })}
                    after={JSON.stringify({
                      schemaType: review.input.draft.schemaType,
                      references: review.input.draft.references,
                    })}
                    mode="json"
                  />
                </>
              ) : null}
              <Alert severity="info">
                Writer, history, references and policy are rechecked before registration. External
                clients can still race the Registry write.
              </Alert>
              <TextField
                fullWidth
                label={`Type ${review.input.draft.subject} to confirm registration`}
                value={confirmation}
                disabled={busy}
                onChange={(event) => setConfirmation(event.target.value)}
              />
            </>
          ) : null}
          {outcome ? (
            <Alert
              severity={
                outcome.state === "acknowledged" && outcome.verification === "verified"
                  ? "success"
                  : "warning"
              }
              role="status"
            >
              {outcome.detail}{" "}
              {outcome.id === null ? "" : `Acknowledged writer ID: ${String(outcome.id)}.`}
            </Alert>
          ) : null}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={busy} onClick={onClose}>
          Close
        </Button>
        {outcome ? (
          <Button
            disabled={busy || !enabled}
            variant="outlined"
            onClick={() => onRegistered(subject.trim())}
          >
            Refresh subject
          </Button>
        ) : (
          <>
            <Button
              disabled={locked || !subject.trim() || !schema}
              variant="outlined"
              onClick={() => void prepare()}
            >
              Review schema change
            </Button>
            <Button
              disabled={
                locked ||
                !review?.compatible ||
                confirmation !== review.input.draft.subject ||
                Date.now() >= Date.parse(review.expiresAt)
              }
              variant="contained"
              onClick={() => void apply()}
            >
              Register reviewed schema
            </Button>
          </>
        )}
      </DialogActions>
    </Dialog>
  );
}
