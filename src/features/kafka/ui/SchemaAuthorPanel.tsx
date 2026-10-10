import { useEffect, useMemo, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseSchemaAuthoringInput,
  type SchemaAuthoringResult,
} from "../contracts/schema-authoring";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

import { SchemaRecordPublication } from "./SchemaRecordPublication";

export function SchemaAuthorPanel({
  schema,
  host,
  enabled,
}: {
  readonly schema: SchemaVersionDetail;
  readonly host: StreamSkopeHost;
  readonly enabled: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [payload, setPayload] = useState("{}");
  const [messageType, setMessageType] = useState("");
  const [result, setResult] = useState<SchemaAuthoringResult>();
  const [error, setError] = useState<string>();
  const [validating, setValidating] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const revision = useRef(0);
  const records = useMemo(
    () => (result?.state === "valid" ? [result.record] : undefined),
    [result],
  );
  useEffect(() => {
    revision.current++;
    setResult(undefined);
    setError(undefined);
    setValidating(false);
    return (): void => {
      revision.current++;
    };
  }, [host, schema, enabled]);
  const edited = (): void => {
    revision.current++;
    setResult(undefined);
    setError(undefined);
  };
  const validate = async (): Promise<void> => {
    const request = ++revision.current;
    setValidating(true);
    setResult(undefined);
    setError(undefined);
    try {
      const input = parseSchemaAuthoringInput({
        subject: schema.subject,
        version: schema.version,
        schemaId: schema.id,
        messageType,
        payload,
      });
      const response = await host.execute({
        command: "schemas.author",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (request !== revision.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setResult(response.result.authoring);
    } catch (failure) {
      if (request === revision.current)
        setError(
          failure instanceof Error
            ? failure.message
            : "Authoring validation could not reach the host.",
        );
    } finally {
      if (request === revision.current) setValidating(false);
    }
  };
  const sample = async (): Promise<void> => {
    const request = ++revision.current;
    setValidating(true);
    setResult(undefined);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "schemas.samples",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {
          subject: schema.subject,
          version: schema.version,
          messageType,
          seed: 1,
          count: 1,
        },
      });
      if (request !== revision.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setPayload(response.result.samples.samples[0]!.json);
    } catch (failure) {
      if (request === revision.current)
        setError(
          failure instanceof Error
            ? failure.message
            : "Sample generation failed. You can still write and validate a payload manually.",
        );
    } finally {
      if (request === revision.current) setValidating(false);
    }
  };
  const locked = validating || publishing;
  return (
    <>
      <Button variant="outlined" disabled={!enabled} onClick={() => setOpen(true)}>
        Author record
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
          Author record — {schema.subject}@{schema.version}
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography>
              Edit a value using this exact registered writer schema. Validation writes nothing.
              Review the encoded projection before choosing and confirming a Kafka destination.
            </Typography>
            <Alert severity="info">
              {schema.schemaType}: {schema.subject}@{schema.version}, schema ID {schema.id}. Keys
              are null and headers empty. JSON null is an encoded value, not a Kafka tombstone.
            </Alert>
            {schema.schemaType === "PROTOBUF" ? (
              <TextField
                label="Protobuf message type"
                value={messageType}
                disabled={locked}
                helperText="Fully qualified type; empty selects the first writer message."
                onChange={(event) => {
                  setMessageType(event.target.value);
                  edited();
                }}
              />
            ) : null}
            <TextField
              label="Record payload JSON"
              multiline
              minRows={8}
              maxRows={16}
              value={payload}
              disabled={locked || !enabled}
              helperText="At most 16 KiB UTF-8. Use decimal strings for 64-bit integers; Avro uses named union branches and byte strings, Protobuf uses Base64 bytes and enum names."
              onChange={(event) => {
                setPayload(event.target.value);
                edited();
              }}
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
              <Button
                disabled={!enabled || locked}
                onClick={() => {
                  void sample();
                }}
              >
                Start from one sample
              </Button>
              <Button
                variant="contained"
                disabled={!enabled || locked}
                onClick={() => {
                  void validate();
                }}
              >
                {validating ? "Validating…" : "Validate payload"}
              </Button>
            </Stack>
            {result?.state === "invalid" ? (
              <Alert severity="error">
                <Stack>
                  {result.issues.map((issue, index) => (
                    <Typography key={index}>
                      {issue.path || "Payload"}: {issue.detail}
                    </Typography>
                  ))}
                </Stack>
              </Alert>
            ) : null}
            {result?.state === "valid" && records ? (
              <>
                <Alert severity="success">
                  Validated against {result.writer.subject}@{result.writer.version}, schema ID{" "}
                  {result.writer.id}. {result.encoding}
                  {result.messageType ? `; message ${result.messageType}` : ""}.
                </Alert>
                <Typography variant="subtitle2">Encoded projection</Typography>
                <StudioCodeBlock
                  aria-label="Validated record projection"
                  sx={{ m: 0, p: 2, maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap" }}
                >
                  {result.json}
                </StudioCodeBlock>
                <SchemaRecordPublication
                  host={host}
                  records={records}
                  enabled={enabled}
                  onBusyChange={setPublishing}
                />
              </>
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
