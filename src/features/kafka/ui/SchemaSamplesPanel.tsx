import { useEffect, useMemo, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import { parseSchemaSampleInput, type SchemaSamples } from "../contracts/schema-samples";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

import { SchemaRecordPublication } from "./SchemaRecordPublication";

export function SchemaSamplesPanel({
  schema,
  host,
  enabled,
}: {
  readonly schema: SchemaVersionDetail;
  readonly host: StreamSkopeHost;
  readonly enabled: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [seed, setSeed] = useState("1");
  const [count, setCount] = useState("5");
  const [messageType, setMessageType] = useState("");
  const [samples, setSamples] = useState<SchemaSamples>();
  const [selected, setSelected] = useState(0);
  const [error, setError] = useState<string>();
  const [generating, setGenerating] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const generation = useRef(0);
  const records = useMemo(() => samples?.samples.map((sample) => sample.record), [samples]);
  useEffect(() => {
    generation.current++;
    setSamples(undefined);
    setGenerating(false);
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [schema, enabled, host]);
  const reset = (): void => {
    setSamples(undefined);
    setError(undefined);
  };
  const generate = async (): Promise<void> => {
    const request = ++generation.current;
    setGenerating(true);
    reset();
    try {
      const payload = parseSchemaSampleInput({
        subject: schema.subject,
        version: schema.version,
        seed: Number(seed),
        count: Number(count),
        messageType,
      });
      const response = await host.execute({
        command: "schemas.samples",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload,
      });
      if (request !== generation.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setSamples(response.result.samples);
      setSelected(0);
    } catch (failure) {
      if (request === generation.current)
        setError(failure instanceof Error ? failure.message : "Generation failed.");
    } finally {
      if (request === generation.current) setGenerating(false);
    }
  };
  const locked = generating || publishing;
  return (
    <>
      <Button variant="outlined" disabled={!enabled} onClick={() => setOpen(true)}>
        Generate samples
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
          Schema samples — {schema.subject}@{schema.version}
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography>
              Generate a reproducible preview from this exact schema. Preview writes nothing.
              Publishing requires a separate destination review and confirmation.
            </Typography>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Seed"
                type="number"
                value={seed}
                disabled={locked}
                onChange={(event) => {
                  setSeed(event.target.value);
                  reset();
                }}
              />
              <TextField
                label="Sample count"
                type="number"
                value={count}
                helperText="1–50"
                disabled={locked}
                onChange={(event) => {
                  setCount(event.target.value);
                  reset();
                }}
              />
            </Stack>
            {schema.schemaType === "PROTOBUF" ? (
              <TextField
                label="Protobuf message type"
                value={messageType}
                helperText="Fully qualified type; empty selects the first writer message."
                disabled={locked}
                onChange={(event) => {
                  setMessageType(event.target.value);
                  reset();
                }}
              />
            ) : null}
            <Button
              disabled={!enabled || locked}
              onClick={() => {
                void generate();
              }}
            >
              {generating ? "Generating…" : "Generate preview"}
            </Button>
            {samples && records ? (
              <>
                <Alert severity="info">
                  {samples.encoding}. Seed {samples.seed}; {samples.samples.length} valid samples.
                  Original record keys are null and headers empty.
                </Alert>
                <TextField
                  select
                  label="Preview sample"
                  value={selected}
                  onChange={(event) => setSelected(Number(event.target.value))}
                >
                  {samples.samples.map((_sample, index) => (
                    <MenuItem key={index} value={index}>
                      Sample {index + 1}
                    </MenuItem>
                  ))}
                </TextField>
                <StudioCodeBlock
                  aria-label="Generated sample"
                  sx={{ m: 0, p: 2, maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap" }}
                >
                  {samples.samples[selected]?.json}
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
