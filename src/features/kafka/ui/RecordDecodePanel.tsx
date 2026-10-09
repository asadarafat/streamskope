import { useState } from "react";
import { Stack, Typography } from "@mui/material";

import type { StructuredRecord } from "../contracts/structured-record";
import {
  StudioAlert as Alert,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

import { formatRecordJson, recordCodecLabels } from "./record-presentation";

export function RecordDecodePanel({
  structured,
}: {
  readonly structured: StructuredRecord | undefined;
}): React.JSX.Element {
  const [part, setPart] = useState<"key" | "value">("value");
  const field = structured?.[part];
  return (
    <Stack spacing={1.5} sx={{ p: 1.5 }} aria-label="Decoded record">
      <Typography variant="body2">
        This is the same protected projection used by the grid, filters, comparison, tracing and
        export. Change the saved key or value encoding in Workbench Preferences → Records, then read
        again. Original bytes stay unchanged.
      </Typography>
      <TextField
        select
        label="Record part"
        value={part}
        onChange={(event) => setPart(event.target.value as "key" | "value")}
      >
        <MenuItem value="value">Value</MenuItem>
        <MenuItem value="key">Key</MenuItem>
      </TextField>
      {field === undefined ? (
        <Alert severity="info">
          Structured evidence was not captured for this record. Read it again to use the saved
          encoding settings.
        </Alert>
      ) : (
        <>
          <Typography variant="caption">Encoding: {recordCodecLabels[field.codec]}</Typography>
          <Typography variant="caption">
            {field.writerSchema === null
              ? "No writer schema used"
              : `Writer schema ID ${String(field.writerSchema.id)} · ${field.writerSchema.format}`}
          </Typography>
          {field.writerSchema?.messageType ? (
            <Typography variant="caption">
              Message type: {field.writerSchema.messageType}
            </Typography>
          ) : null}
          {field.writerSchema?.registry ? (
            <Typography variant="caption">Registry: {field.writerSchema.registry}</Typography>
          ) : null}
          {field.state === "error" ? (
            <Alert severity="warning">
              Decoding unavailable ({field.code}): {field.detail}
            </Alert>
          ) : field.state === "null" ? (
            <Alert severity="info">
              Kafka null {part}
              {part === "value" ? " (tombstone)" : ""}; no encoded payload.
            </Alert>
          ) : field.state === "masked" ? (
            <Alert severity="info">[MASKED] — withheld by the record protection policy.</Alert>
          ) : (
            <StudioCodeBlock
              aria-label={field.json === null ? "Decoded text" : "Decoded JSON"}
              sx={{ m: 0, overflow: "auto", whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
            >
              {formatRecordJson(field.json) ?? field.text}
            </StudioCodeBlock>
          )}
        </>
      )}
    </Stack>
  );
}
