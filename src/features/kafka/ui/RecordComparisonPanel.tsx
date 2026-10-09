import { useEffect, useState } from "react";
import { Stack, Typography } from "@mui/material";

import { type KafkaExploredMessage } from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";

import { DocumentDiff } from "./DocumentDiff";
import { recordFieldComparison } from "./record-presentation";

export interface RecordComparisonProperties {
  readonly current: KafkaExploredMessage;
  readonly baseline: KafkaExploredMessage | null;
  readonly onPin: (message: KafkaExploredMessage | null) => void;
  readonly enabled: boolean;
}
const identity = (message: KafkaExploredMessage): string =>
  `${message.topic} / partition ${String(message.partition)} / offset ${message.offset}`;
export function RecordComparisonPanel({
  current,
  baseline,
  onPin,
  enabled,
}: RecordComparisonProperties): React.JSX.Element {
  const [part, setPart] = useState<"key" | "value" | "headers">("value");
  const [format, setFormat] = useState<"projection" | "bytes">("projection");
  const [result, setResult] = useState<{ before: string; after: string }>();
  const [error, setError] = useState<string>();
  useEffect(() => {
    setResult(undefined);
    setError(undefined);
  }, [current, baseline, part, format, enabled]);
  const complete = (message: KafkaExploredMessage): boolean =>
    format === "bytes"
      ? message.original?.state === "complete"
      : message.structured !== undefined &&
        (part === "headers"
          ? message.structured.headersState === "complete" &&
            message.structured.headers.every((header) => header.error === null)
          : message.structured[part].state !== "error");
  const available = enabled && baseline !== null && complete(current) && complete(baseline);
  const compare = (): void => {
    if (!available || !baseline) return;
    setError(undefined);
    setResult(undefined);
    try {
      const read = (message: KafkaExploredMessage): string => {
        if (format === "projection") {
          if (!message.structured)
            throw new Error("Read this record again for structured evidence.");
          if (part === "headers") return JSON.stringify(message.structured.headers);
          const value = recordFieldComparison(message.structured[part]);
          if (value === null) throw new Error("Decoding failed for one of the compared fields.");
          return value;
        }
        if (message.original?.state !== "complete")
          throw new Error("Complete original bytes are unavailable.");
        return part === "headers"
          ? JSON.stringify(message.original.headers)
          : JSON.stringify({ encoding: "base64", bytes: message.original[part] });
      };
      const [before, after] = [read(baseline), read(current)];
      setResult({ before, after });
    } catch (error) {
      setError(error instanceof Error ? error.message : "Comparison failed.");
    }
  };
  return (
    <Stack spacing={2} sx={{ p: 2 }}>
      <Typography variant="body2">
        Pin one record, select another, then compare. The pinned snapshot survives rolling-window
        eviction and topic navigation in this connection. Changing connections, encoding or
        protection settings clears loaded content; saved positions can be reloaded explicitly.
      </Typography>
      <Stack direction="row" spacing={1}>
        <Button disabled={!enabled} onClick={() => onPin(current)}>
          Pin as baseline
        </Button>
        <Button disabled={!baseline} onClick={() => onPin(null)}>
          Clear baseline
        </Button>
      </Stack>
      <Typography variant="body2">
        Before: {baseline ? identity(baseline) : "No baseline pinned"}
      </Typography>
      <Typography variant="body2">After: {identity(current)}</Typography>
      {!available && baseline ? (
        <Alert severity="warning">
          {format === "bytes"
            ? "Complete original bytes are required for both records. Masked or truncated originals cannot establish byte equality."
            : "Both records need structured evidence without decoding errors. Read them again using the saved encoding settings."}
        </Alert>
      ) : null}
      <TextField
        select
        label="Compare field"
        value={part}
        onChange={(event) => setPart(event.target.value as typeof part)}
      >
        <MenuItem value="value">Value</MenuItem>
        <MenuItem value="key">Key</MenuItem>
        <MenuItem value="headers">Ordered headers</MenuItem>
      </TextField>
      <TextField
        select
        label="Comparison representation"
        value={format}
        onChange={(event) => setFormat(event.target.value as typeof format)}
      >
        <MenuItem value="projection">Protected projection</MenuItem>
        <MenuItem value="bytes">Original bytes (Base64)</MenuItem>
      </TextField>
      <Typography variant="caption">
        {format === "projection"
          ? "Compares the same protected fields shown in the inspector. Masked values remain masked; this does not establish equality of undisclosed bytes."
          : "Compares exact Base64 bytes, including duplicate headers in their original order."}
      </Typography>
      <Button
        variant="outlined"
        disabled={!available}
        onClick={() => {
          compare();
        }}
      >
        Compare records
      </Button>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {result ? <DocumentDiff {...result} mode="json" /> : null}
    </Stack>
  );
}
