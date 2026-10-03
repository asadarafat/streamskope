import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaExploredMessage,
  type StreamSkopeHost,
} from "../contracts";
import type { RecordFormat } from "../contracts/record-codec";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";

import { DocumentDiff } from "./DocumentDiff";

export interface RecordComparisonProperties {
  readonly current: KafkaExploredMessage;
  readonly baseline: KafkaExploredMessage | null;
  readonly onPin: (message: KafkaExploredMessage | null) => void;
  readonly host?: StreamSkopeHost | undefined;
  readonly enabled: boolean;
}
const identity = (message: KafkaExploredMessage): string =>
  `${message.topic} / partition ${String(message.partition)} / offset ${message.offset}`;
export function RecordComparisonPanel({
  current,
  baseline,
  onPin,
  host,
  enabled,
}: RecordComparisonProperties): React.JSX.Element {
  const [part, setPart] = useState<"key" | "value" | "headers">("value");
  const [format, setFormat] = useState<RecordFormat | "bytes">("bytes");
  const [result, setResult] = useState<{ before: string; after: string }>();
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setResult(undefined);
    setError(undefined);
    setBusy(false);
    return (): void => {
      generation.current++;
    };
  }, [current, baseline, part, format, enabled]);
  const available =
    enabled && current.original?.state === "complete" && baseline?.original?.state === "complete";
  const compare = async (): Promise<void> => {
    if (!available || !baseline) return;
    const request = ++generation.current;
    setBusy(true);
    setError(undefined);
    setResult(undefined);
    try {
      const read = async (message: KafkaExploredMessage): Promise<string> => {
        if (message.original?.state !== "complete")
          throw new Error("Complete original bytes are unavailable.");
        if (part === "headers") return JSON.stringify(message.original.headers);
        const bytes = message.original[part];
        if (format === "bytes") return JSON.stringify({ encoding: "base64", bytes });
        if (!host) throw new Error("The application host is unavailable.");
        const response = await host.execute({
          command: "records.decode",
          id: globalThis.crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { format, bytes },
        });
        if (!response.ok) throw new Error(response.error.summary);
        const decoded = response.result.decoded;
        if (decoded.state === "error") throw new Error(decoded.detail);
        // A Kafka tombstone must remain distinct from a JSON null value.
        return decoded.state === "null"
          ? JSON.stringify({ kind: "Kafka null" })
          : `{"kind":"decoded","value":${decoded.json}}`;
      };
      const [before, after] = await Promise.all([read(baseline), read(current)]);
      if (generation.current === request) setResult({ before, after });
    } catch (error) {
      if (generation.current === request)
        setError(error instanceof Error ? error.message : "Comparison failed.");
    } finally {
      if (generation.current === request) setBusy(false);
    }
  };
  return (
    <Stack spacing={2} sx={{ p: 2 }}>
      <Typography variant="body2">
        Pin one record, select another, then compare. The pinned snapshot survives rolling-window
        eviction; switching topics, connections or protection settings clears it.
      </Typography>
      <Stack direction="row" spacing={1}>
        <Button disabled={!enabled || busy} onClick={() => onPin(current)}>
          Pin as baseline
        </Button>
        <Button disabled={!baseline || busy} onClick={() => onPin(null)}>
          Clear baseline
        </Button>
      </Stack>
      <Typography variant="body2">
        Before: {baseline ? identity(baseline) : "No baseline pinned"}
      </Typography>
      <Typography variant="body2">After: {identity(current)}</Typography>
      {!available && baseline ? (
        <Alert severity="warning">
          Complete original bytes are required for both records. A truncated or masked preview
          cannot establish equality.
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
      {part !== "headers" ? (
        <TextField
          select
          label="Comparison encoding"
          value={format}
          onChange={(event) => setFormat(event.target.value as typeof format)}
        >
          <MenuItem value="bytes">Original bytes (Base64)</MenuItem>
          <MenuItem value="json">JSON</MenuItem>
          <MenuItem value="avro">Confluent Avro</MenuItem>
          <MenuItem value="protobuf">Confluent Protobuf</MenuItem>
        </TextField>
      ) : (
        <Typography variant="caption">
          Header names and values use exact Base64 bytes; duplicate names and order are preserved.
        </Typography>
      )}
      <Button
        variant="outlined"
        disabled={!available || busy}
        onClick={() => {
          void compare();
        }}
      >
        {busy ? "Comparing…" : "Compare records"}
      </Button>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {result ? <DocumentDiff {...result} mode="json" /> : null}
    </Stack>
  );
}
