import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaOriginalRecord,
  type RecordDecodeResult,
  type RecordFormat,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

export function RecordDecodePanel({
  original,
  host,
  enabled,
}: {
  readonly original: KafkaOriginalRecord | undefined;
  readonly host: StreamSkopeHost;
  readonly enabled: boolean;
}): React.JSX.Element {
  const [format, setFormat] = useState<RecordFormat>("json");
  const [part, setPart] = useState<"key" | "value">("value");
  const [result, setResult] = useState<RecordDecodeResult>();
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
  }, [original, enabled]);
  const reset = (): void => {
    generation.current++;
    setResult(undefined);
    setError(undefined);
    setBusy(false);
  };
  const decode = async (): Promise<void> => {
    if (original?.state !== "complete" || !enabled) return;
    const request = ++generation.current;
    setBusy(true);
    setError(undefined);
    setResult(undefined);
    try {
      const response = await host.execute({
        command: "records.decode",
        version: HOST_PROTOCOL_VERSION,
        id: crypto.randomUUID(),
        payload: { format, bytes: original[part] },
      });
      if (generation.current !== request) return;
      if (response.ok) setResult(response.result.decoded);
      else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (generation.current === request)
        setError("The host could not decode this record. Check the connection and retry.");
    } finally {
      if (generation.current === request) setBusy(false);
    }
  };
  const pretty =
    result?.state === "decoded" ? JSON.stringify(JSON.parse(result.json) as unknown, null, 2) : "";
  return (
    <Stack spacing={1.5} sx={{ p: 1.5 }} aria-label="Decoded record">
      <Typography variant="body2">
        Choose the writer encoding. Avro and Protobuf use the active profile’s Schema Registry and
        Confluent schema-ID framing. Original bytes stay unchanged.
      </Typography>
      <TextField
        select
        label="Record part"
        value={part}
        onChange={(event) => {
          reset();
          setPart(event.target.value as "key" | "value");
        }}
      >
        <MenuItem value="value">Value</MenuItem>
        <MenuItem value="key">Key</MenuItem>
      </TextField>
      <TextField
        select
        label="Writer encoding"
        value={format}
        onChange={(event) => {
          reset();
          setFormat(event.target.value as RecordFormat);
        }}
      >
        <MenuItem value="json">UTF-8 JSON</MenuItem>
        <MenuItem value="avro">Confluent Avro</MenuItem>
        <MenuItem value="protobuf">Confluent Protobuf</MenuItem>
      </TextField>
      {original?.state !== "complete" ? (
        <Alert severity="info">
          Complete original bytes are unavailable
          {original?.state === "unavailable" ? ` (${original.reason})` : ""}. Decoding cannot use a
          truncated preview or bypass masking.
        </Alert>
      ) : null}
      {!enabled ? (
        <Alert severity="info">
          Connect to the record’s cluster before decoding. Stale records cannot use another
          cluster’s schema IDs.
        </Alert>
      ) : null}
      <Button
        variant="outlined"
        disabled={busy || !enabled || original?.state !== "complete"}
        onClick={() => {
          void decode();
        }}
      >
        {busy ? "Decoding…" : "Decode record"}
      </Button>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {result?.state === "error" ? <Alert severity="warning">{result.detail}</Alert> : null}
      {result?.state === "null" ? (
        <Alert severity="info">
          Kafka null {part}
          {part === "value" ? " (tombstone)" : ""}; no encoded payload.
        </Alert>
      ) : null}
      {result?.state === "decoded" ? (
        <>
          <Typography variant="caption">
            {result.schemaId === null
              ? "No Registry schema used"
              : `Writer schema ID ${String(result.schemaId)}`}
            {result.messageType === null ? "" : ` · ${result.messageType}`}
          </Typography>
          <StudioCodeBlock
            aria-label="Decoded JSON"
            sx={{ m: 0, overflow: "auto", whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
          >
            {pretty.length > 512 * 1_024 ? result.json : pretty}
          </StudioCodeBlock>
          <Typography variant="caption">{result.notes}</Typography>
        </>
      ) : null}
    </Stack>
  );
}
