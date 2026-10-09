import { useEffect, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type KafkaOperationalPreferenceSnapshot,
  type StreamSkopeHost,
} from "../contracts";
import type { RecordCodecSelection } from "../contracts/structured-record";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { recordCodecLabels } from "./record-presentation";

const defaults = { key: "auto", value: "auto" } as const;

export function RecordCodecPreferencesPanel({
  host,
  snapshot,
  disconnected,
}: {
  readonly host: StreamSkopeHost;
  readonly snapshot: KafkaOperationalPreferenceSnapshot | null;
  readonly disconnected: boolean;
}): React.JSX.Element {
  const saved = snapshot?.preferences.codecs ?? defaults;
  const [draft, setDraft] = useState(saved);
  const [confirmed, setConfirmed] = useState(saved);
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<string>();
  const [status, setStatus] = useState("");
  useEffect(() => {
    setDraft(saved);
    setConfirmed(saved);
    setStatus("");
    setFailure(undefined);
  }, [saved]);
  const disabled = pending || !disconnected || snapshot?.store.state !== "ready";
  const dirty = draft.key !== confirmed.key || draft.value !== confirmed.value;
  async function save(): Promise<void> {
    if (disabled || !dirty) return;
    setPending(true);
    setFailure(undefined);
    setStatus("");
    try {
      const response = await host.execute({
        version: HOST_PROTOCOL_VERSION,
        id: crypto.randomUUID(),
        command: "preferences.update",
        payload: { patch: { codecs: draft } },
      });
      if (!response.ok) setFailure(`${response.error.summary} ${response.error.recovery}`);
      else {
        setConfirmed(response.result.snapshot.preferences.codecs);
        setStatus(
          snapshot?.store.durability === "durable"
            ? "Record encodings saved. Reconnect and read records using these settings."
            : "Record encodings saved for this session. Reconnect and read records using these settings.",
        );
      }
    } catch {
      setFailure(
        "The host did not confirm record encodings. The last confirmed settings remain active.",
      );
    } finally {
      setPending(false);
    }
  }
  return (
    <Stack spacing={2}>
      <Typography component="h3" variant="subtitle1">
        Record encodings
      </Typography>
      <Typography variant="body2">
        These saved defaults apply to every Kafka connection on this host. Key and value encodings
        are independent. Selecting an encoding overrides detection for the next read.
      </Typography>
      {!disconnected ? (
        <Alert severity="info">
          Disconnect Kafka before changing record encodings. Existing records keep their captured
          interpretation.
        </Alert>
      ) : null}
      {snapshot?.store.state !== "ready" ? (
        <Alert severity="warning">
          Preference storage is not ready. Restore it before saving record encodings.
        </Alert>
      ) : null}
      {(["key", "value"] as const).map((part) => (
        <TextField
          key={part}
          select
          label={part === "key" ? "Default key encoding" : "Default value encoding"}
          value={draft[part]}
          disabled={disabled}
          onChange={(event) => {
            setDraft({ ...draft, [part]: event.target.value as RecordCodecSelection });
            setStatus("");
          }}
        >
          {(Object.entries(recordCodecLabels) as [RecordCodecSelection, string][]).map(
            ([value, label]) => (
              <MenuItem value={value} key={value}>
                {label}
              </MenuItem>
            ),
          )}
        </TextField>
      ))}
      <Typography variant="body2" color="text.secondary">
        Detection uses valid UTF-8 JSON, text, or Confluent schema-ID framing resolved through the
        active profile’s Schema Registry. Unknown or malformed records show an explicit decoding
        error; detection does not guess a schema or silently change an override. Choose Bytes to
        inspect arbitrary binary payloads. Kafka null values remain tombstones.
      </Typography>
      <Typography variant="body2" color="text.secondary">
        The same protected projection feeds the grid, filters, comparison, tracing and export.
        Saving clears retained records and comparison baselines. Original key, value and ordered
        header bytes are never rewritten; masking can withhold them from disclosure.
      </Typography>
      {failure ? <Alert severity="error">{failure}</Alert> : null}
      {status ? (
        <Alert severity="success" role="status">
          {status}
        </Alert>
      ) : null}
      <Button
        variant="contained"
        disabled={disabled || !dirty}
        onClick={() => {
          void save();
        }}
      >
        {pending ? "Saving encodings…" : "Save record encodings"}
      </Button>
    </Stack>
  );
}
