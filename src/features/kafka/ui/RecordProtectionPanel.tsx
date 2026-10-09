import { useEffect, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  parseKafkaRecordProtection,
  type KafkaOperationalPreferenceSnapshot,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioLabeledControl as FormControlLabel,
  StudioSwitch as Switch,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

interface Properties {
  readonly host: StreamSkopeHost;
  readonly snapshot: KafkaOperationalPreferenceSnapshot | null;
  readonly disconnected: boolean;
}

export function RecordProtectionPanel({
  host,
  snapshot,
  disconnected,
}: Properties): React.JSX.Element {
  const policy = snapshot?.preferences.protection ?? KAFKA_RECORD_PROTECTION_DEFAULTS;
  const [confirmed, setConfirmed] = useState(policy);
  const [readOnly, setReadOnly] = useState(policy.readOnly);
  const [maskKey, setMaskKey] = useState(policy.maskKey);
  const [maskHeaders, setMaskHeaders] = useState(policy.maskHeaders.join("\n"));
  const [valuePaths, setValuePaths] = useState(
    policy.valuePaths.filter((path) => path !== "").join("\n"),
  );
  const [maskValue, setMaskValue] = useState(policy.valuePaths.includes(""));
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<string>();
  const [status, setStatus] = useState("");
  useEffect(() => {
    setConfirmed(policy);
    setReadOnly(policy.readOnly);
    setMaskKey(policy.maskKey);
    setMaskHeaders(policy.maskHeaders.join("\n"));
    setValuePaths(policy.valuePaths.filter((path) => path !== "").join("\n"));
    setMaskValue(policy.valuePaths.includes(""));
  }, [policy]);
  const lines = (value: string): string[] => (value === "" ? [] : value.split("\n"));
  const draft = {
    readOnly,
    maskKey,
    maskHeaders: lines(maskHeaders),
    valuePaths: maskValue ? [""] : lines(valuePaths),
  };
  const dirty = JSON.stringify(draft) !== JSON.stringify(confirmed);
  let issue: string | undefined;
  try {
    parseKafkaRecordProtection(draft);
  } catch (error) {
    issue = error instanceof Error ? error.message : "Review the masking rules.";
  }
  const disabled = pending || !disconnected || snapshot?.store.state !== "ready";
  async function save(): Promise<void> {
    if (disabled || issue !== undefined || !dirty) return;
    setPending(true);
    setFailure(undefined);
    setStatus("");
    try {
      const result = await host.execute({
        version: HOST_PROTOCOL_VERSION,
        id: crypto.randomUUID(),
        command: "preferences.update",
        payload: { patch: { protection: draft } },
      });
      if (!result.ok) setFailure(`${result.error.summary} ${result.error.recovery}`);
      else {
        setConfirmed(result.result.snapshot.preferences.protection);
        setStatus(
          "Protection saved. Retained records were cleared; reconnect to read using these settings.",
        );
      }
    } catch {
      setFailure(
        "The host did not confirm protection settings. The last confirmed policy remains active.",
      );
    } finally {
      setPending(false);
    }
  }
  return (
    <Stack spacing={2}>
      <Typography component="h3" variant="subtitle1">
        Record protection
      </Typography>
      <Typography variant="body2">
        These settings apply to every Kafka connection on this host. Broker permissions remain the
        authority for access.
      </Typography>
      {snapshot?.store.state !== "ready" && (
        <Alert severity="warning">
          Protection storage is not ready. Restore it or reset workbench preferences while
          disconnected, then review these controls.
        </Alert>
      )}
      {!disconnected && (
        <Alert severity="info">
          Disconnect Kafka before changing protection. Finish or cancel other remote operations
          first.
        </Alert>
      )}
      <FormControlLabel
        control={
          <Switch
            checked={readOnly}
            disabled={disabled}
            onChange={(_event, checked) => setReadOnly(checked)}
          />
        }
        label="Read-only mode"
      />
      <Typography variant="body2" color="text.secondary">
        Blocks remote mutations, latency probes, remote credential acquisition and plugin actions.
        Managed capture connections require plugin lifecycle hooks and are also blocked. Ordinary
        Kafka connections and reads remain available.
      </Typography>
      <FormControlLabel
        control={
          <Switch
            checked={maskKey}
            disabled={disabled}
            onChange={(_event, checked) => setMaskKey(checked)}
          />
        }
        label="Mask record keys"
      />
      <TextField
        label="Header names to mask"
        multiline
        minRows={2}
        value={maskHeaders}
        disabled={disabled}
        onChange={(event) => setMaskHeaders(event.target.value)}
        helperText="One exact, case-sensitive header name per line. Maximum 32. All duplicate occurrences are covered."
      />
      <FormControlLabel
        control={
          <Switch
            checked={maskValue}
            disabled={disabled}
            onChange={(_event, checked) => setMaskValue(checked)}
          />
        }
        label="Mask entire record values"
      />
      <TextField
        label="Decoded JSON value paths to mask"
        multiline
        minRows={3}
        value={valuePaths}
        disabled={disabled || maskValue}
        onChange={(event) => setValuePaths(event.target.value)}
        helperText="One JSON Pointer per line, e.g. /customer/email or /items/0/token. Use ~1 for / and ~0 for ~. Maximum 32 paths and 16 segments. No wildcard matching."
      />
      <Typography variant="body2" color="text.secondary">
        Masking replaces selected fields with [MASKED] before inspection, copy, export and rule
        evaluation. Paths apply to the decoded JSON projection, including Avro and Protobuf.
        Undecodable or incomplete values are fully masked when value paths are configured. Original
        bytes are unavailable while masking is active. Filters and tracing use only the protected
        projection.
      </Typography>
      <Typography variant="body2" color="text.secondary">
        Previously copied or exported data cannot be recalled. Installed plugins are trusted code;
        this setting is not a sandbox or multi-user access control.
      </Typography>
      {(issue ?? failure) !== undefined && <Alert severity="error">{issue ?? failure}</Alert>}
      {status && (
        <Alert severity="success" role="status">
          {status}
        </Alert>
      )}
      <Button
        disabled={disabled || issue !== undefined || !dirty}
        onClick={() => {
          void save();
        }}
        variant="contained"
      >
        {pending ? "Saving protection…" : "Save protection"}
      </Button>
    </Stack>
  );
}
