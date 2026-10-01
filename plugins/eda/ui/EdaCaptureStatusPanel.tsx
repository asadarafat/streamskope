import { useCallback, useEffect, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  sameEdaCaptureSource,
  type EdaCaptureSessionStatus,
  type ProfileEdaCaptureSource,
} from "../contracts";
import { StudioDetailRow } from "../../../src/platform/ui/StudioPropertyRow";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
} from "../../../src/platform/ui/controls";

import type { EdaUiHost as StreamSkopeHost } from "./host";

export function EdaCaptureStatusPanel({
  host,
  source,
  onResume,
  onStopped,
  connected = false,
}: {
  readonly host: StreamSkopeHost;
  readonly source: ProfileEdaCaptureSource;
  readonly onResume?: () => void;
  readonly onStopped?: () => void;
  readonly connected?: boolean;
}): React.JSX.Element {
  const [status, setStatus] = useState<EdaCaptureSessionStatus>();
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState(false);
  const refresh = useCallback(async (): Promise<void> => {
    setBusy(true);
    setStatus(undefined);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "edaCapture.status",
        id: crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      if (!("captureSession" in response.result))
        throw new Error("Invalid capture status response.");
      setStatus(response.result.captureSession);
    } catch {
      setError("Capture status could not be checked. Retry before changing this capture.");
    } finally {
      setBusy(false);
    }
  }, [host]);
  useEffect(() => {
    setStatus(undefined);
    void refresh();
  }, [refresh, source]);

  async function stop(remove: boolean): Promise<void> {
    setBusy(true);
    setStatus(undefined);
    setError(undefined);
    try {
      const response = await host.execute({
        command: remove ? "edaCapture.remove" : "edaCapture.stop",
        id: crypto.randomUUID(),
        payload: { source },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      if (!("captureSession" in response.result))
        throw new Error("Invalid capture status response.");
      setStatus(response.result.captureSession);
      setConfirmRemove(false);
      onStopped?.();
    } catch {
      setError(
        "The host did not confirm the operation. Resources may remain; refresh status and check Activity before retrying.",
      );
    } finally {
      setBusy(false);
    }
  }
  const matching = status?.source !== undefined && sameEdaCaptureSource(source, status.source);
  const running =
    matching &&
    status?.state === "ready" &&
    status.tunnel === "open" &&
    source.sessionId !== undefined &&
    source.sessionId === status.source?.sessionId;
  const locked = connected || busy || status === undefined;
  return (
    <Box
      component="section"
      aria-label="Managed capture"
      sx={{ border: 1, borderColor: "divider", p: 1.5 }}
    >
      <Stack spacing={1}>
        <Typography component="h3" variant="subtitle2">
          Managed capture
        </Typography>
        <Box component="dl" sx={{ m: 0 }}>
          <StudioDetailRow
            label="Source"
            value={`${source.source.namespace}/${source.source.name}`}
          />
          <StudioDetailRow
            label="EDA API"
            value={source.edaApiUrl ?? "Legacy capture — select the intended source when resuming"}
          />
          <StudioDetailRow
            label={source.context === "eda-agent" ? "Capture provider" : "Kubernetes context"}
            value={
              source.context === "eda-agent"
                ? "EDA application"
                : (source.context ?? "Not recorded")
            }
          />
          <StudioDetailRow
            label="Capture session"
            value={
              status === undefined
                ? busy
                  ? "Checking capture"
                  : "Status unavailable"
                : running
                  ? "Running"
                  : "Not running for this connection"
            }
          />
          <StudioDetailRow
            label="Local tunnel"
            value={running ? "Open" : "Not verified for this connection"}
          />
          <StudioDetailRow
            label="Broker verification"
            value={
              matching && status?.verifiedAt !== undefined
                ? `Last verified ${status.verifiedAt}; not continuously monitored`
                : "Not verified in this host session"
            }
          />
        </Box>
        <Typography variant="body2" color="text.secondary">
          {source.context === "eda-agent"
            ? "Disconnect Kafka ends only your client connection. Stopping capture removes its temporary exporter, broker and messages; the original EDA exporter remains."
            : "Disconnect Kafka ends your client connection only. Stop capture stops export and the tunnel but retains the broker. Removing resources also deletes the temporary broker and its messages."}
        </Typography>
        {matching ? (
          <Typography role="status" variant="body2">
            {status?.detail}
          </Typography>
        ) : null}
        {connected ? (
          <Typography variant="body2">
            Disconnect Kafka before changing capture resources.
          </Typography>
        ) : null}
        {error === undefined ? null : <Alert severity="error">{error}</Alert>}
        <Stack direction="row" useFlexGap sx={{ flexWrap: "wrap" }} spacing={1}>
          {!running && onResume !== undefined ? (
            <Button disabled={locked} onClick={onResume} variant="contained">
              Resume capture
            </Button>
          ) : null}
          {source.context === "eda-agent" ? null : (
            <Button disabled={locked} onClick={() => void stop(false)}>
              Stop capture
            </Button>
          )}
          <Button disabled={busy} onClick={() => void refresh()}>
            Refresh status
          </Button>
          <Button color="error" disabled={locked} onClick={() => setConfirmRemove(true)}>
            Stop and remove capture
          </Button>
        </Stack>
      </Stack>
      <Dialog
        open={confirmRemove}
        onClose={busy ? undefined : (): void => setConfirmRemove(false)}
        aria-labelledby="remove-capture-title"
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle id="remove-capture-title">Remove capture resources</DialogTitle>
        <DialogContent dividers>
          <Typography>
            Remove the capture exporter, broker and service for {source.source.namespace}/
            {source.source.name} from{" "}
            {source.context === "eda-agent"
              ? "the EDA application"
              : `Kubernetes context ${source.context ?? "not recorded"}`}
            ? Captured messages will be discarded. The original EDA producer is not deleted.
            Temporary connection profiles are removed only after remote cleanup is confirmed.
          </Typography>
          {error === undefined ? null : <Alert severity="error">{error}</Alert>}
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setConfirmRemove(false)}>
            Cancel
          </Button>
          <Button color="error" variant="contained" disabled={busy} onClick={() => void stop(true)}>
            Remove capture resources
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
