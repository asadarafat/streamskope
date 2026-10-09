import { Box, Stack, Typography } from "@mui/material";

import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";
import type { RecordExportReason } from "../contracts/record-export";

import type { RecordExportController } from "./use-record-export";

const REASONS: Record<RecordExportReason, string> = {
  "range-complete": "The captured range was scanned completely.",
  "records-unavailable":
    "Some scanned records could not be evaluated by the filter; matching coverage is incomplete.",
  "record-limit": "The exported-record limit was reached.",
  "byte-limit": "The output or scan byte limit was reached.",
  "scan-limit": "The scanned-record limit was reached.",
  deadline: "The export time limit was reached.",
  "pass-limit": "The read-pass limit was reached.",
  cancelled: "Export cancelled after the acknowledged records were written.",
  "checkpoint-unavailable": "The host could not continue the captured read.",
  "read-failed": "The Kafka read failed.",
  "storage-failed": "Export storage failed. No download is available.",
  "cleanup-failed": "Cleanup is unresolved. Retry cleanup before starting another export.",
  revoked: "The export was revoked by a connection or host lifecycle change.",
};

export function RecordExportStatus({
  controller,
}: {
  readonly controller: RecordExportController;
}): React.JSX.Element | null {
  const operation = controller.snapshot?.operation;
  if (!operation && !controller.error && !controller.notice && !controller.uncertainStart)
    return null;
  const active = operation && ["preparing", "reading", "stopping"].includes(operation.state);
  const available = operation?.artifact && !controller.expired && controller.connected;
  return (
    <Box
      role="region"
      aria-label="Range export"
      sx={{ borderBottom: 1, borderColor: "divider", px: 2, py: 1 }}
    >
      <Stack spacing={0.75}>
        {operation && (
          <>
            <Typography variant="subtitle2">
              {operation.state === "expired" || controller.expired
                ? "Range export expired"
                : operation.state === "completed"
                  ? "Range export complete"
                  : operation.state === "partial"
                    ? "Partial range export"
                    : `Range export · ${operation.state}`}{" "}
              · {operation.input.topic} · {operation.input.format.toUpperCase()}
            </Typography>
            <Typography variant="body2" aria-live="polite" role="status">
              {operation.counts.writtenRecords.toLocaleString()} records written ·{" "}
              {operation.counts.scannedRecords.toLocaleString()} scanned ·{" "}
              {(operation.counts.writtenBytes / 1_048_576).toFixed(2)} MiB output ·{" "}
              {operation.counts.passes.toLocaleString()} passes
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {operation.input.range.mode === "earliest"
                ? "From the earliest retained offsets"
                : `${new Date(operation.input.range.startTimeMs).toISOString()} → ${new Date(operation.input.range.endTimeMs).toISOString()}`}{" "}
              · {operation.source.connectionName} · Key: {operation.settings.codecs.key}, value:{" "}
              {operation.settings.codecs.value}. Settings and filters were captured at start.
            </Typography>
            {operation.reason && (
              <Typography variant="body2">
                {REASONS[operation.reason]}{" "}
                {operation.state === "partial"
                  ? "This file contains an incomplete prefix; inspect its receipt before drawing conclusions."
                  : ""}
              </Typography>
            )}
            {operation.counts.unavailableRecords > 0 && (
              <Alert severity="warning">
                {operation.counts.unavailableRecords.toLocaleString()} records could not be
                evaluated by the filter. Coverage is incomplete.
              </Alert>
            )}
            {(operation.counts.decodeErrorRecords > 0 ||
              operation.counts.originalUnavailableRecords > 0) && (
              <Typography variant="body2" color="text.secondary">
                {operation.counts.decodeErrorRecords.toLocaleString()} exported records have
                decoding errors; {operation.counts.originalUnavailableRecords.toLocaleString()} lack
                original bytes. Their explicit states are retained in the file.
              </Typography>
            )}
            {operation.error && (
              <Alert severity="error">
                {operation.error.summary} {operation.error.recovery}
              </Alert>
            )}
            {operation.artifact && (
              <Typography variant="caption" color="text.secondary">
                {controller.expired
                  ? "Download expired. Start a new export."
                  : `Download expires ${new Date(operation.artifact.expiresAt).toISOString()}. The receipt includes SHA-256 and partition coverage.`}
              </Typography>
            )}
          </>
        )}
        <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }} useFlexGap>
          {active && (
            <Button
              disabled={controller.busy || operation.state === "stopping"}
              onClick={() => void controller.cancel()}
            >
              Cancel export
            </Button>
          )}
          {operation?.artifact && (
            <>
              <Button
                disabled={controller.busy || !available}
                onClick={() => void controller.download("data")}
              >
                Download export
              </Button>
              <Button
                disabled={controller.busy || !available}
                onClick={() => void controller.download("receipt")}
              >
                Download receipt
              </Button>
            </>
          )}
          {operation && !active && (
            <Button disabled={controller.busy} onClick={() => void controller.discard()}>
              {operation.reason === "cleanup-failed" ? "Retry export cleanup" : "Discard export"}
            </Button>
          )}
          <Button disabled={controller.busy} onClick={() => void controller.refresh()}>
            Refresh export status
          </Button>
          {controller.uncertainStart && (
            <Button disabled={controller.busy} onClick={() => void controller.retryStart()}>
              Retry same export request
            </Button>
          )}
        </Stack>
        {controller.error && <Alert severity="error">{controller.error}</Alert>}
        {controller.notice && (
          <Typography role="status" variant="body2">
            {controller.notice}
          </Typography>
        )}
      </Stack>
    </Box>
  );
}
