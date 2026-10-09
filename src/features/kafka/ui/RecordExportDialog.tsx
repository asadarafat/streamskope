import { useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as LabeledControl,
} from "../../../platform/ui/controls";
import { RECORD_EXPORT_LIMITS, type RecordExportFormat } from "../contracts/record-export";
import { parseRecordExportInput } from "../contracts/record-export-validation";

import type { KafkaMessageFilters } from "./message-operations";
import type { RecordExportController } from "./use-record-export";
import {
  initialKafkaTimeWindow,
  kafkaTimeWindowError,
  resolveKafkaTimeWindow,
} from "./query-time-window";
import { QueryTimeWindowControls } from "./QueryTimeWindowControls";

export function RecordExportDialog({
  topic,
  filters,
  controller,
  onClose,
}: {
  readonly topic: string;
  readonly filters: KafkaMessageFilters;
  readonly controller: RecordExportController;
  readonly onClose: () => void;
}): React.JSX.Element {
  const [range, setRange] = useState<"earliest" | "time-window">("earliest");
  const [window, setWindow] = useState(initialKafkaTimeWindow);
  const [format, setFormat] = useState<RecordExportFormat>("jsonl");
  const [maximum, setMaximum] = useState(String(RECORD_EXPORT_LIMITS.records));
  const [replace, setReplace] = useState(false);
  const [error, setError] = useState<string>();
  const existing = controller.snapshot?.operation;
  const active =
    existing !== undefined &&
    existing !== null &&
    ["preparing", "reading", "stopping"].includes(existing.state);
  const replacing = existing?.artifact !== null && existing?.artifact !== undefined;
  const timeError = range === "time-window" ? kafkaTimeWindowError(window) : undefined;
  const blocked =
    filters.activeRuleMatchesOnly ||
    active ||
    existing?.reason === "cleanup-failed" ||
    controller.busy ||
    controller.uncertainStart ||
    !controller.connected ||
    controller.snapshot?.available !== true ||
    timeError !== undefined ||
    (replacing && !replace);
  async function start(): Promise<void> {
    setError(undefined);
    try {
      const { key, value, offset, offsetExact, timestamp, partition, expression } = filters;
      const input = parseRecordExportInput(
        {
          requestId: crypto.randomUUID(),
          topic,
          format,
          maxRecords: Number(maximum),
          range:
            range === "earliest"
              ? { mode: "earliest" }
              : { mode: "time-window", ...resolveKafkaTimeWindow(window) },
          search: {
            key,
            value,
            offset,
            timestamp,
            partition,
            ...(offsetExact === undefined ? {} : { offsetExact }),
            ...(expression === undefined ? {} : { expression }),
          },
        },
        "Export",
      );
      if (await controller.start(input)) onClose();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Check the export inputs.");
    }
  }
  return (
    <Dialog
      open
      onClose={controller.busy ? undefined : onClose}
      fullWidth
      maxWidth="sm"
      aria-labelledby="record-export-title"
    >
      <DialogTitle id="record-export-title">Export a topic range</DialogTitle>
      <DialogContent>
        <Stack spacing={2}>
          <Typography variant="body2">
            Read <strong>{topic}</strong> directly from Kafka using the current message filters.
            This export is independent of the rows retained in the grid.
          </Typography>
          <Stack direction="row" spacing={1}>
            <Select
              inputProps={{ "aria-label": "Export range" }}
              value={range}
              onChange={(event) => setRange(event.target.value)}
              disabled={controller.busy}
              fullWidth
            >
              <MenuItem value="earliest">From beginning</MenuItem>
              <MenuItem value="time-window">Time interval</MenuItem>
            </Select>
            <Select
              inputProps={{ "aria-label": "Export format" }}
              value={format}
              onChange={(event) => setFormat(event.target.value)}
              disabled={controller.busy}
            >
              <MenuItem value="jsonl">JSONL</MenuItem>
              <MenuItem value="csv">CSV</MenuItem>
            </Select>
          </Stack>
          {range === "time-window" && (
            <QueryTimeWindowControls
              value={window}
              onChange={setWindow}
              error={timeError}
              disabled={controller.busy}
              actionLabel="Start export"
            />
          )}
          <TextField
            label="Maximum exported records"
            value={maximum}
            disabled={controller.busy}
            onChange={(event) => setMaximum(event.target.value)}
            slotProps={{ htmlInput: { inputMode: "numeric", maxLength: 6 } }}
            helperText={`Up to ${RECORD_EXPORT_LIMITS.records.toLocaleString()} matching records.`}
          />
          <Typography variant="body2" color="text.secondary">
            The host captures the topic offsets, codec preferences and masking settings at start.
            Newly arriving records are excluded. The first limit reached stops the export: 5
            minutes, 1,000,000 scanned records, 1 GiB scanned, or 256 MiB output. A receipt records
            the exact coverage and any partial result.
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Downloads remain available for 15 minutes. Disconnecting, locking or closing the host
            removes them. Each download has a five-minute deadline. Files you download remain on
            your device.
          </Typography>
          {filters.activeRuleMatchesOnly && (
            <Alert severity="warning">
              Turn off “Rule matches only” before exporting a range. Stored live-rule annotations
              cannot be applied to a new Kafka read. The JSON filter expression is supported.
            </Alert>
          )}
          {active && (
            <Alert severity="info">
              Finish or cancel the current range export before starting another.
            </Alert>
          )}
          {replacing && (
            <LabeledControl
              control={
                <Checkbox
                  checked={replace}
                  onChange={(event) => setReplace(event.target.checked)}
                  disabled={controller.busy}
                />
              }
              label="Replace the previous prepared download"
            />
          )}
          {(error ?? controller.error) && (
            <Alert severity="error">{error ?? controller.error}</Alert>
          )}
          {controller.uncertainStart && (
            <Button
              disabled={controller.busy}
              onClick={() => {
                void controller.retryStart().then((accepted) => {
                  if (accepted) onClose();
                });
              }}
            >
              Retry same export request
            </Button>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button disabled={blocked} onClick={() => void start()} variant="contained">
          Start export
        </Button>
      </DialogActions>
    </Dialog>
  );
}
