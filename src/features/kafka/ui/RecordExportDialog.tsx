import { useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
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
  FiniteRangeControls,
  initialFiniteRangeDraft,
  finiteRangeError,
  finiteRangeInput,
} from "./FiniteRangeControls";

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
  const [range, setRange] = useState(() => initialFiniteRangeDraft(RECORD_EXPORT_LIMITS.records));
  const [format, setFormat] = useState<RecordExportFormat>("jsonl");
  const [replace, setReplace] = useState(false);
  const [error, setError] = useState<string>();
  const existing = controller.snapshot?.operation;
  const active =
    existing !== undefined &&
    existing !== null &&
    ["preparing", "reading", "stopping"].includes(existing.state);
  const replacing = existing?.artifact !== null && existing?.artifact !== undefined;
  const timeError = finiteRangeError(range);
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
      const input = parseRecordExportInput(
        { ...finiteRangeInput(topic, filters, range), requestId: crypto.randomUUID(), format },
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
          <FiniteRangeControls
            value={range}
            onChange={setRange}
            disabled={controller.busy}
            maximum={RECORD_EXPORT_LIMITS.records}
            rangeLabel="Export range"
            limitLabel="Maximum exported records"
            actionLabel="Start export"
            ruleMatchesOnly={filters.activeRuleMatchesOnly}
            suffix={
              <Select
                inputProps={{ "aria-label": "Export format" }}
                value={format}
                onChange={(event) => setFormat(event.target.value)}
                disabled={controller.busy}
              >
                <MenuItem value="jsonl">JSONL</MenuItem>
                <MenuItem value="csv">CSV</MenuItem>
              </Select>
            }
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
