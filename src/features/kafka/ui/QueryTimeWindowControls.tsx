import { useId } from "react";
import { Box, Typography } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { type KafkaTimeWindowDraft, resolveKafkaTimeWindow } from "./query-time-window";

export interface QueryTimeWindowControlsProps {
  readonly value: KafkaTimeWindowDraft;
  readonly onChange: (value: KafkaTimeWindowDraft) => void;
  readonly error: string | undefined;
}

export function QueryTimeWindowControls({
  disabled,
  actionLabel = "Load messages",
  value,
  onChange,
  error,
}: QueryTimeWindowControlsProps & {
  readonly actionLabel?: string;
  readonly disabled: boolean;
}): React.JSX.Element {
  const zoneId = useId();
  const bounds =
    value.mode === "custom" && error === undefined ? resolveKafkaTimeWindow(value) : null;
  return (
    <Box
      aria-label="Historical time interval"
      role="region"
      sx={{ borderBottom: 1, borderColor: "divider", px: 2, py: 1 }}
    >
      <Box sx={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 1 }}>
        <Select
          disabled={disabled}
          inputProps={{ "aria-label": "Time interval" }}
          onChange={(event) => onChange({ ...value, mode: event.target.value })}
          value={value.mode}
          sx={{ minWidth: 175 }}
        >
          <MenuItem value="recent">Last 2 minutes</MenuItem>
          <MenuItem value="custom">Custom interval</MenuItem>
        </Select>
        {value.mode === "custom" ? (
          <>
            <TextField
              disabled={disabled}
              error={error?.startsWith("Start time") === true}
              label="Start time (inclusive)"
              onChange={(event) => onChange({ ...value, start: event.target.value })}
              placeholder="2026-07-24T14:03:00Z"
              slotProps={{ htmlInput: { maxLength: 40, "aria-describedby": zoneId } }}
              sx={{ flex: "1 1 240px" }}
              value={value.start}
            />
            <TextField
              disabled={disabled}
              error={error?.startsWith("End time") === true}
              label="End time (exclusive)"
              onChange={(event) => onChange({ ...value, end: event.target.value })}
              placeholder="2026-07-24T14:04:00Z"
              slotProps={{ htmlInput: { maxLength: 40, "aria-describedby": zoneId } }}
              sx={{ flex: "1 1 240px" }}
              value={value.end}
            />
          </>
        ) : null}
      </Box>
      <Typography
        color="text.secondary"
        component="p"
        id={zoneId}
        variant="caption"
        sx={{ mt: 0.5 }}
      >
        {value.mode === "custom"
          ? "Use ISO 8601 with seconds and Z (UTC) or an explicit offset, such as +02:00."
          : `The interval ends when you click ${actionLabel}.`}
        {" Only records still retained by Kafka can be read."}
      </Typography>
      {bounds === null ? null : (
        <Typography
          aria-label="Requested interval in UTC"
          color="text.secondary"
          component="p"
          variant="caption"
        >
          Next read (UTC): {new Date(bounds.startTimeMs).toISOString()} →{" "}
          {new Date(bounds.endTimeMs).toISOString()}
        </Typography>
      )}
      {error === undefined ? null : (
        <Alert severity="error" sx={{ mt: 1 }}>
          {error}
        </Alert>
      )}
    </Box>
  );
}
