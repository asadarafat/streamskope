import type { ReactNode } from "react";
import { Stack } from "@mui/material";

import type { FiniteRecordInput } from "../contracts/finite-record-read";
import {
  StudioAlert as Alert,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import type { KafkaMessageFilters } from "./message-operations";
import {
  initialKafkaTimeWindow,
  kafkaTimeWindowError,
  resolveKafkaTimeWindow,
  type KafkaTimeWindowDraft,
} from "./query-time-window";
import { QueryTimeWindowControls } from "./QueryTimeWindowControls";

export interface FiniteRangeDraft {
  readonly mode: "earliest" | "time-window";
  readonly window: KafkaTimeWindowDraft;
  readonly maximum: string;
}
export function initialFiniteRangeDraft(maximum: number): FiniteRangeDraft {
  return { mode: "earliest", window: initialKafkaTimeWindow(), maximum: String(maximum) };
}
export function finiteRangeError(draft: FiniteRangeDraft): string | undefined {
  return draft.mode === "time-window" ? kafkaTimeWindowError(draft.window) : undefined;
}
export function finiteRangeInput(
  topic: string,
  filters: KafkaMessageFilters,
  draft: FiniteRangeDraft,
): FiniteRecordInput {
  const { key, value, offset, offsetExact, timestamp, partition, expression } = filters;
  return {
    topic,
    maxRecords: Number(draft.maximum),
    range:
      draft.mode === "earliest"
        ? { mode: "earliest" }
        : { mode: "time-window", ...resolveKafkaTimeWindow(draft.window) },
    search: {
      key,
      value,
      offset,
      timestamp,
      partition,
      ...(offsetExact === undefined ? {} : { offsetExact }),
      ...(expression === undefined ? {} : { expression }),
    },
  };
}
export function FiniteRangeControls({
  value,
  onChange,
  disabled,
  maximum,
  rangeLabel,
  limitLabel,
  actionLabel,
  ruleMatchesOnly,
  suffix,
}: {
  readonly value: FiniteRangeDraft;
  readonly onChange: (value: FiniteRangeDraft) => void;
  readonly disabled: boolean;
  readonly maximum: number;
  readonly rangeLabel: string;
  readonly limitLabel: string;
  readonly actionLabel: string;
  readonly ruleMatchesOnly: boolean;
  readonly suffix?: ReactNode;
}): React.JSX.Element {
  return (
    <>
      <Stack direction="row" spacing={1}>
        <Select
          inputProps={{ "aria-label": rangeLabel }}
          value={value.mode}
          onChange={(event) => onChange({ ...value, mode: event.target.value })}
          disabled={disabled}
          fullWidth
        >
          <MenuItem value="earliest">From beginning</MenuItem>
          <MenuItem value="time-window">Time interval</MenuItem>
        </Select>
        {suffix}
      </Stack>
      {value.mode === "time-window" && (
        <QueryTimeWindowControls
          value={value.window}
          onChange={(window) => onChange({ ...value, window })}
          error={finiteRangeError(value)}
          disabled={disabled}
          actionLabel={actionLabel}
        />
      )}
      <TextField
        label={limitLabel}
        value={value.maximum}
        disabled={disabled}
        onChange={(event) => onChange({ ...value, maximum: event.target.value })}
        slotProps={{ htmlInput: { inputMode: "numeric", maxLength: 6 } }}
        helperText={`Up to ${maximum.toLocaleString()} matching records.`}
      />
      {ruleMatchesOnly && (
        <Alert severity="warning">
          Turn off “Rule matches only” before reading a range. Stored live-rule annotations cannot
          be applied to a new Kafka read. The JSON filter expression is supported.
        </Alert>
      )}
    </>
  );
}
