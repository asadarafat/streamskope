import { Stack, Typography } from "@mui/material";

import {
  StudioButton as Button,
  StudioTextField as TextField,
  StudioResourceAutocomplete as Autocomplete,
} from "../../../platform/ui/controls";

export interface ObservationControlsProperties {
  readonly topic: string;
  readonly groupId: string;
  readonly topics: readonly string[];
  readonly groups: readonly string[];
  readonly busy: boolean;
  readonly running: boolean;
  readonly connected: boolean;
  readonly backendAvailable: boolean;
  readonly historyReady: boolean;
  readonly cooldownSeconds: number;
  readonly operation: "history" | "capture" | "clear" | null;
  readonly selectionError: string | null;
  readonly inventoryStatus: string;
  readonly onTopicChange: (value: string) => void;
  readonly onGroupChange: (value: string) => void;
  readonly onRefresh: () => void;
  readonly onCapture: () => void;
  readonly onStart: () => void;
  readonly onStop: () => void;
}

export function ObservationControls(p: ObservationControlsProperties): React.JSX.Element {
  const disabled = p.busy || p.running;
  const canCapture =
    p.connected &&
    p.backendAvailable &&
    p.historyReady &&
    !disabled &&
    p.cooldownSeconds === 0 &&
    p.topic.trim().length > 0;
  return (
    <Stack component="section" aria-label="Observation selection" spacing={1.5}>
      <Stack direction={{ xs: "column", md: "row" }} spacing={1.5}>
        <Autocomplete
          freeSolo
          clearText="Clear observed topic"
          options={[...p.topics]}
          value={p.topic || null}
          inputValue={p.topic}
          disabled={disabled || !p.connected || !p.backendAvailable}
          onInputChange={(_event, value) => p.onTopicChange(value)}
          onChange={(_event, value) => p.onTopicChange(value ?? "")}
          renderInput={(params) => (
            <TextField
              {...params}
              label="Observed topic"
              helperText="Search existing topics, or enter a permitted topic name."
            />
          )}
        />
        <Autocomplete
          freeSolo
          clearText="Clear observed consumer group"
          options={[...p.groups]}
          value={p.groupId || null}
          inputValue={p.groupId}
          disabled={disabled || !p.connected || !p.backendAvailable}
          onInputChange={(_event, value) => p.onGroupChange(value)}
          onChange={(_event, value) => p.onGroupChange(value ?? "")}
          renderInput={(params) => (
            <TextField
              {...params}
              label="Observed consumer group (optional)"
              helperText="Add a group to observe committed positions and lag."
            />
          )}
        />
      </Stack>
      <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap", alignItems: "center" }}>
        <Button variant="contained" disabled={!canCapture} onClick={p.onCapture}>
          Capture observation
        </Button>
        <Button disabled={!canCapture} onClick={p.onStart}>
          Start observing
        </Button>
        <Button disabled={!p.running && p.operation !== "capture"} onClick={p.onStop}>
          Stop observing
        </Button>
        <Button disabled={disabled || !p.connected || !p.backendAvailable} onClick={p.onRefresh}>
          Refresh resources
        </Button>
      </Stack>
      <Typography
        role="status"
        aria-label="Observation collection status"
        aria-live="polite"
        variant="body2"
        color="text.secondary"
      >
        {!p.backendAvailable
          ? "Host unavailable — restore the application host before collecting new evidence."
          : !p.connected
            ? "Disconnected — connect a profile to collect new evidence."
            : p.operation === "capture"
              ? "Collecting observation… One request at a time; stops after 15 seconds."
              : p.operation === "history"
                ? "Loading retained observations…"
                : p.operation === "clear"
                  ? "Clearing retained history…"
                  : !p.historyReady
                    ? "Retained history is unavailable. Reload it or explicitly clear it before collecting."
                    : p.cooldownSeconds > 0
                      ? `${p.running ? "Observing" : "Stopped"} · Next capture available in ${p.cooldownSeconds} seconds.`
                      : p.running
                        ? "Observing · Waiting for the next capture."
                        : "Stopped · Capture once or observe every 10 seconds after collection completes."}
      </Typography>
      <Typography variant="caption" color="text.secondary">
        {p.inventoryStatus}
      </Typography>
      {p.selectionError && (
        <Typography role="alert" color="error">
          {p.selectionError}
        </Typography>
      )}
    </Stack>
  );
}
