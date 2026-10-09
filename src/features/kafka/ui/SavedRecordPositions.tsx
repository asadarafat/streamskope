import { useState } from "react";
import { Stack, Typography } from "@mui/material";

import type { KafkaRecordLocator } from "../contracts/record-locator";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";

import type { InvestigationRecordsController, SavedRecordSlot } from "./use-investigation-records";

export function recordPositionLabel(locator: KafkaRecordLocator): string {
  return `${locator.topic} · partition ${String(locator.partition)} · offset ${locator.offset}`;
}

export function SavedRecordPositions({
  controller,
  connected,
  readBlocked,
  onOpenTopic,
  onChoose,
}: {
  readonly controller: InvestigationRecordsController;
  readonly connected: boolean;
  readonly readBlocked: boolean;
  readonly onOpenTopic: (topic: string) => void;
  readonly onChoose: (locator: KafkaRecordLocator, slot: SavedRecordSlot) => void;
}): React.JSX.Element | null {
  const [bookmarkId, setBookmarkId] = useState("");
  const { references, outcomes, busy, error } = controller;
  const bookmark = references.bookmarks.find((entry) => entry.id === bookmarkId);
  if (
    references.selected === null &&
    references.comparison === null &&
    references.bookmarks.length === 0 &&
    !busy &&
    !error
  )
    return null;
  return (
    <Stack
      component="section"
      aria-label="Saved record positions"
      spacing={0.75}
      sx={{
        borderBottom: 1,
        borderColor: "divider",
        px: 2,
        py: 1,
        minWidth: 0,
        maxHeight: 240,
        overflow: "auto",
        flexShrink: 0,
      }}
    >
      <Stack
        direction="row"
        sx={{ alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 1 }}
      >
        <Typography variant="subtitle2">Saved record positions</Typography>
        {busy ? (
          <Button onClick={() => void controller.cancel()}>
            {controller.cleanupPending ? "Retry stop" : "Stop reload"}
          </Button>
        ) : null}
        <Button disabled={busy} onClick={controller.clear}>
          Clear saved positions
        </Button>
      </Stack>
      <Typography variant="caption">
        Clearing affects this workspace only; saved views remain unchanged.
      </Typography>
      {(["selected", "comparison"] as const).map((slot) => {
        const locator = references[slot];
        if (locator === null) return null;
        const outcome = outcomes[slot];
        return (
          <Stack key={slot} direction="row" sx={{ alignItems: "center", flexWrap: "wrap", gap: 1 }}>
            <Typography variant="body2" sx={{ flex: "1 1 220px", overflowWrap: "anywhere" }}>
              {slot === "selected" ? "Selected" : "Baseline"}: {recordPositionLabel(locator)}
            </Typography>
            <Typography variant="caption" role="status" sx={{ overflowWrap: "anywhere" }}>
              {outcome?.state === "loaded"
                ? "Reloaded with current protection"
                : (outcome?.detail ?? "Not loaded")}
            </Typography>
            <Button
              disabled={!connected || readBlocked || busy}
              onClick={() => {
                if (slot === "selected") onOpenTopic(locator.topic);
                void controller.reload(slot);
              }}
            >
              {slot === "selected" ? "Reload selected" : "Load baseline"}
            </Button>
          </Stack>
        );
      })}
      {references.bookmarks.length > 0 ? (
        <Stack direction="row" sx={{ alignItems: "center", flexWrap: "wrap", gap: 1 }}>
          <TextField
            select
            size="small"
            label="Record bookmark"
            value={bookmark?.id ?? ""}
            disabled={busy}
            sx={{ minWidth: 180, flex: "1 1 180px" }}
            onChange={(event) => setBookmarkId(event.target.value)}
          >
            <MenuItem value="">Choose a bookmark</MenuItem>
            {references.bookmarks.map((entry) => (
              <MenuItem key={entry.id} value={entry.id}>
                {entry.name}
              </MenuItem>
            ))}
          </TextField>
          <Button
            disabled={bookmark === undefined || busy}
            onClick={() => {
              if (bookmark) onChoose(bookmark.locator, "selected");
            }}
          >
            Use as selection
          </Button>
          <Button
            disabled={bookmark === undefined || busy}
            onClick={() => {
              if (bookmark) onChoose(bookmark.locator, "comparison");
            }}
          >
            Use as baseline
          </Button>
        </Stack>
      ) : null}
      {!connected || readBlocked ? (
        <Typography variant="caption">
          Connect the saved cluster and stop the current read or probe before reloading. Choosing a
          position never reads it.
        </Typography>
      ) : null}
      {error ? <Alert severity="error">{error}</Alert> : null}
    </Stack>
  );
}
