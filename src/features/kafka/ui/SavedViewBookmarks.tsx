import { useEffect, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HostContractValidationError,
  parseKafkaSavedView,
  type KafkaSavedView,
} from "../contracts";
import {
  KAFKA_RECORD_LOCATOR_LIMITS,
  sameKafkaRecordLocator,
  type KafkaRecordLocator,
} from "../contracts/record-locator";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";

import type { KafkaViewSettings } from "./investigation-view-settings";
import { recordPositionLabel } from "./SavedRecordPositions";

/** Changes positions in one reviewed view; the existing library remains the sole writer. */
export function SavedViewBookmarks({
  selected,
  candidate,
  busy,
  libraryCount,
  newViewAllowed,
  newViewName,
  profileId,
  captureCurrent,
  write,
  onRestore,
  readActive,
}: {
  readonly selected: KafkaSavedView | undefined;
  readonly candidate: KafkaRecordLocator | undefined;
  readonly busy: boolean;
  readonly libraryCount: number;
  readonly newViewAllowed: boolean;
  readonly newViewName: string;
  readonly profileId: string | undefined;
  readonly captureCurrent: () => KafkaViewSettings;
  readonly write: (
    view: KafkaSavedView,
    expected: KafkaSavedView | null,
    status: string,
  ) => Promise<boolean>;
  readonly onRestore: (settings: KafkaViewSettings, profileId: string | undefined) => void;
  readonly readActive: boolean;
}): React.JSX.Element | null {
  const [candidateName, setCandidateName] = useState("");
  const [bookmarkId, setBookmarkId] = useState("");
  const [name, setName] = useState("");
  const [error, setError] = useState<string>();
  const bookmark = selected?.records.bookmarks.find((entry) => entry.id === bookmarkId);
  const existing =
    candidate === undefined
      ? undefined
      : selected?.records.bookmarks.find((entry) =>
          sameKafkaRecordLocator(entry.locator, candidate),
        );
  const atCapacity =
    existing === undefined &&
    ((selected?.records.bookmarks.length ?? 0) >= KAFKA_RECORD_LOCATOR_LIMITS.bookmarksPerView ||
      libraryCount >= KAFKA_RECORD_LOCATOR_LIMITS.bookmarksPerLibrary);
  const cluster =
    selected?.records.selected?.clusterId ??
    selected?.records.comparison?.clusterId ??
    selected?.records.bookmarks[0]?.locator.clusterId;
  const wrongCluster =
    candidate !== undefined && cluster !== undefined && cluster !== candidate.clusterId;
  useEffect(() => {
    setCandidateName(
      existing?.name ??
        (candidate === undefined
          ? ""
          : recordPositionLabel(candidate).slice(0, KAFKA_RECORD_LOCATOR_LIMITS.nameCharacters)),
    );
    setError(undefined);
  }, [candidate, existing?.name, selected?.id]);
  useEffect(() => {
    setBookmarkId("");
    setName("");
    setError(undefined);
  }, [selected?.id]);

  async function saveCandidate(): Promise<void> {
    if (candidate === undefined || atCapacity || wrongCluster) return;
    try {
      setError(undefined);
      const target = selected ?? {
        id: crypto.randomUUID(),
        name: newViewName,
        ...(profileId === undefined ? {} : { profileId }),
        ...captureCurrent(),
      };
      const match = target.records.bookmarks.find((entry) =>
        sameKafkaRecordLocator(entry.locator, candidate),
      );
      const next = {
        id: match?.id ?? crypto.randomUUID(),
        name: candidateName,
        locator: candidate,
      };
      const view = parseKafkaSavedView({
        ...target,
        records: {
          ...target.records,
          bookmarks:
            match === undefined
              ? [...target.records.bookmarks, next]
              : target.records.bookmarks.map((entry) => (entry.id === match.id ? next : entry)),
        },
      });
      if (
        await write(
          view,
          selected ?? null,
          match === undefined
            ? "Bookmark saved. Open the view to use its positions."
            : "Bookmark updated.",
        )
      ) {
        setBookmarkId(next.id);
        setName(next.name);
      }
    } catch (failure) {
      setError(
        failure instanceof HostContractValidationError
          ? failure.message
          : "The bookmark could not be saved. Review the selected view and retry.",
      );
    }
  }
  async function changeBookmark(remove: boolean): Promise<void> {
    if (selected === undefined || bookmark === undefined) return;
    try {
      setError(undefined);
      const view = parseKafkaSavedView({
        ...selected,
        records: {
          ...selected.records,
          bookmarks: remove
            ? selected.records.bookmarks.filter((entry) => entry.id !== bookmark.id)
            : selected.records.bookmarks.map((entry) =>
                entry.id === bookmark.id ? { ...entry, name } : entry,
              ),
        },
      });
      if (
        await write(
          view,
          selected,
          remove ? "Bookmark removed. Its record remains in Kafka." : "Bookmark renamed.",
        )
      ) {
        if (remove) {
          setBookmarkId("");
          setName("");
        }
      }
    } catch (failure) {
      setError(
        failure instanceof HostContractValidationError
          ? failure.message
          : "The bookmark could not be changed. Refresh the view and retry.",
      );
    }
  }
  function openPosition(locator: KafkaRecordLocator): void {
    if (selected === undefined) return;
    onRestore(
      {
        configuration: {
          schemaVersion: 1,
          request: { topic: locator.topic, mode: "earliest", maxMessages: 1 },
        },
        view: { ...selected.view, destination: { kind: "topic", workspace: "messages" } },
        records: { ...selected.records, selected: locator },
      },
      profileId,
    );
  }
  if (
    candidate === undefined &&
    (selected?.records.bookmarks.length ?? 0) === 0 &&
    !selected?.records.selected &&
    !selected?.records.comparison
  )
    return null;
  return (
    <Stack
      component="section"
      aria-label="Saved view bookmarks"
      spacing={1}
      sx={{ borderTop: 1, borderColor: "divider", pt: 1 }}
    >
      <Typography variant="subtitle2">Record positions and bookmarks</Typography>
      <Typography variant="caption">
        {selected?.records.bookmarks.length ?? 0} / {KAFKA_RECORD_LOCATOR_LIMITS.bookmarksPerView}{" "}
        in this view · {libraryCount} / {KAFKA_RECORD_LOCATOR_LIMITS.bookmarksPerLibrary} in the
        library. Only positions are saved.
      </Typography>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {(["selected", "comparison"] as const).map((slot) => {
        const position = selected?.records[slot];
        return position ? (
          <Stack key={slot} direction="row" sx={{ alignItems: "center", flexWrap: "wrap", gap: 1 }}>
            <Typography variant="body2" sx={{ flex: "1 1 200px", overflowWrap: "anywhere" }}>
              {slot === "selected" ? "Selected" : "Baseline"}: {recordPositionLabel(position)}
            </Typography>
            <Button disabled={busy || readActive} onClick={() => openPosition(position)}>
              {slot === "selected" ? "Open selected record topic" : "Open baseline record topic"}
            </Button>
          </Stack>
        ) : null;
      })}
      {candidate !== undefined ? (
        <>
          <Typography variant="body2" sx={{ overflowWrap: "anywhere" }}>
            Save: {recordPositionLabel(candidate)}
          </Typography>
          <TextField
            label="New bookmark name"
            value={candidateName}
            disabled={busy}
            onChange={(event) => setCandidateName(event.target.value)}
            slotProps={{ htmlInput: { maxLength: KAFKA_RECORD_LOCATOR_LIMITS.nameCharacters } }}
          />
          {wrongCluster ? (
            <Alert severity="warning">
              This view contains positions from another cluster. Choose or create a view for this
              Kafka cluster.
            </Alert>
          ) : null}
          {atCapacity ? (
            <Alert severity="info">
              Bookmark capacity reached. Remove a bookmark before saving a new position. Existing
              bookmarks can still be renamed.
            </Alert>
          ) : null}
          <Typography variant="caption">
            {selected
              ? `Adds to “${selected.name}” and preserves its settings and other positions.`
              : "Enter a new View name above to save the current view and bookmark together."}
          </Typography>
          <Button
            disabled={
              busy ||
              candidateName.trim().length === 0 ||
              atCapacity ||
              wrongCluster ||
              (selected === undefined && !newViewAllowed)
            }
            onClick={() => void saveCandidate()}
          >
            {existing ? "Update bookmark" : "Save bookmark"}
          </Button>
        </>
      ) : null}
      {(selected?.records.bookmarks.length ?? 0) > 0 ? (
        <>
          <TextField
            select
            label="Saved bookmark"
            value={bookmark?.id ?? ""}
            disabled={busy}
            onChange={(event) => {
              const next = selected?.records.bookmarks.find(
                (entry) => entry.id === event.target.value,
              );
              setBookmarkId(next?.id ?? "");
              setName(next?.name ?? "");
              setError(undefined);
            }}
          >
            <MenuItem value="">Choose a saved bookmark</MenuItem>
            {selected?.records.bookmarks.map((entry) => (
              <MenuItem key={entry.id} value={entry.id}>
                {entry.name}
              </MenuItem>
            ))}
          </TextField>
          {bookmark ? (
            <>
              <Typography variant="body2" sx={{ overflowWrap: "anywhere" }}>
                {recordPositionLabel(bookmark.locator)}
              </Typography>
              <TextField
                label="Bookmark name"
                value={name}
                disabled={busy}
                onChange={(event) => setName(event.target.value)}
                slotProps={{ htmlInput: { maxLength: KAFKA_RECORD_LOCATOR_LIMITS.nameCharacters } }}
              />
              <Stack direction="row" sx={{ flexWrap: "wrap", gap: 1 }}>
                <Button
                  disabled={busy || name.trim().length === 0}
                  onClick={() => void changeBookmark(false)}
                >
                  Rename bookmark
                </Button>
                <Button disabled={busy} onClick={() => void changeBookmark(true)}>
                  Remove bookmark
                </Button>
                <Button
                  disabled={busy || readActive}
                  onClick={() => openPosition(bookmark.locator)}
                >
                  Open bookmarked topic
                </Button>
              </Stack>
              <Typography variant="caption">
                Opening restores this position without reading. Use Reload selected to retrieve it
                with current protection.
              </Typography>
            </>
          ) : null}
        </>
      ) : null}
    </Stack>
  );
}
