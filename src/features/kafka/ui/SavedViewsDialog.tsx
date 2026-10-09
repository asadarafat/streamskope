import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  type KafkaSavedView,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { useInvestigationLibrary } from "./use-investigation-library";
import { queryViewSettings, type KafkaViewSettings } from "./investigation-view-settings";
import { QueryTransferControls } from "./QueryTransferControls";
import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";

export function SavedViewsDialog({
  host,
  profiles,
  currentResource,
  currentQueryAvailable,
  readActive,
  captureCurrent,
  restoreError,
  onRestore,
  onClose,
  transfer = browserTextDocumentTransfer,
  initialImport,
}: {
  readonly host: StreamSkopeHost;
  readonly profiles: readonly ProfileSummary[];
  readonly currentResource: string | null;
  readonly currentQueryAvailable: boolean;
  readonly readActive: boolean;
  readonly captureCurrent: () => KafkaViewSettings;
  readonly onRestore: (settings: KafkaViewSettings, profileId: string | undefined) => void;
  readonly restoreError?: string | undefined;
  readonly onClose: () => void;
  readonly transfer?: TextDocumentTransferPort | undefined;
  readonly initialImport?: string | undefined;
}): React.JSX.Element {
  const { snapshot, busy, error: hostError, execute, refresh } = useInvestigationLibrary(host);
  const [selectedId, setSelectedId] = useState("");
  const [name, setName] = useState("");
  const [profileId, setProfileId] = useState("");
  const [error, setError] = useState<string>();
  const [status, setStatus] = useState("");
  const [confirmDelete, setConfirmDelete] = useState(false);
  const nameInput = useRef<HTMLInputElement>(null);
  const focusAfterWrite = useRef(false);
  const selected = snapshot?.queries.find((query) => query.id === selectedId);
  const profileMissing =
    profileId.length > 0 && !profiles.some((profile) => profile.id === profileId);
  const nameUsed =
    snapshot?.queries.some((query) => query.name.toLowerCase() === name.trim().toLowerCase()) ??
    false;

  useEffect(() => {
    if (!busy && focusAfterWrite.current) {
      focusAfterWrite.current = false;
      nameInput.current?.focus();
    }
  }, [busy, status]);

  async function save(replace: boolean): Promise<void> {
    try {
      focusAfterWrite.current = true;
      setError(undefined);
      setStatus("");
      const query: KafkaSavedView = {
        id: replace && selected !== undefined ? selected.id : globalThis.crypto.randomUUID(),
        name,
        ...(profileId.length === 0 ? {} : { profileId }),
        ...captureCurrent(),
      };
      if (
        await execute({
          command: "queries.put",
          payload: { query },
          id: globalThis.crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
        })
      ) {
        setSelectedId(query.id);
        setStatus("View saved.");
      }
    } catch (failure) {
      setError(
        failure instanceof HostContractValidationError
          ? failure.message
          : "Choose a topic or consumer group and valid view settings before saving.",
      );
    }
  }

  return (
    <Dialog open onClose={onClose} fullWidth maxWidth="sm" aria-labelledby="saved-view-title">
      <DialogTitle id="saved-view-title">Saved views</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ pt: 1 }}>
          <Typography variant="body2">
            Save a topic task or consumer group with its query settings, columns and layout. Opening
            restores the controls; connection and reads remain explicit actions.
          </Typography>
          {snapshot?.durability === "session" ? (
            <Alert severity="info">
              This host keeps views only until it restarts. Durable desktop and browser hosts save
              views across restarts.
            </Alert>
          ) : null}
          {error || hostError || restoreError ? (
            <Alert severity="error">{error ?? hostError ?? restoreError}</Alert>
          ) : null}
          {status.length === 0 ? null : <Typography role="status">{status}</Typography>}
          <TextField
            select
            label="Saved view"
            value={selected?.id ?? ""}
            disabled={busy}
            onChange={(event) => {
              const query = snapshot?.queries.find(
                (candidate) => candidate.id === event.target.value,
              );
              setSelectedId(query?.id ?? "");
              setName(query?.name ?? "");
              setProfileId(query?.profileId ?? "");
              setConfirmDelete(false);
              setError(undefined);
              setStatus("");
            }}
          >
            <MenuItem value="">Choose a saved view</MenuItem>
            {snapshot?.queries.map((query) => (
              <MenuItem key={query.id} value={query.id}>
                {query.name}
              </MenuItem>
            ))}
          </TextField>
          <TextField
            label="View name"
            inputRef={nameInput}
            value={name}
            disabled={busy}
            onChange={(event) => setName(event.target.value)}
            slotProps={{ htmlInput: { maxLength: 128 } }}
            helperText={
              nameUsed
                ? "This name is already saved. Replace the selected view or choose a new name."
                : "Use a name that describes the investigation."
            }
          />
          <TextField
            select
            label="Local connection profile"
            value={profileId}
            disabled={busy}
            onChange={(event) => setProfileId(event.target.value)}
          >
            <MenuItem value="">Choose a connection when opening</MenuItem>
            {profileMissing ? <MenuItem value={profileId}>Profile unavailable</MenuItem> : null}
            {profiles.map((profile) => (
              <MenuItem key={profile.id} value={profile.id}>
                {profile.name}
              </MenuItem>
            ))}
          </TextField>
          {profileMissing ? (
            <Alert severity="warning">
              The saved profile is unavailable. Choose an existing profile or clear the reference
              before opening.
            </Alert>
          ) : null}
          {selected && (
            <Typography component="section" aria-label="Selected view settings" variant="body2">
              {selected.view.destination.kind === "topic"
                ? `${selected.configuration!.request.topic} · ${selected.view.destination.workspace}`
                : `Consumer group ${selected.view.destination.groupId}`}
              {` · ${selected.view.messages.visibleColumns.length} visible columns · inspector ${selected.view.messages.inspectorWidth}px`}
            </Typography>
          )}
          <Typography variant="caption">
            Views save settings only. Records, bookmarks, probe/configuration drafts, encodings,
            protection policy and active jobs are not restored. Current host protection still
            applies.
          </Typography>
          <Typography variant="caption">
            {currentResource === null
              ? "Choose a topic task or consumer group to save a view."
              : `Current resource: ${currentResource}`}
          </Typography>
          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
            <Button
              disabled={
                busy ||
                snapshot === undefined ||
                currentResource === null ||
                name.trim().length === 0 ||
                nameUsed ||
                profileMissing ||
                snapshot.queries.length >= 100
              }
              onClick={() => void save(false)}
            >
              Save current view
            </Button>
            <Button
              disabled={
                busy ||
                selected === undefined ||
                currentResource === null ||
                name.trim().length === 0 ||
                profileMissing
              }
              onClick={() => void save(true)}
            >
              Replace selected
            </Button>
            <Button
              disabled={busy || selected === undefined}
              onClick={() => setConfirmDelete(true)}
            >
              Delete selected
            </Button>
          </Stack>
          {confirmDelete && selected !== undefined ? (
            <Alert severity="warning">
              Delete saved view “{selected.name}”? This removes only its saved settings.
              <Stack direction="row" spacing={1}>
                <Button disabled={busy} onClick={() => setConfirmDelete(false)}>
                  Keep view
                </Button>
                <Button
                  disabled={busy}
                  onClick={() => {
                    focusAfterWrite.current = true;
                    void execute({
                      command: "queries.delete",
                      id: globalThis.crypto.randomUUID(),
                      payload: { id: selected.id },
                      version: HOST_PROTOCOL_VERSION,
                    }).then((done) => {
                      if (done) {
                        setSelectedId("");
                        setConfirmDelete(false);
                        setStatus("View deleted.");
                      }
                    });
                  }}
                >
                  Delete view
                </Button>
              </Stack>
            </Alert>
          ) : null}
          {readActive ? (
            <Typography variant="caption">
              Stop the current read or latency probe before opening another view.
            </Typography>
          ) : null}
          <details open={initialImport !== undefined}>
            <Typography component="summary" variant="body2">
              Import/share query settings
            </Typography>
            <QueryTransferControls
              transfer={transfer}
              captureExport={() =>
                selected ? selected.configuration : captureCurrent().configuration
              }
              exportAvailable={selected ? selected.configuration !== null : currentQueryAvailable}
              profiles={profiles}
              readActive={readActive}
              onRestore={(query, id) => onRestore(queryViewSettings(query), id)}
              initialImport={initialImport}
            />
          </details>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={busy} onClick={() => void refresh()}>
          Refresh views
        </Button>
        <Button onClick={onClose}>Close</Button>
        <Button
          variant="contained"
          disabled={busy || selected === undefined || profileMissing || readActive}
          onClick={() => {
            if (selected !== undefined)
              onRestore(
                { configuration: selected.configuration, view: selected.view },
                profileId.length === 0 ? undefined : profileId,
              );
          }}
        >
          Open view
        </Button>
      </DialogActions>
    </Dialog>
  );
}
