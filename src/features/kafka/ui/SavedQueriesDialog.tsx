import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  type HostCommand,
  type KafkaInvestigationQuery,
  type KafkaQueryLibrarySnapshot,
  type KafkaSavedQuery,
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

type QueryCommand = Extract<
  HostCommand,
  { readonly command: "queries.list" | "queries.put" | "queries.delete" }
>;

import { QueryTransferControls } from "./QueryTransferControls";
import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";

export function SavedQueriesDialog({
  host,
  profiles,
  currentTopic,
  readActive,
  captureCurrent,
  onRestore,
  onClose,
  transfer = browserTextDocumentTransfer,
  initialImport,
}: {
  readonly host: StreamSkopeHost;
  readonly profiles: readonly ProfileSummary[];
  readonly currentTopic: string | null;
  readonly readActive: boolean;
  readonly captureCurrent: () => KafkaInvestigationQuery;
  readonly onRestore: (query: KafkaInvestigationQuery, profileId: string | undefined) => void;
  readonly onClose: () => void;
  readonly transfer?: TextDocumentTransferPort | undefined;
  readonly initialImport?: string | undefined;
}): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<KafkaQueryLibrarySnapshot>();
  const [selectedId, setSelectedId] = useState("");
  const [name, setName] = useState("");
  const [profileId, setProfileId] = useState("");
  const [error, setError] = useState<string>();
  const [status, setStatus] = useState("");
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const alive = useRef(true);
  const selected = snapshot?.queries.find((query) => query.id === selectedId);
  const profileMissing =
    profileId.length > 0 && !profiles.some((profile) => profile.id === profileId);
  const nameUsed =
    snapshot?.queries.some((query) => query.name.toLowerCase() === name.trim().toLowerCase()) ??
    false;

  useEffect(() => {
    let current = true;
    alive.current = true;
    setBusy(true);
    void host
      .execute({
        command: "queries.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      })
      .then((response) => {
        if (!current) return;
        if (response.ok) setSnapshot(response.result.snapshot);
        else setError(response.error.summary);
      })
      .catch(() => {
        if (current) setError("Saved queries could not be loaded. Reopen this dialog to retry.");
      })
      .finally(() => {
        if (current) setBusy(false);
      });
    return (): void => {
      current = false;
      alive.current = false;
    };
  }, [host]);

  async function execute(command: QueryCommand): Promise<boolean> {
    setBusy(true);
    setError(undefined);
    setStatus("");
    try {
      const response = await host.execute(command);
      if (!alive.current) return false;
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return false;
      }
      setSnapshot(response.result.snapshot);
      return true;
    } catch {
      if (alive.current)
        setError("The host did not confirm the saved-query operation. Refresh before retrying.");
      return false;
    } finally {
      if (alive.current) setBusy(false);
    }
  }

  async function save(replace: boolean): Promise<void> {
    try {
      const query: KafkaSavedQuery = {
        id: replace && selected !== undefined ? selected.id : globalThis.crypto.randomUUID(),
        name,
        ...(profileId.length === 0 ? {} : { profileId }),
        configuration: captureCurrent(),
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
        setStatus("Query saved.");
      }
    } catch (failure) {
      setError(
        failure instanceof HostContractValidationError
          ? failure.message
          : "Choose a topic and valid query settings before saving.",
      );
    }
  }

  return (
    <Dialog open onClose={onClose} fullWidth maxWidth="sm" aria-labelledby="saved-query-title">
      <DialogTitle id="saved-query-title">Saved queries</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ pt: 1 }}>
          <Typography variant="body2">
            Save topic, read bounds, filters and limits. Opening restores the controls; choose a
            connection and run the read explicitly.
          </Typography>
          {snapshot?.durability === "session" ? (
            <Alert severity="info">
              Browser development keeps queries until the host restarts. The desktop app saves them
              across restarts.
            </Alert>
          ) : null}
          {error === undefined ? null : <Alert severity="error">{error}</Alert>}
          {status.length === 0 ? null : <Typography role="status">{status}</Typography>}
          <TextField
            select
            label="Saved query"
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
            <MenuItem value="">Choose a saved query</MenuItem>
            {snapshot?.queries.map((query) => (
              <MenuItem key={query.id} value={query.id}>
                {query.name}
              </MenuItem>
            ))}
          </TextField>
          <TextField
            label="Query name"
            value={name}
            disabled={busy}
            onChange={(event) => setName(event.target.value)}
            slotProps={{ htmlInput: { maxLength: 128 } }}
            helperText={
              nameUsed
                ? "This name is already saved. Replace the selected query or choose a new name."
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
          <Typography variant="caption">
            {currentTopic === null
              ? "Choose a topic to save a new query."
              : `Current topic: ${currentTopic}`}
          </Typography>
          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
            <Button
              disabled={
                busy ||
                snapshot === undefined ||
                currentTopic === null ||
                name.trim().length === 0 ||
                nameUsed ||
                profileMissing ||
                snapshot.queries.length >= 100
              }
              onClick={() => void save(false)}
            >
              Save current as new
            </Button>
            <Button
              disabled={
                busy ||
                selected === undefined ||
                currentTopic === null ||
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
              Delete saved query “{selected.name}”? This removes only its saved settings.
              <Stack direction="row" spacing={1}>
                <Button disabled={busy} onClick={() => setConfirmDelete(false)}>
                  Keep query
                </Button>
                <Button
                  disabled={busy}
                  onClick={() => {
                    void execute({
                      command: "queries.delete",
                      id: globalThis.crypto.randomUUID(),
                      payload: { id: selected.id },
                      version: HOST_PROTOCOL_VERSION,
                    }).then((done) => {
                      if (done) {
                        setSelectedId("");
                        setConfirmDelete(false);
                        setStatus("Query deleted.");
                      }
                    });
                  }}
                >
                  Delete query
                </Button>
              </Stack>
            </Alert>
          ) : null}
          {readActive ? (
            <Typography variant="caption">
              Stop the current read before opening another query.
            </Typography>
          ) : null}
          <QueryTransferControls
            transfer={transfer}
            captureExport={() => selected?.configuration ?? captureCurrent()}
            profiles={profiles}
            readActive={readActive}
            onRestore={onRestore}
            initialImport={initialImport}
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button
          disabled={busy}
          onClick={() =>
            void execute({
              command: "queries.list",
              id: globalThis.crypto.randomUUID(),
              payload: {},
              version: HOST_PROTOCOL_VERSION,
            })
          }
        >
          Refresh queries
        </Button>
        <Button onClick={onClose}>Close</Button>
        <Button
          variant="contained"
          disabled={busy || selected === undefined || profileMissing || readActive}
          onClick={() => {
            if (selected !== undefined)
              onRestore(selected.configuration, profileId.length === 0 ? undefined : profileId);
          }}
        >
          Open query
        </Button>
      </DialogActions>
    </Dialog>
  );
}
