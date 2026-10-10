import { useEffect, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import List from "@mui/material/List";
import ListItemText from "@mui/material/ListItemText";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import {
  HOST_PROTOCOL_VERSION,
  type SchemaRegistryDetailSnapshot,
  type SchemaRegistryInventorySnapshot,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioListItemButton as ListItemButton,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

import { SchemaEvolutionDialog } from "./SchemaEvolutionDialog";
import { SchemaClientPanel } from "./SchemaClientPanel";
import { SchemaSamplesPanel } from "./SchemaSamplesPanel";
import { SchemaAuthorPanel } from "./SchemaAuthorPanel";
import { SchemaHistoryPanel } from "./SchemaHistoryPanel";
import { ResourcePageHeader } from "./ResourcePageHeader";
import { WorkbenchIcon } from "./WorkbenchIcons";
import { useHostCommand } from "./use-host-command";

interface SchemaRegistryPageProperties {
  readonly compatibility: import("../contracts").SchemaCompatibilitySnapshot | null;
  readonly connected: boolean;
  readonly detail: SchemaRegistryDetailSnapshot;
  readonly host: StreamSkopeHost;
  readonly inventory: SchemaRegistryInventorySnapshot;
}

export function SchemaRegistryPage({
  connected,
  detail,
  host,
  inventory,
}: SchemaRegistryPageProperties): React.JSX.Element {
  const [filter, setFilter] = useState("");
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [registerOpen, setRegisterOpen] = useState(false);
  const [evolution, setEvolution] = useState<import("../contracts").SchemaVersionDetail | null>(
    null,
  );
  const {
    busy,
    requestError,
    run: execute,
  } = useHostCommand(
    host,
    "The application host did not accept the Schema Registry request. Open Activity for diagnostics.",
  );
  const [deletion, setDeletion] = useState<{
    readonly kind: "subject" | "version";
    readonly version?: number;
  } | null>(null);
  const [confirmation, setConfirmation] = useState("");
  const [deletionMode, setDeletionMode] = useState<"permanent" | "soft">("soft");

  const refresh = (): void => {
    void execute({
      command: "schemas.list",
      id: globalThis.crypto.randomUUID(),
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
  };

  useEffect(() => {
    if (connected) refresh();
    else setSelectedSubject(null);
    // The active connection owns this refresh lifecycle.
  }, [connected, inventory.connectionName]);

  const selectSubject = (next: string, version: number | "latest" = "latest"): void => {
    setSelectedSubject(next);
    void execute({
      command: "schemas.load",
      id: globalThis.crypto.randomUUID(),
      payload: { subject: next, version },
      version: HOST_PROTOCOL_VERSION,
    });
  };
  const normalized = filter.trim().toLocaleLowerCase("en-US");
  const subjects = useMemo(
    () =>
      inventory.subjects.filter(
        (candidate) =>
          normalized.length === 0 || candidate.toLocaleLowerCase("en-US").includes(normalized),
      ),
    [inventory.subjects, normalized],
  );
  const selectedSchema = detail.subject === selectedSubject ? detail.schema : null;
  const expectedConfirmation =
    deletion?.kind === "version" && deletion.version !== undefined
      ? `${selectedSubject ?? ""}@${String(deletion.version)}`
      : (selectedSubject ?? "");

  return (
    <Box
      component="main"
      sx={{ display: "grid", gridTemplateRows: "auto minmax(0, 1fr)", minHeight: 0 }}
    >
      <ResourcePageHeader
        action={
          <Stack direction="row" spacing={1}>
            <Button
              disabled={!connected || busy}
              onClick={() => {
                setEvolution(null);
                setRegisterOpen(true);
              }}
              startIcon={<WorkbenchIcon name="add" />}
              variant="contained"
            >
              Create subject
            </Button>
            <Button
              disabled={!connected || busy}
              onClick={refresh}
              startIcon={<WorkbenchIcon name="refresh" />}
              variant="outlined"
            >
              Refresh
            </Button>
          </Stack>
        }
        description="Browse, validate, register, and explicitly delete versioned schemas."
        title="Schema Registry"
      />
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: "minmax(260px, 30%) minmax(0, 1fr)",
          minHeight: 0,
        }}
      >
        <Box
          sx={{
            bgcolor: "background.paper",
            borderRight: 1,
            borderColor: "divider",
            minHeight: 0,
            overflow: "auto",
            p: 2,
          }}
        >
          <TextField
            fullWidth
            onChange={(event) => setFilter(event.target.value)}
            placeholder="Search subjects"
            slotProps={{ htmlInput: { "aria-label": "Search schema subjects", type: "search" } }}
            value={filter}
          />
          <Typography color="text.secondary" sx={{ my: 1 }} variant="caption">
            {subjects.length.toLocaleString()} subjects
          </Typography>
          {inventory.state === "not-configured" ? (
            <Alert severity="info">
              Configure a Schema Registry URL in the active connection profile.
            </Alert>
          ) : null}
          {inventory.state === "loading" ? (
            <Typography role="status">Loading schema subjects…</Typography>
          ) : null}
          {inventory.state === "empty" ? (
            <Typography color="text.secondary">
              No schema subjects exist in this Registry.
            </Typography>
          ) : null}
          {inventory.state === "invalid-response" ? (
            <Alert severity="error">
              The Schema Registry response is unsupported or exceeds supported limits.
            </Alert>
          ) : null}
          {inventory.error === undefined ? null : (
            <Alert severity="error">
              {inventory.error.summary} {inventory.error.recovery}
            </Alert>
          )}
          <List dense disablePadding aria-label="Schema subjects">
            {subjects.map((candidate) => (
              <ListItemButton
                key={candidate}
                onClick={() => selectSubject(candidate)}
                selected={candidate === selectedSubject}
              >
                <ListItemText
                  primary={
                    <Typography noWrap title={candidate} variant="body2">
                      {candidate}
                    </Typography>
                  }
                />
              </ListItemButton>
            ))}
          </List>
        </Box>
        <Box sx={{ minHeight: 0, overflow: "auto", p: { md: 4, xs: 2 } }}>
          {requestError === undefined ? null : (
            <Alert severity="error" sx={{ mb: 2 }}>
              {requestError}
            </Alert>
          )}
          {selectedSubject === null ? (
            <Typography color="text.secondary">
              Select a schema subject to inspect its latest version.
            </Typography>
          ) : (
            <Stack spacing={2}>
              <Stack direction="row" sx={{ alignItems: "center", gap: 2 }}>
                <Box sx={{ flex: 1 }}>
                  <Typography component="h2" variant="h6">
                    {selectedSubject}
                  </Typography>
                  <Typography color="text.secondary" variant="caption">
                    Compatibility: {detail.compatibilityLevel ?? "Unavailable"}
                  </Typography>
                </Box>
                <FormControl sx={{ minWidth: 130 }}>
                  <InputLabel id="schema-version-label">Version</InputLabel>
                  <Select
                    label="Version"
                    labelId="schema-version-label"
                    onChange={(event) => selectSubject(selectedSubject, Number(event.target.value))}
                    value={selectedSchema?.version ?? ""}
                  >
                    {detail.versions.map((version) => (
                      <MenuItem key={version} value={version}>
                        {version}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Stack>
              {detail.state === "loading" ? (
                <Typography role="status">Loading schema…</Typography>
              ) : selectedSchema === null ? (
                <Alert severity="info">No schema version detail is available.</Alert>
              ) : (
                <>
                  <Stack direction="row" spacing={3}>
                    <Typography variant="body2">
                      Type <strong>{selectedSchema.schemaType}</strong>
                    </Typography>
                    <Typography variant="body2">
                      ID <strong>{selectedSchema.id}</strong>
                    </Typography>
                    <Typography variant="body2">
                      References <strong>{selectedSchema.references.length}</strong>
                    </Typography>
                  </Stack>
                  {selectedSchema.references.length === 0 ? null : (
                    <Box>
                      <Typography component="h3" variant="subtitle2">
                        References
                      </Typography>
                      <Stack spacing={0.5} sx={{ mt: 1 }}>
                        {selectedSchema.references.map((reference) => (
                          <Typography
                            key={`${reference.name}:${reference.subject}:${String(reference.version)}`}
                            variant="body2"
                          >
                            <strong>{reference.name}</strong> → {reference.subject}@
                            {reference.version}
                          </Typography>
                        ))}
                      </Stack>
                    </Box>
                  )}
                  <StudioCodeBlock
                    sx={{
                      bgcolor: "background.paper",
                      border: 1,
                      borderColor: "divider",
                      m: 0,
                      overflow: "auto",
                      p: 2,
                      whiteSpace: "pre-wrap",
                    }}
                  >
                    {selectedSchema.schema}
                  </StudioCodeBlock>
                  <Button
                    variant="outlined"
                    disabled={!connected || busy || detail.state !== "ready"}
                    onClick={() => {
                      setEvolution(selectedSchema);
                      setRegisterOpen(true);
                    }}
                  >
                    Evolve selected schema
                  </Button>
                  <SchemaClientPanel
                    schema={selectedSchema}
                    host={host}
                    enabled={connected && !busy && detail.state === "ready"}
                  />
                  <SchemaAuthorPanel
                    key={`author-${inventory.connectionName ?? ""}`}
                    schema={selectedSchema}
                    host={host}
                    enabled={connected && !busy && detail.state === "ready"}
                  />
                  <SchemaSamplesPanel
                    key={inventory.connectionName}
                    schema={selectedSchema}
                    host={host}
                    enabled={connected && !busy && detail.state === "ready"}
                  />
                  <SchemaHistoryPanel
                    schema={selectedSchema}
                    versions={detail.versions}
                    host={host}
                    enabled={connected && !busy && detail.state === "ready"}
                    onSelect={selectSubject}
                  />
                  <Stack direction="row" spacing={1}>
                    <Button
                      color="error"
                      onClick={() => {
                        setDeletion({ kind: "version", version: selectedSchema.version });
                        setDeletionMode("soft");
                        setConfirmation("");
                      }}
                      variant="outlined"
                    >
                      Delete version
                    </Button>
                    <Button
                      color="error"
                      onClick={() => {
                        setDeletion({ kind: "subject" });
                        setDeletionMode("soft");
                        setConfirmation("");
                      }}
                      variant="text"
                    >
                      Delete subject
                    </Button>
                  </Stack>
                </>
              )}
            </Stack>
          )}
        </Box>
      </Box>

      {registerOpen ? (
        <SchemaEvolutionDialog
          key={inventory.connectionName}
          host={host}
          initial={evolution}
          enabled={
            connected &&
            !busy &&
            (evolution === null ||
              (detail.state === "ready" &&
                selectedSchema?.id === evolution.id &&
                selectedSchema.version === evolution.version))
          }
          onClose={() => setRegisterOpen(false)}
          onRegistered={(next) => {
            setRegisterOpen(false);
            selectSubject(next);
          }}
        />
      ) : null}

      <Dialog
        fullWidth
        maxWidth="sm"
        onClose={() => !busy && setDeletion(null)}
        open={deletion !== null}
      >
        <DialogTitle>
          {deletion?.kind === "version" ? "Delete schema version" : "Delete schema subject"}
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Alert severity="warning">
              {deletionMode === "permanent" ? (
                <>
                  Permanently delete <strong>{expectedConfirmation}</strong>? This cannot be undone.
                </>
              ) : (
                <>
                  Soft-delete <strong>{expectedConfirmation}</strong>? It will be removed from
                  normal Registry listings.
                </>
              )}
            </Alert>
            <FormControl fullWidth>
              <InputLabel id="schema-deletion-mode-label">Deletion mode</InputLabel>
              <Select
                label="Deletion mode"
                labelId="schema-deletion-mode-label"
                onChange={(event) => setDeletionMode(event.target.value)}
                value={deletionMode}
              >
                <MenuItem value="soft">Soft delete</MenuItem>
                <MenuItem value="permanent">Permanent delete</MenuItem>
              </Select>
            </FormControl>
            <TextField
              autoFocus
              fullWidth
              label={`Type ${expectedConfirmation} to confirm`}
              onChange={(event) => setConfirmation(event.target.value)}
              value={confirmation}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setDeletion(null)}>
            Cancel
          </Button>
          <Button
            color="error"
            disabled={
              busy ||
              confirmation !== expectedConfirmation ||
              deletion === null ||
              selectedSubject === null
            }
            onClick={() => {
              if (deletion === null || selectedSubject === null) return;
              const target =
                deletion.kind === "subject"
                  ? { kind: "subject" as const, subject: selectedSubject }
                  : {
                      kind: "version" as const,
                      subject: selectedSubject,
                      version: deletion.version!,
                    };
              void execute({
                command: "schemas.delete",
                id: globalThis.crypto.randomUUID(),
                payload: { confirmation, mode: deletionMode, target },
                version: HOST_PROTOCOL_VERSION,
              }).then((ok) => {
                if (ok) {
                  setDeletion(null);
                  setSelectedSubject(null);
                }
              });
            }}
            variant="contained"
          >
            {deletionMode === "permanent" ? "Delete permanently" : "Soft delete"}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
