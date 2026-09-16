import { useEffect, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import List from "@mui/material/List";
import ListItemText from "@mui/material/ListItemText";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  type HostCommand,
  type SchemaCompatibilitySnapshot,
  type SchemaRegistryDetailSnapshot,
  type SchemaRegistryInventorySnapshot,
  type SchemaRegistryType,
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

import { ResourcePageHeader } from "./ResourcePageHeader";
import { WorkbenchIcon } from "./WorkbenchIcons";

interface SchemaRegistryPageProperties {
  readonly compatibility: SchemaCompatibilitySnapshot | null;
  readonly connected: boolean;
  readonly detail: SchemaRegistryDetailSnapshot;
  readonly host: StreamSkopeHost;
  readonly inventory: SchemaRegistryInventorySnapshot;
}

export function SchemaRegistryPage({
  compatibility,
  connected,
  detail,
  host,
  inventory,
}: SchemaRegistryPageProperties): React.JSX.Element {
  const [filter, setFilter] = useState("");
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [registerOpen, setRegisterOpen] = useState(false);
  const [subject, setSubject] = useState("");
  const [schemaType, setSchemaType] = useState<SchemaRegistryType>("AVRO");
  const [schema, setSchema] = useState("");
  const [references, setReferences] = useState("[]");
  const [requestError, setRequestError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [deletion, setDeletion] = useState<{
    readonly kind: "subject" | "version";
    readonly version?: number;
  } | null>(null);
  const [confirmation, setConfirmation] = useState("");
  const [deletionMode, setDeletionMode] = useState<"permanent" | "soft">("soft");

  const execute = async (command: HostCommand): Promise<boolean> => {
    setBusy(true);
    setRequestError(undefined);
    try {
      const response = await host.execute(command);
      if (!response.ok) {
        setRequestError(`${response.error.summary} ${response.error.recovery}`);
        return false;
      }
      return true;
    } catch {
      setRequestError(
        "The application host did not accept the Schema Registry request. Open Activity for diagnostics.",
      );
      return false;
    } finally {
      setBusy(false);
    }
  };

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
  const registrationCommand = (
    command: "schemas.compatibility.check" | "schemas.register",
  ): HostCommand | null => {
    try {
      const parsedReferences = JSON.parse(references) as unknown;
      return parseHostCommand({
        command,
        id: globalThis.crypto.randomUUID(),
        payload:
          command === "schemas.register"
            ? {
                normalize: true,
                references: parsedReferences,
                schema,
                schemaType,
                subject: subject.trim(),
                version: "latest",
              }
            : {
                references: parsedReferences,
                schema,
                schemaType,
                subject: subject.trim(),
                version: "latest",
              },
        version: HOST_PROTOCOL_VERSION,
      });
    } catch (error) {
      setRequestError(
        error instanceof Error
          ? `Schema registration input is invalid: ${error.message}`
          : "Schema registration input is invalid.",
      );
      return null;
    }
  };

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
              onClick={() => setRegisterOpen(true)}
              startIcon={<WorkbenchIcon name="add" />}
              variant="contained"
            >
              Register schema
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

      <Dialog
        fullWidth
        maxWidth="md"
        onClose={() => !busy && setRegisterOpen(false)}
        open={registerOpen}
      >
        <DialogTitle>Register schema version</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ pt: 0.5 }}>
            <TextField
              autoFocus
              fullWidth
              label="Subject"
              onChange={(event) => setSubject(event.target.value)}
              value={subject}
            />
            <FormControl fullWidth>
              <InputLabel id="new-schema-type-label">Schema type</InputLabel>
              <Select
                label="Schema type"
                labelId="new-schema-type-label"
                onChange={(event) => setSchemaType(event.target.value)}
                value={schemaType}
              >
                <MenuItem value="AVRO">Avro</MenuItem>
                <MenuItem value="JSON">JSON Schema</MenuItem>
                <MenuItem value="PROTOBUF">Protobuf</MenuItem>
              </Select>
            </FormControl>
            <TextField
              fullWidth
              label="Schema"
              minRows={14}
              multiline
              onChange={(event) => setSchema(event.target.value)}
              value={schema}
            />
            <TextField
              fullWidth
              helperText='JSON array: [{"name":"Customer","subject":"customer-value","version":2}]'
              label="References"
              minRows={3}
              multiline
              onChange={(event) => setReferences(event.target.value)}
              value={references}
            />
            {compatibility?.subject === subject.trim() ? (
              <Alert severity={compatibility.compatible ? "success" : "warning"}>
                {compatibility.compatible
                  ? "Compatible with the latest registered version."
                  : compatibility.messages.join(" ") || "The proposed schema is incompatible."}
              </Alert>
            ) : null}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setRegisterOpen(false)}>
            Cancel
          </Button>
          <Button
            disabled={busy || subject.trim().length === 0 || schema.length === 0}
            onClick={() => {
              const command = registrationCommand("schemas.compatibility.check");
              if (command !== null) void execute(command);
            }}
            variant="outlined"
          >
            Check compatibility
          </Button>
          <Button
            disabled={
              busy ||
              subject.trim().length === 0 ||
              schema.length === 0 ||
              compatibility?.subject !== subject.trim() ||
              !compatibility.compatible
            }
            onClick={() => {
              const command = registrationCommand("schemas.register");
              if (command !== null)
                void execute(command).then((ok) => {
                  if (ok) {
                    setRegisterOpen(false);
                    setSelectedSubject(subject.trim());
                  }
                });
            }}
            variant="contained"
          >
            Register
          </Button>
        </DialogActions>
      </Dialog>

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
