import { useEffect, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import List from "@mui/material/List";
import ListItemText from "@mui/material/ListItemText";
import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type RedpandaTransformDetailSnapshot,
  type RedpandaTransformInventorySnapshot,
  type RedpandaTransformLogsSnapshot,
  type StreamSkopeHost,
} from "../contracts";
import { StudioTechnicalText } from "../../ui/StudioCodeBlock";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioListItemButton as ListItemButton,
  StudioTextField as TextField,
} from "../../ui/controls";

import { ResourcePageHeader } from "./ResourcePageHeader";
import { WorkbenchIcon } from "./WorkbenchIcons";

export function TransformsPage({
  connected,
  detail,
  host,
  inventory,
  logs,
}: {
  readonly connected: boolean;
  readonly detail: RedpandaTransformDetailSnapshot;
  readonly host: StreamSkopeHost;
  readonly inventory: RedpandaTransformInventorySnapshot;
  readonly logs: RedpandaTransformLogsSnapshot;
}): React.JSX.Element {
  const [filter, setFilter] = useState("");
  const [selectedName, setSelectedName] = useState<string | null>(null);
  const [requestError, setRequestError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmation, setConfirmation] = useState("");
  const run = async (command: HostCommand): Promise<boolean> => {
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
        "The application host did not accept the transform request. Open Activity for diagnostics.",
      );
      return false;
    } finally {
      setBusy(false);
    }
  };
  const refresh = (): void => {
    void run({
      command: "transforms.list",
      id: globalThis.crypto.randomUUID(),
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
  };
  useEffect(() => {
    if (connected) refresh();
    else setSelectedName(null);
  }, [connected, inventory.connectionName]);
  const select = async (name: string): Promise<void> => {
    setSelectedName(name);
    const loaded = await run({
      command: "transforms.load",
      id: globalThis.crypto.randomUUID(),
      payload: { name },
      version: HOST_PROTOCOL_VERSION,
    });
    if (loaded)
      await run({
        command: "transforms.logs.load",
        id: globalThis.crypto.randomUUID(),
        payload: { name },
        version: HOST_PROTOCOL_VERSION,
      });
  };
  const normalized = filter.trim().toLocaleLowerCase("en-US");
  const transforms = useMemo(
    () =>
      inventory.transforms.filter(
        (transform) =>
          normalized.length === 0 ||
          transform.name.toLocaleLowerCase("en-US").includes(normalized) ||
          transform.inputTopic.toLocaleLowerCase("en-US").includes(normalized),
      ),
    [inventory.transforms, normalized],
  );
  const transform = detail.transformName === selectedName ? detail.transform : null;
  return (
    <Box
      component="main"
      sx={{ display: "grid", gridTemplateRows: "auto minmax(0, 1fr)", minHeight: 0 }}
    >
      <ResourcePageHeader
        action={
          <Button
            disabled={!connected || busy}
            onClick={refresh}
            startIcon={<WorkbenchIcon name="refresh" />}
            variant="outlined"
          >
            Refresh
          </Button>
        }
        description="Inspect deployed Redpanda data transforms, partition health, lag, and recent logs."
        title="Data Transforms"
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
            placeholder="Search transforms"
            slotProps={{ htmlInput: { "aria-label": "Search transforms", type: "search" } }}
            value={filter}
          />
          <Typography color="text.secondary" sx={{ my: 1 }} variant="caption">
            {inventory.state === "ready" ||
            inventory.state === "empty" ||
            inventory.state === "stale"
              ? `${transforms.length.toLocaleString()} transforms`
              : "Transform inventory unavailable"}
          </Typography>
          {inventory.state === "not-configured" ? (
            <Alert severity="info">
              Configure a Redpanda Admin URL in the active connection profile.
            </Alert>
          ) : null}
          {inventory.state === "unsupported" ? (
            <Alert severity="info">
              This cluster does not expose the supported Redpanda transform API.
            </Alert>
          ) : null}
          {inventory.state === "invalid-response" ? (
            <Alert severity="error">
              The Admin API returned unsupported or malformed transform metadata.
            </Alert>
          ) : null}
          {inventory.state === "empty" ? (
            <Typography color="text.secondary">No transforms are deployed.</Typography>
          ) : null}
          {inventory.error === undefined ? null : (
            <Alert severity="error">
              {inventory.error.summary} {inventory.error.recovery}
            </Alert>
          )}
          <List dense disablePadding>
            {transforms.map((candidate) => (
              <ListItemButton
                key={candidate.name}
                onClick={() => {
                  void select(candidate.name);
                }}
                selected={candidate.name === selectedName}
              >
                <ListItemText
                  primary={
                    <Typography noWrap variant="body2">
                      {candidate.name}
                    </Typography>
                  }
                  secondary={
                    <Typography color="text.secondary" noWrap variant="caption">
                      {candidate.aggregateStatus} · lag {candidate.maximumLag.toLocaleString()} ·{" "}
                      {candidate.inputTopic} → {candidate.outputTopics.join(", ")}
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
          {selectedName === null ? (
            <Typography color="text.secondary">
              Select a transform to inspect its deployment and logs.
            </Typography>
          ) : transform === null ? (
            <Typography role="status">
              {detail.state === "loading"
                ? "Loading transform…"
                : "Transform metadata is unavailable."}
            </Typography>
          ) : (
            <Stack spacing={3}>
              <Stack direction="row" sx={{ alignItems: "center", gap: 2 }}>
                <Box sx={{ flex: 1 }}>
                  <Typography component="h2" variant="h6">
                    {transform.name}
                  </Typography>
                  <Typography color="text.secondary" variant="body2">
                    {transform.inputTopic} → {transform.outputTopics.join(", ")}
                  </Typography>
                </Box>
                <Button
                  color="error"
                  onClick={() => {
                    setConfirmation("");
                    setDeleteOpen(true);
                  }}
                  variant="outlined"
                >
                  Delete transform
                </Button>
              </Stack>
              <Stack direction="row" spacing={4}>
                <Box>
                  <Typography color="text.secondary" variant="caption">
                    Aggregate state
                  </Typography>
                  <Typography>{transform.aggregateStatus}</Typography>
                </Box>
                <Box>
                  <Typography color="text.secondary" variant="caption">
                    Maximum lag
                  </Typography>
                  <Typography>{transform.maximumLag.toLocaleString()}</Typography>
                </Box>
                <Box>
                  <Typography color="text.secondary" variant="caption">
                    Compression
                  </Typography>
                  <Typography>{transform.compression}</Typography>
                </Box>
                <Box>
                  <Typography color="text.secondary" variant="caption">
                    Start offset
                  </Typography>
                  <Typography>
                    {transform.offset === null
                      ? "Default"
                      : `${transform.offset.format} ${transform.offset.value}`}
                  </Typography>
                </Box>
                <Box>
                  <Typography color="text.secondary" variant="caption">
                    Environment
                  </Typography>
                  <Typography>
                    {transform.environment.length === 0
                      ? "None"
                      : transform.environment
                          .map(
                            (variable) =>
                              `${variable.name} (${variable.valuePresent ? "set" : "empty"})`,
                          )
                          .join(", ")}
                  </Typography>
                </Box>
              </Stack>
              <Box>
                <Typography component="h3" variant="subtitle2">
                  Partition status
                </Typography>
                <TableContainer sx={{ border: 1, borderColor: "divider", mt: 1 }}>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Partition</TableCell>
                        <TableCell>Node</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell align="right">Lag</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {transform.statuses.map((status) => (
                        <TableRow key={`${String(status.partition)}:${String(status.nodeId)}`}>
                          <TableCell>{status.partition}</TableCell>
                          <TableCell>{status.nodeId}</TableCell>
                          <TableCell>{status.status}</TableCell>
                          <TableCell align="right">{status.lag.toLocaleString()}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
              <Box>
                <Stack direction="row" sx={{ alignItems: "center" }}>
                  <Typography component="h3" sx={{ flex: 1 }} variant="subtitle2">
                    Recent logs
                    {logs.omittedLogs === 0
                      ? ""
                      : ` · ${String(logs.omittedLogs)} older matching records omitted`}
                  </Typography>
                  <Button
                    disabled={busy}
                    onClick={() => {
                      void run({
                        command: "transforms.logs.load",
                        id: globalThis.crypto.randomUUID(),
                        payload: { name: transform.name },
                        version: HOST_PROTOCOL_VERSION,
                      });
                    }}
                    startIcon={<WorkbenchIcon name="refresh" />}
                    variant="text"
                  >
                    Refresh logs
                  </Button>
                </Stack>
                <Box
                  aria-label="Transform logs"
                  sx={{
                    bgcolor: "background.paper",
                    border: 1,
                    borderColor: "divider",
                    maxHeight: 280,
                    mt: 1,
                    overflow: "auto",
                  }}
                >
                  {logs.transformName !== transform.name || logs.logs.length === 0 ? (
                    <Typography color="text.secondary" sx={{ p: 2 }}>
                      {logs.state === "loading" ? "Loading logs…" : "No recent logs found."}
                    </Typography>
                  ) : (
                    logs.logs.map((entry) => (
                      <Box
                        key={`${String(entry.partition)}:${entry.offset}`}
                        sx={{
                          borderBottom: 1,
                          borderColor: "divider",
                          display: "grid",
                          gap: 1.5,
                          gridTemplateColumns: "140px 60px minmax(0, 1fr)",
                          p: 1,
                        }}
                      >
                        <StudioTechnicalText component="time">
                          {entry.timestamp ?? "No timestamp"}
                        </StudioTechnicalText>
                        <StudioTechnicalText>{entry.level}</StudioTechnicalText>
                        <StudioTechnicalText sx={{ whiteSpace: "pre-wrap" }}>
                          {entry.message}
                        </StudioTechnicalText>
                      </Box>
                    ))
                  )}
                </Box>
              </Box>
            </Stack>
          )}
        </Box>
      </Box>
      <Dialog
        fullWidth
        maxWidth="sm"
        onClose={() => !busy && setDeleteOpen(false)}
        open={deleteOpen}
      >
        <DialogTitle>Delete transform</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Alert severity="warning">
              Delete transform <strong>{selectedName}</strong>? It will stop processing records from{" "}
              <strong>{transform?.inputTopic ?? "its input topic"}</strong> into{" "}
              <strong>{transform?.outputTopics.join(", ") || "its output topics"}</strong>. The
              deployment cannot be restored from StreamSkope.
            </Alert>
            <TextField
              autoFocus
              fullWidth
              label={`Type ${selectedName ?? ""} to confirm`}
              onChange={(event) => setConfirmation(event.target.value)}
              value={confirmation}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setDeleteOpen(false)}>
            Cancel
          </Button>
          <Button
            color="error"
            disabled={busy || selectedName === null || confirmation !== selectedName}
            onClick={() => {
              if (selectedName === null) return;
              void run({
                command: "transforms.delete",
                id: globalThis.crypto.randomUUID(),
                payload: { confirmation, name: selectedName },
                version: HOST_PROTOCOL_VERSION,
              }).then((ok) => {
                if (ok) {
                  setDeleteOpen(false);
                  setSelectedName(null);
                }
              });
            }}
            variant="contained"
          >
            Delete transform
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
