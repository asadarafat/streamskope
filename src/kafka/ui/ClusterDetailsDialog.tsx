import { useEffect, useMemo, useRef, useState } from "react";
import {
  Box,
  Divider,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import { DataGrid, type GridColDef } from "@mui/x-data-grid";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type KafkaClusterDiagnosticsSnapshot,
  type KafkaConfigurationEntry,
  type StreamSkopeHost,
} from "../contracts";
import { streamSkopeMuiMonospaceTypography } from "../../ui/createStreamSkopeTheme";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioTextField as TextField,
} from "../../ui/controls";

import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";
import { formatUtcTimestamp } from "./timestamp-presentation";

export type ClusterDetailsTransferPort = TextDocumentTransferPort;

export interface ClusterDetailsDialogProperties {
  readonly host: StreamSkopeHost;
  readonly loadOnOpen?: boolean;
  readonly onClose: () => void;
  readonly snapshot: KafkaClusterDiagnosticsSnapshot;
  readonly transfer?: ClusterDetailsTransferPort;
}

const configurationColumns: readonly GridColDef<KafkaConfigurationEntry>[] = [
  {
    field: "name",
    flex: 1,
    headerName: "Configuration",
    minWidth: 220,
  },
  {
    field: "value",
    flex: 1,
    headerName: "Value",
    minWidth: 180,
    valueGetter: (_value, row): string => (row.isSensitive ? "••••" : (row.value ?? "Not set")),
  },
  {
    field: "source",
    headerName: "Source",
    minWidth: 150,
    width: 170,
  },
  {
    field: "type",
    headerName: "Type",
    minWidth: 100,
    width: 110,
  },
  {
    field: "access",
    headerName: "Access",
    minWidth: 110,
    sortable: false,
    valueGetter: (_value, row): string => (row.readOnly ? "Read-only" : "Writable"),
    width: 120,
  },
];

function command(commandName: "clusterDetails.export" | "clusterDetails.load"): HostCommand {
  return {
    command: commandName,
    id: globalThis.crypto.randomUUID(),
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  };
}

function profileName(snapshot: KafkaClusterDiagnosticsSnapshot): string {
  return snapshot.profile?.name ?? "Connected cluster";
}

function statusMessage(snapshot: KafkaClusterDiagnosticsSnapshot): React.JSX.Element | null {
  switch (snapshot.state) {
    case "loading":
      return (
        <Alert severity="info">
          Loading fresh cluster metadata and broker configuration. Existing export is unavailable
          until this request completes.
        </Alert>
      );
    case "partial":
      return snapshot.cluster.configurationIssue === undefined ? (
        <Alert severity="warning">
          Cluster details are partially available. Verified fields remain visible and exportable.
        </Alert>
      ) : (
        <Alert severity="warning">
          {snapshot.cluster.configurationIssue.summary}{" "}
          {snapshot.cluster.configurationIssue.recovery}
        </Alert>
      );
    case "stale":
      return (
        <Alert severity="warning">
          Displayed cluster data is stale. {snapshot.error.summary} {snapshot.error.recovery}
        </Alert>
      );
    case "failed":
      return (
        <Alert severity="error">
          {snapshot.error.summary} {snapshot.error.recovery}
        </Alert>
      );
    case "unavailable":
      return (
        <Alert severity="info">
          Cluster details are unavailable. Connect a Kafka profile, then open Cluster details.
        </Alert>
      );
    case "ready":
      return null;
  }
}

function SummaryField({
  label,
  title,
  value,
}: {
  readonly label: string;
  readonly title?: string;
  readonly value: string;
}): React.JSX.Element {
  return (
    <Box sx={{ minWidth: 0 }}>
      <Typography color="text.secondary" component="dt" variant="caption">
        {label}
      </Typography>
      <Typography component="dd" noWrap sx={{ m: 0 }} title={title ?? value} variant="body2">
        {value}
      </Typography>
    </Box>
  );
}

export function ClusterDetailsDialog({
  host,
  loadOnOpen = false,
  onClose,
  snapshot,
  transfer = browserTextDocumentTransfer,
}: ClusterDetailsDialogProperties): React.JSX.Element {
  const [busy, setBusy] = useState<"copy" | "download" | "refresh">();
  const [filter, setFilter] = useState("");
  const [localError, setLocalError] = useState<string>();
  const [transferStatus, setTransferStatus] = useState("");
  const initialLoadRequested = useRef(false);
  const normalizedFilter = filter.trim().toLocaleLowerCase("en-US");
  const configuration = snapshot.cluster?.configuration ?? [];
  const visibleConfiguration = useMemo(
    () =>
      normalizedFilter.length === 0
        ? configuration
        : configuration.filter(
            (entry) =>
              entry.name.toLocaleLowerCase("en-US").includes(normalizedFilter) ||
              entry.source.toLocaleLowerCase("en-US").includes(normalizedFilter) ||
              entry.type.toLocaleLowerCase("en-US").includes(normalizedFilter) ||
              (!entry.isSensitive &&
                (entry.value ?? "").toLocaleLowerCase("en-US").includes(normalizedFilter)),
          ),
    [configuration, normalizedFilter],
  );
  const exportEnabled = snapshot.state === "ready" || snapshot.state === "partial";

  async function refresh(): Promise<void> {
    setBusy("refresh");
    setLocalError(undefined);
    setTransferStatus("");
    try {
      const response = await host.execute(command("clusterDetails.load"));
      if (!response.ok) {
        setLocalError(`${response.error.summary} ${response.error.recovery}`);
      }
    } catch {
      setLocalError(
        "The application host did not accept the refresh request. Existing cluster data was not changed. Open Activity for diagnostics.",
      );
    } finally {
      setBusy(undefined);
    }
  }

  async function transferDocument(kind: "copy" | "download"): Promise<void> {
    setBusy(kind);
    setLocalError(undefined);
    setTransferStatus("");
    try {
      const response = await host.execute(command("clusterDetails.export"));
      if (!response.ok) {
        setLocalError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      if (!("document" in response.result)) {
        setLocalError(
          "The application host returned no cluster JSON document. Refresh cluster details and retry.",
        );
        return;
      }
      if (kind === "copy") {
        await transfer.copy(response.result.document.content);
        setTransferStatus("Cluster JSON copied.");
      } else {
        const outcome = await transfer.download(response.result.document);
        setTransferStatus(
          outcome === "cancelled"
            ? "Cluster JSON export cancelled."
            : outcome === "saved"
              ? "Cluster JSON saved."
              : "Cluster JSON download started.",
        );
      }
    } catch {
      setLocalError(
        kind === "copy"
          ? "The cluster JSON could not be copied. Check clipboard permission and retry."
          : "The JSON export failed. No file was saved. Retry the export.",
      );
    } finally {
      setBusy(undefined);
    }
  }

  useEffect(() => {
    if (!loadOnOpen || initialLoadRequested.current) {
      return;
    }
    initialLoadRequested.current = true;
    void refresh();
  }, [loadOnOpen]);

  const cluster = snapshot.cluster;
  const titleId = "cluster-details-title";

  return (
    <Dialog
      aria-labelledby={titleId}
      fullWidth
      maxWidth="lg"
      onClose={
        busy === undefined
          ? (_event, reason): void => {
              if (reason === "backdropClick") {
                onClose();
              }
            }
          : undefined
      }
      onKeyDown={(event) => {
        if (event.key === "Escape" && busy === undefined) {
          event.stopPropagation();
          onClose();
        }
      }}
      open
      slotProps={{
        paper: {
          sx: {
            maxHeight: "calc(100dvh - 32px)",
          },
        },
      }}
    >
      <DialogTitle id={titleId}>Cluster details — {profileName(snapshot)}</DialogTitle>
      <DialogContent dividers sx={{ p: 0 }}>
        <Stack divider={<Divider flexItem />} spacing={0}>
          <Stack spacing={1.5} sx={{ p: 2 }}>
            {statusMessage(snapshot)}
            {localError === undefined ? null : <Alert severity="error">{localError}</Alert>}
            <Typography
              aria-live="polite"
              role="status"
              sx={{ minHeight: "1.25rem" }}
              variant="body2"
            >
              {transferStatus.length > 0
                ? transferStatus
                : snapshot.state === "ready"
                  ? "Current cluster data."
                  : ""}
            </Typography>

            {cluster === null ? null : (
              <Box
                component="dl"
                sx={{
                  display: "grid",
                  gap: 1.5,
                  gridTemplateColumns: {
                    xs: "minmax(0, 1fr)",
                    sm: "repeat(2, minmax(0, 1fr))",
                    md: "repeat(4, minmax(0, 1fr))",
                  },
                  m: 0,
                }}
              >
                <SummaryField label="Cluster ID" value={cluster.clusterId ?? "Not reported"} />
                <SummaryField
                  label="Controller"
                  value={
                    cluster.controllerId === null
                      ? "Not reported"
                      : `Broker ${cluster.controllerId.toLocaleString()}`
                  }
                />
                <SummaryField
                  label="Brokers"
                  value={`${cluster.brokers.length.toLocaleString()} ${
                    cluster.brokers.length === 1 ? "broker" : "brokers"
                  }`}
                />
                <SummaryField
                  label="Fetched"
                  title={snapshot.fetchedAt ?? undefined}
                  value={
                    snapshot.fetchedAt === null
                      ? "Not fetched"
                      : formatUtcTimestamp(snapshot.fetchedAt)
                  }
                />
                <SummaryField label="Endpoint" value={snapshot.endpoint ?? "Not connected"} />
                <SummaryField
                  label="Profile brokers"
                  value={snapshot.profile?.brokers.join(", ") ?? "Not available"}
                />
              </Box>
            )}
          </Stack>

          {cluster === null ? null : (
            <Box sx={{ p: 2 }}>
              <Typography component="h3" gutterBottom variant="subtitle1">
                Brokers
              </Typography>
              {cluster.brokers.length === 0 ? (
                <Typography color="text.secondary" variant="body2">
                  Kafka returned no brokers for this cluster.
                </Typography>
              ) : (
                <TableContainer>
                  <Table aria-label="Kafka cluster brokers">
                    <TableHead>
                      <TableRow>
                        <TableCell align="right">Broker ID</TableCell>
                        <TableCell>Host</TableCell>
                        <TableCell align="right">Port</TableCell>
                        <TableCell>Rack</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {cluster.brokers.map((broker) => (
                        <TableRow key={broker.nodeId}>
                          <TableCell align="right">{broker.nodeId}</TableCell>
                          <TableCell sx={streamSkopeMuiMonospaceTypography}>
                            {broker.host}
                          </TableCell>
                          <TableCell align="right">{broker.port}</TableCell>
                          <TableCell>{broker.rack ?? "—"}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>
          )}

          {cluster === null ? null : (
            <Box sx={{ p: 2 }}>
              <Stack
                direction={{ xs: "column", sm: "row" }}
                spacing={1.5}
                sx={{ alignItems: { sm: "center" }, justifyContent: "space-between", mb: 1.5 }}
              >
                <Box>
                  <Typography component="h3" variant="subtitle1">
                    Broker configuration
                  </Typography>
                  <Typography color="text.secondary" variant="caption">
                    {cluster.configurationSourceBrokerId === null
                      ? "No configuration source broker"
                      : `Source: Broker ${cluster.configurationSourceBrokerId.toLocaleString()}`}
                  </Typography>
                </Box>
                <TextField
                  label="Filter broker configuration"
                  onChange={(event) => {
                    setFilter(event.target.value);
                  }}
                  slotProps={{ htmlInput: { type: "search" } }}
                  sx={{ minWidth: { sm: 280 } }}
                  value={filter}
                />
              </Stack>
              {configuration.length === 0 ? (
                <Typography color="text.secondary" variant="body2">
                  No broker configuration entries are available.
                </Typography>
              ) : visibleConfiguration.length === 0 ? (
                <Typography color="text.secondary" variant="body2">
                  No broker configuration entries match the current filter.
                </Typography>
              ) : (
                <Box sx={{ height: 320, minWidth: 0 }}>
                  <DataGrid
                    aria-label="Broker configuration entries"
                    columnHeaderHeight={38}
                    columns={configurationColumns}
                    density="compact"
                    disableMultipleRowSelection
                    getRowId={(row) => row.name}
                    hideFooter
                    rowHeight={36}
                    rows={visibleConfiguration}
                    sx={{ border: 0 }}
                  />
                </Box>
              )}
            </Box>
          )}
        </Stack>
      </DialogContent>
      <DialogActions aria-label="Cluster detail actions" role="group">
        <Button
          aria-label="Refresh cluster details"
          disabled={busy !== undefined || snapshot.profile === null || snapshot.state === "loading"}
          onClick={() => {
            void refresh();
          }}
        >
          {busy === "refresh" ? "Refreshing…" : "Refresh"}
        </Button>
        <Button
          aria-label="Copy cluster details JSON"
          disabled={!exportEnabled || busy !== undefined}
          onClick={() => {
            void transferDocument("copy");
          }}
        >
          {busy === "copy" ? "Copying…" : "Copy JSON"}
        </Button>
        <Box sx={{ flex: 1 }} />
        <Button disabled={busy !== undefined} onClick={onClose}>
          Close
        </Button>
        <Button
          aria-label="Download cluster details JSON"
          disabled={!exportEnabled || busy !== undefined}
          onClick={() => {
            void transferDocument("download");
          }}
          variant="contained"
        >
          {busy === "download" ? "Starting download…" : "Download JSON"}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default ClusterDetailsDialog;
