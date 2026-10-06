import { Fragment, useState, type ReactNode } from "react";
import { Box, Stack, Typography } from "@mui/material";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import type { GridColDef } from "@mui/x-data-grid";

import {
  StudioAlert,
  StudioButton,
  StudioIconButton,
  StudioMenu,
  StudioMenuItem,
  StudioTextField,
  StudioTooltip,
} from "./controls";
import { StudioInventoryCellAction, StudioInventoryGrid } from "./StudioInventoryGrid";
import type {
  ProviderProfileSummary,
  ProviderProfilesSnapshot,
  ProviderWorkspaceRegistration,
} from "./provider-workspaces";

export interface CatalogProfile extends ProviderProfileSummary {
  readonly providerId: string;
  readonly providerLabel: string;
  readonly profileId: string;
}

export function catalogProfileId(providerId: string, profileId: string): string {
  return JSON.stringify([providerId, profileId]);
}

export interface ConnectionProfilesCatalogProperties {
  readonly workspaces: readonly ProviderWorkspaceRegistration[];
  readonly snapshots: readonly ProviderProfilesSnapshot[];
  readonly filter: string;
  readonly busy: boolean;
  readonly selectedId: string | null;
  readonly management: ReactNode;
  readonly onFilter: (value: string) => void;
  readonly onSelect: (profile: CatalogProfile) => void;
  readonly onAction: (profile: CatalogProfile, action: "edit" | "delete") => void;
  readonly onProviderAction: (profile: CatalogProfile, actionId: string) => void;
  readonly onCreate: (providerId: string, actionId: string) => void;
  readonly onConnect: (profile: CatalogProfile) => void;
  readonly onDisconnect: () => void;
  readonly onRefresh: () => void;
}

/** Safe summaries share one catalog; credentials and editors remain with their provider. */
export function ConnectionProfilesCatalog({
  workspaces,
  snapshots,
  filter,
  busy,
  selectedId,
  management,
  onFilter,
  onSelect,
  onAction,
  onProviderAction,
  onCreate,
  onConnect,
  onDisconnect,
  onRefresh,
}: ConnectionProfilesCatalogProperties): React.JSX.Element {
  const [addAnchor, setAddAnchor] = useState<HTMLElement | null>(null);
  const [rowMenu, setRowMenu] = useState<{ anchor: HTMLElement; profile: CatalogProfile } | null>(
    null,
  );
  const query = filter.trim().toLocaleLowerCase("en-US");
  const rows = workspaces.flatMap((workspace, index) =>
    (snapshots[index]?.profiles ?? []).map((profile) => ({
      ...profile,
      id: catalogProfileId(workspace.id, profile.id),
      profileId: profile.id,
      providerId: workspace.id,
      providerLabel: workspace.label,
    })),
  );
  const visibleRows = rows.filter((profile) =>
    [profile.name, profile.providerLabel, profile.source, ...profile.endpoints].some((value) =>
      value.toLocaleLowerCase("en-US").includes(query),
    ),
  );
  const ready = (profile: CatalogProfile): boolean => {
    const snapshot =
      snapshots[workspaces.findIndex((workspace) => workspace.id === profile.providerId)];
    return !busy && snapshot?.available === true && snapshot.storageReady && !snapshot.loading;
  };
  const columns: GridColDef<CatalogProfile>[] = [
    {
      field: "name",
      headerName: "Profile",
      minWidth: 180,
      flex: 1,
      renderCell: ({ row }) => (
        <StudioInventoryCellAction
          accessibleName={`Select profile ${row.name}`}
          onActivate={() => onSelect(row)}
        >
          <Typography noWrap variant="body2" sx={{ fontWeight: row.id === selectedId ? 600 : 400 }}>
            {row.name}
          </Typography>
        </StudioInventoryCellAction>
      ),
    },
    { field: "providerLabel", headerName: "System", width: 82 },
    {
      field: "endpoints",
      headerName: "Endpoint",
      minWidth: 160,
      flex: 1,
      valueGetter: (_value, row) => row.endpoints.join(", "),
    },
    {
      field: "source",
      headerName: "Source",
      minWidth: 140,
      flex: 0.7,
      renderCell: ({ row }) => (
        <Stack direction="row" sx={{ height: "100%", alignItems: "center", gap: 0.75 }}>
          <Typography noWrap variant="body2">
            {row.source}
          </Typography>
          {row.transport.startsWith("Plaintext") ? (
            <StudioTooltip title="This connection sends credentials and messages without encryption.">
              <Typography variant="caption" color="warning.main">
                Plaintext
              </Typography>
            </StudioTooltip>
          ) : null}
        </Stack>
      ),
    },
    {
      field: "actions",
      headerName: "Connection",
      sortable: false,
      width: 154,
      renderCell: ({ row }) => (
        <Stack direction="row" sx={{ height: "100%", alignItems: "center" }}>
          <StudioButton
            disabled={busy || (!row.active && !ready(row))}
            aria-label={`${row.active ? "Disconnect" : row.transport.startsWith("Plaintext") ? "Connect insecure plaintext" : "Connect"} profile ${row.name}`}
            onClick={() => (row.active ? onDisconnect() : onConnect(row))}
          >
            {row.active ? "Disconnect" : "Connect"}
          </StudioButton>
          <StudioIconButton
            aria-label={`Profile actions ${row.name}`}
            aria-haspopup="menu"
            disabled={!ready(row)}
            onClick={(event) => setRowMenu({ anchor: event.currentTarget, profile: row })}
            size="small"
          >
            <MoreHorizIcon fontSize="small" />
          </StudioIconButton>
        </Stack>
      ),
    },
  ];
  return (
    <Box
      component="main"
      aria-label="Connection profiles page"
      sx={{
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        overflow: "hidden",
        bgcolor: "background.default",
      }}
    >
      <Box
        component="header"
        sx={{
          bgcolor: "background.paper",
          borderBottom: 1,
          borderColor: "divider",
          px: { xs: 1.5, md: 2 },
          py: 1.25,
        }}
      >
        <Typography component="h1" variant="h5">
          Connection Profiles
        </Typography>
        <Typography color="text.secondary" variant="body2" sx={{ mt: 0.5 }}>
          Store, test, and activate Kafka and NATS connections. Browsing profiles keeps your current
          connection running.
        </Typography>
      </Box>
      <Stack direction="row" sx={{ p: 1.5, alignItems: "center", gap: 1 }}>
        <StudioTextField
          aria-label="Search profiles"
          slotProps={{ htmlInput: { type: "search" } }}
          placeholder="Search profiles…"
          value={filter}
          onChange={(event) => onFilter(event.target.value)}
        />
        <StudioButton
          aria-label="Add connection"
          aria-haspopup="menu"
          aria-expanded={addAnchor !== null}
          disabled={busy}
          onClick={(event) => setAddAnchor(event.currentTarget)}
          sx={{ whiteSpace: "nowrap", flexShrink: 0 }}
          variant="contained"
        >
          Add connection
        </StudioButton>
        <StudioButton disabled={busy} onClick={onRefresh}>
          Refresh
        </StudioButton>
      </Stack>
      {workspaces.map((workspace, index) => {
        const snapshot = snapshots[index];
        return snapshot?.failure === null || snapshot === undefined ? null : (
          <StudioAlert key={workspace.id} severity="error" sx={{ mx: 1.5, mb: 1 }}>
            {workspace.label}: {snapshot.failure.summary} {snapshot.failure.recovery}
          </StudioAlert>
        );
      })}
      <Box
        sx={{
          display: "flex",
          flex: "1 1 0",
          minHeight: 0,
          overflow: "auto",
          flexDirection: { xs: "column", lg: "row" },
          gap: 1.5,
          px: 1.5,
          pb: 1.5,
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            flex: "1 1 0",
            minWidth: 0,
            minHeight: { xs: 260, lg: 0 },
          }}
        >
          <StudioInventoryGrid
            ariaLabel="Connection profiles"
            testId="connection-profiles-grid"
            columns={columns}
            rows={visibleRows}
            loading={rows.length === 0 && snapshots.some((snapshot) => snapshot.loading)}
            loadingMessage="Loading connection profiles…"
            emptyMessage={
              query.length > 0
                ? "No profiles match your search."
                : "Add a Kafka or NATS connection to get started."
            }
            stateLabel="Connection profile inventory"
          />
          <Box role="status" aria-label="Profile storage status">
            {workspaces.map((workspace, index) => (
              <Typography
                key={workspace.id}
                component="p"
                variant="caption"
                color="text.secondary"
                sx={{ mt: 0.5 }}
              >
                {snapshots[index]?.storageLabel.startsWith(workspace.label)
                  ? snapshots[index]?.storageLabel
                  : `${workspace.label}: ${snapshots[index]?.storageLabel ?? "Loading storage…"}`}
              </Typography>
            ))}
          </Box>
        </Box>
        <Box
          component="section"
          aria-label="Selected connection profile"
          tabIndex={0}
          sx={{ flex: { lg: "0 1 36%" }, minWidth: { lg: 280 }, overflow: "auto" }}
        >
          {selectedId === null ? (
            <Typography color="text.secondary" variant="body2" sx={{ p: 2 }}>
              Select a profile to inspect its connection settings.
            </Typography>
          ) : null}
          {management}
        </Box>
      </Box>
      <StudioMenu
        anchorEl={addAnchor}
        open={addAnchor !== null && !busy}
        onClose={() => setAddAnchor(null)}
        aria-label="Add connection"
      >
        {workspaces.map((workspace, index) => (
          <Fragment key={workspace.id}>
            {(snapshots[index]?.creationActions ?? []).map((action) => (
              <StudioMenuItem
                key={action.id}
                disabled={!action.available || busy}
                onClick={() => {
                  setAddAnchor(null);
                  onCreate(workspace.id, action.id);
                }}
              >
                {action.label}
              </StudioMenuItem>
            ))}
          </Fragment>
        ))}
      </StudioMenu>
      <StudioMenu
        anchorEl={rowMenu?.anchor ?? null}
        slotProps={{
          list: {
            "aria-label": `Profile actions for ${rowMenu?.profile.name ?? "selected profile"}`,
          },
        }}
        open={rowMenu !== null && !busy}
        onClose={() => setRowMenu(null)}
      >
        {(rowMenu?.profile.actions ?? []).map((action) => (
          <StudioMenuItem
            key={action.id}
            disabled={!action.available}
            onClick={() => {
              const profile = rowMenu?.profile;
              setRowMenu(null);
              if (profile !== undefined) onProviderAction(profile, action.id);
            }}
          >
            {action.label}
          </StudioMenuItem>
        ))}
        {(["edit", "delete"] as const).map((action) => (
          <StudioMenuItem
            key={action}
            disabled={rowMenu?.profile.active === true}
            onClick={() => {
              const profile = rowMenu?.profile;
              setRowMenu(null);
              if (profile !== undefined) onAction(profile, action);
            }}
          >
            {action === "edit" ? "Edit" : "Delete"}
          </StudioMenuItem>
        ))}
      </StudioMenu>
    </Box>
  );
}
