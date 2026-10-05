import { useId, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import type { GridColDef } from "@mui/x-data-grid";

import type { NatsProfileStoreCapability, NatsProfileSummary } from "../contracts";
import { StudioInventoryGrid } from "../../../platform/ui/StudioInventoryGrid";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "../../../platform/ui/controls";
import { streamSkopeMuiMonospaceTypography } from "../../../platform/ui/createStreamSkopeTheme";

import { ProfileEditor } from "./ProfileEditor";
import type { NatsWorkspaceController } from "./use-nats-workspace";

function capabilityText(capability: NatsProfileStoreCapability | undefined): string {
  if (capability === undefined) return "Profile storage capability is not yet available.";
  if (capability.state === "unavailable")
    return `Profile storage unavailable. ${capability.recovery ?? "Restore protected storage and retry."}`;
  if (capability.durability === "session" && capability.protection === "memory")
    return "Session profiles · credentials held in memory. Profiles are lost when this development host restarts.";
  return "Durable profiles · credentials protected by the operating system.";
}

function captureProfile(profile: NatsProfileSummary): NatsProfileSummary {
  return {
    ...profile,
    servers: [...profile.servers],
    authentication: { ...profile.authentication },
    tls: { ...profile.tls },
  };
}

export function ProfileWorkspace({
  controller,
  isInteractive,
}: {
  readonly controller: NatsWorkspaceController;
  readonly isInteractive: () => boolean;
}): React.JSX.Element {
  const [editor, setEditor] = useState<{ readonly profile: NatsProfileSummary | null } | null>(
    null,
  );
  const [deleting, setDeleting] = useState<NatsProfileSummary | null>(null);
  const [deletePending, setDeletePending] = useState(false);
  const [deleteFailed, setDeleteFailed] = useState(false);
  const deleteTitle = useId();
  const busy = controller.pending.length > 0;
  const disabled = !controller.available || busy || !isInteractive();
  const capability = controller.profiles?.capability;
  const storageReady = capability?.state === "ready";
  const connected = controller.connection.state !== "disconnected";

  const columns = useMemo<readonly GridColDef<NatsProfileSummary>[]>(
    () => [
      { field: "name", headerName: "Profile", flex: 1, minWidth: 160 },
      {
        field: "servers",
        headerName: "NATS servers",
        flex: 1.5,
        minWidth: 200,
        valueGetter: (_value, row): string => row.servers.join(", "),
        renderCell: ({ row }): React.JSX.Element => (
          <Box
            component="span"
            sx={{
              ...streamSkopeMuiMonospaceTypography,
              minWidth: 0,
              overflow: "hidden",
              textOverflow: "ellipsis",
            }}
            title={row.servers.join(", ")}
          >
            {row.servers.join(", ")}
          </Box>
        ),
      },
      {
        field: "authentication",
        headerName: "Authentication",
        width: 130,
        valueGetter: (_value, row): string =>
          row.authentication.mode === "token" ? "Token" : "None",
      },
      {
        field: "tls",
        headerName: "Transport",
        width: 130,
        valueGetter: (_value, row): string =>
          row.tls.mode === "tls" ? "Verified TLS" : "Plaintext",
      },
      {
        field: "actions",
        headerName: "Actions",
        width: 246,
        sortable: false,
        filterable: false,
        renderCell: ({ row }): React.JSX.Element => (
          <Stack direction="row" spacing={0.5} sx={{ alignItems: "center", height: "100%" }}>
            <Button
              aria-label={`Connect profile ${row.name}`}
              disabled={disabled || !storageReady}
              onClick={() => {
                if (isInteractive()) void controller.connectProfile(captureProfile(row));
              }}
              variant="text"
            >
              Connect
            </Button>
            <Button
              aria-label={`Edit profile ${row.name}`}
              disabled={disabled || !storageReady}
              onClick={() => {
                if (isInteractive()) setEditor({ profile: captureProfile(row) });
              }}
              variant="text"
            >
              Edit
            </Button>
            <Button
              aria-label={`Delete profile ${row.name}`}
              disabled={disabled || !storageReady}
              onClick={() => {
                if (!isInteractive()) return;
                setDeleteFailed(false);
                setDeleting(captureProfile(row));
              }}
              variant="text"
            >
              Delete
            </Button>
          </Stack>
        ),
      },
    ],
    [controller, disabled, isInteractive, storageReady],
  );

  async function deleteProfile(): Promise<void> {
    if (deleting === null || disabled || !isInteractive()) return;
    setDeletePending(true);
    setDeleteFailed(false);
    try {
      if (await controller.deleteProfile(deleting)) setDeleting(null);
      else setDeleteFailed(true);
    } finally {
      setDeletePending(false);
    }
  }

  return (
    <Box
      aria-label="Connection Profiles"
      component="main"
      sx={{
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <Box sx={{ bgcolor: "background.paper", borderBottom: 1, borderColor: "divider", p: 2 }}>
        <Stack
          direction="row"
          spacing={1.5}
          sx={{ alignItems: "center", justifyContent: "space-between", minWidth: 0 }}
        >
          <Box sx={{ minWidth: 0 }}>
            <Typography component="h2" variant="h5">
              Connection Profiles
            </Typography>
            <Typography color="text.secondary" variant="body2">
              Connect to Core NATS using a reusable profile.
            </Typography>
          </Box>
          <Stack direction="row" spacing={1} sx={{ flexShrink: 0 }}>
            <Button
              disabled={disabled}
              onClick={() => {
                if (isInteractive()) void controller.refresh();
              }}
              variant="outlined"
            >
              Refresh profiles
            </Button>
            <Button
              aria-label="Add NATS profile"
              disabled={disabled || !storageReady}
              onClick={() => {
                if (isInteractive()) setEditor({ profile: null });
              }}
              variant="contained"
            >
              Add NATS profile
            </Button>
          </Stack>
        </Stack>
        <Typography
          aria-label="Profile storage"
          color={capability?.state === "unavailable" ? "error" : "text.secondary"}
          sx={{ mt: 1, overflowWrap: "anywhere" }}
          variant="body2"
        >
          {controller.available
            ? capabilityText(capability)
            : "Profile storage cannot be verified while the NATS host is unavailable."}
        </Typography>
      </Box>
      <Box
        sx={{
          display: "flex",
          flex: "1 1 0",
          minHeight: 0,
          minWidth: 0,
          p: 1.5,
          "& > [data-testid]": { minWidth: 0, maxWidth: "100%" },
        }}
      >
        <StudioInventoryGrid
          ariaLabel="NATS profiles"
          columns={columns}
          emptyMessage={
            controller.available
              ? "No profiles yet. Add a NATS profile to connect."
              : "NATS profiles unavailable."
          }
          loading={controller.loading}
          loadingMessage="Loading NATS profiles…"
          rows={controller.profiles?.profiles ?? []}
          stateLabel="NATS profile inventory status"
          testId="nats-profiles-grid"
        />
      </Box>
      <Box
        sx={{
          alignItems: "center",
          borderTop: 1,
          borderColor: "divider",
          display: "flex",
          gap: 1.5,
          minWidth: 0,
          px: 2,
          py: 1,
        }}
      >
        <Typography
          color="text.secondary"
          noWrap
          sx={{ flex: 1, minWidth: 0 }}
          variant="body2"
          title={controller.connection.profile?.name}
        >
          Active profile: {controller.connection.profile?.name ?? "None"}
        </Typography>
        <Button
          disabled={disabled || !connected}
          onClick={() => {
            if (isInteractive()) void controller.disconnect();
          }}
          variant="outlined"
        >
          Disconnect
        </Button>
      </Box>
      {editor === null ? null : (
        <ProfileEditor
          available={controller.available}
          failure={controller.failure}
          isInteractive={isInteractive}
          onClose={() => setEditor(null)}
          onCreate={controller.createProfile}
          onUpdate={controller.updateProfile}
          profile={editor.profile}
        />
      )}
      <Dialog
        aria-labelledby={deleteTitle}
        maxWidth="xs"
        onClose={deletePending ? undefined : (): void => setDeleting(null)}
        open={deleting !== null}
      >
        <DialogTitle id={deleteTitle}>Delete NATS profile</DialogTitle>
        <DialogContent>
          <Typography sx={{ overflowWrap: "anywhere" }} variant="body2">
            Delete profile <strong>{deleting?.name}</strong> and its host-held credentials?
          </Typography>
          {deleteFailed ? (
            <Alert severity="error">
              {controller.failure?.summary ?? "The NATS host did not delete this profile."}{" "}
              {controller.failure?.recovery}
            </Alert>
          ) : null}
        </DialogContent>
        <DialogActions>
          <Button disabled={deletePending} onClick={() => setDeleting(null)}>
            Cancel
          </Button>
          <Button
            color="error"
            disabled={disabled || deletePending}
            onClick={() => {
              if (isInteractive()) void deleteProfile();
            }}
            variant="contained"
          >
            {deletePending ? "Deleting profile…" : "Delete profile"}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
