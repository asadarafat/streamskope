import { lazy, Suspense, useEffect, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type ConnectionTemplateSnapshot,
  type KafkaClusterDiagnosticsSnapshot,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../contracts";
import { StudioDetailRow } from "../../ui/StudioPropertyRow";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "../../ui/controls";
import { streamSkopeMuiMonospaceTypography } from "../../ui/createStreamSkopeTheme";

import { ProfileDialog } from "./ProfileDialog";
import type { ProfileAction } from "./ProfilePanel";
import type { TextDocumentTransferPort } from "./text-document-transfer";
import { formatUtcTimestamp } from "./timestamp-presentation";
import { WorkspaceState } from "./WorkspaceState";

const LazyClusterDetailsDialog = lazy(async () => {
  const module = await import("./ClusterDetailsDialog");
  return { default: module.ClusterDetailsDialog };
});

export interface ProfileWorkspaceProperties {
  readonly component?: "main" | "section";
  readonly activityOpen: boolean;
  readonly action: ProfileAction | null;
  readonly clusterDiagnostics: KafkaClusterDiagnosticsSnapshot;
  readonly host: StreamSkopeHost;
  readonly onOpenActivity: (correlationId?: string) => void;
  readonly onActionClose: () => void;
  readonly profile: ProfileSummary | null;
  readonly templateLoading: boolean;
  readonly templateRequestError?: string;
  readonly templateSnapshot: ConnectionTemplateSnapshot | null;
  readonly transfer: TextDocumentTransferPort;
}

function profileErrorText(summary: string, recovery: string): string {
  return `${summary} ${recovery}`;
}

function serviceAuthenticationLabel(authentication: "none" | "oauth"): string {
  return authentication === "oauth" ? "Profile OAuth bearer token" : "No HTTP authorization";
}

function TechnicalValue({ children }: { readonly children: React.ReactNode }): React.JSX.Element {
  return (
    <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
      {children}
    </Box>
  );
}

function ProfileEvidenceSection({
  children,
  description,
  title,
  twoColumns = false,
}: {
  readonly children: React.ReactNode;
  readonly description?: string;
  readonly title: string;
  readonly twoColumns?: boolean;
}): React.JSX.Element {
  return (
    <Box component="section" sx={{ border: 1, borderColor: "divider", minWidth: 0 }}>
      <Box sx={{ borderBottom: 1, borderColor: "divider", px: 1.25, py: 1 }}>
        <Typography component="h3" variant="subtitle2">
          {title}
        </Typography>
        {description === undefined ? null : (
          <Typography color="text.secondary" variant="body2">
            {description}
          </Typography>
        )}
      </Box>
      <Box
        component="dl"
        sx={{
          display: twoColumns ? "grid" : "block",
          gridTemplateColumns: twoColumns
            ? { md: "repeat(2, minmax(0, 1fr))", xs: "minmax(0, 1fr)" }
            : undefined,
          m: 0,
        }}
      >
        {children}
      </Box>
    </Box>
  );
}

function ProfileDeleteDialog({
  host,
  onClose,
  profile,
}: {
  readonly host: StreamSkopeHost;
  readonly onClose: () => void;
  readonly profile: ProfileSummary;
}): React.JSX.Element {
  const [error, setError] = useState<string>();
  const [submitting, setSubmitting] = useState(false);

  async function deleteProfile(): Promise<void> {
    setError(undefined);
    setSubmitting(true);
    try {
      const response = await host.execute({
        command: "profiles.delete",
        id: globalThis.crypto.randomUUID(),
        payload: { profileId: profile.id },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setError(profileErrorText(response.error.summary, response.error.recovery));
        return;
      }
      onClose();
    } catch {
      setError(
        "The application host did not accept the delete request. The profile remains stored. Open Activity for diagnostics.",
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Dialog
      aria-labelledby="delete-profile-title"
      maxWidth="xs"
      onClose={submitting ? undefined : onClose}
      open
    >
      <DialogTitle id="delete-profile-title">Delete Kafka profile {profile.name}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={1.5}>
          <Typography variant="body2">
            Delete profile <strong>{profile.name}</strong> for{" "}
            <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
              {profile.brokers.join(", ")}
            </Box>
            ?
          </Typography>
          <Typography color="text.secondary" variant="body2">
            This permanently removes its stored credentials and trust material. This action cannot
            be undone.
          </Typography>
          {error === undefined ? null : <Alert severity="error">{error}</Alert>}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={submitting} onClick={onClose}>
          Cancel
        </Button>
        <Button
          color="error"
          disabled={submitting}
          onClick={() => {
            void deleteProfile();
          }}
          variant="contained"
        >
          Delete profile
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export function ProfileWorkspace({
  activityOpen,
  action,
  clusterDiagnostics,
  component = "main",
  host,
  onOpenActivity,
  onActionClose,
  profile,
  templateLoading,
  templateRequestError,
  templateSnapshot,
  transfer,
}: ProfileWorkspaceProperties): React.JSX.Element {
  const [activityRequestedFromDialog, setActivityRequestedFromDialog] = useState(false);

  useEffect(() => {
    if (activityRequestedFromDialog && !activityOpen) {
      setActivityRequestedFromDialog(false);
    }
  }, [activityOpen, activityRequestedFromDialog]);

  if (profile === null) {
    return (
      <Box
        aria-label="Connection profile workspace"
        component={component}
        tabIndex={0}
        sx={{
          bgcolor: "background.paper",
          display: "grid",
          minHeight: 0,
          overflow: "auto",
          p: 2,
          placeItems: "center",
        }}
      >
        <WorkspaceState
          detail="Select an existing profile, or add and test a new one in the resource pane."
          label="No profile selected"
          title="Choose a connection profile"
        />
      </Box>
    );
  }

  const authentication = profile.oauth === undefined ? "TLS" : "OAuth 2.0";
  const schemaRegistry = profile.services?.schemaRegistry;
  const redpandaAdmin = profile.services?.redpandaAdmin;
  return (
    <Box
      aria-label="Connection profile workspace"
      component={component}
      tabIndex={0}
      sx={{
        bgcolor: "background.paper",
        containerName: "studio-workspace",
        containerType: "inline-size",
        minHeight: 0,
        overflow: "auto",
      }}
    >
      <Stack
        aria-label="Connection profile details"
        component="section"
        spacing={1.5}
        sx={{ minWidth: 0, p: 2 }}
      >
        <Box sx={{ minWidth: 0 }}>
          <Typography component="h2" variant="h6">
            {profile.name}
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Connection profile
          </Typography>
        </Box>

        <Box
          data-testid="profile-safe-evidence"
          sx={{
            display: "grid",
            gap: 1.5,
            gridTemplateColumns: { lg: "repeat(2, minmax(0, 1fr))", xs: "minmax(0, 1fr)" },
            minWidth: 0,
          }}
        >
          <ProfileEvidenceSection title="Kafka connection">
            <StudioDetailRow
              label="Brokers"
              value={<TechnicalValue>{profile.brokers.join(", ")}</TechnicalValue>}
            />
            <StudioDetailRow label="Authentication" value={authentication} />
            <StudioDetailRow
              label="OAuth client"
              value={profile.oauth?.clientId ?? "Not configured"}
            />
            <StudioDetailRow label="OAuth scope" value={profile.oauth?.scope ?? "Not configured"} />
            <StudioDetailRow
              label="Token endpoint"
              value={
                profile.oauth === undefined ? (
                  "Not configured"
                ) : (
                  <TechnicalValue>{profile.oauth.tokenEndpoint}</TechnicalValue>
                )
              }
            />
            <StudioDetailRow
              label="OAuth secret"
              value={
                profile.oauth === undefined
                  ? "Not configured"
                  : profile.oauth.clientSecretPresent
                    ? "OAuth secret retained by host"
                    : "OAuth secret not stored"
              }
            />
          </ProfileEvidenceSection>

          <ProfileEvidenceSection
            description="Sensitive values remain in the application host and are never returned to this view."
            title="Transport and profile"
          >
            <StudioDetailRow label="Trust format" value={profile.trust.kind.toUpperCase()} />
            <StudioDetailRow
              label="Trust material"
              value={
                profile.trust.materialPresent
                  ? `${profile.trust.kind.toUpperCase()} trust material present`
                  : "Trust material unavailable"
              }
            />
            <StudioDetailRow
              label="Trust password"
              value={
                profile.trust.passwordPresent
                  ? "Trust password retained by host"
                  : "No trust password stored"
              }
            />
            <StudioDetailRow
              label="Hostname verification"
              value="Enabled by the application host"
            />
            <StudioDetailRow
              label="Created"
              value={
                <time dateTime={profile.createdAt} title={profile.createdAt}>
                  {formatUtcTimestamp(profile.createdAt)}
                </time>
              }
            />
            <StudioDetailRow
              label="Last updated"
              value={
                <time dateTime={profile.updatedAt} title={profile.updatedAt}>
                  {formatUtcTimestamp(profile.updatedAt)}
                </time>
              }
            />
          </ProfileEvidenceSection>

          <Box sx={{ gridColumn: { lg: "1 / -1" } }}>
            <ProfileEvidenceSection title="Cluster services" twoColumns>
              <StudioDetailRow
                label="Schema Registry"
                value={
                  schemaRegistry === undefined ? (
                    "Not configured"
                  ) : (
                    <TechnicalValue>{schemaRegistry.baseUrl}</TechnicalValue>
                  )
                }
              />
              <StudioDetailRow
                label="Admin API"
                value={
                  redpandaAdmin === undefined ? (
                    "Not configured"
                  ) : (
                    <TechnicalValue>{redpandaAdmin.baseUrl}</TechnicalValue>
                  )
                }
              />
              <StudioDetailRow
                label="Schema authentication"
                value={
                  schemaRegistry === undefined
                    ? "Not configured"
                    : serviceAuthenticationLabel(schemaRegistry.authentication)
                }
              />
              <StudioDetailRow
                label="Admin authentication"
                value={
                  redpandaAdmin === undefined
                    ? "Not configured"
                    : serviceAuthenticationLabel(redpandaAdmin.authentication)
                }
              />
            </ProfileEvidenceSection>
          </Box>
        </Box>
      </Stack>

      {action === "edit" ? (
        <ProfileDialog
          host={host}
          onClose={() => {
            onActionClose();
            setActivityRequestedFromDialog(false);
          }}
          onOpenActivity={(correlationId) => {
            setActivityRequestedFromDialog(true);
            onOpenActivity(correlationId);
          }}
          open={!activityRequestedFromDialog}
          profile={profile}
          templateLoading={templateLoading}
          templateSnapshot={templateSnapshot}
          transfer={transfer}
          {...(templateRequestError === undefined ? {} : { templateRequestError })}
        />
      ) : null}
      {action === "delete" ? (
        <ProfileDeleteDialog
          host={host}
          onClose={() => {
            onActionClose();
          }}
          profile={profile}
        />
      ) : null}
      {action === "cluster" ? (
        <Suspense
          fallback={
            <Dialog aria-labelledby="cluster-details-loading-title" open>
              <DialogTitle id="cluster-details-loading-title">Cluster details</DialogTitle>
              <DialogContent dividers>
                <Typography aria-live="polite" role="status" variant="body2">
                  Loading cluster-details workspace…
                </Typography>
              </DialogContent>
            </Dialog>
          }
        >
          <LazyClusterDetailsDialog
            host={host}
            loadOnOpen
            onClose={() => {
              onActionClose();
            }}
            snapshot={clusterDiagnostics}
            transfer={transfer}
          />
        </Suspense>
      ) : null}
    </Box>
  );
}
