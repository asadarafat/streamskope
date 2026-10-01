import { useEffect, useMemo, useState } from "react";
import { Box, ButtonGroup, List, ListItem, ListItemText, Stack, Typography } from "@mui/material";

import {
  type ProfileStoreCapability,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../contracts";
import {
  streamSkopeGeometry,
  streamSkopeMuiMonospaceTypography,
  streamSkopeMuiResourceTypography,
} from "../../../platform/ui/createStreamSkopeTheme";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioListItemButton as ListItemButton,
  StudioMenu as Menu,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { PluginView } from "./PluginView";
import { pluginProfileText, usePlugins, type LoadedPluginRenderer } from "./PluginsProvider";
import { ProfileDialog } from "./ProfileDialog";
import { WorkbenchIcon } from "./WorkbenchIcons";

export interface ProfileConnectionOperation {
  readonly action: "connect" | "disconnect";
  readonly profileId: string;
}

export type ProfileAction = "cluster" | "delete" | "edit";

function profileEndpointSummary(
  profile: ProfileSummary,
  plugins: readonly LoadedPluginRenderer[],
): string {
  const transport = (profile.transport ?? "tls") === "plaintext" ? "Plaintext · insecure" : "TLS";
  const source = profile.source;
  const label =
    source === undefined
      ? transport
      : (pluginProfileText(
          plugins.find((plugin) => plugin.manifest.id === source.pluginId)?.renderer,
          "profileSummary",
          source,
        ) ?? "Plugin-managed connection");
  return `${label} · ${profile.brokers.join(", ")}`;
}

export interface ProfilePanelProperties {
  readonly transfer?: import("./text-document-transfer").TextDocumentTransferPort | undefined;
  readonly activityOpen: boolean;
  readonly connected: boolean;
  readonly connectionError?: string;
  readonly connectionOperation: ProfileConnectionOperation | null;
  readonly filter: string;
  readonly host: StreamSkopeHost;
  readonly loading: boolean;
  readonly onFilterChange: (value: string) => void;
  readonly onOpenActivity: (correlationId?: string) => void;
  readonly onOpenPlugins?: (() => void) | undefined;
  readonly onProfileAction: (profileId: string, action: ProfileAction) => void;
  readonly onSelectProfile: (profileId: string | null) => void;
  readonly onToggleConnection: (profile: ProfileSummary) => void;
  readonly profiles: readonly ProfileSummary[];
  readonly requestError?: string;
  readonly selectedProfileId: string | null;
  readonly store: ProfileStoreCapability | null;
}

export function ProfilePanel({
  transfer,
  activityOpen,
  connected,
  connectionError,
  connectionOperation,
  filter,
  host,
  loading,
  onFilterChange,
  onOpenActivity,
  onOpenPlugins,
  onProfileAction,
  onSelectProfile,
  onToggleConnection,
  profiles,
  requestError,
  selectedProfileId,
  store,
}: ProfilePanelProperties): React.JSX.Element {
  const [activityRequestedFromDialog, setActivityRequestedFromDialog] = useState(false);
  const { plugins } = usePlugins();
  const [pluginAction, setPluginAction] = useState<{
    readonly pluginId: string;
    readonly actionId: string;
    readonly activationId: string;
  }>();
  const activePlugin = plugins.find(
    (plugin) =>
      plugin.manifest.id === pluginAction?.pluginId &&
      plugin.activationId === pluginAction.activationId,
  );
  useEffect(() => {
    if (pluginAction !== undefined && activePlugin === undefined) setPluginAction(undefined);
  }, [pluginAction, activePlugin]);
  const [pendingPluginProfileId, setPendingPluginProfileId] = useState<string | null>(null);
  const [profileDialogOpen, setProfileDialogOpen] = useState(false);
  const [initialDestination, setInitialDestination] = useState<{
    name: string;
    brokers: readonly string[];
  }>();
  const [addAnchor, setAddAnchor] = useState<HTMLElement | null>(null);
  const [profileActionMenu, setProfileActionMenu] = useState<{
    readonly anchor: HTMLElement;
    readonly profile: ProfileSummary;
  } | null>(null);
  const normalizedFilter = filter.trim().toLocaleLowerCase("en-US");
  const filteredProfiles = useMemo(
    () =>
      normalizedFilter.length === 0
        ? profiles
        : profiles.filter(
            (profile) =>
              profile.name.toLocaleLowerCase("en-US").includes(normalizedFilter) ||
              profile.brokers.some((broker) =>
                broker.toLocaleLowerCase("en-US").includes(normalizedFilter),
              ),
          ),
    [normalizedFilter, profiles],
  );
  const unavailable = store?.state === "unavailable";
  const storageLabel =
    store?.state !== "ready"
      ? null
      : store.durability === "durable"
        ? "OS-protected profiles. Encrypted by the operating-system credential service."
        : "Session-only profiles. Available only while this development host is running.";

  useEffect(() => {
    if (activityRequestedFromDialog && !activityOpen) {
      setActivityRequestedFromDialog(false);
    }
  }, [activityOpen, activityRequestedFromDialog]);

  useEffect(() => {
    if (loading || unavailable) return;
    if (pendingPluginProfileId !== null) {
      const captured = profiles.find((profile) => profile.id === pendingPluginProfileId);
      if (captured !== undefined) {
        if (filter.length > 0) onFilterChange("");
        onSelectProfile(captured.id);
        setPendingPluginProfileId(null);
        return;
      }
    }
    const nextSelectedProfileId = filteredProfiles[0]?.id ?? null;
    if (
      selectedProfileId !== null &&
      filteredProfiles.some((profile) => profile.id === selectedProfileId)
    ) {
      return;
    }
    if (selectedProfileId !== nextSelectedProfileId) {
      onSelectProfile(nextSelectedProfileId);
    }
  }, [
    filter.length,
    filteredProfiles,
    loading,
    onFilterChange,
    onSelectProfile,
    pendingPluginProfileId,
    profiles,
    selectedProfileId,
    unavailable,
  ]);

  return (
    <Box component="section" sx={{ minWidth: 0 }}>
      <Stack
        data-testid="profile-search-actions"
        direction="row"
        sx={{
          alignItems: "center",
          borderBottom: 1,
          borderColor: "divider",
          boxSizing: "border-box",
          gap: 0,
          minHeight: streamSkopeGeometry.resourceSearchHeight,
          px: `${String(streamSkopeGeometry.resourcePanelInline)}px`,
          py: `${String(streamSkopeGeometry.resourceTabGap)}px`,
          "& .MuiOutlinedInput-root": {
            borderBottomRightRadius: 0,
            borderTopRightRadius: 0,
          },
        }}
      >
        <TextField
          onChange={(event) => onFilterChange(event.target.value)}
          placeholder="Search profiles…"
          slotProps={{ htmlInput: { "aria-label": "Search profiles", type: "search" } }}
          value={filter}
        />
        <Button
          aria-label="Add connection"
          aria-haspopup="menu"
          aria-expanded={addAnchor !== null}
          disabled={loading || unavailable}
          onClick={(event) => setAddAnchor(event.currentTarget)}
          startIcon={<WorkbenchIcon fontSize="small" name="add" />}
          sx={{
            border: 1,
            borderColor: "divider",
            borderLeft: 0,
            borderBottomLeftRadius: 0,
            borderTopLeftRadius: 0,
            flex: "0 0 auto",
            whiteSpace: "nowrap",
          }}
          variant="outlined"
        >
          Add connection
        </Button>
      </Stack>

      <Menu anchorEl={addAnchor} open={addAnchor !== null} onClose={() => setAddAnchor(null)}>
        <MenuItem
          onClick={() => {
            setAddAnchor(null);
            setInitialDestination(undefined);
            setProfileDialogOpen(true);
          }}
        >
          Existing Kafka cluster
        </MenuItem>
        {plugins.flatMap((plugin) =>
          plugin.renderer.connectionActions.map((action) => (
            <MenuItem
              key={`${plugin.manifest.id}:${action.id}`}
              disabled={connected}
              onClick={() => {
                setAddAnchor(null);
                setPluginAction({
                  pluginId: plugin.manifest.id,
                  actionId: action.id,
                  activationId: plugin.activationId,
                });
              }}
            >
              {action.label}
            </MenuItem>
          )),
        )}
        {onOpenPlugins === undefined ? null : (
          <MenuItem
            onClick={() => {
              setAddAnchor(null);
              onOpenPlugins();
            }}
          >
            Manage plugins
          </MenuItem>
        )}
      </Menu>

      <Box sx={{ minWidth: 0 }}>
        {loading ? (
          <Typography
            aria-label="Profile list status"
            aria-live="polite"
            color="text.secondary"
            role="status"
            sx={{ p: 1 }}
            variant="body2"
          >
            Loading profiles…
          </Typography>
        ) : null}
        {unavailable ? (
          <Alert severity="error" sx={{ mx: 0.75, my: 0.5 }}>
            <Typography component="p" variant="subtitle2">
              Profile storage unavailable
            </Typography>
            <Typography component="p" variant="body2">
              Existing connection state was not changed.
            </Typography>
            <Typography component="p" variant="body2">
              {store.recovery ?? "Restart StreamSkope after protected storage becomes available."}
            </Typography>
          </Alert>
        ) : null}
        {requestError === undefined ? null : (
          <Alert severity="error" sx={{ mx: 0.75, my: 0.5 }}>
            <Typography component="p" variant="subtitle2">
              Profiles could not be loaded
            </Typography>
            {requestError}
          </Alert>
        )}
        {connectionError === undefined ? null : (
          <Alert severity="error" sx={{ mx: 0.75, my: 0.5 }}>
            <Typography component="p" variant="subtitle2">
              Connection action failed
            </Typography>
            <Typography component="p" variant="body2">
              {connectionError}
            </Typography>
          </Alert>
        )}

        {!loading && !unavailable && profiles.length === 0 ? (
          <Box
            aria-label="No profiles configured"
            role="status"
            sx={{ px: `${String(streamSkopeGeometry.resourcePanelInline)}px`, py: 1 }}
          >
            <Typography color="text.secondary" component="p" variant="body2">
              Add a connection profile to connect to Kafka.
            </Typography>
          </Box>
        ) : null}
        {!loading && !unavailable && profiles.length > 0 && filteredProfiles.length === 0 ? (
          <Box sx={{ p: 1 }}>
            <Typography color="text.secondary" variant="body2">
              No profiles match &quot;{filter.trim()}&quot;.
            </Typography>
            <Typography color="text.secondary" variant="caption">
              Clear the profile search to show saved profiles.
            </Typography>
          </Box>
        ) : null}
        {!loading && filteredProfiles.length > 0 ? (
          <List aria-label="Kafka connection profiles" dense disablePadding>
            {filteredProfiles.map((profile) => (
              <ListItem
                disablePadding
                key={profile.id}
                sx={{
                  alignItems: "stretch",
                  bgcolor: selectedProfileId === profile.id ? "background.default" : "transparent",
                  boxShadow:
                    selectedProfileId === profile.id
                      ? "inset 2px 0 0 var(--mui-palette-primary-main)"
                      : "none",
                  minHeight: streamSkopeGeometry.profileRowHeight,
                  "&:hover": {
                    bgcolor:
                      selectedProfileId === profile.id ? "background.default" : "action.hover",
                  },
                }}
              >
                <ListItemButton
                  aria-label={`Select profile ${profile.name}`}
                  onClick={() => {
                    onSelectProfile(profile.id);
                  }}
                  selected={selectedProfileId === profile.id}
                  sx={{
                    borderRadius: 0,
                    bgcolor: "transparent",
                    minHeight: streamSkopeGeometry.profileRowHeight,
                    minWidth: 0,
                    px: `${String(streamSkopeGeometry.resourcePanelInline)}px`,
                    "&:hover": { bgcolor: "transparent" },
                    "&.Mui-selected": { bgcolor: "transparent" },
                    "&.Mui-selected:hover": { bgcolor: "transparent" },
                  }}
                >
                  <ListItemText
                    primary={profile.name}
                    secondary={profileEndpointSummary(profile, plugins)}
                    slotProps={{
                      primary: {
                        noWrap: true,
                        sx: streamSkopeMuiResourceTypography,
                      },
                      secondary: {
                        noWrap: true,
                        sx: streamSkopeMuiMonospaceTypography,
                        variant: "caption",
                      },
                    }}
                    sx={{ my: 0 }}
                    title={`${profile.name} · ${profileEndpointSummary(profile, plugins)}`}
                  />
                </ListItemButton>
                <Box
                  sx={{
                    alignItems: "center",
                    display: "flex",
                    flex: "0 0 auto",
                    pr: `${String(streamSkopeGeometry.resourcePanelInline)}px`,
                  }}
                >
                  <ButtonGroup
                    aria-label={`Actions for profile ${profile.name}`}
                    size="small"
                    variant="outlined"
                    sx={{
                      "& .MuiButtonGroup-grouped": {
                        minWidth: 0,
                      },
                    }}
                  >
                    <Button
                      aria-label={`${connected && profile.active ? "Disconnect" : "Connect"} ${(profile.transport ?? "tls") === "plaintext" ? "insecure plaintext " : ""}profile ${profile.name}`}
                      aria-busy={connectionOperation?.profileId === profile.id || undefined}
                      color={connected && profile.active ? "inherit" : "primary"}
                      disabled={
                        connectionOperation !== null ||
                        (!(connected && profile.active) &&
                          profile.source !== undefined &&
                          !plugins.some(
                            (plugin) => plugin.manifest.id === profile.source?.pluginId,
                          ))
                      }
                      onClick={() => {
                        onToggleConnection(profile);
                      }}
                      sx={{
                        minWidth: 0,
                        px: 1,
                        whiteSpace: "nowrap",
                      }}
                      variant={
                        selectedProfileId === profile.id || (connected && profile.active)
                          ? "contained"
                          : "outlined"
                      }
                    >
                      <WorkbenchIcon
                        data-testid={
                          connected && profile.active ? "profile-stop-icon" : "profile-play-icon"
                        }
                        fontSize="small"
                        name={connected && profile.active ? "stop" : "play"}
                      />
                      {connected && profile.active ? "Disconnect" : "Connect"}
                    </Button>
                    <Button
                      aria-controls={
                        profileActionMenu?.profile.id === profile.id
                          ? "kafka-profile-actions-menu"
                          : undefined
                      }
                      aria-expanded={profileActionMenu?.profile.id === profile.id}
                      aria-haspopup="menu"
                      aria-label={`More actions for profile ${profile.name}`}
                      onClick={(event) => {
                        onSelectProfile(profile.id);
                        setProfileActionMenu({ anchor: event.currentTarget, profile });
                      }}
                      title="More actions"
                      variant="outlined"
                      sx={{ px: 0.75 }}
                    >
                      <WorkbenchIcon fontSize="small" name="more" />
                    </Button>
                  </ButtonGroup>
                </Box>
              </ListItem>
            ))}
          </List>
        ) : null}
      </Box>

      <Menu
        anchorEl={profileActionMenu?.anchor}
        id="kafka-profile-actions-menu"
        onClose={() => setProfileActionMenu(null)}
        open={profileActionMenu !== null}
        slotProps={{
          list: {
            "aria-label":
              profileActionMenu === null
                ? "Profile actions"
                : `Profile actions for ${profileActionMenu.profile.name}`,
          },
        }}
      >
        <MenuItem
          disabled={connected && (profileActionMenu?.profile.active ?? false)}
          onClick={() => {
            if (profileActionMenu === null) return;
            onProfileAction(profileActionMenu.profile.id, "edit");
            setProfileActionMenu(null);
          }}
        >
          Edit
        </MenuItem>
        <MenuItem
          disabled={!(connected && (profileActionMenu?.profile.active ?? false))}
          onClick={() => {
            if (profileActionMenu === null) return;
            onProfileAction(profileActionMenu.profile.id, "cluster");
            setProfileActionMenu(null);
          }}
        >
          Cluster detail
        </MenuItem>
        <MenuItem
          disabled={connected && (profileActionMenu?.profile.active ?? false)}
          onClick={() => {
            if (profileActionMenu === null) return;
            onProfileAction(profileActionMenu.profile.id, "delete");
            setProfileActionMenu(null);
          }}
          sx={{ color: "error.main" }}
        >
          Delete
        </MenuItem>
      </Menu>

      {storageLabel === null ? null : (
        <Typography
          aria-label="Profile storage status"
          aria-live="polite"
          color="text.secondary"
          role="status"
          sx={{ px: `${String(streamSkopeGeometry.resourcePanelInline)}px`, py: 0.5 }}
          variant="caption"
          title={storageLabel}
        >
          {store?.durability === "durable" ? "OS-protected profiles" : "Session-only profiles"}
        </Typography>
      )}

      {profileDialogOpen ? (
        <ProfileDialog
          {...(initialDestination === undefined ? {} : { initialDestination })}
          host={host}
          onClose={() => {
            setProfileDialogOpen(false);
            setActivityRequestedFromDialog(false);
          }}
          onOpenActivity={(correlationId) => {
            setActivityRequestedFromDialog(true);
            onOpenActivity(correlationId);
          }}
          open={!activityRequestedFromDialog}
          transfer={transfer}
        />
      ) : null}
      {pluginAction !== undefined && activePlugin !== undefined ? (
        <PluginView
          renderer={activePlugin.renderer}
          lifetime={activePlugin.lifetime}
          activationId={activePlugin.activationId}
          context={{
            view: "connection",
            actionId: pluginAction.actionId,
            host,
            profiles,
            onClose: () => setPluginAction(undefined),
            onExistingDestination: (destination) => {
              setInitialDestination(destination);
              setPluginAction(undefined);
              setProfileDialogOpen(true);
            },
            onProfileReady: (profileId) => {
              setPendingPluginProfileId(profileId);
              setPluginAction(undefined);
            },
          }}
        />
      ) : null}
    </Box>
  );
}
