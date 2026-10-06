import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

import type {
  ProviderConnectionOutcome,
  ProviderProfileManagementControls,
} from "../../../platform/ui/provider-workspaces";
import {
  StudioAlert as Alert,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioButton as Button,
} from "../../../platform/ui/controls";
import type {
  ExternalUrlOpenResult,
  HostCommand,
  HostCommandResponse,
  StreamSkopeHost,
} from "../contracts";

import { ActivityLogDrawer } from "./ActivityLogDrawer";
import { PluginsProvider, usePlugins, type LoadedPluginRenderer } from "./PluginsProvider";
import { PluginsPanel } from "./PluginsPanel";
import { PluginView } from "./PluginView";
import { ProfileDialog } from "./ProfileDialog";
import { ProfileDeleteDialog, ProfileWorkspace } from "./ProfileWorkspace";
import { createTextDocumentTransfer } from "./text-document-transfer";
import type { KafkaProfileCatalog } from "./profile-catalog";

/** Management views can resolve/save credentials and plugins; stream commands require activation. */
export function createKafkaProfileManagementHost(
  host: StreamSkopeHost,
  isInteractive: () => boolean,
  canReadCluster: () => boolean = (): boolean => false,
): StreamSkopeHost {
  const requireInteractive = (): void => {
    if (!isInteractive())
      throw new Error("This Kafka profile management view is no longer active.");
  };
  return {
    execute: async <Command extends HostCommand>(
      command: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      requireInteractive();
      if (!(
        (command.command.startsWith("profiles.") && command.command !== "profiles.connect") ||
        command.command.startsWith("recipes.") ||
        command.command.startsWith("trustAcquisition.") ||
        command.command.startsWith("plugins.") ||
        command.command === "plugin.execute" ||
        ((command.command === "clusterDetails.load" ||
          command.command === "clusterDetails.export") &&
          canReadCluster())
      ))
        throw new Error(
          "Activate a saved profile through the connection catalog for this command.",
        );
      return host.execute(command);
    },
    openExternalUrl: async (url): Promise<ExternalUrlOpenResult> => {
      requireInteractive();
      return host.openExternalUrl(url);
    },
    subscribe: (listener): (() => void) =>
      isInteractive() ? host.subscribe(listener) : (): void => undefined,
  };
}

function KafkaConnectorView({
  plugin,
  actionId,
  catalog,
  controls,
  host,
  onExistingDestination,
}: {
  readonly plugin: LoadedPluginRenderer;
  readonly actionId: string;
  readonly catalog: KafkaProfileCatalog;
  readonly controls: ProviderProfileManagementControls;
  readonly host: StreamSkopeHost;
  readonly onExistingDestination: (destination: {
    readonly name: string;
    readonly brokers: readonly string[];
  }) => void;
}): React.JSX.Element | null {
  const [captured] = useState(plugin);
  const { plugins } = usePlugins();
  const current = plugins.some(
    (entry) =>
      entry.activationId === captured.activationId && entry.manifest.id === captured.manifest.id,
  );
  useEffect(() => {
    if (!current && controls.isInteractive()) controls.onClose();
  }, [current, controls]);
  if (!current) return null;
  return (
    <PluginView
      renderer={captured.renderer}
      lifetime={captured.lifetime}
      activationId={captured.activationId}
      context={{
        view: "connection",
        actionId,
        host,
        profiles: catalog.getManagementSnapshot().profiles,
        onClose: controls.onClose,
        onProfileReady: (profileId): void => {
          if (!controls.isInteractive()) return;
          controls.onProfileReady(profileId);
          controls.onClose();
        },
        onExistingDestination,
      }}
    />
  );
}

export function KafkaProfileManagement({
  catalog,
  controls,
}: {
  readonly catalog: KafkaProfileCatalog;
  readonly controls: ProviderProfileManagementControls;
}): React.JSX.Element {
  return (
    <PluginsProvider host={catalog.properties.host} importer={catalog.properties.pluginImporter}>
      <KafkaProfileManagementContents catalog={catalog} controls={controls} />
    </PluginsProvider>
  );
}

function KafkaProfileManagementContents({
  catalog,
  controls,
}: {
  readonly catalog: KafkaProfileCatalog;
  readonly controls: ProviderProfileManagementControls;
}): React.JSX.Element | null {
  const native = useSyncExternalStore(catalog.subscribe, catalog.getManagementSnapshot);
  const { plugins, loading } = usePlugins();
  useEffect(() => catalog.setPlugins(plugins, loading), [catalog, plugins, loading]);
  useEffect(() => {
    void catalog.facet.refresh();
  }, [catalog]);
  const current = useRef(controls);
  current.current = controls;
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return (): void => {
      mounted.current = false;
    };
  }, []);
  const action = controls.action;
  const ownsAction = useMemo(
    () => (): boolean =>
      mounted.current && current.current.action === action && current.current.isInteractive(),
    [action],
  );
  const host = useMemo(
    () =>
      createKafkaProfileManagementHost(catalog.properties.host, ownsAction, (): boolean => {
        if (action?.kind !== "provider" || action.actionId !== "cluster") return false;
        const current = catalog.getManagementSnapshot();
        return (
          current.connectionEvent?.payload.state === "connected" &&
          current.profiles.some((profile) => profile.id === action.profileId && profile.active)
        );
      }),
    [catalog, ownsAction, action],
  );
  const transfer = useMemo(() => createTextDocumentTransfer(catalog.properties.desktop), [catalog]);
  const [activity, setActivity] = useState<{ readonly correlationId?: string } | null>(null);
  const [managingPlugins, setManagingPlugins] = useState(false);
  const [destination, setDestination] = useState<{
    readonly name: string;
    readonly brokers: readonly string[];
  }>();
  useEffect(() => {
    setActivity(null);
    setManagingPlugins(false);
    setDestination(undefined);
  }, [action]);
  const close = (): void => {
    if (ownsAction()) controls.onClose();
  };
  const profileReady = (profileId: string): void => {
    if (ownsAction()) controls.onProfileReady(profileId);
  };
  const connect = (profileId: string): Promise<ProviderConnectionOutcome> => {
    if (!ownsAction())
      return Promise.resolve({
        ok: false,
        summary: "This profile editor is no longer active.",
        recovery: "Open the saved profile in the catalog and retry connecting.",
      });
    const revision = catalog
      .getManagementSnapshot()
      .profiles.find((profile) => profile.id === profileId)?.revision;
    return controls.onConnect({ id: profileId, ...(revision === undefined ? {} : { revision }) });
  };
  if (action === null) return null;
  const profile =
    action.kind === "create"
      ? undefined
      : native.profiles.find((entry) => entry.id === action.profileId);
  const managePlugins =
    managingPlugins || (action.kind === "create" && action.actionId === "manage-plugins");
  const creation =
    action.kind === "create"
      ? plugins
          .flatMap((plugin) =>
            plugin.renderer.connectionActions.map((contribution) => ({
              plugin,
              contribution,
              id: `plugin:${plugin.manifest.id}:${contribution.id}`,
            })),
          )
          .find((entry) => entry.id === action.actionId)
      : undefined;
  return (
    <>
      {managePlugins ? (
        <Dialog
          aria-labelledby="catalog-plugin-management-title"
          open
          fullWidth
          maxWidth="lg"
          onClose={managingPlugins ? (): void => setManagingPlugins(false) : close}
        >
          <DialogTitle id="catalog-plugin-management-title">Manage plugins</DialogTitle>
          <DialogContent dividers>
            <Box inert={!controls.isInteractive()}>
              <PluginsPanel host={host} />
            </Box>
          </DialogContent>
          <DialogActions>
            <Button onClick={managingPlugins ? (): void => setManagingPlugins(false) : close}>
              Close
            </Button>
          </DialogActions>
        </Dialog>
      ) : action.kind === "create" && action.actionId !== "direct" && destination === undefined ? (
        creation === undefined ? (
          <Alert severity="warning">
            This Kafka connection workflow is no longer available. Choose another connection source.
          </Alert>
        ) : (
          <KafkaConnectorView
            key={creation.id}
            plugin={creation.plugin}
            actionId={creation.contribution.id}
            catalog={catalog}
            controls={{ ...controls, isInteractive: ownsAction, onClose: close }}
            host={host}
            onExistingDestination={(next): void => {
              if (ownsAction()) setDestination(next);
            }}
          />
        )
      ) : action.kind === "create" || action.kind === "edit" ? (
        action.kind === "edit" && profile === undefined ? (
          <Alert severity="warning">
            This Kafka profile is no longer available. Refresh the catalog.
          </Alert>
        ) : (
          <ProfileDialog
            key={
              action.kind === "create" ? `create:${action.actionId}` : `edit:${action.profileId}`
            }
            host={host}
            profile={profile}
            initialDestination={destination}
            open={activity === null}
            onClose={close}
            onOpenActivity={(correlationId): void => {
              if (ownsAction()) setActivity(correlationId === undefined ? {} : { correlationId });
            }}
            onConnectProfile={connect}
            onProfileSaved={profileReady}
            transfer={transfer}
          />
        )
      ) : profile === undefined ? (
        <Alert severity="warning">
          This Kafka profile is no longer available. Refresh the catalog.
        </Alert>
      ) : action.kind === "delete" ? (
        <ProfileDeleteDialog host={host} profile={profile} onClose={close} />
      ) : (
        <ProfileWorkspace
          activityOpen={activity !== null}
          action={action.kind === "provider" && action.actionId === "cluster" ? "cluster" : null}
          clusterDiagnostics={native.clusterDiagnostics}
          component="section"
          host={host}
          onActionClose={close}
          onOpenActivity={(correlationId): void => {
            if (ownsAction()) setActivity(correlationId === undefined ? {} : { correlationId });
          }}
          onOpenPlugins={(): void => {
            if (ownsAction()) setManagingPlugins(true);
          }}
          profile={profile}
          transfer={transfer}
        />
      )}
      {activity === null ? null : (
        <Dialog
          aria-labelledby="catalog-kafka-activity-title"
          open
          fullWidth
          maxWidth="lg"
          onClose={(): void => setActivity(null)}
        >
          <DialogTitle id="catalog-kafka-activity-title">Kafka activity</DialogTitle>
          <DialogContent dividers>
            <Typography color="text.secondary" variant="body2">
              Host-confirmed Kafka profile and connection activity.
            </Typography>
            <ActivityLogDrawer
              entries={native.activities}
              open
              onClose={(): void => setActivity(null)}
              {...(activity.correlationId === undefined
                ? {}
                : { initialQuery: activity.correlationId })}
              transfer={transfer}
            />
          </DialogContent>
        </Dialog>
      )}
    </>
  );
}
