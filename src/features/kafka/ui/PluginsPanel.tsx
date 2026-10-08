import { Box, Stack, Typography } from "@mui/material";

import type { StreamSkopeHost } from "../contracts";
import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";

import { usePlugins } from "./PluginsProvider";
import { usePluginInventory } from "./usePluginInventory";
import { usePluginChanges } from "./usePluginChanges";
import { PluginCachedPackages, PluginManagementCards } from "./PluginManagementCards";
import { PluginPackageReviewDialog } from "./PluginPackageReviewDialog";
import { PluginLocalChangeDialog } from "./PluginLocalChangeDialog";
import { usePluginAcquisitions } from "./usePluginAcquisitions";
import { usePluginNetwork } from "./usePluginNetwork";
import { PluginNetworkSettings } from "./PluginNetworkSettings";
import { PluginAcquisitionStatus } from "./PluginAcquisitionStatus";

export function PluginsPanel({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const {
    errors: rendererErrors,
    loading: renderersLoading,
    refresh: refreshRenderers,
  } = usePlugins();
  const acquisitions = usePluginAcquisitions(host);
  const inventory = usePluginInventory(host, acquisitions.execute);
  const changes = usePluginChanges(host, inventory, refreshRenderers, acquisitions);
  const network = usePluginNetwork(host, acquisitions);
  const {
    snapshot,
    catalog,
    delivery,
    deliveryFailure,
    installedLoading,
    catalogLoading,
    deliveryLoading,
  } = inventory;
  const disabled =
    installedLoading || changes.pending !== undefined || snapshot.error !== undefined;
  const localInspectionPending =
    changes.inspectionPending !== undefined && changes.inspectionPending !== "catalog";
  const remoteDisabled =
    network.snapshot?.configuration === null || network.snapshot?.configuration?.offline === true;
  const completedEntry = snapshot.plugins.find((entry) => entry.id === changes.completed?.id);
  const busyPluginIds = snapshot.plugins
    .filter((entry) => entry.transition !== undefined)
    .map((entry) => entry.id);
  const reviewTransition = snapshot.plugins.find(
    (entry) => entry.id === changes.review?.manifest.id,
  )?.transition;
  const confirmationTransition = snapshot.plugins.find(
    (entry) => entry.id === changes.confirmation?.plugin.id,
  )?.transition;
  const showStatus =
    changes.status.length > 0 &&
    (changes.completed === undefined ||
      (completedEntry?.installed?.version === changes.completed.version &&
        completedEntry?.active?.version === changes.completed.version &&
        completedEntry?.error === undefined &&
        !renderersLoading &&
        rendererErrors[changes.completed.id] === undefined));
  return (
    <Stack spacing={2}>
      <Stack
        direction="row"
        spacing={2}
        sx={{ alignItems: "start", justifyContent: "space-between" }}
      >
        <Box>
          <Typography component="h3" variant="subtitle1">
            Plugins
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Add optional connection workflows from official StreamSkope releases. Install, update
            and remove plugins without restarting StreamSkope.
          </Typography>
        </Box>
        <Button
          disabled={catalogLoading || remoteDisabled}
          variant="text"
          onClick={(): void => {
            void Promise.all([
              inventory.refreshInstalled(),
              inventory.refreshCatalog(),
              inventory.refreshDelivery(),
              refreshRenderers(),
            ]);
          }}
        >
          Check for updates
        </Button>
      </Stack>
      <PluginNetworkSettings network={network} />
      {network.snapshot?.configuration?.offline !== true ? null : (
        <Alert severity="info">
          Plugin downloads are offline. Installed plugins, signed local files and cached packages
          remain available.
        </Alert>
      )}
      <Stack spacing={1}>
        <Button
          disabled={
            delivery?.fileInstallationAvailable !== true ||
            changes.pending !== undefined ||
            localInspectionPending
          }
          variant="outlined"
          onClick={(): void => {
            void changes.inspect({ source: "file" });
          }}
          sx={{ alignSelf: "start" }}
        >
          Install from file
        </Button>
        <Typography color="text.secondary" variant="body2">
          {deliveryLoading && delivery === undefined
            ? "Checking local installation support…"
            : delivery === undefined
              ? "File installation support could not be checked."
              : delivery.fileInstallationAvailable
                ? "Select a publisher-signed .skope-plugin file and review it before installing."
                : "File installation requires the desktop app."}
        </Typography>
      </Stack>
      {snapshot.error === undefined ? null : (
        <Alert severity="error">Installed plugins could not be read. {snapshot.error}</Alert>
      )}
      {deliveryFailure === undefined ? null : (
        <Alert severity="warning">
          Local plugin packages could not be checked. {deliveryFailure}
        </Alert>
      )}
      {catalog.error === undefined ? null : (
        <Alert severity="warning">
          {catalog.source === "live"
            ? "The plugin catalog was refreshed, but its local copy could not be saved. "
            : "The plugin catalog is unavailable. Installed plugins remain available. "}
          {catalog.error}
        </Alert>
      )}
      {changes.failure === undefined ? null : <Alert severity="error">{changes.failure}</Alert>}
      <PluginAcquisitionStatus
        acquisitions={acquisitions}
        onCancelInspection={changes.cancelInspection}
      />
      {changes.inspectionStatus.length === 0 ? null : (
        <Typography role="status" variant="body2">
          {changes.inspectionStatus}
        </Typography>
      )}
      {!installedLoading ? null : (
        <Typography role="status" variant="body2">
          Loading installed plugins…
        </Typography>
      )}
      {!catalogLoading ? null : (
        <Typography role="status" variant="body2">
          Checking for plugin updates…
        </Typography>
      )}
      {catalog.checkedAt === undefined ? null : (
        <Typography color="text.secondary" variant="body2">
          {catalog.source === "cache" ? "Cached catalog · Last checked " : "Catalog checked "}
          {new Date(catalog.checkedAt).toLocaleString()}
        </Typography>
      )}
      {!showStatus ? null : (
        <Typography role="status" aria-live="polite" variant="body2">
          {changes.status}
        </Typography>
      )}
      <PluginManagementCards
        installations={snapshot.plugins}
        catalog={catalog}
        rendererErrors={rendererErrors}
        installedLoading={installedLoading}
        disabled={disabled}
        remoteDisabled={remoteDisabled || changes.inspectionPending !== undefined}
        onInspect={changes.inspect}
        onLocal={changes.prepareLocal}
      />
      {delivery === undefined ? null : (
        <PluginCachedPackages
          packages={delivery.cachedPackages}
          disabled={disabled || localInspectionPending}
          busyPluginIds={busyPluginIds}
          onInspect={changes.inspect}
        />
      )}
      <PluginPackageReviewDialog
        review={changes.review}
        prompt={changes.reviewPrompt}
        pending={changes.pending !== undefined}
        transition={reviewTransition}
        onApply={changes.applyReview}
        onClose={changes.closeReview}
      />
      <PluginLocalChangeDialog
        confirmation={changes.confirmation}
        pending={changes.pending !== undefined}
        transition={confirmationTransition}
        onCancel={changes.cancelLocal}
        onConfirm={changes.confirmLocal}
      />
    </Stack>
  );
}
