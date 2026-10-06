import { Box, Stack, Typography } from "@mui/material";

import type { StreamSkopeHost } from "../contracts";

import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";
import { usePlugins } from "./PluginsProvider";
import { usePluginInventory } from "./usePluginInventory";
import { usePluginChanges } from "./usePluginChanges";
import { PluginCachedPackages, PluginManagementCards } from "./PluginManagementCards";
import { PluginPackageReviewDialog } from "./PluginPackageReviewDialog";
import { PluginLocalChangeDialog } from "./PluginLocalChangeDialog";

export function PluginsPanel({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const {
    errors: rendererErrors,
    loading: renderersLoading,
    refresh: refreshRenderers,
  } = usePlugins();
  const inventory = usePluginInventory(host);
  const changes = usePluginChanges(host, inventory, refreshRenderers);
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
  const completedEntry = snapshot.plugins.find((entry) => entry.id === changes.completed?.id);
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
          disabled={catalogLoading || changes.pending !== undefined}
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
      <Stack spacing={1}>
        <Button
          disabled={delivery?.fileInstallationAvailable !== true || changes.pending !== undefined}
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
        onInspect={changes.inspect}
        onLocal={changes.prepareLocal}
      />
      {delivery === undefined ? null : (
        <PluginCachedPackages
          packages={delivery.cachedPackages}
          disabled={disabled}
          onInspect={changes.inspect}
        />
      )}
      <PluginPackageReviewDialog
        review={changes.review}
        prompt={changes.reviewPrompt}
        pending={changes.pending !== undefined}
        onApply={changes.applyReview}
        onClose={changes.closeReview}
      />
      <PluginLocalChangeDialog
        confirmation={changes.confirmation}
        pending={changes.pending !== undefined}
        onCancel={changes.cancelLocal}
        onConfirm={changes.confirmLocal}
      />
    </Stack>
  );
}
