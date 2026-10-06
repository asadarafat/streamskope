import { Box, Stack, Typography } from "@mui/material";

import type {
  PluginCatalogSnapshot,
  PluginInstallation,
  PluginManifest,
} from "../../../plugins/contracts";
import { comparePluginManifests } from "../../../plugins/validation";
import { StudioAlert as Alert, StudioButton as Button } from "../../../platform/ui/controls";

import { PluginCompatibility } from "./PluginPackageReviewDialog";
import type {
  PluginActionTarget,
  PluginInspectionInput,
  PluginLocalCommand,
} from "./usePluginChanges";
import type { PluginDelivery } from "./usePluginInventory";

interface CardActions {
  readonly disabled: boolean;
  readonly remoteDisabled: boolean;
  readonly onInspect: (input: PluginInspectionInput) => Promise<void>;
  readonly onLocal: (command: PluginLocalCommand, plugin: PluginActionTarget) => Promise<void>;
}

function PluginCard({
  manifest,
  installation,
  available,
  reference,
  error,
  disabled,
  remoteDisabled,
  onInspect,
  onLocal,
}: CardActions & {
  readonly manifest: PluginManifest;
  readonly installation: PluginInstallation | undefined;
  readonly available: PluginManifest | undefined;
  readonly reference: NonNullable<PluginCatalogSnapshot["packages"]>[number] | undefined;
  readonly error: string | undefined;
}): React.JSX.Element {
  const installed = installation?.installed;
  const current = [installed, installation?.active]
    .filter((entry): entry is PluginManifest => entry !== undefined)
    .sort(comparePluginManifests)
    .at(-1);
  const catalogIsOlder =
    available !== undefined &&
    current !== undefined &&
    comparePluginManifests(available, current) < 0;
  const update =
    installed !== undefined &&
    available !== undefined &&
    comparePluginManifests(available, installed) > 0;
  const remoteAction =
    available !== undefined && !catalogIsOlder && (installation === undefined || update);
  return (
    <Box
      component="section"
      aria-label={manifest.name}
      sx={{ p: 2, border: 1, borderColor: "divider", borderRadius: 1 }}
    >
      <Stack spacing={1}>
        <Typography component="h5" variant="subtitle1">
          {manifest.name}
        </Typography>
        {manifest.description === undefined ? null : (
          <Typography variant="body2">{manifest.description}</Typography>
        )}
        <Typography color="text.secondary" variant="body2">
          {installed === undefined
            ? `Available version ${manifest.version}`
            : `Installed version ${installed.version}`}
          {manifest.targetEdaVersion === undefined
            ? ""
            : ` · Target EDA ${manifest.targetEdaVersion}`}
        </Typography>
        <PluginCompatibility
          manifest={update && available !== undefined ? available : manifest}
          update={update}
        />
        <Typography variant="body2">
          {installation?.active !== undefined
            ? `Active version ${installation.active.version}`
            : installed === undefined
              ? "Not installed"
              : "Installed, but not active"}
        </Typography>
        {error === undefined ? null : <Alert severity="error">{error}</Alert>}
        {error === undefined || !catalogIsOlder ? null : (
          <Alert severity="warning">
            The catalog offers older version {available?.version}. Installed version{" "}
            {current?.version} is newer; this older package cannot be used as an update.
          </Alert>
        )}
        <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
          {!remoteAction ? null : (
            <Button
              disabled={disabled || remoteDisabled || reference === undefined}
              variant="contained"
              onClick={(): void => {
                if (reference !== undefined) void onInspect({ source: "catalog", ...reference });
              }}
            >
              {update ? `Update to ${available.version}` : "Install"}
            </Button>
          )}
          {installed === undefined ||
          (installation?.active !== undefined && error === undefined) ? null : (
            <Button
              disabled={disabled}
              variant="contained"
              onClick={(): void => {
                void onLocal("plugins.retry", installed);
              }}
            >
              Retry activation
            </Button>
          )}
          {installation === undefined ? null : (
            <Button
              disabled={disabled}
              color="error"
              variant="text"
              onClick={(): void => {
                void onLocal("plugins.remove", installed ?? manifest);
              }}
            >
              Remove
            </Button>
          )}
        </Stack>
        {!remoteAction || reference !== undefined ? null : (
          <Typography variant="body2" color="text.secondary">
            Refresh the catalog before installing this version.
          </Typography>
        )}
      </Stack>
    </Box>
  );
}

export function PluginManagementCards({
  installations,
  catalog,
  rendererErrors,
  installedLoading,
  disabled,
  remoteDisabled,
  onInspect,
  onLocal,
}: CardActions & {
  readonly installations: readonly PluginInstallation[];
  readonly catalog: PluginCatalogSnapshot;
  readonly rendererErrors: Readonly<Record<string, string>>;
  readonly installedLoading: boolean;
}): React.JSX.Element {
  const manifests = new Map<string, PluginManifest>();
  for (const entry of installations) {
    const manifest =
      entry.installed ??
      entry.active ??
      entry.previous ??
      catalog.plugins.find((candidate) => candidate.id === entry.id);
    if (manifest !== undefined) manifests.set(entry.id, manifest);
  }
  for (const manifest of catalog.plugins)
    if (!manifests.has(manifest.id)) manifests.set(manifest.id, manifest);
  return (
    <>
      {(["Installed", "Available"] as const).map((section) => (
        <Stack component="section" aria-label={`${section} plugins`} spacing={2} key={section}>
          <Typography component="h4" variant="subtitle1">
            {section}
          </Typography>
          {section !== "Installed" || installedLoading || installations.length > 0 ? null : (
            <Typography color="text.secondary" variant="body2">
              No plugins are installed.
            </Typography>
          )}
          {[...manifests.values()]
            .filter((manifest) =>
              section === "Installed"
                ? installations.some((entry) => entry.id === manifest.id)
                : !installations.some((entry) => entry.id === manifest.id),
            )
            .map((manifest) => {
              const installation = installations.find((entry) => entry.id === manifest.id);
              const available = catalog.plugins.find((entry) => entry.id === manifest.id);
              return (
                <PluginCard
                  key={manifest.id}
                  manifest={manifest}
                  installation={installation}
                  available={available}
                  reference={
                    available === undefined
                      ? undefined
                      : catalog.packages?.find(
                          (entry) =>
                            entry.pluginId === available.id && entry.version === available.version,
                        )
                  }
                  error={installation?.error ?? rendererErrors[manifest.id]}
                  disabled={disabled}
                  remoteDisabled={remoteDisabled}
                  onInspect={onInspect}
                  onLocal={onLocal}
                />
              );
            })}
          {section !== "Installed"
            ? null
            : installations
                .filter((entry) => !manifests.has(entry.id))
                .map((entry) => (
                  <Box
                    component="section"
                    aria-label={entry.id}
                    key={entry.id}
                    sx={{ p: 2, border: 1, borderColor: "divider", borderRadius: 1 }}
                  >
                    <Stack spacing={1}>
                      <Typography component="h5" variant="subtitle1">
                        {entry.id}
                      </Typography>
                      <Alert severity="error">
                        {entry.error ??
                          rendererErrors[entry.id] ??
                          "This plugin could not be activated."}
                      </Alert>
                      <Button
                        disabled={disabled}
                        color="error"
                        variant="text"
                        onClick={(): void => {
                          void onLocal("plugins.remove", { id: entry.id, name: entry.id });
                        }}
                      >
                        Remove
                      </Button>
                    </Stack>
                  </Box>
                ))}
        </Stack>
      ))}
    </>
  );
}

export function PluginCachedPackages({
  packages,
  disabled,
  onInspect,
}: {
  readonly packages: PluginDelivery["cachedPackages"];
  readonly disabled: boolean;
  readonly onInspect: (input: PluginInspectionInput) => Promise<void>;
}): React.JSX.Element {
  return (
    <Stack component="section" aria-label="Cached plugin packages" spacing={2}>
      <Typography component="h4" variant="subtitle1">
        Cached packages
      </Typography>
      <Typography color="text.secondary" variant="body2">
        Verified package files can be installed without downloading them again. A cached catalog
        alone does not provide package files.
      </Typography>
      {packages.length > 0 ? (
        packages.map((entry) => (
          <Box
            component="section"
            aria-label={`Cached ${entry.manifest.name} ${entry.manifest.version}`}
            key={`${entry.manifest.id}:${entry.sha256}`}
            sx={{ p: 2, border: 1, borderColor: "divider", borderRadius: 1 }}
          >
            <Stack spacing={1}>
              <Typography component="h5" variant="subtitle1">
                {entry.manifest.name} {entry.manifest.version}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Cached {new Date(entry.cachedAt).toLocaleString()}
                {entry.publisher === undefined ? "" : ` · ${entry.publisher.name}`}
              </Typography>
              <PluginCompatibility manifest={entry.manifest} />
              <Button
                disabled={disabled}
                onClick={(): void => {
                  void onInspect({
                    source: "cache",
                    pluginId: entry.manifest.id,
                    version: entry.manifest.version,
                    sha256: entry.sha256,
                  });
                }}
              >
                Use cached package
              </Button>
            </Stack>
          </Box>
        ))
      ) : (
        <Typography color="text.secondary" variant="body2">
          No package files are cached.
        </Typography>
      )}
    </Stack>
  );
}
