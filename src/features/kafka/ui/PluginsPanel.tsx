import { useCallback, useEffect, useRef, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type {
  PluginCatalogSnapshot,
  PluginChangePrompt,
  PluginManifest,
  PluginSnapshot,
} from "../../../plugins/contracts";
import { comparePluginManifests } from "../../../plugins/validation";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "../../../platform/ui/controls";

import { usePlugins } from "./PluginsProvider";

type PluginActionTarget = Pick<PluginManifest, "id" | "name"> & { readonly version?: string };
type PluginChangeCommand = "plugins.install" | "plugins.retry" | "plugins.remove";

function failureMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The plugin operation could not be completed.";
}

export function PluginsPanel({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const {
    errors: rendererErrors,
    loading: renderersLoading,
    refresh: refreshRenderers,
  } = usePlugins();
  const [snapshot, setSnapshot] = useState<PluginSnapshot>({ revision: 0, plugins: [] });
  const [catalog, setCatalog] = useState<PluginCatalogSnapshot>({ plugins: [] });
  const [installedLoading, setInstalledLoading] = useState(true);
  const [catalogLoading, setCatalogLoading] = useState(true);
  const installedRequest = useRef(0);
  const catalogRequest = useRef(0);
  const [failure, setFailure] = useState<string>();
  const [pending, setPending] = useState<string>();
  const [status, setStatus] = useState("");
  const [completedInstallation, setCompletedInstallation] = useState<{
    readonly id: string;
    readonly version?: string;
  }>();
  const [confirmation, setConfirmation] = useState<{
    readonly command: PluginChangeCommand;
    readonly plugin: PluginActionTarget;
    readonly prompt: PluginChangePrompt | null;
  }>();

  const refreshInstalled = useCallback(async (): Promise<void> => {
    const request = ++installedRequest.current;
    setInstalledLoading(true);
    try {
      const response = await host.execute({
        command: "plugins.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (request !== installedRequest.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      const next = response.result.pluginSnapshot;
      setSnapshot((current) => (next.revision >= current.revision ? next : current));
    } catch (error) {
      if (request === installedRequest.current)
        setSnapshot((current) => ({ ...current, error: failureMessage(error) }));
    } finally {
      if (request === installedRequest.current) setInstalledLoading(false);
    }
  }, [host]);

  const refreshCatalog = useCallback(async (): Promise<void> => {
    const request = ++catalogRequest.current;
    setCatalogLoading(true);
    // Show verified local discovery before waiting for the optional network refresh.
    for (const refresh of [false, true]) {
      try {
        const response = await host.execute({
          command: "plugins.catalog",
          id: globalThis.crypto.randomUUID(),
          payload: { refresh },
          version: HOST_PROTOCOL_VERSION,
        });
        if (request !== catalogRequest.current) return;
        if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
        const next = response.result.pluginCatalog;
        setCatalog((current) =>
          next.error !== undefined &&
          next.source !== "live" &&
          next.plugins.length === 0 &&
          current.plugins.length > 0
            ? { ...current, source: "cache", error: next.error }
            : next,
        );
      } catch (error) {
        if (request !== catalogRequest.current) return;
        if (refresh)
          setCatalog((current) => ({
            ...current,
            source: current.checkedAt === undefined ? "unavailable" : "cache",
            error: failureMessage(error),
          }));
      }
    }
    if (request === catalogRequest.current) setCatalogLoading(false);
  }, [host]);

  useEffect(() => {
    const unsubscribe = host.subscribe((event) => {
      if (event.event === "plugins.changed") {
        setSnapshot((current) =>
          event.payload.revision >= current.revision ? event.payload : current,
        );
      }
    });
    void refreshInstalled();
    void refreshCatalog();
    return (): void => {
      unsubscribe();
      installedRequest.current++;
      catalogRequest.current++;
    };
  }, [host, refreshInstalled, refreshCatalog]);

  async function change(
    command: PluginChangeCommand,
    plugin: PluginActionTarget,
    confirmationToken?: string,
  ): Promise<void> {
    setPending(plugin.id);
    setFailure(undefined);
    setCompletedInstallation(undefined);
    setStatus(
      command === "plugins.install"
        ? `Downloading and verifying ${plugin.name}…`
        : command === "plugins.retry"
          ? `Verifying the installed ${plugin.name} package and reloading…`
          : `Removing ${plugin.name}…`,
    );
    try {
      const response = await host.execute({
        command,
        id: globalThis.crypto.randomUUID(),
        payload: {
          pluginId: plugin.id,
          ...(confirmationToken === undefined ? {} : { confirmationToken }),
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setSnapshot((current) =>
        response.result.pluginSnapshot.revision >= current.revision
          ? response.result.pluginSnapshot
          : current,
      );
      setConfirmation(undefined);
      if (command === "plugins.retry") await refreshRenderers();
      if (command !== "plugins.remove")
        setCompletedInstallation({
          id: plugin.id,
          ...(plugin.version === undefined ? {} : { version: plugin.version }),
        });
      setStatus(
        command === "plugins.install"
          ? `${plugin.name} ${plugin.version ?? ""} is installed.`
          : command === "plugins.retry"
            ? `${plugin.name} ${plugin.version ?? ""} is active.`
            : `${plugin.name} has been removed. Saved connection settings are retained.`,
      );
    } catch (error) {
      setConfirmation(undefined);
      setFailure(failureMessage(error));
      setStatus("");
      await refreshInstalled();
    } finally {
      setPending(undefined);
    }
  }

  async function prepareChange(
    command: PluginChangeCommand,
    plugin: PluginActionTarget,
  ): Promise<void> {
    setPending(plugin.id);
    setFailure(undefined);
    setCompletedInstallation(undefined);
    setStatus(`Checking ${plugin.name}…`);
    try {
      const response = await host.execute({
        command: "plugins.change.prepare",
        id: globalThis.crypto.randomUUID(),
        payload: {
          pluginId: plugin.id,
          operation:
            command === "plugins.install"
              ? "install"
              : command === "plugins.retry"
                ? "retry"
                : "remove",
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      const prompt = response.result.pluginChange;
      if (prompt !== null || command === "plugins.remove") {
        setConfirmation({ command, plugin, prompt });
        setStatus("");
      } else {
        await change(command, plugin);
      }
    } catch (error) {
      setFailure(failureMessage(error));
      setStatus("");
    } finally {
      setPending(undefined);
    }
  }

  const disabled = installedLoading || pending !== undefined;
  const manifests = new Map<string, PluginManifest>();
  for (const entry of snapshot.plugins) {
    const manifest =
      entry.installed ??
      entry.active ??
      entry.previous ??
      catalog.plugins.find((candidate) => candidate.id === entry.id);
    if (manifest !== undefined) manifests.set(entry.id, manifest);
  }
  for (const manifest of catalog.plugins)
    if (!manifests.has(manifest.id)) manifests.set(manifest.id, manifest);
  const completedEntry = snapshot.plugins.find((entry) => entry.id === completedInstallation?.id);
  const showStatus =
    status.length > 0 &&
    (completedInstallation === undefined ||
      (completedEntry?.installed?.version === completedInstallation.version &&
        completedEntry?.active?.version === completedInstallation.version &&
        completedEntry?.error === undefined &&
        !renderersLoading &&
        rendererErrors[completedInstallation.id] === undefined));

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
          disabled={catalogLoading || pending !== undefined}
          onClick={() => {
            void Promise.all([refreshInstalled(), refreshCatalog(), refreshRenderers()]);
          }}
          variant="text"
        >
          Check for updates
        </Button>
      </Stack>
      {snapshot.error === undefined ? null : (
        <Alert severity="error">Installed plugins could not be read. {snapshot.error}</Alert>
      )}
      {catalog.error === undefined ? null : (
        <Alert severity="warning">
          {catalog.source === "live"
            ? "The plugin catalog was refreshed, but its local copy could not be saved. "
            : "The plugin catalog is unavailable. Installed plugins remain available. "}
          {catalog.error}
        </Alert>
      )}
      {failure === undefined ? null : <Alert severity="error">{failure}</Alert>}
      {installedLoading ? (
        <Typography role="status" variant="body2">
          Loading installed plugins…
        </Typography>
      ) : null}
      {catalogLoading ? (
        <Typography role="status" variant="body2">
          Checking for plugin updates…
        </Typography>
      ) : null}
      {catalog.checkedAt === undefined ? null : (
        <Typography color="text.secondary" variant="body2">
          {catalog.source === "cache" ? "Cached catalog · Last checked " : "Catalog checked "}
          {new Date(catalog.checkedAt).toLocaleString()}
        </Typography>
      )}
      {!showStatus ? null : (
        <Typography role="status" aria-live="polite" variant="body2">
          {status}
        </Typography>
      )}
      {!installedLoading &&
      !catalogLoading &&
      manifests.size === 0 &&
      snapshot.plugins.length === 0 &&
      catalog.error === undefined ? (
        <Typography variant="body2">
          No compatible plugins are available for this StreamSkope release.
        </Typography>
      ) : null}
      {(["Installed", "Available"] as const).map((section) => (
        <Stack component="section" aria-label={`${section} plugins`} spacing={2} key={section}>
          <Typography component="h4" variant="subtitle1">
            {section}
          </Typography>
          {section === "Installed" && !installedLoading && snapshot.plugins.length === 0 ? (
            <Typography color="text.secondary" variant="body2">
              No plugins are installed.
            </Typography>
          ) : null}
          {[...manifests.values()]
            .filter((manifest) =>
              section === "Installed"
                ? snapshot.plugins.some((entry) => entry.id === manifest.id)
                : !snapshot.plugins.some((entry) => entry.id === manifest.id),
            )
            .map((manifest) => {
              const installation = snapshot.plugins.find((entry) => entry.id === manifest.id);
              const available = catalog.plugins.find((entry) => entry.id === manifest.id);
              const installed = installation?.installed;
              const currentManifest = [installed, installation?.active]
                .filter((entry): entry is PluginManifest => entry !== undefined)
                .sort(comparePluginManifests)
                .at(-1);
              const catalogIsOlder =
                available !== undefined &&
                currentManifest !== undefined &&
                comparePluginManifests(available, currentManifest) < 0;
              const updateAvailable =
                installed !== undefined &&
                available !== undefined &&
                comparePluginManifests(available, installed) > 0;
              const presented = updateAvailable && available !== undefined ? available : manifest;
              const error = installation?.error ?? rendererErrors[manifest.id];
              return (
                <Box
                  component="section"
                  aria-label={manifest.name}
                  key={manifest.id}
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
                    {presented.compatibility === undefined ? null : (
                      <Typography variant="body2">
                        {updateAvailable ? "Available update: " : ""}
                        Requires StreamSkope {presented.compatibility.streamskope.minimum}
                        {presented.compatibility.streamskope.maximumExclusive === undefined
                          ? " or later"
                          : ` up to, but excluding, ${presented.compatibility.streamskope.maximumExclusive}`}
                        {" · "}
                        Supports {presented.compatibility.target.system.toUpperCase()}{" "}
                        {presented.compatibility.target.minimum}–
                        {presented.compatibility.target.maximum}
                        {" (inclusive) · Plugin API "}
                        {presented.apiVersion}
                      </Typography>
                    )}
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
                        {currentManifest?.version} is newer; this older package cannot be used as an
                        update.
                      </Alert>
                    )}
                    <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
                      {available === undefined ||
                      catalogIsOlder ||
                      (installation !== undefined && !updateAvailable) ? null : (
                        <Button
                          disabled={disabled || snapshot.error !== undefined}
                          variant="contained"
                          onClick={() => {
                            void prepareChange("plugins.install", available);
                          }}
                        >
                          {pending === manifest.id
                            ? "Downloading and verifying…"
                            : updateAvailable
                              ? `Update to ${available.version}`
                              : "Install"}
                        </Button>
                      )}
                      {installed === undefined ||
                      (installation?.active !== undefined && error === undefined) ? null : (
                        <Button
                          disabled={disabled || snapshot.error !== undefined}
                          variant="contained"
                          onClick={() => {
                            void prepareChange("plugins.retry", installed);
                          }}
                        >
                          Retry activation
                        </Button>
                      )}
                      {installation === undefined ? null : (
                        <Button
                          disabled={disabled || snapshot.error !== undefined}
                          color="error"
                          variant="text"
                          onClick={() => {
                            void prepareChange("plugins.remove", installed ?? manifest);
                          }}
                        >
                          Remove
                        </Button>
                      )}
                    </Stack>
                  </Stack>
                </Box>
              );
            })}
          {section !== "Installed"
            ? null
            : snapshot.plugins
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
                        disabled={disabled || snapshot.error !== undefined}
                        color="error"
                        variant="text"
                        onClick={() => {
                          void prepareChange("plugins.remove", { id: entry.id, name: entry.id });
                        }}
                      >
                        Remove
                      </Button>
                    </Stack>
                  </Box>
                ))}
        </Stack>
      ))}
      <Dialog
        open={confirmation !== undefined}
        onClose={pending === undefined ? (): void => setConfirmation(undefined) : undefined}
        aria-labelledby="change-plugin-title"
      >
        <DialogTitle id="change-plugin-title">
          {confirmation?.prompt?.title ?? `Remove ${confirmation?.plugin.name ?? "plugin"}?`}
        </DialogTitle>
        <DialogContent>
          <Stack spacing={1}>
            {confirmation?.prompt === null || confirmation?.prompt === undefined ? null : (
              <>
                <Typography variant="body2">{confirmation.prompt.message}</Typography>
                <Typography variant="body2">{confirmation.prompt.detail}</Typography>
              </>
            )}
            <Typography variant="body2">
              {confirmation?.command === "plugins.remove"
                ? "Saved connection settings are retained. Its connections will require reinstalling the plugin."
                : confirmation?.command === "plugins.retry"
                  ? "The verified installed package is reloaded locally. Your saved connection settings are retained."
                  : "Your saved connection settings are retained. The new version becomes available immediately."}
            </Typography>
            {failure === undefined ? null : <Alert severity="error">{failure}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={pending !== undefined} onClick={() => setConfirmation(undefined)}>
            Cancel
          </Button>
          <Button
            disabled={pending !== undefined}
            color={confirmation?.command === "plugins.remove" ? "error" : "primary"}
            variant="contained"
            onClick={() => {
              if (confirmation !== undefined)
                void change(confirmation.command, confirmation.plugin, confirmation.prompt?.token);
            }}
          >
            {pending !== undefined
              ? "Applying change…"
              : (confirmation?.prompt?.confirmLabel ?? "Remove plugin")}
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
