import { useCallback, useEffect, useState } from "react";
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

function failureMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The plugin operation could not be completed.";
}

export function PluginsPanel({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const { errors: rendererErrors, refresh: refreshRenderers } = usePlugins();
  const [snapshot, setSnapshot] = useState<PluginSnapshot>({ revision: 0, plugins: [] });
  const [catalog, setCatalog] = useState<PluginCatalogSnapshot>({ plugins: [] });
  const [loading, setLoading] = useState(true);
  const [failure, setFailure] = useState<string>();
  const [pending, setPending] = useState<string>();
  const [status, setStatus] = useState("");
  const [completedInstallation, setCompletedInstallation] = useState<{
    readonly id: string;
    readonly version?: string;
  }>();
  const [confirmation, setConfirmation] = useState<{
    readonly command: "plugins.install" | "plugins.remove";
    readonly plugin: PluginActionTarget;
    readonly prompt: PluginChangePrompt | null;
  }>();

  const refresh = useCallback(async (): Promise<void> => {
    setLoading(true);
    const [installed, available] = await Promise.allSettled([
      host.execute({
        command: "plugins.list",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
      host.execute({
        command: "plugins.catalog",
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ]);
    if (installed.status === "rejected") {
      setSnapshot((current) => ({ ...current, error: failureMessage(installed.reason) }));
    } else if (!installed.value.ok) {
      const error = `${installed.value.error.summary} ${installed.value.error.recovery}`;
      setSnapshot((current) => ({ ...current, error }));
    } else {
      const next = installed.value.result.pluginSnapshot;
      setSnapshot((current) => (next.revision >= current.revision ? next : current));
    }
    if (available.status === "rejected") {
      setCatalog({ plugins: [], error: failureMessage(available.reason) });
    } else if (!available.value.ok) {
      setCatalog({
        plugins: [],
        error: `${available.value.error.summary} ${available.value.error.recovery}`,
      });
    } else {
      setCatalog(available.value.result.pluginCatalog);
    }
    setLoading(false);
  }, [host]);

  useEffect(() => {
    const unsubscribe = host.subscribe((event) => {
      if (event.event === "plugins.changed") {
        setSnapshot((current) =>
          event.payload.revision >= current.revision ? event.payload : current,
        );
      }
    });
    void refresh();
    return unsubscribe;
  }, [host, refresh]);

  async function change(
    command: "plugins.install" | "plugins.remove",
    plugin: PluginActionTarget,
    confirmationToken?: string,
  ): Promise<void> {
    setPending(plugin.id);
    setFailure(undefined);
    setCompletedInstallation(undefined);
    setStatus(
      command === "plugins.install"
        ? `Downloading and verifying ${plugin.name}…`
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
      if (command === "plugins.install")
        setCompletedInstallation({
          id: plugin.id,
          ...(plugin.version === undefined ? {} : { version: plugin.version }),
        });
      setStatus(
        command === "plugins.install"
          ? `${plugin.name} ${plugin.version ?? ""} is installed.`
          : `${plugin.name} has been removed. Saved connection settings are retained.`,
      );
    } catch (error) {
      setConfirmation(undefined);
      setFailure(failureMessage(error));
      setStatus("");
      await refresh();
    } finally {
      setPending(undefined);
    }
  }

  async function prepareChange(
    command: "plugins.install" | "plugins.remove",
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
          operation: command === "plugins.install" ? "install" : "remove",
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

  const manifests = new Map<string, PluginManifest>();
  for (const entry of snapshot.plugins) {
    const manifest = entry.installed ?? entry.active ?? entry.previous;
    if (manifest !== undefined) manifests.set(entry.id, manifest);
  }
  for (const manifest of catalog.plugins) {
    const current = manifests.get(manifest.id);
    if (current === undefined || comparePluginManifests(manifest, current) > 0) {
      manifests.set(manifest.id, manifest);
    }
  }
  const disabled = loading || pending !== undefined;
  const completedEntry = snapshot.plugins.find((entry) => entry.id === completedInstallation?.id);
  const showStatus =
    status.length > 0 &&
    (completedInstallation === undefined ||
      (completedEntry?.installed?.version === completedInstallation.version &&
        completedEntry?.error === undefined &&
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
          disabled={disabled}
          onClick={() => {
            void Promise.all([refresh(), refreshRenderers()]);
          }}
          variant="text"
        >
          Refresh plugins
        </Button>
      </Stack>
      {snapshot.error === undefined ? null : (
        <Alert severity="error">Installed plugins could not be read. {snapshot.error}</Alert>
      )}
      {catalog.error === undefined ? null : (
        <Alert severity="warning">
          The plugin catalog is unavailable. Installed plugins remain available. {catalog.error}
        </Alert>
      )}
      {failure === undefined ? null : <Alert severity="error">{failure}</Alert>}
      {loading ? (
        <Typography role="status" variant="body2">
          Loading plugins…
        </Typography>
      ) : null}
      {!showStatus ? null : (
        <Typography role="status" aria-live="polite" variant="body2">
          {status}
        </Typography>
      )}
      {!loading &&
      manifests.size === 0 &&
      snapshot.plugins.length === 0 &&
      catalog.error === undefined ? (
        <Typography variant="body2">
          No compatible plugins are available for this StreamSkope release.
        </Typography>
      ) : null}
      {[...manifests.values()].map((manifest) => {
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
        const error = installation?.error ?? rendererErrors[manifest.id];
        return (
          <Box
            component="section"
            aria-label={manifest.name}
            key={manifest.id}
            sx={{ p: 2, border: 1, borderColor: "divider", borderRadius: 1 }}
          >
            <Stack spacing={1}>
              <Typography component="h4" variant="subtitle1">
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
              {manifest.compatibility === undefined ? null : (
                <Typography variant="body2">
                  {installed !== undefined && installed.version !== manifest.version
                    ? "Available update: "
                    : ""}
                  Requires StreamSkope {manifest.compatibility.streamskope.minimum}
                  {manifest.compatibility.streamskope.maximumExclusive === undefined
                    ? " or later"
                    : ` up to, but excluding, ${manifest.compatibility.streamskope.maximumExclusive}`}
                  {" · "}
                  Supports {manifest.compatibility.target.system.toUpperCase()}{" "}
                  {manifest.compatibility.target.minimum}–{manifest.compatibility.target.maximum}
                  {" (inclusive) · Plugin API "}
                  {manifest.apiVersion}
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
                  The catalog offers older version {available?.version}. Retry requires version{" "}
                  {currentManifest?.version} or newer. Refresh plugins when that version is
                  available.
                </Alert>
              )}
              <Stack direction="row" spacing={1}>
                {available === undefined ||
                catalogIsOlder ||
                (installed !== undefined &&
                  installation?.active !== undefined &&
                  !updateAvailable &&
                  error === undefined) ? null : (
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
                        : installed === undefined
                          ? "Install"
                          : "Retry activation"}
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
      {snapshot.plugins
        .filter((entry) => !manifests.has(entry.id))
        .map((entry) => (
          <Box
            component="section"
            aria-label={entry.id}
            key={entry.id}
            sx={{ p: 2, border: 1, borderColor: "divider", borderRadius: 1 }}
          >
            <Stack spacing={1}>
              <Typography component="h4" variant="subtitle1">
                {entry.id}
              </Typography>
              <Alert severity="error">
                {entry.error ?? rendererErrors[entry.id] ?? "This plugin could not be activated."}
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
