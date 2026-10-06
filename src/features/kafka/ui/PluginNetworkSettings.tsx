import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { Stack, Typography } from "@mui/material";
import { useEffect, useState } from "react";

import type {
  PluginNetworkConfiguration,
  PluginProxyCredentialsChange,
} from "../../../plugins/contracts";
import { parsePluginNetworkConfiguration } from "../../../plugins/network-validation";
import {
  StudioAccordion as Accordion,
  StudioAccordionDetails as AccordionDetails,
  StudioAccordionSummary as AccordionSummary,
  StudioAlert as Alert,
  StudioButton as Button,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import type { PluginNetwork } from "./usePluginNetwork";

function proxyFailure(value: string): string | undefined {
  try {
    parsePluginNetworkConfiguration({ mode: "custom", offline: false, proxyUrl: value });
    return undefined;
  } catch (error) {
    return error instanceof Error ? error.message : "Enter an HTTP or HTTPS proxy origin.";
  }
}

export function PluginNetworkSettings({
  network,
}: {
  readonly network: PluginNetwork;
}): React.JSX.Element {
  const { snapshot, saving, testing, testResult } = network;
  const [mode, setMode] = useState<PluginNetworkConfiguration["mode"]>("system");
  const [offline, setOffline] = useState(false);
  const [proxyUrl, setProxyUrl] = useState("");
  const [credentialAction, setCredentialAction] =
    useState<PluginProxyCredentialsChange["action"]>("unchanged");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  useEffect(() => {
    if (snapshot?.configuration == null) return;
    setMode(snapshot.configuration.mode);
    setOffline(snapshot.configuration.offline);
    setProxyUrl(snapshot.configuration.proxyUrl ?? "");
    setCredentialAction("unchanged");
    setUsername("");
    setPassword("");
  }, [snapshot]);
  const invalidProxy =
    proxyUrl.trim().length > 0 || mode === "custom" ? proxyFailure(proxyUrl.trim()) : undefined;
  const configuration: PluginNetworkConfiguration = {
    mode,
    offline,
    proxyUrl:
      proxyUrl.trim().length === 0
        ? null
        : invalidProxy === undefined
          ? new URL(proxyUrl.trim()).origin
          : proxyUrl.trim(),
  };
  const dirty =
    snapshot?.configuration !== undefined &&
    (snapshot.configuration === null ||
      configuration.mode !== snapshot.configuration.mode ||
      configuration.offline !== snapshot.configuration.offline ||
      configuration.proxyUrl !== snapshot.configuration.proxyUrl ||
      credentialAction !== "unchanged");
  const endpointChanged =
    snapshot?.configuration != null && configuration.proxyUrl !== snapshot.configuration.proxyUrl;
  const credentialsNeedDecision =
    snapshot?.credentialsConfigured === true && endpointChanged && credentialAction === "unchanged";
  const proxySupported =
    mode !== "custom" ||
    (snapshot?.nativeAvailable === true &&
      configuration.proxyUrl !== null &&
      invalidProxy === undefined &&
      snapshot.supportedProxyProtocols.some(
        (protocol) => `${protocol}:` === new URL(configuration.proxyUrl!).protocol,
      ));
  const canSave =
    snapshot?.configuration != null &&
    dirty &&
    !saving &&
    invalidProxy === undefined &&
    !credentialsNeedDecision &&
    proxySupported &&
    (credentialAction !== "replace" || (username.trim().length > 0 && password.length > 0));
  function clearOldCredentials(): void {
    setUsername("");
    setPassword("");
    if (snapshot?.credentialsConfigured) setCredentialAction("clear");
    else setCredentialAction("unchanged");
  }
  return (
    <Accordion>
      <AccordionSummary expandIcon={<ExpandMoreIcon />} aria-controls="plugin-network-settings">
        <Typography variant="subtitle2">
          Plugin download settings
          {snapshot?.configuration?.offline
            ? " · Offline"
            : snapshot?.configuration?.mode === "custom"
              ? " · Custom proxy"
              : ""}
        </Typography>
      </AccordionSummary>
      <AccordionDetails id="plugin-network-settings">
        <Stack spacing={2}>
          <Typography variant="body2" color="text.secondary">
            These settings apply only to plugin catalog checks and package downloads. Kafka
            connections and running plugins keep their own connection settings. Offline mode keeps
            installed plugins, signed local files and cached packages available.
          </Typography>
          {network.loading ? (
            <Typography variant="body2">Loading download settings…</Typography>
          ) : null}
          {network.failure === undefined ? null : (
            <Alert severity="warning">
              Download settings could not be applied or tested. {network.failure}
            </Alert>
          )}
          {snapshot === undefined || snapshot.configuration !== null ? null : (
            <>
              <Alert severity="warning">
                {snapshot.error ?? "Stored download settings could not be read."} Remote acquisition
                is blocked. Local plugin management remains available.
              </Alert>
              <Button
                disabled={saving}
                onClick={(): void => {
                  void network.save({
                    configuration: { mode: "system", offline: false, proxyUrl: null },
                    credentials: { action: "clear" },
                  });
                }}
              >
                Reset download settings
              </Button>
            </>
          )}
          {snapshot?.configuration == null ? null : (
            <>
              {!snapshot.nativeAvailable ? (
                <Typography variant="body2" color="text.secondary">
                  System proxy discovery and custom proxies require the desktop app. Offline mode is
                  available in this host.
                </Typography>
              ) : null}
              <TextField
                select
                label="Proxy mode"
                value={mode}
                disabled={saving}
                onChange={(event): void => {
                  const selected = event.target.value === "custom" ? "custom" : "system";
                  setMode(selected);
                  if (selected === "system" && invalidProxy !== undefined) {
                    setProxyUrl(snapshot.configuration?.proxyUrl ?? "");
                    setCredentialAction("unchanged");
                    setUsername("");
                    setPassword("");
                  }
                }}
              >
                <MenuItem value="system">Use system proxy</MenuItem>
                <MenuItem value="custom" disabled={!snapshot.nativeAvailable}>
                  Use custom proxy
                </MenuItem>
              </TextField>
              {mode !== "custom" ? null : (
                <>
                  <TextField
                    label="Proxy URL"
                    value={proxyUrl}
                    disabled={saving}
                    error={invalidProxy !== undefined || !proxySupported}
                    helperText={
                      invalidProxy ??
                      (!proxySupported
                        ? "This host does not support this proxy protocol."
                        : undefined) ??
                      "HTTP or HTTPS origin. Keep authentication in the separate fields below."
                    }
                    onChange={(event): void => {
                      setProxyUrl(event.target.value);
                      clearOldCredentials();
                    }}
                  />
                  <Typography variant="body2" color="text.secondary">
                    {snapshot.credentialsConfigured
                      ? "Proxy credentials are stored; their values are never returned to this view."
                      : "No proxy credentials are configured."}{" "}
                    {snapshot.credentialStorage === "encrypted"
                      ? "Credentials use protected local storage."
                      : snapshot.credentialStorage === "session"
                        ? "Credentials last for this app session only."
                        : "Protected credential storage is unavailable."}
                  </Typography>
                  <TextField
                    select
                    label="Proxy credentials"
                    value={credentialAction}
                    disabled={saving}
                    onChange={(event): void => {
                      setCredentialAction(
                        event.target.value === "replace"
                          ? "replace"
                          : event.target.value === "clear"
                            ? "clear"
                            : "unchanged",
                      );
                    }}
                  >
                    <MenuItem value="unchanged">Keep existing credentials</MenuItem>
                    <MenuItem value="replace">Replace credentials</MenuItem>
                    <MenuItem value="clear">Clear credentials</MenuItem>
                  </TextField>
                  {!credentialsNeedDecision ? null : (
                    <Typography variant="body2" color="error">
                      Choose Replace or Clear credentials for the new proxy endpoint.
                    </Typography>
                  )}
                  {credentialAction !== "replace" ? null : (
                    <Stack spacing={2}>
                      <TextField
                        label="Proxy username"
                        value={username}
                        disabled={saving}
                        autoComplete="off"
                        onChange={(event): void => setUsername(event.target.value)}
                      />
                      <TextField
                        label="Proxy password"
                        value={password}
                        type="password"
                        disabled={saving}
                        autoComplete="new-password"
                        onChange={(event): void => setPassword(event.target.value)}
                      />
                    </Stack>
                  )}
                </>
              )}
              <FormControlLabel
                control={
                  <Checkbox
                    checked={offline}
                    disabled={saving}
                    onChange={(_, checked): void => setOffline(checked)}
                  />
                }
                label="Offline plugin downloads"
              />
              <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
                <Button
                  variant="outlined"
                  disabled={!canSave}
                  onClick={(): void => {
                    void network.save({
                      configuration,
                      credentials:
                        credentialAction === "replace"
                          ? { action: "replace", username: username.trim(), password }
                          : { action: credentialAction },
                    });
                  }}
                >
                  {saving ? "Saving settings…" : "Save settings"}
                </Button>
                <Button
                  disabled={dirty || saving || testing || offline}
                  onClick={(): void => {
                    void network.test();
                  }}
                >
                  {testing ? "Testing connection…" : "Test connection"}
                </Button>
              </Stack>
              {!dirty ? null : (
                <Typography variant="body2" color="text.secondary">
                  Save changes before testing them.
                </Typography>
              )}
              {testResult === undefined ? null : (
                <Alert severity="success">
                  {testResult.scope === "catalog-and-assets"
                    ? "Release catalog and package download are reachable."
                    : "Release catalog is reachable; no package download was tested."}
                  {testResult.detail === undefined ? "" : ` ${testResult.detail}`} Checked{" "}
                  {new Date(testResult.checkedAt).toLocaleString()} using applied settings revision{" "}
                  {testResult.settingsRevision}.
                </Alert>
              )}
            </>
          )}
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
}
