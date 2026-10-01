import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  EDA_CAPTURE_APPLICATION,
  HOST_PROTOCOL_VERSION,
  type EdaApiCredentialsInput,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../src/platform/ui/controls";

import type { EdaUiHost as StreamSkopeHost } from "./host";

interface EdaCaptureApplicationSetupProperties {
  readonly edaApi: EdaApiCredentialsInput;
  readonly host: StreamSkopeHost;
  readonly onInstalled: () => void;
  readonly requiresAdministrator?: boolean;
}

function responseError(error: { readonly recovery: string; readonly summary: string }): string {
  return `${error.summary} ${error.recovery}`;
}

export function EdaCaptureApplicationSetup({
  edaApi,
  host,
  onInstalled,
  requiresAdministrator = false,
}: EdaCaptureApplicationSetupProperties): React.JSX.Element {
  const [administratorRequired, setAdministratorRequired] = useState(requiresAdministrator);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [checking, setChecking] = useState(!requiresAdministrator);
  const [installed, setInstalled] = useState(false);
  const [error, setError] = useState<string>();
  const onInstalledRef = useRef(onInstalled);

  useEffect(() => {
    onInstalledRef.current = onInstalled;
  }, [onInstalled]);

  useEffect(() => {
    if (requiresAdministrator) return;
    let active = true;
    setChecking(true);
    void host
      .execute({
        command: "edaCapture.application.status",
        id: crypto.randomUUID(),
        payload: { edaApi },
        version: HOST_PROTOCOL_VERSION,
      })
      .then((response) => {
        if (!active) return;
        if (!response.ok) {
          setError(responseError(response.error));
          return;
        }
        if (!("application" in response.result)) {
          setError("EDA returned an invalid application status.");
          return;
        }
        if (response.result.application.state === "installed") {
          setInstalled(true);
          onInstalledRef.current();
        }
      })
      .catch(() => {
        if (active) setError("The StreamSkope Capture application status could not be checked.");
      })
      .finally(() => {
        if (active) setChecking(false);
      });
    return (): void => {
      active = false;
    };
  }, [
    edaApi.baseUrl,
    edaApi.password,
    edaApi.username,
    edaApi.verifyTls,
    host,
    requiresAdministrator,
  ]);

  async function install(): Promise<void> {
    setBusy(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "edaCapture.application.install",
        id: crypto.randomUUID(),
        payload: {
          edaApi,
          ...(administratorRequired
            ? { authorization: { password, username: username.trim() } }
            : {}),
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        if (response.error.code === "AUTHORIZATION_DENIED" && !administratorRequired) {
          setAdministratorRequired(true);
          setError(
            "Your EDA account cannot install applications. An EDA administrator can approve this one-time installation below.",
          );
          return;
        }
        setError(responseError(response.error));
        return;
      }
      if (
        !("application" in response.result) ||
        response.result.application.state !== "installed"
      ) {
        setError("EDA did not confirm that the StreamSkope Capture application is ready.");
        return;
      }
      setInstalled(true);
      onInstalledRef.current();
    } catch (requestError) {
      setError(
        requestError instanceof Error ? requestError.message : "Installation did not complete.",
      );
    } finally {
      setUsername("");
      setPassword("");
      setBusy(false);
    }
  }

  return (
    <Stack spacing={1.5}>
      <Stack spacing={0.25}>
        <Typography component="h3" variant="subtitle2">
          Enable temporary capture
        </Typography>
        <Typography color="text.secondary" variant="body2">
          With your confirmation, StreamSkope will register its public catalog at
          https://github.com/asadarafat/streamskope.git and its pinned public signing key if either
          is absent, then ask EDA to install {EDA_CAPTURE_APPLICATION.publisher} Capture{" "}
          {EDA_CAPTURE_APPLICATION.version}. An existing catalog or signing key is validated and
          never overwritten. The application manages temporary broker and exporter cleanup inside
          EDA; StreamSkope does not store administrator credentials.
        </Typography>
      </Stack>
      {installed ? (
        <Alert severity="success">StreamSkope Capture is installed in this EDA system.</Alert>
      ) : administratorRequired ? (
        <Stack spacing={1}>
          <Typography color="text.secondary" variant="body2">
            Administrator approval is used only for this request and is cleared immediately after
            EDA responds.
          </Typography>
          <TextField
            autoComplete="username"
            disabled={busy}
            fullWidth
            label="EDA administrator username"
            onChange={(event) => setUsername(event.target.value)}
            value={username}
          />
          <TextField
            autoComplete="current-password"
            disabled={busy}
            fullWidth
            label="EDA administrator password"
            onChange={(event) => setPassword(event.target.value)}
            type="password"
            value={password}
          />
        </Stack>
      ) : null}
      {error === undefined ? null : <Alert severity="warning">{error}</Alert>}
      <Button
        disabled={
          checking ||
          busy ||
          installed ||
          (administratorRequired && (!username.trim() || !password))
        }
        onClick={() => void install()}
        variant="contained"
      >
        {checking
          ? "Checking…"
          : busy
            ? "Installing…"
            : installed
              ? "Installed"
              : "Install and continue"}
      </Button>
    </Stack>
  );
}
