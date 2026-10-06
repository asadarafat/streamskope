import { useCallback, useEffect, useRef, useState } from "react";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import LinearProgress from "@mui/material/LinearProgress";

import {
  StudioAlert,
  StudioButton,
  StudioCheckbox,
  StudioDialog,
  StudioDialogActions,
  StudioDialogContent,
  StudioDialogTitle,
  StudioLabeledControl,
  StudioMenuItem,
  StudioTextField,
} from "../../../src/platform/ui/controls";
import {
  parseNspConnectInput,
  type NspConnectInput,
  type NspProfileSource,
  type NspProgress,
  type NspStatus,
} from "../contracts";

import type { NspUiHost } from "./host";

interface Props {
  readonly host: NspUiHost;
  readonly source?: NspProfileSource;
  readonly profileId?: string;
  readonly onClose: () => void;
  readonly onProfileReady: (profileId: string) => void;
}

export function NspCaptureDialog({
  host,
  source,
  profileId,
  onClose,
  onProfileReady,
}: Props): React.JSX.Element {
  const [apiUrl, setApiUrl] = useState(source?.apiUrl ?? "");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [verifyCertificate, setVerifyCertificate] = useState(true);
  const [override, setOverride] = useState(source !== undefined);
  const [authentication, setAuthentication] = useState<"auto" | "tls" | "oauth">(
    source?.authentication ?? "auto",
  );
  const [brokerText, setBrokerText] = useState(source?.brokers.join(", ") ?? "");
  const [requestId, setRequestId] = useState<string>();
  const [cancelling, setCancelling] = useState(false);
  const [progress, setProgress] = useState<NspProgress>();
  const [status, setStatus] = useState<NspStatus>({ state: "idle" });
  const [error, setError] = useState<string>();
  const [notice, setNotice] = useState<string>();
  const active = useRef(true);
  const pending = useRef<string | undefined>(undefined);
  const busy = requestId !== undefined || status.state === "running";
  const title = profileId === undefined ? "Connect via NSP" : "Refresh NSP connection";

  const refreshStatus = useCallback(async (): Promise<void> => {
    try {
      const result = await host.execute("nspCapture.status", {});
      if (!active.current) return;
      if (result.ok && result.status !== undefined) setStatus(result.status);
      else if (!result.ok) setError(`${result.error.summary} ${result.error.recovery}`);
    } catch {
      if (active.current) setError("NSP plugin status is unavailable. Reopen this view and retry.");
    }
  }, [host]);

  useEffect(() => {
    active.current = true;
    void refreshStatus();
    const unsubscribe = host.subscribe((event) => {
      if (event.requestId === pending.current) setProgress(event);
    });
    return (): void => {
      active.current = false;
      unsubscribe();
      const current = pending.current;
      if (current !== undefined)
        void host.execute("nspCapture.cancel", { requestId: current }).catch(() => undefined);
    };
  }, [host, refreshStatus]);

  async function run(operation: "connect" | "cleanup"): Promise<void> {
    if (pending.current !== undefined) return;
    setError(undefined);
    setNotice(undefined);
    setProgress(undefined);
    let input: NspConnectInput;
    try {
      input = parseNspConnectInput({
        apiUrl,
        username,
        password,
        verifyCertificate,
        authentication,
        ...(profileId === undefined ? {} : { profileId }),
        ...(override ? { brokers: brokerText.split(/[,\s]+/u).filter(Boolean) } : {}),
      });
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "Check the NSP connection details.");
      return;
    }
    const id = globalThis.crypto.randomUUID();
    pending.current = id;
    setRequestId(id);
    try {
      const result =
        operation === "connect"
          ? await host.execute("nspCapture.connect", input, id)
          : await host.execute(
              "nspCapture.cleanup",
              {
                apiUrl: input.apiUrl,
                username: input.username,
                password: input.password,
                verifyCertificate: input.verifyCertificate,
              },
              id,
            );
      if (!active.current) return;
      if (!result.ok) setError(`${result.error.summary} ${result.error.recovery}`);
      else if (result.profileId !== undefined) {
        setPassword("");
        onProfileReady(result.profileId);
      } else {
        setNotice(
          result.cancelled === true
            ? "Operation cancelled. No new profile was saved."
            : "Remote cleanup confirmed.",
        );
      }
      if (result.ok && result.status !== undefined) setStatus(result.status);
    } catch {
      if (active.current)
        setError("NSP operation could not be confirmed. Check its status before retrying.");
    } finally {
      pending.current = undefined;
      if (active.current) {
        setRequestId(undefined);
        setCancelling(false);
        await refreshStatus();
      }
    }
  }

  async function cancel(): Promise<void> {
    const id = pending.current ?? status.requestId;
    if (id === undefined || cancelling) return;
    setCancelling(true);
    try {
      const result = await host.execute("nspCapture.cancel", { requestId: id });
      if (!active.current) return;
      if (!result.ok) {
        setError(`${result.error.summary} ${result.error.recovery}`);
        setCancelling(false);
      }
      if (pending.current === undefined) {
        await refreshStatus();
        setCancelling(false);
      }
    } catch {
      if (active.current) {
        setError(
          "Cancellation could not be confirmed. Wait for the operation or refresh its status.",
        );
        setCancelling(false);
      }
    }
  }

  return (
    <StudioDialog
      open
      maxWidth="sm"
      fullWidth
      onClose={busy ? undefined : onClose}
      aria-labelledby="nsp-capture-title"
    >
      <StudioDialogTitle id="nsp-capture-title">{title}</StudioDialogTitle>
      <StudioDialogContent>
        <Stack spacing={2} sx={{ pt: 1 }}>
          <Typography variant="body2">
            Sign in to retrieve the Kafka truststore, prepare broker settings, and test your
            connection. StreamSkope keeps retrieved credentials in the application host.
          </Typography>
          <StudioTextField
            label="NSP API URL"
            placeholder="https://nsp.example.com"
            value={apiUrl}
            onChange={(event) => setApiUrl(event.target.value)}
            disabled={busy || profileId !== undefined}
            autoComplete="url"
            fullWidth
          />
          <StudioTextField
            label="NSP username"
            value={username}
            onChange={(event) => setUsername(event.target.value)}
            disabled={busy}
            autoComplete="username"
            fullWidth
          />
          <StudioTextField
            label="NSP password"
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            disabled={busy}
            autoComplete="current-password"
            fullWidth
          />
          <StudioLabeledControl
            label="Verify NSP API certificate"
            control={
              <StudioCheckbox
                checked={verifyCertificate}
                onChange={(event) => setVerifyCertificate(event.target.checked)}
                disabled={busy}
              />
            }
          />
          {!verifyCertificate ? (
            <StudioAlert severity="warning">
              Certificate verification is disabled for this NSP API request. Use only with a trusted
              development lab. Kafka broker certificate verification stays enabled.
            </StudioAlert>
          ) : null}
          <StudioLabeledControl
            label="Override default Kafka brokers"
            control={
              <StudioCheckbox
                checked={override}
                onChange={(event) => setOverride(event.target.checked)}
                disabled={busy}
              />
            }
          />
          {override ? (
            <StudioTextField
              label="Kafka broker endpoints"
              value={brokerText}
              onChange={(event) => setBrokerText(event.target.value)}
              helperText="Comma-separated host:port endpoints reachable from this computer. TLS identity must match the broker certificate."
              disabled={busy}
              fullWidth
            />
          ) : null}
          <StudioTextField
            select
            label="Kafka authentication"
            value={authentication}
            onChange={(event) => setAuthentication(event.target.value as "auto" | "tls" | "oauth")}
            helperText="Automatic detection tests TLS, then OAuth when the broker requires authentication."
            disabled={busy}
            fullWidth
          >
            <StudioMenuItem value="auto">Detect automatically</StudioMenuItem>
            <StudioMenuItem value="tls">TLS only</StudioMenuItem>
            <StudioMenuItem value="oauth">TLS and NSP OAuth</StudioMenuItem>
          </StudioTextField>
          {busy ? (
            <Stack spacing={1} role="status" aria-live="polite">
              <LinearProgress />
              <Typography variant="body2">
                {cancelling
                  ? "Cancellation requested. Waiting for remote cleanup to finish."
                  : (progress?.message ?? status.message ?? "Preparing NSP connection…")}
              </Typography>
            </Stack>
          ) : null}
          {status.state === "cleanup-required" ? (
            <StudioAlert severity="warning">
              {status.message ??
                "An earlier NSP operation needs cleanup. Supply credentials for the same NSP server and retry cleanup before creating a profile."}
            </StudioAlert>
          ) : null}
          {notice === undefined ? null : <StudioAlert severity="info">{notice}</StudioAlert>}
          {error === undefined ? null : <StudioAlert severity="error">{error}</StudioAlert>}
          <Typography variant="caption" color="text.secondary">
            StreamSkope runs an owned NSP workflow and removes temporary execution output. Existing
            NSP workflows and Kafka data are retained.
          </Typography>
        </Stack>
      </StudioDialogContent>
      <StudioDialogActions>
        <StudioButton
          onClick={() => {
            void refreshStatus();
          }}
          disabled={requestId !== undefined}
        >
          Refresh status
        </StudioButton>
        {busy ? (
          <StudioButton
            onClick={() => {
              void cancel();
            }}
            disabled={
              cancelling || (pending.current === undefined && status.requestId === undefined)
            }
          >
            Cancel operation
          </StudioButton>
        ) : (
          <StudioButton onClick={onClose}>Close</StudioButton>
        )}
        {status.state === "cleanup-required" ? (
          <StudioButton
            variant="contained"
            disabled={busy || !apiUrl || !username || !password}
            onClick={() => {
              void run("cleanup");
            }}
          >
            Retry cleanup
          </StudioButton>
        ) : (
          <StudioButton
            variant="contained"
            disabled={busy || !apiUrl || !username || !password}
            onClick={() => {
              void run("connect");
            }}
          >
            {profileId === undefined ? "Create connection profile" : "Refresh credentials"}
          </StudioButton>
        )}
      </StudioDialogActions>
    </StudioDialog>
  );
}
