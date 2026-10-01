import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  EDA_CAPTURE_DEFAULTS,
  EDA_CAPTURE_PROGRESS_PHASES,
  HOST_PROTOCOL_VERSION,
  type EdaApiCredentialsInput,
  type EdaCaptureInspection,
  type EdaCaptureProgress,
  type EdaCaptureProgressPhase,
  type EdaCaptureSource,
  type EdaCaptureHostStatus,
  type ProfileEdaCaptureSource,
  sameEdaCaptureSource,
  fromPluginProfileSource,
  toPluginProfileSource,
} from "../contracts";
import type {
  ProfileCreateInput,
  ProfileSummary,
  ProfileUpdateInput,
} from "../../../src/features/kafka/contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioCheckbox as Checkbox,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../src/platform/ui/controls";

import type { EdaUiHost as StreamSkopeHost } from "./host";
import { EdaCaptureStatusPanel } from "./EdaCaptureStatusPanel";
import { EdaCaptureApplicationSetup } from "./EdaCaptureApplicationSetup";

interface EdaCaptureDialogProperties {
  readonly onExistingDestination?: (destination: {
    name: string;
    brokers: readonly string[];
  }) => void;
  readonly host: StreamSkopeHost;
  readonly onClose: () => void;
  readonly onProfileReady: (profileId: string) => void;
  readonly open: boolean;
  readonly profiles: readonly ProfileSummary[];
  readonly resume?: ProfileEdaCaptureSource | undefined;
  readonly resumeProfileId?: string;
}

const PROGRESS_LABELS: Readonly<Record<EdaCaptureProgressPhase, string>> = {
  authenticating: "Authenticate and load EDA exporters",
  "preparing-image": "Prepare the host-owned capture broker",
  "opening-tunnel": "Reserve the local StreamSkope tunnel",
  "deploying-broker": "Deploy the capture broker",
  "waiting-broker": "Wait for broker readiness",
  "configuring-exporter": "Configure the selected EDA exporter",
  "waiting-topics": "Verify the capture broker endpoint",
  ready: "Verify the local StreamSkope endpoint",
};

function sourceKey(source: EdaCaptureSource): string {
  return `${source.apiVersion}/${source.namespace}/${source.kind}/${source.name}`;
}

function responseError(error: { readonly recovery: string; readonly summary: string }): string {
  return `${error.summary} ${error.recovery}`;
}

export function EdaCaptureDialog({
  onExistingDestination,
  host,
  onClose,
  onProfileReady,
  open,
  profiles,
  resume,
  resumeProfileId,
}: EdaCaptureDialogProperties): React.JSX.Element {
  const [busy, setBusy] = useState(false);
  const [temporaryCapture, setTemporaryCapture] = useState(resume !== undefined);
  const [edaApiPassword, setEdaApiPassword] = useState("");
  const [edaApiUrl, setEdaApiUrl] = useState(resume?.edaApiUrl ?? "");
  const [edaApiUsername, setEdaApiUsername] = useState("");
  const [credentialRevision, setCredentialRevision] = useState(0);
  const [verifyTls, setVerifyTls] = useState(true);
  const [error, setError] = useState<string>();
  const [applicationNotice, setApplicationNotice] = useState<string>();
  const [applicationInstalled, setApplicationInstalled] = useState(false);
  const [inspection, setInspection] = useState<EdaCaptureInspection>();
  const [inspectionKey, setInspectionKey] = useState<string>();
  const [localPort, setLocalPort] = useState(
    resume?.broker.startsWith("127.0.0.1:")
      ? resume.broker.slice("127.0.0.1:".length)
      : String(EDA_CAPTURE_DEFAULTS.localPort),
  );
  const [progress, setProgress] = useState<EdaCaptureProgress | null>(null);
  const [selectedSource, setSelectedSource] = useState("");
  const [captureHost, setCaptureHost] = useState<EdaCaptureHostStatus>();
  const [pendingProfile, setPendingProfile] = useState<ProfileCreateInput>();
  const [verified, setVerified] = useState(false);
  const [recoverySource, setRecoverySource] = useState<ProfileEdaCaptureSource>();
  const [savedWithoutIdentity, setSavedWithoutIdentity] = useState(false);
  const inputsLocked = busy || pendingProfile !== undefined;
  const pendingDeploy = useRef<string | null>(null);
  const progressIndex =
    progress === null ? -1 : EDA_CAPTURE_PROGRESS_PHASES.indexOf(progress.phase);
  const currentInspectionKey = String(credentialRevision);
  const validInspection = inspectionKey === currentInspectionKey ? inspection : undefined;
  const singleSource =
    validInspection?.sources.length === 1 ? validInspection.sources[0] : undefined;
  const chosenSource =
    singleSource ?? validInspection?.sources.find((source) => sourceKey(source) === selectedSource);

  function invalidateInspection(): void {
    setTemporaryCapture(resume !== undefined);
    setInspection(undefined);
    setInspectionKey(undefined);
    setSelectedSource("");
    setProgress(null);
    setApplicationNotice(undefined);
    setApplicationInstalled(false);
    setCredentialRevision((current) => current + 1);
  }

  function credentials(): EdaApiCredentialsInput | null {
    if (
      edaApiUrl.trim().length === 0 ||
      edaApiUsername.trim().length === 0 ||
      edaApiPassword.length === 0
    ) {
      setError("Enter the EDA API URL, username, and password.");
      return null;
    }
    return {
      baseUrl: edaApiUrl.trim(),
      password: edaApiPassword,
      username: edaApiUsername.trim(),
      verifyTls,
    };
  }

  async function checkHost(): Promise<void> {
    setCaptureHost(undefined);
    try {
      const response = await host.execute({
        command: "edaCapture.preflight",
        id: crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setCaptureHost({ state: "unavailable", detail: responseError(response.error) });
        return;
      }
      if (!("captureHost" in response.result))
        throw new Error("Capture host did not return its configuration.");
      setCaptureHost(response.result.captureHost);
    } catch {
      setCaptureHost({
        state: "unavailable",
        detail: "Capture host could not be checked. Retry the host check.",
      });
    }
  }

  async function inspect(edaApi: EdaApiCredentialsInput): Promise<EdaCaptureInspection | null> {
    setBusy(true);
    setError(undefined);
    setApplicationNotice(undefined);
    try {
      const response = await host.execute({
        command: "edaCapture.inspect",
        id: globalThis.crypto.randomUUID(),
        payload: { edaApi },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) {
        setError(responseError(response.error));
        return null;
      }
      if (!("inspection" in response.result)) {
        setError("The host returned an invalid EDA capture inspection.");
        return null;
      }
      const nextInspection = response.result.inspection;
      setInspection(nextInspection);
      setInspectionKey(currentInspectionKey);
      setSelectedSource(
        resume === undefined
          ? nextInspection.sources[0] === undefined
            ? ""
            : sourceKey(nextInspection.sources[0])
          : sourceKey({ ...resume.source, topics: resume.topics }),
      );
      return nextInspection;
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "The EDA capture inspection did not complete.",
      );
      return null;
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (!open) return;
    setInspection(undefined);
    setInspectionKey(undefined);
    setSelectedSource("");
    setProgress(null);
    setError(undefined);
    setPendingProfile(undefined);
    setRecoverySource(undefined);
    setVerified(false);
    setSavedWithoutIdentity(false);
    setApplicationInstalled(false);
    void checkHost();
    void loadRecovery();
  }, [open]);

  useEffect(
    () =>
      host.subscribe((event) => {
        if (
          event.event === "edaCapture.progress" &&
          event.payload.requestId === pendingDeploy.current
        ) {
          setProgress(event.payload);
        }
      }),
    [host, pendingDeploy],
  );

  async function capture(): Promise<void> {
    if (pendingProfile !== undefined) {
      await finishCapture(pendingProfile, verified);
      return;
    }
    const edaApi = credentials();
    if (edaApi === null) return;
    if (validInspection === undefined) {
      await inspect(edaApi);
      return;
    }
    if (!deploymentReady || captureHost?.context === undefined) return;
    const discovered = validInspection;
    const selected =
      resume !== undefined
        ? discovered.sources.find(
            (candidate) =>
              sourceKey(candidate) === sourceKey({ ...resume.source, topics: resume.topics }),
          )
        : discovered.sources.length === 1
          ? discovered.sources[0]
          : discovered.sources.find((candidate) => sourceKey(candidate) === selectedSource);
    if (selected === undefined) {
      setError(
        resume !== undefined
          ? "The saved capture source is no longer available. No deployment was changed. Close this dialog and start a new capture to choose a different source."
          : discovered.sources.length === 0
            ? "No EDA Kafka exporter source was found."
            : "Multiple EDA exporters are available; select one and capture again.",
      );
      return;
    }
    const parsedPort = Number(localPort);
    const requestId = globalThis.crypto.randomUUID();
    pendingDeploy.current = requestId;
    setBusy(true);
    setError(undefined);
    setProgress({
      detail: "Starting EDA capture.",
      phase: "authenticating",
      requestId,
    });
    try {
      const deploymentResponse = await host.execute({
        command: "edaCapture.deploy",
        id: requestId,
        payload: {
          context: captureHost.context,
          edaApi,
          imageDelivery: "configured",
          localPort: parsedPort,
          ...(resume?.sessionId === undefined ? {} : { sessionId: resume.sessionId }),
          source: {
            apiVersion: selected.apiVersion,
            kind: selected.kind,
            name: selected.name,
            namespace: selected.namespace,
          },
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!deploymentResponse.ok) {
        setError(responseError(deploymentResponse.error));
        await loadRecovery();
        return;
      }
      if (!("deployment" in deploymentResponse.result)) {
        setError("The host returned an invalid EDA capture deployment.");
        return;
      }
      const deployment = deploymentResponse.result.deployment;
      const profile: ProfileCreateInput = {
        brokers: [deployment.broker],
        name: deployment.profileName,
        source: toPluginProfileSource({
          edaApiUrl: new URL(edaApi.baseUrl).origin,
          context: deployment.context,
          ...(deployment.sessionId === undefined ? {} : { sessionId: deployment.sessionId }),
          broker: deployment.broker,
          clusterBroker: deployment.clusterBroker,
          exporterName: deployment.exporterName,
          kind: "eda-capture",
          source: {
            apiVersion: selected.apiVersion,
            kind: selected.kind,
            namespace: selected.namespace,
            name: selected.name,
          },
          state: "ready",
          topics: deployment.topics,
          workloadName: deployment.workloadName,
        }),
        transport: "plaintext",
      };
      setPendingProfile(profile);
      setRecoverySource(fromPluginProfileSource(profile.source));
      await finishCapture(profile, false);
    } catch (requestError) {
      await loadRecovery();
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Capture did not complete. Remote resources may remain; inspect capture status before retrying.",
      );
    } finally {
      pendingDeploy.current = null;
      setBusy(false);
    }
  }

  async function loadRecovery(): Promise<void> {
    try {
      const response = await host.execute({
        command: "edaCapture.status",
        id: crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (response.ok && "captureSession" in response.result)
        setRecoverySource(response.result.captureSession.source);
    } catch {
      /* Keep the original operation error; host status remains unverified. */
    }
  }

  async function finishCapture(profile: ProfileCreateInput, tested: boolean): Promise<void> {
    setBusy(true);
    setError(undefined);
    try {
      const captureSource = fromPluginProfileSource(profile.source);
      const existing = profiles.find((candidate) => {
        const candidateSource = fromPluginProfileSource(candidate.source);
        return (
          candidate.id === resumeProfileId ||
          (candidateSource !== undefined &&
            captureSource !== undefined &&
            sameEdaCaptureSource(candidateSource, captureSource))
        );
      });
      const update: ProfileUpdateInput | undefined =
        existing === undefined
          ? undefined
          : {
              brokers: profile.brokers,
              name: existing.name,
              transport: "plaintext",
              expectedRevision: existing.revision ?? 1,
              ...(profile.source === undefined ? {} : { source: profile.source }),
              ...(existing.services === undefined ? {} : { services: existing.services }),
              ...(existing.oauth === undefined
                ? {}
                : {
                    oauth: {
                      clientId: existing.oauth.clientId,
                      tokenEndpoint: existing.oauth.tokenEndpoint,
                      scope: existing.oauth.scope,
                      clientSecret: { mode: "retain" },
                    },
                  }),
            };
      if (!tested) {
        const testResponse = await host.execute({
          command: "profiles.test",
          id: globalThis.crypto.randomUUID(),
          payload:
            existing === undefined || update === undefined
              ? { mode: "create", profile }
              : { mode: "update", profile: update, profileId: existing.id },
          version: HOST_PROTOCOL_VERSION,
        });
        if (!testResponse.ok) {
          setError(
            `Capture deployed, but its local Kafka connection failed. ${responseError(testResponse.error)}`,
          );
          return;
        }
        setVerified(true);
      }
      const createResponse = await host.execute(
        existing === undefined || update === undefined
          ? {
              command: "profiles.create",
              id: globalThis.crypto.randomUUID(),
              payload: { profile },
              version: HOST_PROTOCOL_VERSION,
            }
          : {
              command: "profiles.update",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: {
                profileId: existing.id,
                profile: update,
              },
            },
      );
      if (!createResponse.ok) {
        setError(
          `Capture is running on ${profile.brokers.join(", ")}, but its profile was not saved. ${responseError(createResponse.error)}`,
        );
        return;
      }
      if (
        !("profileId" in createResponse.result) ||
        createResponse.result.profileId === undefined
      ) {
        setSavedWithoutIdentity(true);
        setError(
          "Capture profile was saved, but the host did not return its identity. Close and select the saved connection.",
        );
        return;
      }
      onProfileReady(createResponse.result.profileId);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "The host did not confirm the connection test or profile save. Capture resources remain available.",
      );
    } finally {
      setBusy(false);
    }
  }

  const validPort =
    Number.isInteger(Number(localPort)) &&
    Number(localPort) >= 1_024 &&
    Number(localPort) <= 65_535;

  const matchingDeployment = ((): boolean => {
    if (captureHost?.edaApiUrl === undefined) return true;
    try {
      return new URL(edaApiUrl).origin === new URL(captureHost.edaApiUrl).origin;
    } catch {
      return false;
    }
  })();
  const deploymentReady =
    captureHost?.state === "configured" &&
    captureHost.context !== undefined &&
    matchingDeployment &&
    applicationInstalled;

  return (
    <Dialog
      aria-labelledby="eda-capture-dialog-title"
      fullWidth
      maxWidth="sm"
      onClose={busy ? undefined : onClose}
      open={open}
    >
      <DialogTitle id="eda-capture-dialog-title">Capture Nokia EDA streams</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2}>
          <Typography variant="body2" color="text.secondary">
            Sign in to discover Kafka destinations. Connect to an existing destination, or set up a
            separate temporary capture without changing the original exporter.
          </Typography>
          {temporaryCapture ? (
            <>
              {captureHost === undefined ? (
                <Typography role="status">Checking capture readiness…</Typography>
              ) : captureHost.state === "configured" && applicationInstalled ? (
                <Alert severity="success">
                  StreamSkope Capture is installed and ready for this EDA system.
                </Alert>
              ) : (
                <Typography color="text.secondary" variant="body2">
                  Discover a source first. StreamSkope will then check whether temporary capture is
                  enabled in EDA.
                </Typography>
              )}
            </>
          ) : null}
          <TextField
            disabled={inputsLocked}
            fullWidth
            label="EDA API URL"
            onChange={(event) => {
              setEdaApiUrl(event.target.value);
              invalidateInspection();
            }}
            value={edaApiUrl}
          />
          <Stack direction={{ sm: "row", xs: "column" }} spacing={1}>
            <TextField
              autoComplete="username"
              disabled={inputsLocked}
              fullWidth
              label="EDA username"
              onChange={(event) => {
                setEdaApiUsername(event.target.value);
                invalidateInspection();
              }}
              value={edaApiUsername}
            />
            <TextField
              autoComplete="current-password"
              disabled={inputsLocked}
              fullWidth
              label="EDA password"
              onChange={(event) => {
                setEdaApiPassword(event.target.value);
                invalidateInspection();
              }}
              type="password"
              value={edaApiPassword}
            />
          </Stack>
          <Stack spacing={1}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={verifyTls}
                  disabled={inputsLocked}
                  onChange={(event) => {
                    setVerifyTls(event.target.checked);
                    invalidateInspection();
                  }}
                />
              }
              label="Verify EDA API certificate"
            />
            {verifyTls ? null : (
              <Alert severity="warning">
                Certificate verification is disabled for this capture request only. Use this only
                for local labs or trusted development environments.
              </Alert>
            )}
          </Stack>
          {temporaryCapture ? (
            <Stack direction={{ sm: "row", xs: "column" }} spacing={1}>
              <TextField
                disabled={inputsLocked}
                error={!validPort}
                fullWidth
                helperText={validPort ? undefined : "Use a local port from 1024 through 65535."}
                label="Local Kafka port"
                onChange={(event) => setLocalPort(event.target.value)}
                slotProps={{ htmlInput: { max: 65_535, min: 1_024, step: 1 } }}
                type="number"
                value={localPort}
              />
            </Stack>
          ) : null}
          {validInspection !== undefined && validInspection.sources.length > 1 ? (
            <TextField
              disabled={inputsLocked || resume !== undefined}
              fullWidth
              label="Exporter source"
              onChange={(event) => setSelectedSource(event.target.value)}
              select
              value={selectedSource}
            >
              {validInspection.sources.map((candidate) => (
                <MenuItem key={sourceKey(candidate)} value={sourceKey(candidate)}>
                  {candidate.kind} · {candidate.name} · {candidate.topics.join(", ")}
                </MenuItem>
              ))}
            </TextField>
          ) : null}
          {singleSource === undefined ? null : (
            <Typography color="text.secondary" variant="body2">
              Source: {singleSource.kind} · {singleSource.name} · {singleSource.topics.join(", ")}
            </Typography>
          )}
          {validInspection !== undefined && validInspection.sources.length === 0 ? (
            <Typography color="text.secondary" variant="body2">
              No Producer or ClusterProducer with exported topics was found.
            </Typography>
          ) : null}
          {chosenSource !== undefined && !temporaryCapture && resume === undefined ? (
            <Stack spacing={1}>
              <Typography variant="body2">
                Kafka destination: {chosenSource.brokers?.join(", ") || "Not reported by EDA"}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Use the existing destination with your Kafka credentials, or create a separate
                temporary capture. The original exporter is not changed.
              </Typography>
              <Button
                variant="contained"
                disabled={!chosenSource.brokers?.length || !onExistingDestination}
                onClick={() =>
                  onExistingDestination?.({
                    name: `EDA · ${chosenSource.name}`,
                    brokers: chosenSource.brokers ?? [],
                  })
                }
              >
                Connect to existing Kafka
              </Button>
              <Button variant="outlined" onClick={() => setTemporaryCapture(true)}>
                Set up temporary capture
              </Button>
            </Stack>
          ) : null}
          {validInspection === undefined || !temporaryCapture ? null : (
            <Stack spacing={1.5}>
              {!applicationInstalled ? (
                <EdaCaptureApplicationSetup
                  edaApi={{
                    baseUrl: edaApiUrl.trim(),
                    password: edaApiPassword,
                    username: edaApiUsername.trim(),
                    verifyTls,
                  }}
                  host={host}
                  onInstalled={() => {
                    setApplicationInstalled(true);
                    setApplicationNotice(
                      "StreamSkope Capture is installed. You can start a temporary capture.",
                    );
                  }}
                />
              ) : null}
              <Alert severity="info">
                Starting capture creates a temporary broker and a separate exporter. The original
                producer is unchanged. Disconnecting Kafka does not stop export. Removing capture
                resources discards temporary messages.
              </Alert>
            </Stack>
          )}
          {pendingProfile === undefined ? null : (
            <Alert severity="info">
              Capture is deployed. Retrying below only verifies or saves this connection; it does
              not deploy again.
            </Alert>
          )}
          {!busy && recoverySource !== undefined ? (
            <EdaCaptureStatusPanel
              key={recoverySource.sessionId}
              host={host}
              source={recoverySource}
              onStopped={() => {
                setPendingProfile(undefined);
                setVerified(false);
                setProgress(null);
              }}
            />
          ) : null}
          {temporaryCapture ? (
            <details open={error !== undefined}>
              <Typography component="summary" variant="body2" sx={{ cursor: "pointer" }}>
                Capture steps
              </Typography>
              <Stack spacing={0.25}>
                {EDA_CAPTURE_PROGRESS_PHASES.map((phase, index) => (
                  <Typography
                    color={
                      index < progressIndex
                        ? "success.main"
                        : index === progressIndex
                          ? "primary.main"
                          : "text.secondary"
                    }
                    key={phase}
                    variant="body2"
                  >
                    {index < progressIndex ? "✓" : index === progressIndex ? "●" : "○"}{" "}
                    {PROGRESS_LABELS[phase]}
                  </Typography>
                ))}
              </Stack>
            </details>
          ) : null}
          {progress === null ? null : (
            <Alert aria-live="polite" severity={progress.phase === "ready" ? "success" : "info"}>
              {progress.detail}
            </Alert>
          )}
          {applicationNotice === undefined ? null : (
            <Alert severity="info">{applicationNotice}</Alert>
          )}
          {error === undefined ? null : <Alert severity="error">{error}</Alert>}
        </Stack>
      </DialogContent>
      <DialogActions>
        {busy && pendingDeploy.current !== null ? (
          <Button
            onClick={() => {
              const requestId = pendingDeploy.current;
              if (requestId === null) return;
              void host
                .execute({
                  command: "edaCapture.cancel",
                  id: crypto.randomUUID(),
                  payload: { requestId },
                  version: HOST_PROTOCOL_VERSION,
                })
                .then((response) => {
                  if (!response.ok) setError(responseError(response.error));
                  else
                    setError(
                      "Cancellation requested. Waiting for the current stage to finish; already applied remote resources may remain.",
                    );
                })
                .catch(() =>
                  setError(
                    "Cancellation could not be confirmed. The capture may still be running.",
                  ),
                );
            }}
          >
            Cancel capture
          </Button>
        ) : null}
        <Button disabled={busy} onClick={onClose}>
          Close
        </Button>
        {validInspection === undefined || temporaryCapture || pendingProfile !== undefined ? (
          <Button
            disabled={
              busy ||
              savedWithoutIdentity ||
              (validInspection !== undefined &&
                (!deploymentReady || !validPort || validInspection.sources.length === 0)) ||
              edaApiUrl.trim().length === 0 ||
              edaApiUsername.trim().length === 0 ||
              edaApiPassword.length === 0
            }
            onClick={() => void capture()}
            variant="contained"
          >
            {busy
              ? "Working…"
              : pendingProfile !== undefined
                ? verified
                  ? "Retry saving profile"
                  : "Retry connection test"
                : validInspection === undefined
                  ? "Discover sources"
                  : resume === undefined
                    ? "Start capture"
                    : "Resume capture"}
          </Button>
        ) : null}
      </DialogActions>
    </Dialog>
  );
}
