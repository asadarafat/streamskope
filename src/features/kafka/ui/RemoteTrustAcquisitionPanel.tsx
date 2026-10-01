import { useEffect, useMemo, useRef, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type ProfileTrustKind,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
} from "../contracts";
import type { RemoteSshAuthentication } from "../contracts/remote-trust-types";
import type { TrustRecipeOAuth } from "../contracts/trust-recipe-types";
import type { RemoteSshAccess } from "../contracts/remote-ssh-access";
import type { HttpsProfileAccess } from "../contracts/https-profile-access";
import type { ProfileCreateInput } from "../contracts/profile-types";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { TrustCertificateDetails } from "./TrustCertificateDetails";
import { SshAccessFields, SshAcquisitionDetails, SshIdentityReview } from "./SshAcquisitionAccess";
import {
  HttpsAcquisitionAccess,
  emptyHttpsAccess,
  httpsAcquisitionOrigin,
  type HttpsAccessDraft,
} from "./HttpsAcquisitionAccess";
import { useRemoteTrustAcquisition } from "./use-remote-trust-acquisition";
import type { ProfileTrustRecipeSelection } from "./ProfileTrustRecipeSelector";
import { TrustOAuthSuggestions } from "./TrustOAuthSuggestions";
import {
  acquisitionStatus,
  emptyTarget,
  type SshTargetDraft,
  type TargetField,
} from "./remote-trust-panel-model";

export interface RemoteTrustAcquisitionPanelProperties {
  readonly onApiAccessChange?: (access: HttpsProfileAccess) => void;
  readonly onApiCaChange?: (value: NonNullable<ProfileCreateInput["apiCa"]>) => void;
  readonly onAccessChange?: (access: RemoteSshAccess) => void;
  readonly recipeSelection: ProfileTrustRecipeSelection | null;
  readonly profile?: { readonly id: string; readonly revision: number } | undefined;
  readonly acquisition: RemoteTrustAcquisitionSummary | null;
  readonly disabled?: boolean;
  readonly host: StreamSkopeHost;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly onAcquisitionChange: (
    acquisition: RemoteTrustAcquisitionSummary | null,
    oauth?: Partial<TrustRecipeOAuth>,
  ) => void;
  readonly currentOAuth?: TrustRecipeOAuth;
  readonly onOpenActivity: (correlationId?: string) => void;
}

export function RemoteTrustAcquisitionPanel({
  acquisition,
  disabled = false,
  host,
  kind,
  label,
  onAcquisitionChange,
  onOpenActivity,
  recipeSelection,
  profile,
  currentOAuth,
  onAccessChange,
  onApiAccessChange,
  onApiCaChange,
}: RemoteTrustAcquisitionPanelProperties): React.JSX.Element {
  const httpsRecipe =
    recipeSelection?.recipe.method === "https" ? recipeSelection.recipe : undefined;
  const [api, setApi] = useState<HttpsAccessDraft>(emptyHttpsAccess);
  const [httpsSupported, setHttpsSupported] = useState(false);
  const [authentication, setAuthentication] = useState<RemoteSshAuthentication>({
    mode: "password",
    password: "",
  });
  const recipeIdentity = recipeSelection?.recipe.id;
  const recipeRevision = recipeSelection?.recipe.revision;
  useEffect(() => {
    const access = recipeSelection?.reference.apiAccess;
    setApi((current) => ({
      ...current,
      ...(access ?? {}),
      secret: "",
      retainCa: recipeSelection?.apiCaPresent === true,
    }));
    setAuthentication((current) =>
      current.mode === "agent"
        ? current
        : current.mode === "private-key"
          ? { mode: "private-key", privateKey: "" }
          : { mode: "password", password: "" },
    );
  }, [recipeIdentity, recipeRevision]);
  const [agentStatus, setAgentStatus] = useState<
    "checking" | "unknown" | "configured" | "unavailable"
  >("checking");
  useEffect(() => {
    let current = true;
    void host
      .execute({
        command: "trustAcquisition.capabilities",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .then((response) => {
        if (current) {
          setHttpsSupported(
            response.ok &&
              "sshAgent" in response.result &&
              response.result.methods?.includes("https") === true,
          );
          setAgentStatus(
            response.ok && "sshAgent" in response.result ? response.result.sshAgent : "unknown",
          );
        }
      })
      .catch(() => {
        if (current) setAgentStatus("unknown");
      });
    return (): void => {
      current = false;
    };
  }, [host]);
  const [target, setTarget] = useState<SshTargetDraft>(emptyTarget);
  const restoredAccess = useRef(false);
  useEffect(() => {
    const access = recipeSelection?.reference.access;
    if (restoredAccess.current || access == null) return;
    restoredAccess.current = true;
    setTarget({ host: access.host, port: String(access.port), username: access.username });
    setAuthentication(
      access.authentication === "agent"
        ? { mode: "agent" }
        : access.authentication === "private-key"
          ? { mode: "private-key", privateKey: "" }
          : { mode: "password", password: "" },
    );
  }, [recipeSelection]);
  const [secretParameters, setSecretParameters] = useState<Record<string, string>>({});
  const [truststorePassword, setTruststorePassword] = useState("");
  const [revealSecrets, setRevealSecrets] = useState(false);
  useEffect(() => {
    setSecretParameters({});
    setTruststorePassword("");
    setRevealSecrets(false);
  }, [recipeIdentity, recipeRevision]);
  const passwordTemplate = useMemo(
    () =>
      recipeSelection?.recipe.method === "ssh" &&
      recipeSelection.recipe.ssh.password.source === "command"
        ? {
            name: recipeSelection.recipe.name,
            template: recipeSelection.recipe.ssh.password.command,
          }
        : undefined,
    [recipeSelection],
  );
  const materialTemplate = useMemo(
    () =>
      recipeSelection == null
        ? undefined
        : {
            name: recipeSelection.recipe.name,
            template:
              recipeSelection.recipe.method === "https"
                ? recipeSelection.recipe.https.material.url
                : recipeSelection.recipe.ssh.value,
          },
    [recipeSelection],
  );
  const needsSuppliedPassword =
    recipeSelection?.recipe.method === "ssh"
      ? recipeSelection.recipe.ssh.password.source === "ask"
      : httpsRecipe?.https.password.source === "ask";
  const templateContext = JSON.stringify(recipeSelection);
  const {
    editorScope,
    candidate,
    identityReview,
    displayedAcquisition,
    activityAvailable,
    activityCorrelation,
    diagnostic,
    busy,
    error,
    notice,
    issues,
    setIssues,
    setError,
    setActivityAvailable,
    oauthFields,
    setOAuthFields,
    prepare,
    cancel,
    discard,
    apply,
    acceptIdentity,
  } = useRemoteTrustAcquisition({
    host,
    kind,
    label,
    acquisition,
    onAcquisitionChange,
    profile,
    recipeSelection,
    templateContext,
    target,
    authentication,
    api,
    secretParameters,
    needsSuppliedPassword,
    truststorePassword,
    passwordTemplate,
    materialTemplate,
  });
  const identityReviewElement = useRef<HTMLDivElement>(null);
  const recoveryElement = useRef<HTMLDivElement>(null);
  useEffect(() => {
    identityReviewElement.current?.scrollIntoView?.({ block: "nearest" });
  }, [identityReview]);
  useEffect(() => {
    recoveryElement.current?.scrollIntoView?.({ block: "nearest" });
  }, [error]);
  const operationActive = busy !== undefined;
  const inputsLocked =
    disabled ||
    operationActive ||
    candidate !== null ||
    identityReview !== null ||
    editorScope.editor === null ||
    recipeSelection === null;
  const targetPort = Number(target.port);
  const targetComplete =
    httpsRecipe !== undefined
      ? httpsSupported &&
        (httpsRecipe.https.authentication === "none" || api.secret.length > 0) &&
        (httpsRecipe.https.authentication !== "basic" || api.username.length > 0) &&
        (api.tls === "system" || api.caPem.length > 0 || api.retainCa)
      : target.host.trim().length > 0 &&
        target.username.trim().length > 0 &&
        ((authentication.mode === "agent" && agentStatus === "configured") ||
          (authentication.mode === "password"
            ? authentication.password.length > 0
            : authentication.mode === "private-key" && authentication.privateKey.length > 0)) &&
        Number.isInteger(targetPort) &&
        targetPort >= 1 &&
        targetPort <= 65_535;

  function update(field: TargetField, value: string): void {
    setTarget((current) => ({ ...current, [field]: value }));
    const next = { ...target, [field]: value };
    onAccessChange?.({
      host: next.host,
      port: Number(next.port),
      username: next.username,
      authentication: authentication.mode,
    });
    setIssues((current) => ({ ...current, [field]: undefined }));
    setError(undefined);
    setActivityAvailable(false);
  }

  return (
    <Box sx={{ border: 1, borderColor: "divider", p: 2 }}>
      <Stack spacing={2}>
        <Box>
          <Typography component="h4" variant="subtitle2">
            {httpsRecipe === undefined ? "Retrieve over SSH" : "Retrieve over HTTPS"}
          </Typography>
          {httpsRecipe === undefined ? (
            <Typography color="text.secondary" variant="body2">
              The SSH login is used only for confirmed remote work. Fetched trust values remain in
              the application host. StreamSkope discovers and pins the server identity before
              sending the login.
            </Typography>
          ) : null}
        </Box>
        {httpsRecipe !== undefined ? (
          <>
            {!httpsSupported ? (
              <Alert severity="warning">
                This host does not advertise HTTPS acquisition. Existing Kafka trust is unchanged.
              </Alert>
            ) : null}
            <HttpsAcquisitionAccess
              value={api}
              disabled={inputsLocked || !httpsSupported}
              authentication={httpsRecipe.https.authentication}
              usesHost={/\{\{\s*host\s*\}\}/u.test(httpsRecipe.https.material.url)}
              origin={httpsAcquisitionOrigin(
                httpsRecipe,
                recipeSelection?.reference.overrides ?? {},
                api.host,
              )}
              onCaChange={onApiCaChange}
              onChange={(next) => {
                setApi(next);
                if (
                  next.host !== api.host ||
                  next.username !== api.username ||
                  next.tls !== api.tls
                )
                  onApiAccessChange?.({ host: next.host, username: next.username, tls: next.tls });
                setError(undefined);
              }}
            />
          </>
        ) : (
          <>
            <SshAccessFields
              inputsLocked={inputsLocked}
              issues={issues}
              target={target}
              update={update}
              agentStatus={agentStatus}
              authentication={authentication}
              onAuthenticationChange={(value) => {
                setAuthentication(value);
                if (value.mode !== authentication.mode)
                  onAccessChange?.({
                    host: target.host,
                    port: Number(target.port),
                    username: target.username,
                    authentication: value.mode,
                  });
                setIssues({});
                setError(undefined);
                setActivityAvailable(false);
              }}
            />
          </>
        )}
        {recipeSelection?.recipe.parameters
          .filter((parameter) => parameter.type === "secret")
          .map((parameter) => (
            <TextField
              key={parameter.key}
              label={parameter.label}
              required={parameter.required}
              helperText={
                parameter.help ??
                "Used for this acquisition only; never saved in the template or profile."
              }
              disabled={inputsLocked}
              type={revealSecrets ? "text" : "password"}
              value={secretParameters[parameter.key] ?? ""}
              onChange={(event) =>
                setSecretParameters((current) => ({
                  ...current,
                  [parameter.key]: event.target.value,
                }))
              }
            />
          ))}
        {needsSuppliedPassword ? (
          <TextField
            label="Acquisition truststore password"
            required
            disabled={inputsLocked}
            type={revealSecrets ? "text" : "password"}
            value={truststorePassword}
            onChange={(event) => setTruststorePassword(event.target.value)}
            helperText="Used to decode the retrieved truststore; retained by the host only with the complete candidate."
          />
        ) : null}
        {needsSuppliedPassword ||
        recipeSelection?.recipe.parameters.some((parameter) => parameter.type === "secret") ? (
          <Button disabled={inputsLocked} onClick={() => setRevealSecrets((value) => !value)}>
            {revealSecrets ? "Hide acquisition secrets" : "Show acquisition secrets"}
          </Button>
        ) : null}
        {httpsRecipe === undefined ? (
          <SshAcquisitionDetails
            targetComplete={targetComplete}
            target={target}
            targetPort={targetPort}
            recipeSelection={recipeSelection}
            kind={kind}
            passwordTemplate={passwordTemplate}
            materialTemplate={materialTemplate}
          />
        ) : null}
        <Stack direction={{ sm: "row", xs: "column" }} spacing={1}>
          <Button
            disabled={
              disabled ||
              identityReview !== null ||
              operationActive ||
              !targetComplete ||
              candidate !== null ||
              materialTemplate === undefined ||
              (httpsRecipe === undefined &&
                kind !== "pem" &&
                passwordTemplate === undefined &&
                !needsSuppliedPassword) ||
              (needsSuppliedPassword && truststorePassword.length === 0)
            }
            onClick={() => {
              void prepare();
            }}
            variant="contained"
          >
            {busy === "discover-material"
              ? "Discovering host identity…"
              : busy === "material"
                ? "Retrieving secrets…"
                : "Retrieve"}
          </Button>
          {busy === "discover-material" || busy === "material" || identityReview !== null ? (
            <Button
              variant="outlined"
              onClick={() => {
                void cancel();
              }}
            >
              Cancel acquisition
            </Button>
          ) : null}
          <Button
            disabled={disabled || operationActive || displayedAcquisition === null}
            onClick={() => {
              void discard();
            }}
            variant="text"
          >
            Discard acquired trust
          </Button>
          {candidate === null ? null : (
            <Button disabled={disabled || operationActive} variant="contained" onClick={apply}>
              Apply to connection
            </Button>
          )}
        </Stack>
        {identityReview === null ? null : (
          <Alert ref={identityReviewElement} severity="warning">
            <SshIdentityReview
              host={target.host}
              port={target.port}
              fingerprint={identityReview.plan.hostKeyFingerprint}
              expiresAt={identityReview.expiresAt}
              onAccept={acceptIdentity}
            />
          </Alert>
        )}
        <Typography
          aria-label="Remote trust acquisition status"
          aria-live="polite"
          color={displayedAcquisition === null ? "text.secondary" : "text.primary"}
          role="status"
          variant="body2"
        >
          {acquisitionStatus(displayedAcquisition)}
        </Typography>
        {displayedAcquisition === null ? null : (
          <TrustCertificateDetails acquisition={displayedAcquisition} />
        )}
        {candidate?.oauth === undefined ? null : (
          <TrustOAuthSuggestions
            current={currentOAuth}
            proposed={candidate.oauth}
            selected={oauthFields}
            disabled={disabled || operationActive}
            onChange={setOAuthFields}
          />
        )}
        {displayedAcquisition === null ||
        displayedAcquisition.target.origin !== undefined ? null : (
          <Typography color="text.secondary" variant="body2">
            Pinned identity {displayedAcquisition.target.hostKeyFingerprint}
          </Typography>
        )}
        {candidate === null ? null : (
          <Typography variant="body2">
            Review before applying. Apply to connection replaces the draft certificate and, when
            retrieved, its truststore password. Selected OAuth suggestions replace only the selected
            fields. Nothing is saved, tested or connected automatically.
          </Typography>
        )}
        {busy === "discover-material" ? (
          <Typography
            aria-label="Remote trust operation status"
            aria-live="polite"
            role="status"
            variant="body2"
          >
            Discovering SSH host identity. No credentials or remote template have been sent.
          </Typography>
        ) : null}
        {notice === undefined ? null : <Alert severity="info">{notice}</Alert>}
        {editorScope.error === undefined ? null : (
          <Alert severity="error">{editorScope.error}</Alert>
        )}
        {error === undefined ? null : (
          <Alert
            ref={recoveryElement}
            action={
              activityAvailable ? (
                <Button
                  color="inherit"
                  onClick={() => onOpenActivity(activityCorrelation)}
                  variant="text"
                >
                  Open activity log
                </Button>
              ) : undefined
            }
            severity="error"
          >
            {error}
            {diagnostic === undefined ? null : (
              <Box component="dl" sx={{ m: 0, mt: 1 }}>
                <StudioDetailRow label="Stage" value={diagnostic.stage} />
                <StudioDetailRow label="Category" value={diagnostic.code} />
                <StudioDetailRow label="Target" value={diagnostic.target ?? "Not available"} />
                <StudioDetailRow
                  label="Active connection changed"
                  value={diagnostic.activeStateChanged ? "Yes" : "No"}
                />
              </Box>
            )}
          </Alert>
        )}
      </Stack>
    </Box>
  );
}
