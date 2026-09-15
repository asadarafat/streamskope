import { useEffect, useMemo, useRef, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  type ConnectionTemplateSnapshot,
  type HostCommand,
  type HostError,
  type ProfileTrustKind,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
} from "../contracts";
import type {
  RemoteSshAuthentication,
  TrustAcquisitionEditor,
} from "../contracts/remote-trust-types";
import type { TrustRecipeOAuth } from "../contracts/trust-recipe-types";
import type { RemoteSshAccess } from "../contracts/remote-ssh-access";
import type { HttpsProfileAccess } from "../contracts/https-profile-access";
import type { ProfileCreateInput } from "../contracts/profile-types";
import { StudioDetailRow } from "../../ui/StudioPropertyRow";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../ui/controls";

import { TrustCertificateDetails } from "./TrustCertificateDetails";
import { SshAccessFields, SshAcquisitionDetails, SshIdentityReview } from "./SshAcquisitionAccess";
import {
  HttpsAcquisitionAccess,
  emptyHttpsAccess,
  httpsAcquisitionOrigin,
  httpsAuthentication,
  type HttpsAccessDraft,
} from "./HttpsAcquisitionAccess";
import { useTrustAcquisitionEditor } from "./use-trust-acquisition-editor";
import type { ProfileTrustRecipeSelection } from "./ProfileTrustRecipeSelector";
import { TrustOAuthSuggestions, type OAuthSuggestionField } from "./TrustOAuthSuggestions";
import {
  acquisitionCommand,
  acquisitionStatus,
  emptyTarget,
  hostKeyDiscoveryCommand,
  issueField,
  selectedTemplate,
  targetInput,
  type AcquisitionPlan,
  type BusyOperation,
  type SshTargetDraft,
  type TargetField,
} from "./remote-trust-panel-model";

export interface RemoteTrustAcquisitionPanelProperties {
  readonly onApiAccessChange?: (access: HttpsProfileAccess) => void;
  readonly onApiCaChange?: (value: NonNullable<ProfileCreateInput["apiCa"]>) => void;
  readonly onAccessChange?: (access: RemoteSshAccess) => void;
  readonly recipeSelection?: ProfileTrustRecipeSelection | null;
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
  readonly templateSnapshot: ConnectionTemplateSnapshot | null;
}

export function RemoteTrustAcquisitionPanel({
  acquisition,
  disabled = false,
  host,
  kind,
  label,
  onAcquisitionChange,
  onOpenActivity,
  templateSnapshot,
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
  const editorScope = useTrustAcquisitionEditor(host, profile);
  const editorRef = useRef<TrustAcquisitionEditor | null>(null);
  editorRef.current = editorScope.editor;
  const [candidate, setCandidate] = useState<RemoteTrustAcquisitionSummary | null>(null);
  const [identityReview, setIdentityReview] = useState<{
    readonly plan: AcquisitionPlan;
    readonly generation: number;
    readonly expiresAt: string;
  } | null>(null);
  const pendingCandidate = useRef<RemoteTrustAcquisitionSummary | null>(null);
  const pendingRequest = useRef<string | null>(null);
  const mounted = useRef(true);
  const generation = useRef(0);
  const templateContext = JSON.stringify(
    recipeSelection === undefined ? templateSnapshot?.catalogs : recipeSelection,
  );
  const displayedAcquisition = candidate ?? acquisition;
  useEffect(() => {
    mounted.current = true;
    generation.current += 1;
    setCandidate(null);
    setIdentityReview(null);
    setBusy(undefined);
    setNotice(undefined);
    return (): void => {
      mounted.current = false;
      generation.current += 1;
      const requestId = pendingRequest.current;
      pendingRequest.current = null;
      if (requestId !== null)
        void host
          .execute({
            command: "trustAcquisition.cancel",
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: {
              requestId,
              ...(editorRef.current === null ? {} : { editorId: editorRef.current.id }),
            },
          })
          .catch(() => undefined);
      const unused = pendingCandidate.current;
      pendingCandidate.current = null;
      if (unused !== null)
        void host
          .execute({
            command: "trustAcquisition.discard",
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: {
              acquisitionId: unused.id,
              ...(unused.editor === undefined ? {} : { editorId: unused.editor.id }),
            },
          })
          .catch(() => undefined);
    };
  }, [host, kind, label, templateContext]);
  const [activityAvailable, setActivityAvailable] = useState(false);
  const [activityCorrelation, setActivityCorrelation] = useState<string>();
  const [diagnostic, setDiagnostic] = useState<HostError>();
  const [busy, setBusy] = useState<BusyOperation>();
  const [error, setError] = useState<string>();
  const identityReviewElement = useRef<HTMLDivElement>(null);
  const recoveryElement = useRef<HTMLDivElement>(null);
  useEffect(() => {
    identityReviewElement.current?.scrollIntoView?.({ block: "nearest" });
  }, [identityReview]);
  useEffect(() => {
    recoveryElement.current?.scrollIntoView?.({ block: "nearest" });
  }, [error]);
  useEffect(() => {
    if (error === undefined) {
      setDiagnostic(undefined);
      setActivityCorrelation(undefined);
    }
  }, [error]);
  const [notice, setNotice] = useState<string>();
  const [issues, setIssues] = useState<Partial<Record<TargetField, string>>>({});
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
  const [oauthFields, setOAuthFields] = useState<readonly OAuthSuggestionField[]>([]);
  useEffect(() => {
    setSecretParameters({});
    setTruststorePassword("");
    setRevealSecrets(false);
  }, [recipeIdentity, recipeRevision]);
  const passwordTemplate = useMemo(
    () =>
      recipeSelection === undefined
        ? selectedTemplate(templateSnapshot, "truststore-password")
        : recipeSelection?.recipe.method === "ssh" &&
            recipeSelection.recipe.ssh.password.source === "command"
          ? {
              name: recipeSelection.recipe.name,
              template: recipeSelection.recipe.ssh.password.command,
            }
          : undefined,
    [templateSnapshot, recipeSelection],
  );
  const materialTemplate = useMemo(
    () =>
      recipeSelection === undefined
        ? selectedTemplate(templateSnapshot, "truststore-fetch")
        : recipeSelection?.recipe.method === "https"
          ? {
              name: recipeSelection.recipe.name,
              template: recipeSelection.recipe.https.material.url,
            }
          : recipeSelection === null
            ? undefined
            : { name: recipeSelection.recipe.name, template: recipeSelection.recipe.ssh.value },
    [templateSnapshot, recipeSelection],
  );
  const templateUnavailable =
    recipeSelection === undefined && templateSnapshot?.store.state === "unavailable";
  const needsSuppliedPassword =
    recipeSelection?.recipe.method === "ssh"
      ? recipeSelection.recipe.ssh.password.source === "ask"
      : httpsRecipe?.https.password.source === "ask";
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

  function materialCommand(
    fingerprint: string,
    editor: TrustAcquisitionEditor,
  ): Extract<HostCommand, { readonly command: "trustAcquisition.material.fetch" }> {
    const command = acquisitionCommand(
      targetInput(target, fingerprint, authentication),
      kind,
      label,
      editor,
    );
    if (recipeSelection == null) return command;
    return {
      ...command,
      payload: {
        ...command.payload,
        recipe: recipeSelection.reference,
        secretParameters,
        ...(needsSuppliedPassword ? { truststorePassword } : {}),
        ...(profile === undefined ? {} : { profile }),
      },
    };
  }

  function prepareAcquisition(
    hostKeyFingerprint: string,
    selectedMaterial: { readonly name: string; readonly template: string },
    editor: TrustAcquisitionEditor,
    identityId: string,
  ): AcquisitionPlan | undefined {
    try {
      parseHostCommand(materialCommand(hostKeyFingerprint, editor));
      setIssues({});
      return {
        identityId,
        editor,
        hostKeyFingerprint,
        materialTemplateName: selectedMaterial.name,
        ...(kind === "pem" || passwordTemplate === undefined
          ? {}
          : {
              passwordTemplateName: passwordTemplate.name,
            }),
      };
    } catch (candidate) {
      if (candidate instanceof HostContractValidationError) {
        const field = issueField(candidate.path);
        if (field !== undefined) {
          setIssues({ [field]: candidate.message.slice(candidate.message.indexOf(":") + 2) });
          return undefined;
        }
      }
      setError(
        candidate instanceof Error
          ? candidate.message
          : "Correct the remote acquisition fields and try again.",
      );
      return undefined;
    }
  }

  async function prepare(): Promise<void> {
    setError(undefined);
    setNotice(undefined);
    setActivityAvailable(false);
    if (httpsRecipe !== undefined && recipeSelection != null) {
      setBusy("material");
      const currentGeneration = ++generation.current;
      try {
        const editor = await editorScope.advance();
        if (!mounted.current || currentGeneration !== generation.current) return;
        const command = parseHostCommand({
          command: "trustAcquisition.https.fetch",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {
            editor,
            recipe: recipeSelection.reference,
            kind,
            label,
            secretParameters,
            ...(profile === undefined ? {} : { profile }),
            ...(needsSuppliedPassword ? { truststorePassword } : {}),
            api: {
              host: api.host,
              authentication: httpsAuthentication(httpsRecipe.https.authentication, api),
              tls:
                api.tls === "system"
                  ? { mode: "system" }
                  : api.retainCa && !api.caPem
                    ? { mode: "retain" }
                    : { mode: "custom", caPem: api.caPem },
            },
          },
        });
        await executeMaterialCommand(command, currentGeneration);
      } catch (failure) {
        if (mounted.current && currentGeneration === generation.current) {
          setError(
            failure instanceof Error ? failure.message : "Check the API access fields and retry.",
          );
          setBusy(undefined);
        }
      }
      return;
    }
    if (materialTemplate === undefined) {
      setError("Select a truststore-fetch template before remote acquisition.");
      return;
    }
    if (kind !== "pem" && passwordTemplate === undefined && !needsSuppliedPassword) {
      setError("Select a truststore-password template before remote acquisition.");
      return;
    }
    if (candidate !== null) {
      setError("Use or discard the current candidate before acquiring replacement material.");
      return;
    }
    setBusy("discover-material");
    const currentGeneration = ++generation.current;
    try {
      const editor = await editorScope.advance();
      if (!mounted.current || currentGeneration !== generation.current) return;
      const command = parseHostCommand(hostKeyDiscoveryCommand(target, editor));
      pendingRequest.current = command.id;
      const response = await host.execute(command);
      if (!mounted.current || currentGeneration !== generation.current) return;
      if (!response.ok) {
        setDiagnostic(response.error);
        setActivityCorrelation(response.error.correlationId);
        setError(`${response.error.summary} ${response.error.recovery}`);
        setActivityAvailable(true);
        return;
      }
      if (!("hostKey" in response.result)) {
        setError("The application host returned no discovered SSH identity.");
        return;
      }
      if (
        response.result.hostKey.target.host !== target.host ||
        response.result.hostKey.target.port !== Number(target.port)
      ) {
        setError("The application host returned SSH identity for a different endpoint.");
        return;
      }
      const review = response.result.hostKey.review;
      if (review === undefined) {
        setError(
          "The host did not return a scoped SSH identity review. Reopen the profile and acquire again.",
        );
        return;
      }
      const candidate = prepareAcquisition(
        response.result.hostKey.fingerprint,
        materialTemplate,
        editor,
        review.id,
      );
      if (candidate !== undefined) {
        if (review.confirmationRequired)
          setIdentityReview({
            plan: candidate,
            generation: currentGeneration,
            expiresAt: review.expiresAt,
          });
        else await executeAcquisition(candidate, currentGeneration);
      }
    } catch (candidate) {
      if (!mounted.current || currentGeneration !== generation.current) return;
      if (candidate instanceof HostContractValidationError) {
        const field = issueField(candidate.path);
        if (field !== undefined) {
          setIssues({ [field]: candidate.message.slice(candidate.message.indexOf(":") + 2) });
          return;
        }
      }
      setActivityAvailable(true);
      setError(
        candidate instanceof Error
          ? candidate.message
          : "Correct the remote acquisition fields and try again.",
      );
    } finally {
      if (mounted.current && currentGeneration === generation.current) {
        pendingRequest.current = null;
        setBusy(undefined);
      }
    }
  }

  async function executeAcquisition(
    candidate: AcquisitionPlan,
    currentGeneration: number,
  ): Promise<void> {
    if (
      materialTemplate?.name !== candidate.materialTemplateName ||
      (kind !== "pem" && passwordTemplate?.name !== candidate.passwordTemplateName)
    ) {
      setError("A selected template changed. Review the current templates before running them.");
      return;
    }
    const base = materialCommand(candidate.hostKeyFingerprint, candidate.editor);
    const command = {
      ...base,
      payload: { ...base.payload, identityId: candidate.identityId, acceptIdentity: true },
    };
    await executeMaterialCommand(command, currentGeneration);
  }

  async function executeMaterialCommand(
    command: HostCommand,
    currentGeneration: number,
  ): Promise<void> {
    setError(undefined);
    setActivityAvailable(false);
    setBusy("material");
    pendingRequest.current = command.id;
    try {
      const response = await host.execute(command);
      if (!mounted.current || currentGeneration !== generation.current) {
        if (response.ok && "acquisition" in response.result) {
          void host
            .execute({
              command: "trustAcquisition.discard",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: {
                acquisitionId: response.result.acquisition.id,
                ...(response.result.acquisition.editor === undefined
                  ? {}
                  : { editorId: response.result.acquisition.editor.id }),
              },
            })
            .catch(() => undefined);
        }
        return;
      }
      if (!response.ok) {
        setDiagnostic(response.error);
        setActivityCorrelation(response.error.correlationId);
        setError(`${response.error.summary} ${response.error.recovery}`);
        setActivityAvailable(true);
        return;
      }
      if (!("acquisition" in response.result)) {
        setError("The application host returned no remote acquisition summary.");
        return;
      }
      pendingCandidate.current = response.result.acquisition;
      setOAuthFields([]);
      setCandidate(response.result.acquisition);
    } catch {
      if (!mounted.current || currentGeneration !== generation.current) return;
      setActivityAvailable(true);
      setError(
        "The application host did not accept the remote acquisition. Open Activity for diagnostics.",
      );
    } finally {
      if (mounted.current && currentGeneration === generation.current) {
        pendingRequest.current = null;
        setBusy(undefined);
      }
    }
  }

  async function cancel(): Promise<void> {
    const requestId = pendingRequest.current;
    pendingRequest.current = null;
    const cancelledGeneration = ++generation.current;
    setBusy(undefined);
    const reviewing = identityReview !== null;
    setIdentityReview(null);
    setNotice("Cancellation requested. Existing trust and the active connection are unchanged.");
    setError(undefined);
    if (requestId === null) {
      if (reviewing) {
        try {
          await editorScope.advance();
        } catch {
          setError(
            "The identity review could not be cancelled in the host. Close this editor before retrying.",
          );
        }
      }
      return;
    }
    try {
      const response = await host.execute({
        command: "trustAcquisition.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {
          requestId,
          ...(editorRef.current === null ? {} : { editorId: editorRef.current.id }),
        },
      });
      if (!mounted.current || cancelledGeneration !== generation.current) return;
      if (!response.ok) {
        setNotice(undefined);
        setDiagnostic(response.error);
        setActivityCorrelation(response.error.correlationId);
        setError(`${response.error.summary} ${response.error.recovery}`);
        setActivityAvailable(true);
      }
    } catch {
      if (!mounted.current || cancelledGeneration !== generation.current) return;
      setNotice(undefined);
      setError(
        "The host did not confirm the cancellation request. Open Activity to check the operation outcome.",
      );
      setActivityAvailable(true);
    }
  }

  async function discard(): Promise<void> {
    if (displayedAcquisition === null) {
      return;
    }
    setBusy("discard");
    setError(undefined);
    setActivityAvailable(false);
    const currentGeneration = generation.current;
    try {
      const response = await host.execute({
        command: "trustAcquisition.discard",
        id: globalThis.crypto.randomUUID(),
        payload: {
          acquisitionId: displayedAcquisition.id,
          ...(displayedAcquisition.editor === undefined
            ? {}
            : { editorId: displayedAcquisition.editor.id }),
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!mounted.current || currentGeneration !== generation.current) return;
      if (!response.ok) {
        setDiagnostic(response.error);
        setActivityCorrelation(response.error.correlationId);
        setError(`${response.error.summary} ${response.error.recovery}`);
        setActivityAvailable(true);
        return;
      }
      if (candidate !== null) {
        pendingCandidate.current = null;
        setCandidate(null);
      } else onAcquisitionChange(null);
    } catch {
      if (!mounted.current || currentGeneration !== generation.current) return;
      setActivityAvailable(true);
      setError(
        "The application host did not accept the discard request. Open Activity for diagnostics.",
      );
    } finally {
      if (mounted.current && currentGeneration === generation.current) setBusy(undefined);
    }
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
        {templateUnavailable ? (
          <Alert severity="error">
            Connection templates are unavailable. Restore template storage before remote
            acquisition.
          </Alert>
        ) : null}
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
            <Button
              disabled={disabled || operationActive}
              variant="contained"
              onClick={() => {
                if (Date.parse(candidate.expiresAt) <= Date.now()) {
                  setError(
                    "This candidate expired. Discard it and acquire trust again. Existing trust is unchanged.",
                  );
                  return;
                }
                if (candidate.editor === undefined) {
                  setError(
                    "The host returned an unscoped candidate. Reopen the profile and acquire again.",
                  );
                  return;
                }
                const applying = candidate;
                const currentGeneration = generation.current;
                setBusy("apply");
                void host
                  .execute({
                    command: "trustAcquisition.apply",
                    id: crypto.randomUUID(),
                    version: HOST_PROTOCOL_VERSION,
                    payload: { acquisitionId: candidate.id, editorId: candidate.editor.id },
                  })
                  .then((response) => {
                    if (!mounted.current || currentGeneration !== generation.current) return;
                    if (!response.ok) {
                      setDiagnostic(response.error);
                      setActivityCorrelation(response.error.correlationId);
                      setError(`${response.error.summary} ${response.error.recovery}`);
                      return;
                    }
                    pendingCandidate.current = null;
                    setCandidate(null);
                    const oauth =
                      applying.oauth === undefined || oauthFields.length === 0
                        ? undefined
                        : Object.fromEntries(oauthFields.map((key) => [key, applying.oauth![key]]));
                    if (oauth === undefined) onAcquisitionChange(applying);
                    else onAcquisitionChange(applying, oauth);
                  })
                  .catch(() => {
                    if (mounted.current && currentGeneration === generation.current)
                      setError(
                        "The host did not confirm candidate application. Existing trust is unchanged.",
                      );
                  })
                  .finally(() => {
                    if (mounted.current && currentGeneration === generation.current)
                      setBusy(undefined);
                  });
              }}
            >
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
              onAccept={() => {
                const review = identityReview;
                setIdentityReview(null);
                if (Date.parse(review.expiresAt) <= Date.now()) {
                  setError("The SSH identity review expired. Acquire again.");
                  return;
                }
                void executeAcquisition(review.plan, review.generation);
              }}
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
