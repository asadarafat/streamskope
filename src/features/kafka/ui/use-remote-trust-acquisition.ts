import { useState, type Dispatch, type SetStateAction } from "react";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  type HostCommand,
  type ProfileTrustKind,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
} from "../contracts";
import type {
  RemoteSshAuthentication,
  TrustAcquisitionEditor,
} from "../contracts/remote-trust-types";
import type { TrustRecipeOAuth } from "../contracts/trust-recipe-types";

import { httpsAuthentication, type HttpsAccessDraft } from "./HttpsAcquisitionAccess";
import type { ProfileTrustRecipeSelection } from "./ProfileTrustRecipeSelector";
import { useTrustAcquisitionEditor } from "./use-trust-acquisition-editor";
import {
  useTrustAcquisitionOperation,
  type TrustAcquisitionOperation,
} from "./use-trust-acquisition-operation";
import {
  acquisitionCommand,
  hostKeyDiscoveryCommand,
  issueField,
  targetInput,
  type AcquisitionPlan,
  type SshTargetDraft,
  type TargetField,
} from "./remote-trust-panel-model";

type SelectedTemplate = { readonly name: string; readonly template: string };
interface RemoteTrustAcquisition extends Pick<
  TrustAcquisitionOperation,
  | "candidate"
  | "identityReview"
  | "displayedAcquisition"
  | "activityAvailable"
  | "activityCorrelation"
  | "diagnostic"
  | "busy"
  | "error"
  | "notice"
  | "oauthFields"
  | "setOAuthFields"
  | "setError"
  | "setActivityAvailable"
  | "cancel"
  | "discard"
  | "apply"
> {
  readonly editorScope: ReturnType<typeof useTrustAcquisitionEditor>;
  readonly issues: Partial<Record<TargetField, string>>;
  readonly setIssues: Dispatch<SetStateAction<Partial<Record<TargetField, string>>>>;
  readonly prepare: () => Promise<void>;
  readonly acceptIdentity: () => void;
}

interface RemoteTrustAcquisitionOptions {
  readonly host: StreamSkopeHost;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly acquisition: RemoteTrustAcquisitionSummary | null;
  readonly onAcquisitionChange: (
    acquisition: RemoteTrustAcquisitionSummary | null,
    oauth?: Partial<TrustRecipeOAuth>,
  ) => void;
  readonly profile?: { readonly id: string; readonly revision: number } | undefined;
  readonly recipeSelection: ProfileTrustRecipeSelection | null;
  readonly templateContext: string;
  readonly target: SshTargetDraft;
  readonly authentication: RemoteSshAuthentication;
  readonly api: HttpsAccessDraft;
  readonly secretParameters: Record<string, string>;
  readonly needsSuppliedPassword: boolean;
  readonly truststorePassword: string;
  readonly passwordTemplate: SelectedTemplate | undefined;
  readonly materialTemplate: SelectedTemplate | undefined;
}

export function useRemoteTrustAcquisition({
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
}: RemoteTrustAcquisitionOptions): RemoteTrustAcquisition {
  const editorScope = useTrustAcquisitionEditor(host, profile);
  const httpsRecipe =
    recipeSelection?.recipe.method === "https" ? recipeSelection.recipe : undefined;
  const operation = useTrustAcquisitionOperation({
    host,
    kind,
    label,
    templateContext,
    editorScope,
    acquisition,
    onAcquisitionChange,
  });
  const {
    candidate,
    setIdentityReview,
    setError,
    setNotice,
    setActivityAvailable,
    setDiagnostic,
    setActivityCorrelation,
    setBusy,
  } = operation;
  const [issues, setIssues] = useState<Partial<Record<TargetField, string>>>({});

  function materialCommand(
    fingerprint: string,
    editor: TrustAcquisitionEditor,
  ): Extract<HostCommand, { readonly command: "trustAcquisition.material.fetch" }> {
    if (recipeSelection == null) throw new Error("Select a retrieval preset before acquisition.");
    const command = acquisitionCommand(
      targetInput(target, fingerprint, authentication),
      kind,
      label,
      editor,
      recipeSelection.reference,
    );
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
      const currentGeneration = operation.begin();
      try {
        const editor = await editorScope.advance();
        if (!operation.isCurrent(currentGeneration)) return;
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
        await operation.executeMaterialCommand(command, currentGeneration);
      } catch (failure) {
        if (operation.isCurrent(currentGeneration)) {
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
    const currentGeneration = operation.begin();
    try {
      const editor = await editorScope.advance();
      if (!operation.isCurrent(currentGeneration)) return;
      const command = parseHostCommand(hostKeyDiscoveryCommand(target, editor));
      operation.setPendingRequest(command.id);
      const response = await host.execute(command);
      if (!operation.isCurrent(currentGeneration)) return;
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
      if (!operation.isCurrent(currentGeneration)) return;
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
      if (operation.isCurrent(currentGeneration)) {
        operation.setPendingRequest(null);
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
    await operation.executeMaterialCommand(command, currentGeneration);
  }

  function acceptIdentity(): void {
    const review = operation.identityReview;
    if (review === null) return;
    setIdentityReview(null);
    if (Date.parse(review.expiresAt) <= Date.now()) {
      setError("The SSH identity review expired. Acquire again.");
      return;
    }
    void executeAcquisition(review.plan, review.generation);
  }

  return { ...operation, editorScope, issues, setIssues, prepare, acceptIdentity };
}
