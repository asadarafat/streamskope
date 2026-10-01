import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from "react";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostError,
  type ProfileTrustKind,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
} from "../contracts";
import type { TrustAcquisitionEditor } from "../contracts/remote-trust-types";
import type { TrustRecipeOAuth } from "../contracts/trust-recipe-types";

import type { useTrustAcquisitionEditor } from "./use-trust-acquisition-editor";
import type { OAuthSuggestionField } from "./TrustOAuthSuggestions";
import type { AcquisitionPlan, BusyOperation } from "./remote-trust-panel-model";

type Setter<T> = Dispatch<SetStateAction<T>>;
interface IdentityReview {
  readonly plan: AcquisitionPlan;
  readonly generation: number;
  readonly expiresAt: string;
}
export interface TrustAcquisitionOperation {
  readonly candidate: RemoteTrustAcquisitionSummary | null;
  readonly identityReview: IdentityReview | null;
  readonly displayedAcquisition: RemoteTrustAcquisitionSummary | null;
  readonly activityAvailable: boolean;
  readonly activityCorrelation: string | undefined;
  readonly diagnostic: HostError | undefined;
  readonly busy: BusyOperation | undefined;
  readonly error: string | undefined;
  readonly notice: string | undefined;
  readonly oauthFields: readonly OAuthSuggestionField[];
  readonly setOAuthFields: Setter<readonly OAuthSuggestionField[]>;
  readonly setIdentityReview: Setter<IdentityReview | null>;
  readonly setError: Setter<string | undefined>;
  readonly setNotice: Setter<string | undefined>;
  readonly setActivityAvailable: Setter<boolean>;
  readonly setDiagnostic: Setter<HostError | undefined>;
  readonly setActivityCorrelation: Setter<string | undefined>;
  readonly setBusy: Setter<BusyOperation | undefined>;
  readonly executeMaterialCommand: (command: HostCommand, generation: number) => Promise<void>;
  readonly cancel: () => Promise<void>;
  readonly discard: () => Promise<void>;
  readonly apply: () => void;
  readonly begin: () => number;
  readonly isCurrent: (generation: number) => boolean;
  readonly setPendingRequest: (requestId: string | null) => void;
}

interface TrustAcquisitionOperationOptions {
  readonly host: StreamSkopeHost;
  readonly kind: ProfileTrustKind;
  readonly label: string;
  readonly templateContext: string;
  readonly editorScope: ReturnType<typeof useTrustAcquisitionEditor>;
  readonly acquisition: RemoteTrustAcquisitionSummary | null;
  readonly onAcquisitionChange: (
    acquisition: RemoteTrustAcquisitionSummary | null,
    oauth?: Partial<TrustRecipeOAuth>,
  ) => void;
}

// Owns request generations, cleanup and candidate application for SSH and HTTPS alike.
export function useTrustAcquisitionOperation({
  host,
  kind,
  label,
  templateContext,
  editorScope,
  acquisition,
  onAcquisitionChange,
}: TrustAcquisitionOperationOptions): TrustAcquisitionOperation {
  const editorRef = useRef<TrustAcquisitionEditor | null>(null);
  editorRef.current = editorScope.editor;
  const [candidate, setCandidate] = useState<RemoteTrustAcquisitionSummary | null>(null);
  const [identityReview, setIdentityReview] = useState<IdentityReview | null>(null);
  const pendingCandidate = useRef<RemoteTrustAcquisitionSummary | null>(null);
  const pendingRequest = useRef<string | null>(null);
  const mounted = useRef(true);
  const generation = useRef(0);
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
  useEffect(() => {
    if (error === undefined) {
      setDiagnostic(undefined);
      setActivityCorrelation(undefined);
    }
  }, [error]);
  const [notice, setNotice] = useState<string>();
  const [oauthFields, setOAuthFields] = useState<readonly OAuthSuggestionField[]>([]);

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

  function apply(): void {
    if (candidate === null) return;
    if (Date.parse(candidate.expiresAt) <= Date.now()) {
      setError(
        "This candidate expired. Discard it and acquire trust again. Existing trust is unchanged.",
      );
      return;
    }
    if (candidate.editor === undefined) {
      setError("The host returned an unscoped candidate. Reopen the profile and acquire again.");
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
          setError("The host did not confirm candidate application. Existing trust is unchanged.");
      })
      .finally(() => {
        if (mounted.current && currentGeneration === generation.current) setBusy(undefined);
      });
  }

  return {
    candidate,
    identityReview,
    displayedAcquisition,
    activityAvailable,
    activityCorrelation,
    diagnostic,
    busy,
    error,
    notice,
    oauthFields,
    setOAuthFields,
    setIdentityReview,
    setError,
    setNotice,
    setActivityAvailable,
    setDiagnostic,
    setActivityCorrelation,
    setBusy,
    executeMaterialCommand,
    cancel,
    discard,
    apply,
    begin: (): number => ++generation.current,
    isCurrent: (value: number): boolean => mounted.current && value === generation.current,
    setPendingRequest: (value: string | null): void => {
      pendingRequest.current = value;
    },
  };
}
