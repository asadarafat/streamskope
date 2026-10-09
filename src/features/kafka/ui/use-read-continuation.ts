import { useCallback, useEffect, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { KafkaSearchProgress } from "../contracts/query-search";

/** The host owns offsets and authority; the renderer only binds its current read controls. */
export function useReadContinuation({
  host,
  context,
  progress,
  connected,
  active,
  stopping,
  topicMatches,
  onError,
  onContinued,
}: {
  readonly host: StreamSkopeHost;
  readonly context: string;
  readonly progress: KafkaSearchProgress | null;
  readonly connected: boolean;
  readonly active: boolean;
  readonly stopping: boolean;
  readonly topicMatches: boolean;
  readonly onError: (message: string | undefined) => void;
  readonly onContinued: () => void;
}): {
  readonly continuationAvailable: boolean;
  readonly continuationBusy: boolean;
  readonly continuationNotice: string | undefined;
  readonly continueConsumption: () => Promise<void>;
  readonly bindContinuation: (context: string | null) => void;
} {
  const [boundContext, setBoundContext] = useState<string | null>(null);
  const [continuationBusy, setContinuationBusy] = useState(false);
  const [expiredContinuation, setExpiredContinuation] = useState<string | null>(null);
  const [submittedContinuation, setSubmittedContinuation] = useState<string | null>(null);
  const continuation = progress?.continuation;
  const bindContinuation = useCallback(
    (readContext: string | null): void => {
      setBoundContext(readContext);
      // Starting a new read cannot revive the previous pass while the host validates it.
      setSubmittedContinuation(continuation?.id ?? null);
    },
    [continuation?.id],
  );

  // Editing and then reverting controls must not silently restore an old continuation.
  useEffect(() => {
    if (!connected || boundContext !== context) setBoundContext(null);
  }, [connected, boundContext, context]);

  useEffect(() => {
    if (!continuation) return;
    const delay = Date.parse(continuation.expiresAt) - Date.now();
    if (delay <= 0) {
      setExpiredContinuation(continuation.id);
      return;
    }
    const timer = setTimeout(
      () => setExpiredContinuation(continuation.id),
      Math.min(delay, 2_147_483_647),
    );
    return (): void => clearTimeout(timer);
  }, [continuation]);

  const continuationNotice = !continuation
    ? undefined
    : expiredContinuation === continuation.id || Date.parse(continuation.expiresAt) <= Date.now()
      ? "This continuation expired. Start a new read."
      : boundContext === null || boundContext !== context
        ? "Start a new read to continue with the current settings."
        : undefined;
  const continuationAvailable = Boolean(
    continuation &&
    continuationNotice === undefined &&
    connected &&
    !active &&
    !stopping &&
    !continuationBusy &&
    continuation.id !== submittedContinuation &&
    topicMatches,
  );

  const continueConsumption = useCallback(async (): Promise<void> => {
    if (!continuationAvailable || !continuation || Date.parse(continuation.expiresAt) <= Date.now())
      return;
    onError(undefined);
    setContinuationBusy(true);
    setSubmittedContinuation(continuation.id);
    try {
      const response = await host.execute({
        command: "messages.continue",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { continuationId: continuation.id },
      });
      if (!response.ok) {
        onError(`${response.error.summary} ${response.error.recovery}`);
        setBoundContext(null);
      } else {
        onContinued();
      }
    } catch {
      onError(
        "The host did not acknowledge continuation. Check the read status before starting again.",
      );
      setBoundContext(null);
    } finally {
      setContinuationBusy(false);
    }
  }, [host, continuation, continuationAvailable, onContinued, onError]);

  return {
    continuationAvailable,
    continuationBusy,
    continuationNotice,
    continueConsumption,
    bindContinuation,
  };
}
