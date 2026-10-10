import { useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  EXTERNAL_METRIC_LIMITS,
  type ExternalMetricsInput,
  type ExternalMetricsSnapshot,
} from "../contracts/external-metrics";

/** Explicit reads only. A late reply cannot populate a replacement connection or page. */
export function useExternalMetrics(
  host: StreamSkopeHost,
  available: boolean,
  endpoint: string | undefined,
): {
  readonly snapshot: ExternalMetricsSnapshot | null;
  readonly pending: boolean;
  readonly error: string | null;
  readonly read: (input: ExternalMetricsInput) => Promise<void>;
  readonly cancel: () => Promise<void>;
} {
  const [snapshot, setSnapshot] = useState<ExternalMetricsSnapshot | null>(null);
  const [, advanceClock] = useState(0);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const generation = useRef(0),
    admission = useRef({ read: false, cancel: false });
  const readEpoch = useRef(0);
  useEffect(() => {
    generation.current++;
    admission.current = { read: false, cancel: false };
    setSnapshot(null);
    setPending(false);
    setError(null);
    // Host invalidation owns cancellation; page attachment does not authorize remote work.
    const unsubscribe = host.subscribe((event): void => {
      if (event.event === "connection.state") {
        generation.current++;
        admission.current = { read: false, cancel: false };
        setSnapshot(null);
        setPending(false);
        setError(null);
      }
    });
    return (): void => {
      generation.current++;
      unsubscribe();
    };
  }, [host, available, endpoint]);
  useEffect(() => {
    const stamps =
      snapshot?.metrics.flatMap((row) =>
        row.state === "ready" && row.scrapedAt !== null ? [row.scrapedAt] : [],
      ) ?? [];
    // At most two expiry boundaries update coverage; they never read or scrape the endpoint.
    const timers = [...new Set(stamps)]
      .map((stamp) => stamp + EXTERNAL_METRIC_LIMITS.staleMs + 1 - Date.now())
      .filter((remaining) => remaining > 0)
      .map((remaining) => setTimeout(() => advanceClock((revision) => revision + 1), remaining));
    return (): void => {
      timers.forEach(clearTimeout);
    };
  }, [snapshot]);
  const execute = async (input?: ExternalMetricsInput): Promise<void> => {
    if (
      !available ||
      (input && !endpoint) ||
      (input ? admission.current.read || admission.current.cancel : admission.current.cancel)
    )
      return;
    const current = generation.current;
    const epoch = ++readEpoch.current;
    const kind = input ? "read" : "cancel";
    admission.current[kind] = true;
    setSnapshot(null);
    setPending(true);
    setError(null);
    try {
      const response = input
        ? await host.execute({
            command: "observations.metrics.capture",
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: input,
          })
        : await host.execute({
            command: "observations.metrics.cancel",
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: {},
          });
      if (current !== generation.current || epoch !== readEpoch.current) return;
      if (!response.ok) setError(`${response.error.summary} ${response.error.recovery}`);
      else if (response.command === "observations.metrics.capture")
        setSnapshot(response.result.snapshot);
      else setSnapshot(null);
    } catch {
      if (current === generation.current && epoch === readEpoch.current)
        setError(
          "The host did not confirm metrics settlement. Restore host access and cancel the original read before retrying; a transport interruption does not prove cleanup.",
        );
    } finally {
      if (current === generation.current) {
        // Cancellation acknowledgement joins original settlement; the read itself also stays admitted until its reply.
        admission.current[kind] = false;
        setPending(admission.current.read || admission.current.cancel);
      }
    }
  };
  return {
    snapshot,
    pending,
    error,
    read: (input): Promise<void> => execute(input),
    cancel: (): Promise<void> => execute(),
  };
}
