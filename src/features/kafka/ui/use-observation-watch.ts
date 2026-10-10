import { useCallback, useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost, type HostError } from "../contracts";
import {
  emptyObservationWatch,
  type ObservationWatchSnapshot,
} from "../contracts/observation-watch";
import type { ObservationInput } from "../contracts/observations";

interface WatchAttachment {
  readonly watch: ObservationWatchSnapshot;
  readonly ready: boolean;
  readonly pending: boolean;
  readonly error: {
    readonly summary: string;
    readonly recovery: string;
    readonly code?: HostError["code"];
  } | null;
  readonly start: (input: ObservationInput) => Promise<void>;
  readonly stop: () => Promise<void>;
}

/** Read-only attachment. Mount, navigation and transport recovery never authorize collection. */
export function useObservationWatch(host: StreamSkopeHost, available: boolean): WatchAttachment {
  const [watch, setWatch] = useState(emptyObservationWatch);
  const [ready, setReady] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<WatchAttachment["error"]>(null);
  const generation = useRef(0);
  const latestRevision = useRef(-1);
  const admission = useRef({ start: false, stop: false });
  const accept = useCallback((value: ObservationWatchSnapshot): void => {
    if (value.revision < latestRevision.current) return;
    latestRevision.current = value.revision;
    setWatch(value);
    setReady(true);
    setError(value.error);
  }, []);
  useEffect(() => {
    const current = ++generation.current;
    latestRevision.current = -1;
    admission.current = { start: false, stop: false };
    setPending(false);
    setReady(false);
    if (!available) return;
    // Subscribe before status so a late attachment response cannot overwrite a newer event.
    const unsubscribe = host.subscribe((event): void => {
      if (current === generation.current && event.event === "observations.watch.changed")
        accept(event.payload);
    });
    void host
      .execute({
        command: "observations.watch.status",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .then((response): void => {
        if (current !== generation.current) return;
        if (response.ok) accept(response.result.watch);
        else setError(response.error);
      })
      .catch((): void => {
        if (current === generation.current)
          setError({
            summary: "The observation watch status is unavailable.",
            recovery:
              "Restore the host connection and reopen Observed health to attach to its current status.",
          });
      });
    return (): void => {
      generation.current++;
      unsubscribe();
    };
  }, [host, available, accept]);
  const execute = useCallback(
    async (input?: ObservationInput): Promise<void> => {
      if (
        !available ||
        !ready ||
        (input ? admission.current.start || admission.current.stop : admission.current.stop)
      )
        return;
      const current = generation.current;
      const kind = input ? "start" : "stop";
      admission.current[kind] = true;
      setPending(true);
      setError(null);
      try {
        const response = input
          ? await host.execute({
              command: "observations.watch.start",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: input,
            })
          : await host.execute({
              command: "observations.watch.stop",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: {},
            });
        if (current !== generation.current) return;
        if (response.ok) accept(response.result.watch);
        else setError(response.error);
      } catch {
        if (current === generation.current)
          setError({
            summary: "The host did not confirm the observation request.",
            recovery:
              "Restore the connection and check watch status before starting another watch. Original work remains owned by the host.",
          });
      } finally {
        if (current === generation.current) {
          admission.current[kind] = false;
          setPending(admission.current.start || admission.current.stop);
        }
      }
    },
    [host, available, ready, accept],
  );
  return {
    watch: available && ready ? watch : { ...watch, current: false },
    ready,
    pending,
    error,
    start: useCallback((input: ObservationInput): Promise<void> => execute(input), [execute]),
    stop: useCallback((): Promise<void> => execute(), [execute]),
  };
}
