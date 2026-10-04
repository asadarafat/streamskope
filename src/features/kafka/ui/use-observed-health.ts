import { useCallback, useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type HostError, type StreamSkopeHost } from "../contracts";
import {
  OBSERVATION_LIMITS as limits,
  observationIdentity,
  type ObservationInput,
  type ObservationSnapshot,
  type ObservationSeries,
  type KafkaObservation,
} from "../contracts/observations";

type Operation = "history" | "capture" | "clear";
export interface ObservationRequestError {
  readonly operation: Operation;
  readonly summary: string;
  readonly recovery: string;
  readonly hostError?: HostError;
}

interface ObservedHealthController {
  readonly snapshot: ObservationSnapshot;
  readonly selected: string;
  readonly setSelected: (value: string) => void;
  readonly series: ObservationSeries | undefined;
  readonly latest: KafkaObservation | undefined;
  readonly fresh: boolean;
  readonly now: number;
  readonly current: boolean;
  readonly running: boolean;
  readonly busy: boolean;
  readonly operation: Operation | null;
  readonly historyReady: boolean;
  readonly error: ObservationRequestError | null;
  readonly cooldownSeconds: number;
  readonly capture: (input: ObservationInput) => Promise<void>;
  readonly start: (input: ObservationInput) => void;
  readonly stop: () => void;
  readonly clear: () => Promise<boolean>;
  readonly refreshHistory: () => Promise<void>;
}

const emptyHistory: ObservationSnapshot = { schemaVersion: 1, series: [], durability: "session" };

/** Owns page-scoped collection, cooldown and late-result cancellation. */
export function useObservedHealth(host: StreamSkopeHost): ObservedHealthController {
  const [snapshot, setSnapshot] = useState(emptyHistory);
  const [selected, setSelected] = useState("");
  const [currentCapture, setCurrentCapture] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [operation, setOperation] = useState<Operation | null>("history");
  const [historyReady, setHistoryReady] = useState(false);
  const [error, setError] = useState<ObservationRequestError | null>(null);
  const [nextCaptureAt, setNextCaptureAt] = useState(0);
  const [now, setNow] = useState(Date.now());
  const mounted = useRef(false);
  const generation = useRef(0);
  const inFlight = useRef(false);
  const cooldown = useRef(0);
  const input = useRef<ObservationInput | null>(null);
  const series = snapshot.series.find((value) => observationIdentity(value) === selected);
  const latest = series?.samples.at(-1);
  const fresh =
    latest !== undefined && now >= latest.observedAt && now - latest.observedAt <= limits.staleMs;
  const cooldownSeconds = Math.max(0, Math.ceil((nextCaptureAt - now) / 1000));

  const update = useCallback((next: ObservationSnapshot): void => {
    setSnapshot(next);
    setSelected((previous) =>
      next.series.some((value) => observationIdentity(value) === previous)
        ? previous
        : next.series.at(-1)
          ? observationIdentity(next.series.at(-1)!)
          : "",
    );
  }, []);
  const failure = useCallback((kind: Operation, hostError?: HostError): void => {
    setError(
      hostError
        ? { operation: kind, summary: hostError.summary, recovery: hostError.recovery, hostError }
        : {
            operation: kind,
            summary: "The application host did not answer the observation request.",
            recovery:
              "Check the connection, then retry. Your retained evidence has been preserved.",
          },
    );
  }, []);
  const refreshHistory = useCallback(async (): Promise<void> => {
    if (inFlight.current) return;
    inFlight.current = true;
    setOperation("history");
    setHistoryReady(false);
    const current = generation.current;
    try {
      const response = await host.execute({
        command: "observations.history",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!mounted.current || generation.current !== current) return;
      if (response.ok) {
        update(response.result.snapshot);
        setHistoryReady(true);
        const recentAttempt = Math.max(
          0,
          ...response.result.snapshot.series.flatMap((value) =>
            value.samples.map((sample) => sample.startedAt),
          ),
        );
        const next = Math.min(Date.now() + limits.intervalMs, recentAttempt + limits.intervalMs);
        if (next > cooldown.current && next > Date.now()) {
          cooldown.current = next;
          setNextCaptureAt(next);
        }
        setError(null);
      } else failure("history", response.error);
    } catch {
      if (mounted.current && generation.current === current) failure("history");
    } finally {
      inFlight.current = false;
      if (mounted.current) setOperation(null);
    }
  }, [host, update, failure]);
  useEffect(() => {
    mounted.current = true;
    const current = generation.current;
    void Promise.resolve().then(() => {
      if (mounted.current && generation.current === current) void refreshHistory();
    });
    return (): void => {
      mounted.current = false;
      generation.current++;
      void host
        .execute({
          command: "observations.cancel",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {},
        })
        .catch(() => undefined);
    };
  }, [host, refreshHistory]);
  useEffect(() => {
    const observedAt = latest?.observedAt;
    const expiry = observedAt === undefined ? 0 : observedAt + limits.staleMs + 1;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = (): void => {
      const timestamp = Date.now();
      setNow(timestamp);
      const remaining = nextCaptureAt - timestamp;
      const nextSecond =
        remaining > 0 ? nextCaptureAt - (Math.ceil(remaining / 1000) - 1) * 1000 : Infinity;
      const nextDeadline = Math.min(nextSecond, expiry > timestamp ? expiry : Infinity);
      if (Number.isFinite(nextDeadline))
        timer = setTimeout(tick, Math.max(1, nextDeadline - timestamp));
    };
    tick();
    return (): void => {
      if (timer !== undefined) clearTimeout(timer);
    };
  }, [latest?.observedAt, nextCaptureAt]);

  const capture = useCallback(
    async (request: ObservationInput): Promise<void> => {
      if (inFlight.current || Date.now() < cooldown.current) return;
      inFlight.current = true;
      input.current = request;
      setOperation("capture");
      setError(null);
      const current = generation.current;
      let retryDelay = limits.intervalMs as number;
      try {
        const response = await host.execute({
          command: "observations.capture",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: request,
        });
        if (!mounted.current || generation.current !== current) return;
        if (!response.ok) {
          if (response.error.code === "OBSERVATION_HISTORY_UNAVAILABLE") setHistoryReady(false);
          retryDelay = Math.max(retryDelay, response.error.retryAfterMs ?? 0);
          failure("capture", response.error);
          setRunning(false);
          return;
        }
        const next = response.result.capture;
        setSnapshot((previous) => ({
          schemaVersion: 1,
          durability: next.durability,
          series: [
            ...previous.series.filter(
              (value) => observationIdentity(value) !== observationIdentity(next.series),
            ),
            next.series,
          ].slice(-limits.series),
        }));
        setSelected(observationIdentity(next.series));
        setCurrentCapture(next.series.samples.at(-1)!.id);
      } catch {
        if (mounted.current && generation.current === current) {
          failure("capture");
          setRunning(false);
        }
      } finally {
        inFlight.current = false;
        if (mounted.current) {
          // Conservative UI spacing also prevents an immediate retry after Stop or a failed read.
          cooldown.current = Date.now() + retryDelay;
          setNextCaptureAt(cooldown.current);
          setNow(Date.now());
          setOperation(null);
        }
      }
    },
    [host, failure],
  );
  const captureRef = useRef(capture);
  captureRef.current = capture;
  useEffect(() => {
    if (!running) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const tick = async (): Promise<void> => {
      if (input.current) await captureRef.current(input.current);
      if (!cancelled)
        timer = setTimeout(
          () => {
            void tick();
          },
          Math.max(limits.intervalMs, cooldown.current - Date.now()),
        );
    };
    void tick();
    return (): void => {
      cancelled = true;
      if (timer !== undefined) clearTimeout(timer);
    };
  }, [running]);
  const stop = useCallback((): void => {
    setRunning(false);
    generation.current++;
    void host
      .execute({
        command: "observations.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .catch(() => undefined);
  }, [host]);
  const start = (request: ObservationInput): void => {
    if (inFlight.current || Date.now() < cooldown.current) return;
    input.current = request;
    setRunning(true);
  };
  const clear = async (): Promise<boolean> => {
    if (inFlight.current) return false;
    stop();
    inFlight.current = true;
    setOperation("clear");
    const current = generation.current;
    try {
      const response = await host.execute({
        command: "observations.clear",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { confirmation: "CLEAR HISTORY" },
      });
      if (!mounted.current || current !== generation.current) return false;
      if (response.ok) {
        update(response.result.snapshot);
        setHistoryReady(true);
        setCurrentCapture(null);
        setError(null);
        return true;
      }
      failure("clear", response.error);
    } catch {
      if (mounted.current && current === generation.current) failure("clear");
    } finally {
      inFlight.current = false;
      if (mounted.current) setOperation(null);
    }
    return false;
  };
  return {
    snapshot,
    selected,
    setSelected,
    series,
    latest,
    fresh,
    now,
    current: latest?.id === currentCapture,
    running,
    busy: operation !== null,
    operation,
    historyReady,
    error,
    cooldownSeconds,
    capture,
    start,
    stop,
    clear,
    refreshHistory,
  };
}
