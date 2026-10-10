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
import type { ObservationWatchSnapshot } from "../contracts/observation-watch";

import { useObservationWatch } from "./use-observation-watch";

type Operation = "history" | "capture" | "clear";
export interface ObservationRequestError {
  readonly operation: Operation;
  readonly summary: string;
  readonly recovery: string;
  readonly hostError?: HostError;
  readonly code?: HostError["code"];
}

interface ObservedHealthController {
  readonly snapshot: ObservationSnapshot;
  readonly watch: ObservationWatchSnapshot;
  readonly selected: string;
  readonly setSelected: (value: string) => void;
  readonly series: ObservationSeries | undefined;
  readonly latest: KafkaObservation | undefined;
  readonly fresh: boolean;
  readonly now: number;
  readonly current: boolean;
  readonly running: boolean;
  readonly canStop: boolean;
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

/** Displays host-owned evidence; its only deadlines update cooldown and freshness text. */
export function useObservedHealth(
  host: StreamSkopeHost,
  backendAvailable = true,
): ObservedHealthController {
  const [snapshot, setSnapshot] = useState(emptyHistory);
  const [selected, setSelected] = useState("");
  const attachment = useObservationWatch(host, backendAvailable);
  const { watch } = attachment;
  const running =
    backendAvailable &&
    attachment.ready &&
    watch.repeated &&
    ["capturing", "waiting", "stopping"].includes(watch.phase);
  const hostBusy =
    backendAvailable && (attachment.pending || ["capturing", "stopping"].includes(watch.phase));
  const loadedSample = useRef<string | null>(null);
  const displayedSample = useRef<string | null>(null);
  const [operation, setOperation] = useState<Operation | null>("history");
  const [historyReady, setHistoryReady] = useState(false);
  const [error, setError] = useState<ObservationRequestError | null>(null);
  const [nextCaptureAt, setNextCaptureAt] = useState(0);
  const [now, setNow] = useState(Date.now());
  const mounted = useRef(false);
  const generation = useRef(0);
  const inFlight = useRef<number | null>(null);
  const readiness = useRef(backendAvailable);
  readiness.current = backendAvailable;
  const previousReadiness = useRef(backendAvailable);
  const cooldown = useRef(0);
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
        ? {
            operation: kind,
            summary: hostError.summary,
            recovery: hostError.recovery,
            code: hostError.code,
            hostError,
          }
        : {
            operation: kind,
            summary: "The application host did not answer the observation request.",
            recovery:
              "Check the connection, then retry. Your retained evidence has been preserved.",
          },
    );
  }, []);
  const refreshHistory = useCallback(async (): Promise<void> => {
    if (!readiness.current || inFlight.current !== null) return;
    const current = generation.current;
    inFlight.current = current;
    setOperation("history");
    setHistoryReady(false);
    try {
      const response = await host.execute({
        command: "observations.history",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!mounted.current || !readiness.current || generation.current !== current) return;
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
      if (mounted.current && readiness.current && generation.current === current)
        failure("history");
    } finally {
      if (inFlight.current === current) {
        inFlight.current = null;
        if (mounted.current) setOperation(null);
      }
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
    };
  }, [host, refreshHistory]);
  useEffect(() => {
    const previouslyAvailable = previousReadiness.current;
    previousReadiness.current = backendAvailable;
    if (backendAvailable) {
      if (!previouslyAvailable && !historyReady) void refreshHistory();
      return;
    }
    setOperation(null);
    if (!previouslyAvailable) return;
    generation.current++;
    inFlight.current = null;
  }, [backendAvailable, historyReady, host, refreshHistory]);
  useEffect(() => {
    if (!backendAvailable) return;
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
  }, [backendAvailable, latest?.observedAt, nextCaptureAt]);

  const capture = useCallback(
    async (request: ObservationInput): Promise<void> => {
      if (!readiness.current || inFlight.current !== null || Date.now() < cooldown.current) return;
      const current = generation.current;
      inFlight.current = current;
      setOperation("capture");
      setError(null);
      let retryDelay = limits.intervalMs as number;
      try {
        const response = await host.execute({
          command: "observations.capture",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: request,
        });
        if (!mounted.current || !readiness.current || generation.current !== current) return;
        if (!response.ok) {
          if (response.error.code === "OBSERVATION_HISTORY_UNAVAILABLE") setHistoryReady(false);
          retryDelay = Math.max(retryDelay, response.error.retryAfterMs ?? 0);
          failure("capture", response.error);
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
        loadedSample.current = next.series.samples.at(-1)!.id;
      } catch {
        if (mounted.current && readiness.current && generation.current === current) {
          failure("capture");
        }
      } finally {
        if (inFlight.current === current) {
          inFlight.current = null;
          if (mounted.current) {
            // Conservative UI spacing also prevents an immediate retry after Stop or a failed read.
            cooldown.current = Date.now() + retryDelay;
            setNextCaptureAt(cooldown.current);
            setNow(Date.now());
            setOperation(null);
          }
        }
      }
    },
    [host, failure],
  );
  // Events carry compact capture receipts. History is read once for a new receipt, never polled.
  useEffect(() => {
    if (watch.nextCaptureAt !== null && watch.nextCaptureAt > cooldown.current) {
      cooldown.current = watch.nextCaptureAt;
      setNextCaptureAt(watch.nextCaptureAt);
    }
    if (
      watch.lastSampleId === null ||
      loadedSample.current === watch.lastSampleId ||
      operation !== null
    )
      return;
    loadedSample.current = watch.lastSampleId;
    void refreshHistory();
  }, [watch.lastSampleId, watch.nextCaptureAt, operation, refreshHistory]);
  useEffect(() => {
    if (watch.lastSampleId === null || displayedSample.current === watch.lastSampleId) return;
    if (
      !snapshot.series.some(
        (series) =>
          observationIdentity(series) === watch.lastSeriesId &&
          series.samples.at(-1)?.id === watch.lastSampleId,
      )
    )
      return;
    displayedSample.current = watch.lastSampleId;
    setSelected(watch.lastSeriesId!);
  }, [watch.lastSampleId, watch.lastSeriesId, snapshot]);
  const stop = useCallback((): void => {
    void attachment.stop();
  }, [attachment.stop]);
  const start = (request: ObservationInput): void => {
    if (!readiness.current || inFlight.current !== null || Date.now() < cooldown.current) return;
    setError(null);
    void attachment.start(request);
  };
  const clear = async (): Promise<boolean> => {
    if (!readiness.current || inFlight.current !== null) return false;
    const current = generation.current;
    inFlight.current = current;
    setOperation("clear");
    try {
      const response = await host.execute({
        command: "observations.clear",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { confirmation: "CLEAR HISTORY" },
      });
      if (!mounted.current || !readiness.current || current !== generation.current) return false;
      if (response.ok) {
        update(response.result.snapshot);
        setHistoryReady(true);
        loadedSample.current = null;
        setError(null);
        return true;
      }
      failure("clear", response.error);
    } catch {
      if (mounted.current && readiness.current && current === generation.current) failure("clear");
    } finally {
      if (inFlight.current === current) {
        inFlight.current = null;
        if (mounted.current) setOperation(null);
      }
    }
    return false;
  };
  return {
    snapshot,
    watch,
    selected,
    setSelected,
    series,
    latest,
    fresh,
    now,
    current:
      backendAvailable &&
      watch.current &&
      series !== undefined &&
      observationIdentity(series) === watch.lastSeriesId &&
      latest?.id === watch.lastSampleId,
    running,
    canStop:
      backendAvailable &&
      attachment.ready &&
      ["capturing", "waiting", "stopping"].includes(watch.phase),
    busy: operation !== null || hostBusy,
    operation: operation ?? (hostBusy ? "capture" : null),
    historyReady:
      historyReady &&
      attachment.ready &&
      attachment.error?.code !== "OBSERVATION_HISTORY_UNAVAILABLE",
    error: error ?? (attachment.error ? { operation: "capture", ...attachment.error } : null),
    cooldownSeconds,
    capture,
    start,
    stop,
    clear,
    refreshHistory,
  };
}
