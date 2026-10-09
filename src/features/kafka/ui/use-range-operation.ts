import { useCallback, useEffect, useRef, useState } from "react";

import type { HostError } from "../contracts";

export interface RangeOperationSnapshot {
  readonly scopeId: string;
  readonly revision: number;
  readonly operation: {
    readonly jobId: string;
    readonly input: { readonly requestId: string };
  } | null;
}
export type RangeOperationResponse<Snapshot> =
  | { readonly ok: true; readonly snapshot: Snapshot }
  | { readonly ok: false; readonly error: HostError };
export function rangeOperationResponse<Snapshot>(
  response:
    | { readonly ok: true; readonly result: { readonly snapshot: Snapshot } }
    | { readonly ok: false; readonly error: HostError },
): RangeOperationResponse<Snapshot> {
  return response.ok ? { ok: true, snapshot: response.result.snapshot } : response;
}
export interface RangeOperationPort<Input, Snapshot> {
  readonly label: string;
  status(): Promise<RangeOperationResponse<Snapshot>>;
  start(input: Input): Promise<RangeOperationResponse<Snapshot>>;
  cancel(jobId: string): Promise<RangeOperationResponse<Snapshot>>;
  discard(jobId: string): Promise<RangeOperationResponse<Snapshot>>;
  subscribe(listener: (snapshot: Snapshot) => void): () => void;
  available(snapshot: Snapshot): boolean;
}
export interface RangeOperationController<Input, Snapshot> {
  readonly snapshot: Snapshot | null;
  readonly connected: boolean;
  readonly busy: boolean;
  readonly error: string | undefined;
  readonly notice: string | undefined;
  readonly uncertainStart: boolean;
  start(input: Input): Promise<boolean>;
  retryStart(): Promise<boolean>;
  refresh(): Promise<void>;
  cancel(): Promise<void>;
  discard(): Promise<void>;
  perform(task: {
    readonly run: (snapshot: Snapshot) => Promise<string | undefined>;
    readonly current: (snapshot: Snapshot) => boolean;
    readonly failure: string;
  }): Promise<void>;
}

/** Shared ownership of range commands; feature ports retain their concrete protocol and output. */
export function useRangeOperation<
  Input extends { readonly requestId: string },
  Snapshot extends RangeOperationSnapshot,
>({
  port,
  connected,
  backendAvailable,
}: {
  readonly port: RangeOperationPort<Input, Snapshot>;
  readonly connected: boolean;
  readonly backendAvailable: boolean;
}): RangeOperationController<Input, Snapshot> {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const latest = useRef<Snapshot | null>(null);
  const retiredScopes = useRef(new Set<string>());
  const generation = useRef(0),
    busyRef = useRef(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [notice, setNotice] = useState<string>();
  const [pendingStart, setPendingStart] = useState<Input | null>(null);
  const accept = useCallback((next: Snapshot): void => {
    if (retiredScopes.current.has(next.scopeId)) return;
    const previous = latest.current;
    if (previous?.scopeId === next.scopeId && previous.revision >= next.revision) return;
    if (previous && previous.scopeId !== next.scopeId) retiredScopes.current.add(previous.scopeId);
    latest.current = next;
    setSnapshot(next);
    setPendingStart((input) =>
      next.operation?.input.requestId === input?.requestId ? null : input,
    );
  }, []);
  const refresh = useCallback(async (): Promise<void> => {
    const current = generation.current;
    try {
      const response = await port.status();
      if (current !== generation.current) return;
      if (response.ok) {
        accept(response.snapshot);
        setError(undefined);
      } else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError(`${port.label} status is unavailable. Refresh before starting another operation.`);
    }
  }, [port, accept]);
  useEffect(() => {
    generation.current++;
    latest.current = null;
    retiredScopes.current.clear();
    setSnapshot(null);
    setPendingStart(null);
    busyRef.current = false;
    setBusy(false);
    setError(undefined);
    setNotice(undefined);
    const current = generation.current;
    const unsubscribe = port.subscribe((next) => {
      if (current === generation.current) accept(next);
    });
    return (): void => {
      generation.current++;
      unsubscribe();
    };
  }, [port, backendAvailable, accept]);
  useEffect(() => {
    if (backendAvailable) void refresh();
  }, [connected, backendAvailable, refresh]);
  const start = useCallback(
    async (input: Input): Promise<boolean> => {
      if (
        busyRef.current ||
        !connected ||
        latest.current === null ||
        !port.available(latest.current)
      )
        return false;
      const current = generation.current;
      busyRef.current = true;
      setBusy(true);
      setError(undefined);
      setNotice(undefined);
      setPendingStart(input);
      try {
        const response = await port.start(input);
        if (current !== generation.current) return false;
        if (!response.ok) {
          setError(`${response.error.summary} ${response.error.recovery}`);
          setPendingStart(null);
          return false;
        }
        accept(response.snapshot);
        setPendingStart(null);
        return true;
      } catch {
        if (current === generation.current)
          setError(
            "The host did not acknowledge this start. Refresh status or retry the same request; do not create a second operation.",
          );
        return false;
      } finally {
        if (current === generation.current) {
          busyRef.current = false;
          setBusy(false);
        }
      }
    },
    [port, connected, accept],
  );
  const mutate = useCallback(
    async (action: "cancel" | "discard"): Promise<void> => {
      const operation = latest.current?.operation;
      if (!operation || busyRef.current) return;
      const current = generation.current;
      busyRef.current = true;
      setBusy(true);
      setError(undefined);
      setNotice(undefined);
      try {
        const response = await port[action](operation.jobId);
        if (current !== generation.current) return;
        if (response.ok) accept(response.snapshot);
        else setError(`${response.error.summary} ${response.error.recovery}`);
      } catch {
        if (current === generation.current)
          setError(
            `The host did not confirm ${port.label.toLowerCase()} cleanup. Refresh status and retry; this operation still owns its resources.`,
          );
      } finally {
        if (current === generation.current) {
          busyRef.current = false;
          setBusy(false);
        }
      }
    },
    [port, accept],
  );
  const perform = useCallback(
    async (
      task: Parameters<RangeOperationController<Input, Snapshot>["perform"]>[0],
    ): Promise<void> => {
      const captured = latest.current;
      if (captured === null || !connected || busyRef.current || !task.current(captured)) return;
      const current = generation.current;
      busyRef.current = true;
      setBusy(true);
      setError(undefined);
      setNotice(undefined);
      try {
        const message = await task.run(captured);
        if (
          current === generation.current &&
          latest.current !== null &&
          task.current(latest.current)
        )
          setNotice(message);
      } catch {
        if (current === generation.current) setError(task.failure);
      } finally {
        if (current === generation.current) {
          busyRef.current = false;
          setBusy(false);
        }
      }
    },
    [connected],
  );
  return {
    snapshot,
    busy,
    connected,
    error,
    notice,
    uncertainStart: pendingStart !== null,
    start,
    retryStart: () => (pendingStart === null ? Promise.resolve(false) : start(pendingStart)),
    refresh,
    cancel: () => mutate("cancel"),
    discard: () => mutate("discard"),
    perform,
  };
}
