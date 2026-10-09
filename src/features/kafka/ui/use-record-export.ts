import { useCallback, useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { RecordExportInput, RecordExportSnapshot } from "../contracts/record-export";

import type { ArtifactTransferPort } from "./artifact-transfer";

export interface RecordExportController {
  readonly snapshot: RecordExportSnapshot | null;
  readonly busy: boolean;
  readonly connected: boolean;
  readonly error: string | undefined;
  readonly notice: string | undefined;
  readonly uncertainStart: boolean;
  readonly expired: boolean;
  readonly start: (input: RecordExportInput) => Promise<boolean>;
  readonly retryStart: () => Promise<boolean>;
  readonly refresh: () => Promise<void>;
  readonly cancel: () => Promise<void>;
  readonly discard: () => Promise<void>;
  readonly download: (part: "data" | "receipt") => Promise<void>;
}

export function useRecordExport({
  host,
  connected,
  backendAvailable,
  transfer,
}: {
  readonly host: StreamSkopeHost;
  readonly connected: boolean;
  readonly backendAvailable: boolean;
  readonly transfer: ArtifactTransferPort;
}): RecordExportController {
  const [snapshot, setSnapshot] = useState<RecordExportSnapshot | null>(null);
  const latest = useRef<RecordExportSnapshot | null>(null);
  const retiredScopes = useRef(new Set<string>());
  const generation = useRef(0);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [error, setError] = useState<string>();
  const [notice, setNotice] = useState<string>();
  const [pendingStart, setPendingStart] = useState<RecordExportInput | null>(null);
  const [expiredId, setExpiredId] = useState<string>();
  const accept = useCallback((next: RecordExportSnapshot): void => {
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
      const result = await host.execute({
        command: "records.export.status",
        payload: {},
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
      });
      if (current !== generation.current) return;
      if (result.ok) accept(result.result.snapshot);
      else setError(`${result.error.summary} ${result.error.recovery}`);
    } catch {
      if (current === generation.current)
        setError("Export status is unavailable. Refresh before starting another export.");
    }
  }, [host, accept]);
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
    const unsubscribe = host.subscribe((event) => {
      if (current !== generation.current) return;
      if (event.event === "records.export.changed") accept(event.payload);
    });
    return (): void => {
      generation.current++;
      unsubscribe();
    };
  }, [host, backendAvailable, accept, refresh]);
  useEffect(() => {
    if (backendAvailable) void refresh();
  }, [connected, backendAvailable, refresh]);
  const artifact = snapshot?.operation?.artifact;
  useEffect(() => {
    if (!artifact) return;
    const delay = Date.parse(artifact.expiresAt) - Date.now();
    if (delay <= 0) {
      setExpiredId(artifact.artifactId);
      return;
    }
    const timer = setTimeout(
      () => setExpiredId(artifact.artifactId),
      Math.min(delay, 2_147_483_647),
    );
    return (): void => clearTimeout(timer);
  }, [artifact]);
  const start = useCallback(
    async (input: RecordExportInput): Promise<boolean> => {
      if (busyRef.current || !connected || latest.current?.available !== true) return false;
      const current = generation.current;
      busyRef.current = true;
      setBusy(true);
      setError(undefined);
      setNotice(undefined);
      setPendingStart(input);
      try {
        const response = await host.execute({
          command: "records.export.start",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: input,
        });
        if (current !== generation.current) return false;
        if (!response.ok) {
          setError(`${response.error.summary} ${response.error.recovery}`);
          setPendingStart(null);
          return false;
        }
        accept(response.result.snapshot);
        setPendingStart(null);
        return true;
      } catch {
        if (current === generation.current)
          setError(
            "The host did not acknowledge this start. Refresh status or retry the same request; do not create a second export.",
          );
        return false;
      } finally {
        if (current === generation.current) {
          busyRef.current = false;
          setBusy(false);
        }
      }
    },
    [host, connected, accept],
  );
  const mutate = useCallback(
    async (command: "records.export.cancel" | "records.export.discard"): Promise<void> => {
      const operation = latest.current?.operation;
      if (!operation || busyRef.current) return;
      const current = generation.current;
      busyRef.current = true;
      setBusy(true);
      setError(undefined);
      setNotice(undefined);
      try {
        const response = await host.execute({
          command,
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { jobId: operation.jobId },
        });
        if (current !== generation.current) return;
        if (response.ok) accept(response.result.snapshot);
        else setError(`${response.error.summary} ${response.error.recovery}`);
      } catch {
        if (current === generation.current)
          setError(
            "The host did not confirm export cleanup. Refresh status and retry; this operation still owns its resources.",
          );
      } finally {
        if (current === generation.current) {
          busyRef.current = false;
          setBusy(false);
        }
      }
    },
    [host, accept],
  );
  const download = useCallback(
    async (part: "data" | "receipt"): Promise<void> => {
      const ready = latest.current?.operation?.artifact;
      if (!ready || !connected || busyRef.current || Date.parse(ready.expiresAt) <= Date.now())
        return;
      const current = generation.current;
      busyRef.current = true;
      setBusy(true);
      setError(undefined);
      setNotice(undefined);
      try {
        const result = await transfer.download({ artifactId: ready.artifactId, part });
        if (
          current !== generation.current ||
          latest.current?.operation?.artifact?.artifactId !== ready.artifactId
        )
          return;
        const label = part === "data" ? "Export" : "Receipt";
        setNotice(
          result === "saved"
            ? `${label} saved.`
            : result === "cancelled"
              ? `${label} save cancelled.`
              : `${label} download started. Check your browser downloads for completion.`,
        );
      } catch {
        if (current === generation.current)
          setError(
            "The download did not complete. Check that the host is unlocked and the export has not expired, then retry.",
          );
      } finally {
        if (current === generation.current) {
          busyRef.current = false;
          setBusy(false);
        }
      }
    },
    [transfer, connected],
  );
  return {
    snapshot,
    busy,
    connected,
    error,
    notice,
    uncertainStart: pendingStart !== null,
    expired:
      artifact !== undefined &&
      artifact !== null &&
      (expiredId === artifact.artifactId || Date.parse(artifact.expiresAt) <= Date.now()),
    start,
    retryStart: () => (pendingStart === null ? Promise.resolve(false) : start(pendingStart)),
    refresh,
    cancel: () => mutate("records.export.cancel"),
    discard: () => mutate("records.export.discard"),
    download,
  };
}
