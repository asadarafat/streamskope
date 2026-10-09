import { useCallback, useEffect, useMemo, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { RecordExportInput, RecordExportSnapshot } from "../contracts/record-export";

import type { ArtifactTransferPort } from "./artifact-transfer";
import {
  rangeOperationResponse,
  useRangeOperation,
  type RangeOperationController,
  type RangeOperationPort,
} from "./use-range-operation";

export interface RecordExportController extends Omit<
  RangeOperationController<RecordExportInput, RecordExportSnapshot>,
  "perform"
> {
  readonly expired: boolean;
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
  const port = useMemo<RangeOperationPort<RecordExportInput, RecordExportSnapshot>>(
    (): RangeOperationPort<RecordExportInput, RecordExportSnapshot> => ({
      label: "Export",
      status: async () =>
        rangeOperationResponse(
          await host.execute({
            command: "records.export.status",
            payload: {},
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      start: async (payload) =>
        rangeOperationResponse(
          await host.execute({
            command: "records.export.start",
            payload,
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      cancel: async (jobId) =>
        rangeOperationResponse(
          await host.execute({
            command: "records.export.cancel",
            payload: { jobId },
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      discard: async (jobId) =>
        rangeOperationResponse(
          await host.execute({
            command: "records.export.discard",
            payload: { jobId },
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      subscribe: (listener) =>
        host.subscribe((event) => {
          if (event.event === "records.export.changed") listener(event.payload);
        }),
      available: (snapshot) => snapshot.available,
    }),
    [host],
  );
  const controller = useRangeOperation({ port, connected, backendAvailable });
  const [expiredId, setExpiredId] = useState<string>();
  const artifact = controller.snapshot?.operation?.artifact;
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
  const download = useCallback(
    async (part: "data" | "receipt"): Promise<void> => {
      const ready = artifact;
      if (!ready) return;
      await controller.perform({
        current: (next) =>
          next.operation?.artifact?.artifactId === ready.artifactId &&
          Date.parse(ready.expiresAt) > Date.now(),
        run: async (): Promise<string> => {
          const outcome = await transfer.download({ artifactId: ready.artifactId, part });
          const label = part === "data" ? "Export" : "Receipt";
          return outcome === "saved"
            ? `${label} saved.`
            : outcome === "cancelled"
              ? `${label} save cancelled.`
              : `${label} download started. Check your browser downloads for completion.`;
        },
        failure:
          "The download did not complete. Check that the host is unlocked and the export has not expired, then retry.",
      });
    },
    [artifact, controller, transfer],
  );
  return {
    ...controller,
    expired:
      artifact !== undefined &&
      artifact !== null &&
      (expiredId === artifact.artifactId || Date.parse(artifact.expiresAt) <= Date.now()),
    download,
  };
}
