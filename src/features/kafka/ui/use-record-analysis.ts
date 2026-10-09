import { useMemo } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { RecordAnalysisInput, RecordAnalysisSnapshot } from "../contracts/record-analysis";

import {
  rangeOperationResponse,
  useRangeOperation,
  type RangeOperationController,
  type RangeOperationPort,
} from "./use-range-operation";

export type RecordAnalysisController = Omit<
  RangeOperationController<RecordAnalysisInput, RecordAnalysisSnapshot>,
  "perform"
>;
export function useRecordAnalysis({
  host,
  connected,
  backendAvailable,
}: {
  readonly host: StreamSkopeHost;
  readonly connected: boolean;
  readonly backendAvailable: boolean;
}): RecordAnalysisController {
  const port = useMemo<RangeOperationPort<RecordAnalysisInput, RecordAnalysisSnapshot>>(
    (): RangeOperationPort<RecordAnalysisInput, RecordAnalysisSnapshot> => ({
      label: "Analysis",
      status: async () =>
        rangeOperationResponse(
          await host.execute({
            command: "records.analysis.status",
            payload: {},
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      start: async (payload) =>
        rangeOperationResponse(
          await host.execute({
            command: "records.analysis.start",
            payload,
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      cancel: async (jobId) =>
        rangeOperationResponse(
          await host.execute({
            command: "records.analysis.cancel",
            payload: { jobId },
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      discard: async (jobId) =>
        rangeOperationResponse(
          await host.execute({
            command: "records.analysis.discard",
            payload: { jobId },
            id: crypto.randomUUID(),
            version: HOST_PROTOCOL_VERSION,
          }),
        ),
      subscribe: (listener) =>
        host.subscribe((event) => {
          if (event.event === "records.analysis.changed") listener(event.payload);
        }),
      available: () => true,
    }),
    [host],
  );
  return useRangeOperation({ port, connected, backendAvailable });
}
