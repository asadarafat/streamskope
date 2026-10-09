import { parseRecordReadId } from "./finite-record-validation";
import type { HostCommand, HostCommandBase, HostCommandResponse, HostEventBase } from "./types";
import { emptyRecord, exactKeys, record, text } from "./validation-primitives";
import {
  parseRecordAnalysisInput,
  parseRecordAnalysisSnapshot,
} from "./record-analysis-validation";
import type { RecordAnalysisInput, RecordAnalysisSnapshot } from "./record-analysis";

export type RecordAnalysisCommand = HostCommandBase &
  (
    | { readonly command: "records.analysis.start"; readonly payload: RecordAnalysisInput }
    | {
        readonly command: "records.analysis.status";
        readonly payload: Readonly<Record<string, never>>;
      }
    | {
        readonly command: "records.analysis.cancel" | "records.analysis.discard";
        readonly payload: { readonly jobId: string };
      }
  );
export interface RecordAnalysisResults {
  readonly "records.analysis.start": {
    readonly correlationId: string;
    readonly snapshot: RecordAnalysisSnapshot;
  };
  readonly "records.analysis.status": RecordAnalysisResults["records.analysis.start"];
  readonly "records.analysis.cancel": RecordAnalysisResults["records.analysis.start"];
  readonly "records.analysis.discard": RecordAnalysisResults["records.analysis.start"];
}

export function parseRecordAnalysisCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  switch (command) {
    case "records.analysis.start":
      return { command, id, version, payload: parseRecordAnalysisInput(value) };
    case "records.analysis.status":
      return { command, id, version, payload: emptyRecord(value, "analysisStatus") };
    case "records.analysis.cancel":
    case "records.analysis.discard": {
      const payload = record(value, "analysisJob");
      exactKeys(payload, ["jobId"], "analysisJob");
      return {
        command,
        id,
        version,
        payload: { jobId: parseRecordReadId(payload.jobId, "analysisJob.jobId") },
      };
    }
    default:
      return undefined;
  }
}

export function parseRecordAnalysisResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  switch (command) {
    case "records.analysis.start":
    case "records.analysis.status":
    case "records.analysis.cancel":
    case "records.analysis.discard":
      exactKeys(result, ["correlationId", "snapshot"], "analysisResult");
      return {
        command,
        id,
        version,
        ok: true,
        result: {
          correlationId: text(result.correlationId, "analysisResult.correlationId", 128),
          snapshot: parseRecordAnalysisSnapshot(result.snapshot),
        },
      };
    default:
      return undefined;
  }
}

export interface RecordAnalysisEvent extends HostEventBase {
  readonly event: "records.analysis.changed";
  readonly payload: RecordAnalysisSnapshot;
}
