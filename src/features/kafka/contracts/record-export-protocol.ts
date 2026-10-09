import type { HostCommand, HostCommandBase, HostCommandResponse, HostEventBase } from "./types";
import { emptyRecord, exactKeys, record, text } from "./validation-primitives";
import {
  parseRecordExportId,
  parseRecordExportInput,
  parseRecordExportSnapshot,
} from "./record-export-validation";
import type { RecordExportInput, RecordExportSnapshot } from "./record-export";

export type RecordExportCommand = HostCommandBase &
  (
    | { readonly command: "records.export.start"; readonly payload: RecordExportInput }
    | {
        readonly command: "records.export.status";
        readonly payload: Readonly<Record<string, never>>;
      }
    | {
        readonly command: "records.export.cancel" | "records.export.discard";
        readonly payload: { readonly jobId: string };
      }
  );
export interface RecordExportResults {
  readonly "records.export.start": {
    readonly correlationId: string;
    readonly snapshot: RecordExportSnapshot;
  };
  readonly "records.export.status": RecordExportResults["records.export.start"];
  readonly "records.export.cancel": RecordExportResults["records.export.start"];
  readonly "records.export.discard": RecordExportResults["records.export.start"];
}

export function parseRecordExportCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  switch (command) {
    case "records.export.start":
      return { command, id, version, payload: parseRecordExportInput(value) };
    case "records.export.status":
      return { command, id, version, payload: emptyRecord(value, "exportStatus") };
    case "records.export.cancel":
    case "records.export.discard": {
      const payload = record(value, "exportJob");
      exactKeys(payload, ["jobId"], "exportJob");
      return {
        command,
        id,
        version,
        payload: { jobId: parseRecordExportId(payload.jobId, "exportJob.jobId") },
      };
    }
    default:
      return undefined;
  }
}

export function parseRecordExportResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  switch (command) {
    case "records.export.start":
    case "records.export.status":
    case "records.export.cancel":
    case "records.export.discard":
      exactKeys(result, ["correlationId", "snapshot"], "exportResult");
      return {
        command,
        id,
        version,
        ok: true,
        result: {
          correlationId: text(result.correlationId, "exportResult.correlationId", 128),
          snapshot: parseRecordExportSnapshot(result.snapshot),
        },
      };
    default:
      return undefined;
  }
}

export interface RecordExportEvent extends HostEventBase {
  readonly event: "records.export.changed";
  readonly payload: RecordExportSnapshot;
}
