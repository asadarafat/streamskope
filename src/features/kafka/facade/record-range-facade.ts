import type { KafkaApplicationSession, KafkaOperationalPreferenceService } from "../application";
import {
  RecordAnalysisOperationError,
  RecordAnalysisService,
} from "../application/record-analysis-service";
import type { RecordExportArtifacts } from "../application/record-export-artifacts";
import {
  RecordExportOperationError,
  RecordExportService,
} from "../application/record-export-service";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
} from "../contracts";
import { HostContractValidationError } from "../contracts/validation-error";
import type { RecordReadSettings } from "../contracts/finite-record-read";
import type { RecordAnalysisCommand } from "../contracts/record-analysis-protocol";
import type { RecordExportCommand } from "../contracts/record-export-protocol";

import { failureResponse, type ActivityInput } from "./facade-support";

type RangeCommand = RecordExportCommand | RecordAnalysisCommand;

export function isRecordRangeCommand(command: HostCommand): command is RangeCommand {
  return (
    command.command.startsWith("records.export.") || command.command.startsWith("records.analysis.")
  );
}

/** Owns the shared authority and settings boundary for finite export and analysis. */
export class RecordRangeFacade {
  private readonly exports: RecordExportService;
  private readonly analysis: RecordAnalysisService;

  constructor(
    session: Pick<KafkaApplicationSession, "recordReadScope">,
    preferences: KafkaOperationalPreferenceService,
    artifacts: RecordExportArtifacts | undefined,
    nextSequence: () => number,
    publish: (event: HostEvent) => void,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    const scope = (): ReturnType<KafkaApplicationSession["recordReadScope"]> =>
      session.recordReadScope();
    const settings = (): RecordReadSettings => {
      const snapshot = preferences.currentSnapshot();
      if (snapshot.store.state !== "ready") throw new Error("Record preferences are unavailable.");
      return { codecs: snapshot.preferences.codecs, protection: snapshot.preferences.protection };
    };
    this.exports = new RecordExportService({
      scope,
      settings,
      ...(artifacts === undefined ? {} : { artifacts }),
      changed: (snapshot): void =>
        publish({
          event: "records.export.changed",
          payload: snapshot,
          sequence: nextSequence(),
          version: HOST_PROTOCOL_VERSION,
        }),
    });
    this.analysis = new RecordAnalysisService({
      scope,
      settings,
      changed: (snapshot): void =>
        publish({
          event: "records.analysis.changed",
          payload: snapshot,
          sequence: nextSequence(),
          version: HOST_PROTOCOL_VERSION,
        }),
    });
  }

  async preparePreferences(
    command: Extract<
      HostCommand,
      { command: "preferences.get" | "preferences.update" | "preferences.reset" }
    >,
  ): Promise<void> {
    if (
      command.command === "preferences.reset" ||
      (command.command === "preferences.update" &&
        (command.payload.patch.protection !== undefined ||
          command.payload.patch.codecs !== undefined))
    ) {
      this.invalidate();
      await this.idle();
    }
  }

  invalidate(): void {
    const failures: Error[] = [];
    for (const owner of [this.exports, this.analysis]) {
      try {
        owner.invalidate();
      } catch {
        failures.push(new Error("A record range owner could not revoke its work."));
      }
    }
    if (failures.length) throw new AggregateError(failures, "Record range revocation failed.");
  }

  async idle(): Promise<void> {
    const outcomes = await Promise.allSettled([this.exports.idle(), this.analysis.idle()]);
    if (outcomes.some((outcome) => outcome.status === "rejected"))
      throw new Error("Record range cleanup could not be confirmed.");
  }

  async execute(command: RangeCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      switch (command.command) {
        case "records.export.start":
        case "records.export.status":
        case "records.export.cancel":
        case "records.export.discard": {
          const snapshot =
            command.command === "records.export.start"
              ? this.exports.start(command.payload)
              : command.command === "records.export.status"
                ? this.exports.snapshot()
                : command.command === "records.export.cancel"
                  ? await this.exports.cancel(command.payload.jobId)
                  : await this.exports.discard(command.payload.jobId);
          return {
            command: command.command,
            id: command.id,
            version: HOST_PROTOCOL_VERSION,
            ok: true,
            result: { correlationId, snapshot },
          };
        }
        case "records.analysis.start":
        case "records.analysis.status":
        case "records.analysis.cancel":
        case "records.analysis.discard": {
          const snapshot =
            command.command === "records.analysis.start"
              ? this.analysis.start(command.payload)
              : command.command === "records.analysis.status"
                ? this.analysis.snapshot()
                : command.command === "records.analysis.cancel"
                  ? await this.analysis.cancel(command.payload.jobId)
                  : await this.analysis.discard(command.payload.jobId);
          return {
            command: command.command,
            id: command.id,
            version: HOST_PROTOCOL_VERSION,
            ok: true,
            result: { correlationId, snapshot },
          };
        }
      }
    } catch (failure) {
      const exporting = command.command.startsWith("records.export.");
      const feature = exporting ? "export" : "analysis";
      const error =
        failure instanceof RecordExportOperationError ||
        failure instanceof RecordAnalysisOperationError
          ? { ...failure.error, correlationId }
          : failure instanceof HostContractValidationError
            ? {
                code: "VALIDATION" as const,
                stage: "validation" as const,
                correlationId,
                activeStateChanged: false,
                retryable: false,
                summary: `The record ${feature} options are invalid.`,
                recovery:
                  "Use a finite range, supported field paths and values within the displayed limits.",
              }
            : {
                code: "INTERNAL" as const,
                stage: exporting ? ("storage" as const) : ("query" as const),
                correlationId,
                activeStateChanged: false,
                retryable: false,
                summary: `The record ${feature} operation could not be completed.`,
                recovery: `Check ${feature} status. Discard the current ${feature} to retry cleanup before starting another.`,
              };
      this.activity({
        correlationId,
        operation: command.command,
        object: `Record ${feature}`,
        detail: `${error.summary} ${error.recovery}`,
        outcome: "failed",
        severity: "error",
      });
      return failureResponse(command, error);
    }
  }
}
