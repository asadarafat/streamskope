import {
  RecordExportService,
  RecordExportOperationError,
} from "../application/record-export-service";
import type { RecordExportArtifacts } from "../application/record-export-artifacts";
import type { KafkaApplicationSession, KafkaOperationalPreferenceService } from "../application";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type RecordExportSnapshot,
  type RecordExportSettings,
  type HostCommand,
  type HostEvent,
} from "../contracts";
import type { RecordExportCommand } from "../contracts/record-export-protocol";

import { failureResponse, type ActivityInput } from "./facade-support";

export function recordExportEvent(snapshot: RecordExportSnapshot, sequence: number): HostEvent {
  return {
    event: "records.export.changed",
    payload: snapshot,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

/** Owns export admission and safe host responses; bytes never cross this facade. */
export class RecordExportFacade {
  private readonly service: RecordExportService;

  constructor(
    session: Pick<KafkaApplicationSession, "recordReadScope">,
    preferences: KafkaOperationalPreferenceService,
    artifacts: RecordExportArtifacts | undefined,
    changed: (snapshot: RecordExportSnapshot) => void,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new RecordExportService({
      scope: (): ReturnType<KafkaApplicationSession["recordReadScope"]> =>
        session.recordReadScope(),
      settings: (): RecordExportSettings => {
        const snapshot = preferences.currentSnapshot();
        if (snapshot.store.state !== "ready")
          throw new Error("Record preferences are unavailable.");
        return { codecs: snapshot.preferences.codecs, protection: snapshot.preferences.protection };
      },
      ...(artifacts === undefined ? {} : { artifacts }),
      changed,
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
    this.service.invalidate();
  }
  idle(): Promise<void> {
    return this.service.idle();
  }

  async execute(command: RecordExportCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      const snapshot =
        command.command === "records.export.start"
          ? this.service.start(command.payload)
          : command.command === "records.export.status"
            ? this.service.snapshot()
            : command.command === "records.export.cancel"
              ? await this.service.cancel(command.payload.jobId)
              : await this.service.discard(command.payload.jobId);
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, snapshot },
      };
    } catch (failure) {
      const error =
        failure instanceof RecordExportOperationError
          ? { ...failure.error, correlationId }
          : {
              code: "INTERNAL" as const,
              stage: "storage" as const,
              correlationId,
              activeStateChanged: false,
              retryable: false,
              summary: "The record export operation could not be completed.",
              recovery:
                "Check export status. Discard the current export to retry cleanup before starting another.",
            };
      this.activity({
        correlationId,
        operation: command.command,
        object: "Record export",
        detail: `${error.summary} ${error.recovery}`,
        outcome: "failed",
        severity: "error",
      });
      return failureResponse(command, error);
    }
  }
}
