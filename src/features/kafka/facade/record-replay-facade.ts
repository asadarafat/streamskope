import {
  RepairJournal,
  RepairJournalStorageError,
  type RepairJobStore,
} from "../application/repair-journal";
import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { RecordReplayService } from "../application/record-replay-service";
import { SavedReplayDestinations } from "../application/replay-destination";
import type {
  KafkaApplicationSession,
  KafkaProfileService,
  KafkaConnectionPort,
} from "../application";

import { failureResponse, successResponse, type ActivityInput } from "./facade-support";

export class RecordReplayFacade {
  private readonly service: RecordReplayService;
  private readonly journal: RepairJournal | undefined;
  constructor(
    session: KafkaApplicationSession,
    profiles: KafkaProfileService,
    connections: KafkaConnectionPort | undefined,
    private readonly activity: (input: ActivityInput) => void,
    repairStore?: RepairJobStore,
  ) {
    this.journal = repairStore ? new RepairJournal(repairStore) : undefined;
    this.service = new RecordReplayService(
      () => session.reviewedWriteScope(),
      connections ? new SavedReplayDestinations(profiles, connections) : undefined,
      undefined,
      this.journal,
    );
  }
  invalidate(): Promise<void> {
    return this.service.invalidate();
  }
  async execute(
    command: Extract<HostCommand, { command: `records.replay.${string}` | "records.repair.list" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (command.command === "records.repair.list")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: {
            correlationId,
            durability: this.journal?.store.durability ?? "unavailable",
            jobs: (await this.journal?.list()) ?? [],
          },
        };
      if (command.command === "records.replay.cancel") {
        await this.service.cancel(command.payload.planId);
        return successResponse(command, correlationId);
      }
      if (command.command === "records.replay.review")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, review: await this.service.review(command.payload) },
        };
      const outcome = await this.service.apply(
        command.payload.planId,
        command.payload.confirmation,
      );
      const acknowledged = outcome.outcomes.filter((r) => r.state === "acknowledged").length;
      const unknown = outcome.outcomes.filter((r) => r.state === "unknown").length;
      this.activity({
        correlationId,
        operation: "Replay reviewed records",
        object: "Reviewed destination",
        outcome: outcome.stopReason === "complete" ? "succeeded" : "failed",
        severity:
          outcome.stopReason === "complete" &&
          outcome.cleanup === "complete" &&
          outcome.journal !== "unavailable"
            ? "info"
            : "warning",
        detail: `${acknowledged} acknowledged; ${unknown} unknown; ${outcome.outcomes.length - acknowledged - unknown} rejected; ${outcome.unsent} unsent. Stopped: ${outcome.stopReason}. Destination cleanup: ${outcome.cleanup}. Journal: ${outcome.journal ?? "not configured"}; storage: ${outcome.durability ?? "session"}; job: ${outcome.jobId ?? "none"}. Repeating a new plan can duplicate records; reconcile receipts before any retry.`,
      });
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, outcome },
      };
    } catch (error) {
      if (error instanceof RepairJournalStorageError)
        return failureResponse(command, {
          code: "BACKEND_UNAVAILABLE",
          stage: "storage",
          correlationId,
          retryable: false,
          activeStateChanged: false,
          summary: error.message,
          recovery:
            "No automatic retry is allowed. Preserve complete application data, reopen Repair history and reconcile any acknowledged or uncertain records. Restore protection or storage access before a new reviewed attempt.",
        });
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "kafka",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "The replay request could not be accepted.",
        recovery:
          "Select complete records within the bounds, check destination permissions and UTF-8 transformations, and review the current profile/topic again. Confirm the exact destination. Inspect any previous uncertain result before creating another plan.",
      });
    }
  }
}
