import {
  RepairJournal,
  RepairJournalStorageError,
  type RepairJobStore,
} from "../application/repair-journal";
import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { RecordReplayService } from "../application/record-replay-service";
import { SavedReplayDestinations } from "../application/replay-destination";
import { RepairRecoveryService } from "../application/repair-recovery-service";
import { RepairReconciliationReader } from "../application/repair-reconciliation-reader";
import { StructuredReplayService } from "../application/structured-replay-service";
import type {
  KafkaApplicationSession,
  KafkaProfileService,
  KafkaConnectionPort,
  KafkaOperationalPreferenceService,
} from "../application";

import type { KafkaBackendFacadeOptions } from "./types";
import { failureResponse, successResponse, type ActivityInput } from "./facade-support";

type ReplayCommand = Extract<
  HostCommand,
  { command: `records.replay.${string}` | `records.repair.${string}` }
>;
export function isRecordReplayCommand(command: HostCommand): command is ReplayCommand {
  return (
    command.command.startsWith("records.replay.") || command.command.startsWith("records.repair.")
  );
}

export class RecordReplayFacade {
  private readonly service: RecordReplayService;
  private readonly journal: RepairJournal | undefined;
  private readonly recovery: RepairRecoveryService | undefined;
  constructor(
    session: KafkaApplicationSession,
    profiles: KafkaProfileService,
    connections: KafkaConnectionPort | undefined,
    private readonly activity: (input: ActivityInput) => void,
    repairStore?: RepairJobStore,
    preferences?: KafkaOperationalPreferenceService,
    encoding?: Pick<KafkaBackendFacadeOptions, "recordCodec" | "schemaLookup" | "sampleGenerator">,
  ) {
    this.journal = repairStore ? new RepairJournal(repairStore) : undefined;
    const destinations = connections
      ? new SavedReplayDestinations(profiles, connections)
      : undefined;
    const activeDestination = ():
      import("../application/replay-destination").ReviewedReplayDestination | null => {
      const scope = session.reviewedWriteScope(),
        readScope = session.recordReadScope(),
        registryScope = session.schemaRegistryReviewScope();
      return scope
        ? {
            scope,
            ...(readScope ? { readScope } : {}),
            ...(registryScope ? { registryScope } : {}),
            close: (): Promise<void> => Promise.resolve(),
          }
        : null;
    };
    const generator = encoding?.sampleGenerator;
    this.service = new RecordReplayService(
      () => session.reviewedWriteScope(),
      destinations,
      undefined,
      this.journal,
      new StructuredReplayService(
        () => session.schemaRegistryReviewScope(),
        encoding?.recordCodec,
        encoding?.schemaLookup,
        generator?.author ? { author: generator.author.bind(generator) } : undefined,
      ),
      activeDestination,
    );
    if (this.journal && preferences)
      this.recovery = new RepairRecoveryService(
        this.journal,
        this.service,
        new RepairReconciliationReader(
          activeDestination,
          () => {
            const snapshot = preferences.currentSnapshot();
            if (snapshot.store.state !== "ready")
              throw new Error("Record preferences are unavailable.");
            return {
              codecs: snapshot.preferences.codecs,
              protection: snapshot.preferences.protection,
            };
          },
          destinations,
        ),
      );
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
    )
      await this.invalidate();
  }
  invalidate(): Promise<void> {
    return this.recovery ? this.recovery.invalidate() : this.service.invalidate();
  }
  async execute(
    command: Extract<
      HostCommand,
      { command: `records.replay.${string}` | `records.repair.${string}` }
    >,
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
            jobs: (await (this.recovery?.list() ?? this.journal?.list())) ?? [],
          },
        };
      if (command.command === "records.repair.review") {
        if (!this.recovery) throw new Error("Repair recovery is unavailable.");
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, continuation: await this.recovery.review(command.payload) },
        };
      }
      if (command.command === "records.repair.reconcile") {
        if (!this.recovery) throw new Error("Repair recovery is unavailable.");
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, finding: await this.recovery.reconcile(command.payload) },
        };
      }
      if (command.command === "records.repair.archive") {
        if (!this.recovery) throw new Error("Repair recovery is unavailable.");
        await this.recovery.archive(command.payload);
        return successResponse(command, correlationId);
      }
      if (command.command === "records.replay.cancel") {
        await this.service.cancel(command.payload.planId);
        return successResponse(command, correlationId);
      }
      if (command.command === "records.replay.review") {
        if (this.recovery && !this.recovery.replayAvailable())
          throw new Error("Repair recovery still owns its work.");
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, review: await this.service.review(command.payload) },
        };
      }
      if (this.recovery && !this.recovery.replayAvailable())
        throw new Error("Repair recovery still owns its work.");
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
          "Select complete records within the bounds. For structured replay, check every source writer mapping, JSON Pointer edit and destination subject/version. Register missing writers through the destination profile's reviewed Schema Registry workflow first. Framed cross-profile records require explicit translation. Review the current profile/topic again and inspect any previous uncertain result before another plan.",
      });
    }
  }
}
