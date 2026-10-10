import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { RecordBatchService } from "../application/record-batch-service";
import { RecordCodecService } from "../application/record-codec-service";
import type {
  SchemaLookupPort,
  RecordCodecPort,
  SchemaSamplePort,
  SchemaClientPort,
  SchemaAuthoringPort,
} from "../application/record-codec-types";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, successResponse, type ActivityInput } from "./facade-support";

type Command = Extract<
  HostCommand,
  { command: "schemas.client" | "schemas.samples" | "schemas.author" | `records.batch.${string}` }
>;
export function isSchemaSamplesCommand(command: HostCommand): command is Command {
  return [
    "schemas.client",
    "schemas.samples",
    "schemas.author",
    "records.batch.review",
    "records.batch.apply",
    "records.batch.cancel",
  ].includes(command.command);
}
export class SchemaSamplesFacade {
  private readonly service: RecordCodecService | undefined;
  private readonly authoringResolver: (() => RecordCodecService) | undefined;
  private readonly pending = new Set<AbortController>();
  private readonly batches: RecordBatchService;
  constructor(
    private readonly session: KafkaApplicationSession,
    codec?: RecordCodecPort,
    lookup?: SchemaLookupPort,
    private readonly generator?: SchemaSamplePort & Partial<SchemaClientPort & SchemaAuthoringPort>,
    private readonly recordActivity?: (input: ActivityInput) => void,
  ) {
    if (codec && lookup) {
      this.service = new RecordCodecService(lookup, codec);
      this.authoringResolver = (): RecordCodecService => new RecordCodecService(lookup, codec);
    }
    this.batches = new RecordBatchService(() => session.reviewedWriteScope());
  }
  invalidate(): void {
    for (const controller of this.pending) controller.abort();
    this.service?.clear();
    this.batches.invalidate();
  }
  async execute(command: Command, correlationId: string): Promise<HostCommandResponse> {
    try {
      switch (command.command) {
        case "records.batch.cancel":
          this.batches.cancel(command.payload.planId);
          return successResponse(command, correlationId);
        case "records.batch.review":
          return {
            command: command.command,
            id: command.id,
            version: HOST_PROTOCOL_VERSION,
            ok: true,
            result: { correlationId, review: await this.batches.review(command.payload) },
          };
        case "records.batch.apply": {
          const outcome = await this.batches.apply(command.payload.planId);
          const acknowledged = outcome.outcomes.filter(
            (item) => item.state === "acknowledged",
          ).length;
          const rejected = outcome.outcomes.filter((item) => item.state === "rejected").length;
          const unknown = outcome.outcomes.filter((item) => item.state === "unknown").length;
          this.recordActivity?.({
            correlationId,
            operation: "Publish reviewed record batch",
            object: "Reviewed destination",
            outcome: outcome.stopReason === "complete" ? "succeeded" : "failed",
            severity: outcome.stopReason === "complete" ? "info" : "warning",
            detail: `${acknowledged} acknowledged; ${rejected} rejected; ${unknown} unknown; ${outcome.unsent} unsent. Stopped: ${outcome.stopReason}. Inspect Kafka before repeating any uncertain write.`,
          });
          return {
            command: command.command,
            id: command.id,
            version: HOST_PROTOCOL_VERSION,
            ok: true,
            result: { correlationId, outcome },
          };
        }
        case "schemas.client":
        case "schemas.author":
        case "schemas.samples": {
          const context = this.session.clusterServiceContext("schemaRegistry");
          if (
            !context ||
            !this.service ||
            !this.generator ||
            this.pending.size >= 2 ||
            this.session.snapshot().state !== "connected"
          )
            throw new Error(
              "Connect Kafka and configure Schema Registry; wait for current generation to finish.",
            );
          const controller = new AbortController();
          this.pending.add(controller);
          const { signal: contextSignal, ...requestContext } = context;
          const signal = AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(15_000),
            ...(contextSignal ? [contextSignal] : []),
          ]);
          try {
            const resolver =
              command.command === "schemas.author" ? this.authoringResolver!() : this.service;
            const bundle = await resolver.resolveVersion(
              command.command === "schemas.author" ? requestContext : context,
              command.payload.subject,
              command.payload.version,
              signal,
            );
            if (command.command === "schemas.client") {
              if (!this.generator.generateClient) throw new Error("Client generator unavailable.");
              const client = await this.generator.generateClient(command.payload, bundle, signal);
              signal.throwIfAborted();
              return {
                command: command.command,
                id: command.id,
                version: HOST_PROTOCOL_VERSION,
                ok: true,
                result: { correlationId, client },
              };
            }
            if (command.command === "schemas.author") {
              if (!this.generator.author) throw new Error("Authoring worker unavailable.");
              const authoring = await this.generator.author(command.payload, bundle, signal);
              signal.throwIfAborted();
              return {
                command: command.command,
                id: command.id,
                version: HOST_PROTOCOL_VERSION,
                ok: true,
                result: { correlationId, authoring },
              };
            }
            const samples = await this.generator.generate(command.payload, bundle, signal);
            signal.throwIfAborted();
            return {
              command: command.command,
              id: command.id,
              version: HOST_PROTOCOL_VERSION,
              ok: true,
              result: { correlationId, samples },
            };
          } finally {
            this.pending.delete(controller);
          }
        }
      }
    } catch (error) {
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "kafka",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary:
          command.command === "schemas.author"
            ? "Record validation could not complete."
            : "The schema sample or batch request could not complete.",
        recovery:
          command.command === "schemas.author"
            ? "Check the active connection and Registry permissions, reload the selected schema, then validate again. Validation publishes no records."
            : command.command === "schemas.samples" && error instanceof Error
              ? `Generation is unsupported or exceeded its limits: ${error.message.slice(0, 256)}. Check the declared schema and active Registry.`
              : "Check the destination, permissions and active connection, then review again. Inspect Kafka before repeating any uncertain write; no automatic resend occurs.",
      });
    }
  }
}
