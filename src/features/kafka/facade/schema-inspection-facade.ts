import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { SCHEMA_INSPECTION_LIMITS } from "../contracts/schema-inspection";
import { inspectSchema } from "../application/schema-inspection";
import type { SchemaLookupPort } from "../application/record-codec-types";
import type { KafkaApplicationSession } from "../application";

import { failureResponse } from "./facade-support";

export class SchemaInspectionFacade {
  private readonly pending = new Set<AbortController>();
  constructor(
    private readonly session: KafkaApplicationSession,
    private readonly lookup?: SchemaLookupPort,
  ) {}
  invalidate(): void {
    for (const controller of this.pending) controller.abort();
  }
  async execute(
    command: Extract<HostCommand, { command: "schemas.inspect" }>,
    correlationId: string,
  ): Promise<HostCommandResponse<"schemas.inspect">> {
    const failure = (recovery: string): HostCommandResponse<"schemas.inspect"> =>
      failureResponse(command, {
        code: "VALIDATION",
        stage: "kafka",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "Schema inspection could not complete.",
        recovery,
      });
    const context = this.session.clusterServiceContext("schemaRegistry");
    if (!context || !this.lookup || this.session.snapshot().state !== "connected")
      return failure(
        "Connect Kafka and configure Schema Registry before inspecting exact versions.",
      );
    if (this.pending.size >= 2) return failure("Wait for the active schema inspections to finish.");
    const controller = new AbortController();
    this.pending.add(controller);
    try {
      const inspection = await inspectSchema(
        command.payload,
        this.lookup,
        context,
        AbortSignal.any([
          controller.signal,
          AbortSignal.timeout(SCHEMA_INSPECTION_LIMITS.milliseconds),
        ]),
      );
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, inspection },
      };
    } catch {
      return failure(
        "Check the exact subject/version, Registry permissions and connection. Inspection stops after 15 seconds or when its 1 MiB data limit is exceeded. Retry after resolving the cause.",
      );
    } finally {
      this.pending.delete(controller);
    }
  }
}
